#!/usr/bin/env python3
"""
Options Chain Blob Unloader

Reads OptionsRecord entries from Redis and uploads them to Netlify Blobs.
Uses the unified OptionsRecord schema (proto/options_contract.proto).

Supports two Redis sources:
  - redis-13258 (OPTIONS_REDIS_HOST) — dedicated options stream data
  - redis-17054 (REDIS_HOST) — legacy polling data

Redis keys consumed (OptionsRecord format):
  options:{OCC_SYMBOL}        (hash with sub-attribute JSON)
  options:index:all           (set of all symbols)
  options:index:{UNDERLYING}  (set of symbols per underlying)

Legacy keys also consumed (backward compat):
  options-chain:history:{SYMBOL}  (RPOP drain)
  options-chain:{SYMBOL}          (latest read)
  options-bars:{SYMBOL}           (latest read)

Env vars required:
  OPTIONS_REDIS_HOST (default: redis-13258.c99.us-east-1-4.ec2.cloud.redislabs.com:13258)
  OPTIONS_REDIS_PASSWORD (or REDIS_PASSWORD)
  REDIS_HOST (default: redis-17054.c99.us-east-1-4.ec2.cloud.redislabs.com:17054)
  REDIS_PASSWORD
  NETLIFY_API_TOKEN
  NETLIFY_SITE_ID
  OPTIONS_SYMBOLS (default: IWN,CRWD)
"""

import json
import os
import sys
from datetime import datetime, timezone

import redis
import requests

# Add parent dir so we can import the accessor
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.options_accessor import RedisOptionsAccessor, OptionsRecord

BLOBS_URL = "https://api.netlify.com/api/v1/blobs"
STORE_NAME = "options-chain"
BATCH_SIZE = 500


def _make_redis(host_env, default_host, pass_env="REDIS_PASSWORD"):
    host = os.getenv(host_env, default_host)
    password = os.getenv(pass_env, os.getenv("REDIS_PASSWORD"))
    port = int(default_host.rsplit(":", 1)[-1])
    if ":" in host:
        host, port_str = host.rsplit(":", 1)
        try:
            port = int(port_str)
        except ValueError:
            pass
    return redis.Redis(host=host, port=port, password=password, decode_responses=True)


def get_options_redis():
    """Connect to the dedicated options Redis (redis-13258)."""
    return _make_redis(
        "OPTIONS_REDIS_HOST",
        "redis-13258.c99.us-east-1-4.ec2.cloud.redislabs.com:13258",
        "OPTIONS_REDIS_PASSWORD",
    )


def get_legacy_redis():
    """Connect to the legacy polling Redis (redis-17054)."""
    return _make_redis(
        "REDIS_HOST",
        "redis-17054.c99.us-east-1-4.ec2.cloud.redislabs.com:17054",
    )


def get_netlify_config():
    token = os.getenv("NETLIFY_API_TOKEN")
    site_id = os.getenv("NETLIFY_SITE_ID")
    if not token or not site_id:
        print("ERROR: NETLIFY_API_TOKEN and NETLIFY_SITE_ID required")
        sys.exit(1)
    return token, site_id


def upload_to_blob(token, site_id, blob_key, data):
    payload = json.dumps(data)
    url = f"{BLOBS_URL}/{site_id}/{STORE_NAME}/{blob_key}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    print(f"  [blob] PUT {STORE_NAME}/{blob_key}")
    print(f"  [blob] Payload size: {len(payload)} bytes")
    resp = requests.put(url, headers=headers, data=payload, timeout=30)
    print(f"  [blob] Response: {resp.status_code} {resp.reason}")
    resp.raise_for_status()


def _record_to_dict(rec: OptionsRecord) -> dict:
    """Serialize an OptionsRecord to a plain dict for blob storage."""
    d = {
        "symbol": rec.symbol,
        "underlying": rec.underlying,
        "expiration": rec.expiration,
        "strike": rec.strike,
        "option_type": rec.option_type,
        "updated_at": rec.updated_at,
    }
    if rec.pricing: d["pricing"] = rec.pricing.to_dict()
    if rec.greeks: d["greeks"] = rec.greeks.to_dict()
    if rec.sizing: d["sizing"] = rec.sizing.to_dict()
    if rec.pnl: d["pnl"] = rec.pnl.to_dict()
    if rec.side: d["side"] = rec.side
    if rec.order_type: d["order_type"] = rec.order_type
    if rec.status: d["status"] = rec.status
    if rec.order_id: d["order_id"] = rec.order_id
    if rec.orders: d["orders"] = [o.to_dict() for o in rec.orders]
    if rec.bars: d["bars"] = [b.to_dict() for b in rec.bars]
    if rec.created_at: d["created_at"] = rec.created_at
    return d


# ── New-format unloader (OptionsRecord from redis-13258) ──


def unload_options_records(accessor: RedisOptionsAccessor, token, site_id, underlying):
    """Read all OptionsRecords for an underlying and upload to blob."""
    print(f"\n[unloader] Processing OptionsRecords for {underlying}")

    records = accessor.get_by_underlying(underlying)
    print(f"  Found {len(records)} contracts")

    if not records:
        print(f"  No OptionsRecord data for {underlying}. Skipping.")
        return

    now = datetime.now(timezone.utc)
    blob_key = f"{underlying}/{now.strftime('%Y-%m-%dT%H-%M-%S')}"
    payload = {
        "timestamp": now.isoformat(),
        "underlying": underlying,
        "blob_key": blob_key,
        "format": "options_record",
        "contracts": [_record_to_dict(r) for r in records],
        "contract_count": len(records),
    }

    upload_to_blob(token, site_id, blob_key, payload)
    print(f"  Done: {len(records)} contracts uploaded")


# ── Legacy-format unloader (options-chain from redis-17054) ──


def drain_history(client, history_key, max_entries=BATCH_SIZE):
    """RPOP entries from the history list (oldest first)."""
    entries = []
    for _ in range(max_entries):
        entry = client.rpop(history_key)
        if entry is None:
            break
        try:
            entries.append(json.loads(entry))
        except json.JSONDecodeError:
            print("  WARNING: skipping malformed history entry")
    return entries


def get_latest_chain(client, symbol):
    """Read the current options-chain:{symbol} hash."""
    raw = client.hgetall(f"options-chain:{symbol}")
    result = {}
    for key, value in raw.items():
        try:
            result[key] = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            result[key] = value
    return result


def get_latest_bars(client, symbol):
    """Read the current options-bars:{symbol} hash."""
    raw = client.hgetall(f"options-bars:{symbol}")
    result = {}
    for key, value in raw.items():
        try:
            result[key] = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            result[key] = value
    return result


def unload_legacy_symbol(client, token, site_id, symbol):
    """Legacy: drain history and upload chain + bars for one underlying."""
    print(f"\n[unloader] Processing legacy format for {symbol}")

    history_key = f"options-chain:history:{symbol}"
    entries = drain_history(client, history_key)
    print(f"  Drained {len(entries)} history entries")

    latest_chain = get_latest_chain(client, symbol)
    num_contracts = len([k for k in latest_chain if k != "_meta"])
    print(f"  Latest chain: {num_contracts} contracts")

    latest_bars = get_latest_bars(client, symbol)
    num_bar_contracts = len([k for k in latest_bars if k != "_meta"])
    print(f"  Latest bars: {num_bar_contracts} contracts with bars")

    if not entries and not latest_chain and not latest_bars:
        print(f"  No legacy data for {symbol}. Skipping.")
        return

    now = datetime.now(timezone.utc)
    blob_key = f"{symbol}/{now.strftime('%Y-%m-%dT%H-%M-%S')}"
    payload = {
        "timestamp": now.isoformat(),
        "underlying": symbol,
        "blob_key": blob_key,
        "format": "legacy",
        "latest_chain": latest_chain,
        "latest_bars": latest_bars,
        "history_count": len(entries),
        "history": entries,
    }

    upload_to_blob(token, site_id, blob_key, payload)
    print(f"  Done: {len(entries)} history + {num_contracts} chain + {num_bar_contracts} bars")


# ── Main ──


def main():
    print(f"[unloader] Options chain blob unloader starting at {datetime.now(timezone.utc).isoformat()}")

    token, site_id = get_netlify_config()

    symbols = os.getenv("OPTIONS_SYMBOLS", "IWN,CRWD").split(",")
    symbols = [s.strip() for s in symbols if s.strip()]
    print(f"[unloader] Symbols: {symbols}")

    # 1. Unload OptionsRecord format from redis-13258
    try:
        options_client = get_options_redis()
        options_client.ping()
        accessor = RedisOptionsAccessor(options_client)
        print(f"[unloader] Connected to options Redis (redis-13258)")
        for symbol in symbols:
            try:
                unload_options_records(accessor, token, site_id, symbol)
            except Exception as e:
                print(f"[unloader] ERROR processing OptionsRecords for {symbol}: {e}")
        options_client.close()
    except Exception as e:
        print(f"[unloader] WARNING: Could not connect to options Redis: {e}")

    # 2. Unload legacy format from redis-17054
    try:
        legacy_client = get_legacy_redis()
        legacy_client.ping()
        print(f"[unloader] Connected to legacy Redis (redis-17054)")
        for symbol in symbols:
            try:
                unload_legacy_symbol(legacy_client, token, site_id, symbol)
            except Exception as e:
                print(f"[unloader] ERROR processing legacy {symbol}: {e}")
        legacy_client.close()
    except Exception as e:
        print(f"[unloader] WARNING: Could not connect to legacy Redis: {e}")

    print(f"\n[unloader] All done.")


if __name__ == "__main__":
    main()
