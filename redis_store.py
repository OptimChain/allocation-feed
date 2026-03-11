"""Redis sync — writes portfolio positions and orders to Redis hashes.

Two Redis hashes are maintained:
  - stocks: current stock positions keyed by symbol
  - orders: open orders keyed by order_id

Transform pipelines are built as pure functions (position → dict,
order → dict).  The pipeline is composed first, then applied at write
time so the mapping logic is testable without Redis.

Uses REDIS_HOST + REDIS_PASSWORD env vars. Only writes when live=True.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Callable, Sequence

log = logging.getLogger(__name__)


# ── Pure transform pipelines ─────────────────────────
# Each transform is a function  object → dict.
# compose_transforms chains them: apply base, then each overlay in order.

Transform = Callable[[object], dict]


def compose_transforms(*fns: Transform) -> Transform:
    """Left-to-right composition: later transforms merge into earlier ones."""
    def composed(obj):
        result: dict = {}
        for fn in fns:
            result.update(fn(obj))
        return result
    return composed


# Position transforms — each is a pure lambda / function

_pos_identity: Transform = lambda pos: {
    "symbol": pos.symbol,
    "name": pos.symbol,
    "type": "stock",
}

_pos_sizing: Transform = lambda pos: {
    "quantity": pos.qty,
    "avg_buy_price": pos.avg_entry,
}

_pos_market: Transform = lambda pos: {
    "current_price": round(pos.market_value / pos.qty, 4) if pos.qty else 0,
    "equity": pos.market_value,
}

_pos_pnl: Transform = lambda pos: {
    "profit_loss": pos.unrealized_pl,
    "profit_loss_pct": pos.unrealized_pl_pct * 100,
    "percent_change": round(pos.unrealized_pl_pct * 100, 2),
    "equity_change": pos.unrealized_pl,
}

position_to_entry: Transform = compose_transforms(
    _pos_identity, _pos_sizing, _pos_market, _pos_pnl,
)

# Order transforms

_order_identity: Transform = lambda o: {
    "order_id": o.id,
    "symbol": o.symbol,
    "side": o.side.upper(),
    "order_type": o.order_type,
}

_order_trigger: Transform = lambda o: {
    "trigger": "stop" if o.order_type in ("stop", "stop_limit") else "immediate",
}

_order_sizing: Transform = lambda o: {
    "state": o.status,
    "quantity": o.qty,
    "limit_price": o.limit_price,
    "stop_price": o.stop_price,
}

_order_meta: Transform = lambda o: {
    "_status": "open",
    "_type": "stock",
}

order_to_entry: Transform = compose_transforms(
    _order_identity, _order_trigger, _order_sizing, _order_meta,
)


# ── Functional sink builder ──────────────────────────

def _build_hash_entries(
    items: Sequence,
    key_fn: Callable[[object], str],
    transform: Transform,
) -> list[tuple[str, str]]:
    """Pure function: map items → list of (hash_key, json_value) pairs."""
    return [(key_fn(item), json.dumps(transform(item))) for item in items]


# ── Redis client ─────────────────────────────────────

def _get_client():
    """Get a Redis client, or None if not configured."""
    try:
        import redis
    except ImportError:
        log.warning("[redis] redis package not installed")
        return None

    host = os.getenv("REDIS_HOST")
    password = os.getenv("REDIS_PASSWORD")
    if host:
        port = 6379
        if ":" in host:
            host, port_str = host.rsplit(":", 1)
            try:
                port = int(port_str)
            except ValueError:
                pass
        try:
            return redis.Redis(
                host=host, port=port, password=password,
                decode_responses=True,
            )
        except Exception as e:
            log.error("[redis] Failed to connect to %s: %s", host, e)
            return None

    url = os.getenv("REDIS_URL")
    if url:
        try:
            import redis as _redis
            return _redis.from_url(url, decode_responses=True)
        except Exception as e:
            log.error("[redis] Failed to connect via URL: %s", e)
            return None

    return None


# ── Write (the only impure boundary) ─────────────────

def _flush_hash(pipe, hash_key: str, entries: list[tuple[str, str]], meta: dict):
    """Write a batch of (field, json_value) pairs + _meta to a Redis hash."""
    pipe.delete(hash_key)
    for field, value in entries:
        pipe.hset(hash_key, field, value)
    pipe.hset(hash_key, "_meta", json.dumps(meta))


def sync_to_redis(
    positions: list,
    open_orders: list,
    account,
    live: bool = False,
    *,
    pos_transform: Transform = position_to_entry,
    order_transform: Transform = order_to_entry,
):
    """Write portfolio positions and orders to Redis.

    The transform pipelines are injected so callers can compose custom
    mappings.  Defaults use the standard position_to_entry / order_to_entry
    chains built from lambdas above.

    Args:
        positions: List of Position from BrokerClient.positions()
        open_orders: List of OpenOrder from BrokerClient.open_orders()
        account: AccountSummary from BrokerClient.account()
        live: Only write when True (skips in dry-run mode)
        pos_transform: position → dict mapping (composable)
        order_transform: order → dict mapping (composable)
    """
    if not live:
        return

    client = _get_client()
    if not client:
        return

    ts = datetime.now(timezone.utc).isoformat()

    # Build entries — pure, no IO
    add_timestamps: Transform = lambda _: {"created_at": ts, "updated_at": ts}

    stock_entries = _build_hash_entries(
        positions,
        key_fn=lambda p: p.symbol,
        transform=pos_transform,
    )
    order_entries = _build_hash_entries(
        open_orders,
        key_fn=lambda o: o.id,
        transform=compose_transforms(order_transform, add_timestamps),
    )

    # Flush to Redis — single impure boundary
    try:
        pipe = client.pipeline()

        _flush_hash(pipe, "stocks", stock_entries, {
            "updated_at": ts,
            "num_stocks": len(positions),
            "num_options": 0,
        })
        _flush_hash(pipe, "orders", order_entries, {
            "updated_at": ts,
            "num_open_stock": len(open_orders),
            "num_open_option": 0,
            "num_historical_stock": 0,
            "num_historical_option": 0,
        })

        pipe.execute()
        log.info("[redis] Synced: %d positions, %d open orders", len(positions), len(open_orders))

    except Exception as e:
        log.error("[redis] FAILED: %s", e)
    finally:
        try:
            client.close()
        except Exception:
            pass
