"""
Fetch historical data for IWN 4/17/2026 $187 Puts.

Data sources (in priority order):
  1. Twelve Data API (requires TWELVE_DATA_API_KEY)
  2. CBOE delayed quotes (free, no key needed)
  3. Alpaca Markets API (requires ALPACA_API_KEY + ALPACA_SECRET_KEY)

OCC Symbol: IWN260417P00187000
"""

import json
import os
import sys
from datetime import datetime, timedelta

import requests

# ── Config ──────────────────────────────────────────

UNDERLYING = "IWN"
EXPIRATION = "2026-04-17"
STRIKE = 187.0
OPTION_TYPE = "put"
OCC_SYMBOL = "IWN260417P00187000"

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "")
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")


# ── Twelve Data ─────────────────────────────────────

def fetch_twelve_data():
    """Fetch options data from Twelve Data API (requires paid plan for options)."""
    if not TWELVE_DATA_API_KEY:
        print("[twelve] No TWELVE_DATA_API_KEY set. Skipping.")
        return None

    base = "https://api.twelvedata.com"

    # Try time_series with OCC symbol for historical bars
    print(f"[twelve] Fetching time_series for {OCC_SYMBOL}...")
    resp = requests.get(f"{base}/time_series", params={
        "symbol": OCC_SYMBOL,
        "interval": "1day",
        "outputsize": 90,
        "apikey": TWELVE_DATA_API_KEY,
    }, timeout=30)

    if resp.status_code == 200:
        data = resp.json()
        if "values" in data:
            print(f"[twelve] Got {len(data['values'])} daily bars")
            return {"source": "twelve_data", "bars": data["values"], "meta": data.get("meta", {})}
        elif "message" in data:
            print(f"[twelve] {data['message']}")

    # Try options chain
    print(f"[twelve] Trying options/chain for {UNDERLYING} {EXPIRATION}...")
    resp = requests.get(f"{base}/options/chain", params={
        "symbol": UNDERLYING,
        "expiration_date": EXPIRATION,
        "side": OPTION_TYPE,
        "apikey": TWELVE_DATA_API_KEY,
    }, timeout=30)

    if resp.status_code == 200:
        data = resp.json()
        if "puts" in data:
            contracts = data["puts"]
            target = [c for c in contracts if abs(float(c.get("strike", 0)) - STRIKE) < 0.01]
            if target:
                print(f"[twelve] Found contract at ${STRIKE}")
                return {"source": "twelve_data_chain", "contract": target[0]}
            else:
                strikes = sorted(set(float(c.get("strike", 0)) for c in contracts))
                nearby = [s for s in strikes if abs(s - STRIKE) <= 5]
                print(f"[twelve] No exact match. Nearby strikes: {nearby}")
        elif "message" in data:
            print(f"[twelve] {data['message']}")

    return None


# ── CBOE ────────────────────────────────────────────

def fetch_cboe():
    """Fetch options snapshot from CBOE delayed quotes (free, no auth)."""
    print(f"[cboe] Fetching delayed quotes for {UNDERLYING} options...")

    resp = requests.get(
        f"https://cdn.cboe.com/api/global/delayed_quotes/options/{UNDERLYING}.json",
        timeout=15,
    )

    if resp.status_code != 200:
        print(f"[cboe] HTTP {resp.status_code}")
        return None

    data = resp.json()
    options = data.get("data", {}).get("options", [])
    if not options:
        print("[cboe] No options data returned")
        return None

    # Find exact contract
    target = next((o for o in options if o.get("option") == OCC_SYMBOL), None)

    if not target:
        # Show nearby puts for the same expiration
        puts_0417 = [o for o in options if "260417P" in o.get("option", "")]
        nearby = [o for o in puts_0417 if abs(int(o["option"][-8:]) / 1000 - STRIKE) <= 5]
        if nearby:
            print(f"[cboe] No exact ${STRIKE} strike. Nearby 4/17 puts:")
            for o in nearby:
                s = int(o["option"][-8:]) / 1000
                print(f"  ${s:.0f}: bid={o['bid']} ask={o['ask']}")
        return None

    # Also grab underlying quote
    underlying_data = data.get("data", {})
    underlying_price = underlying_data.get("close", underlying_data.get("current_price"))

    # Get all 4/17 puts for the chain context
    puts_0417 = sorted(
        [o for o in options if "260417P" in o.get("option", "")],
        key=lambda o: int(o["option"][-8:]),
    )
    nearby_chain = [o for o in puts_0417 if abs(int(o["option"][-8:]) / 1000 - STRIKE) <= 10]

    return {
        "source": "cboe_delayed",
        "contract": target,
        "underlying_price": underlying_price,
        "nearby_chain": nearby_chain,
        "timestamp": data.get("timestamp"),
    }


# ── Alpaca ──────────────────────────────────────────

def fetch_alpaca():
    """Fetch options bars and snapshot from Alpaca."""
    if not ALPACA_API_KEY:
        print("[alpaca] No ALPACA_API_KEY set. Skipping.")
        return None

    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    }
    base = "https://data.alpaca.markets/v1beta1/options"
    end = datetime.utcnow()
    start = end - timedelta(days=30)

    print(f"[alpaca] Fetching bars for {OCC_SYMBOL}...")
    resp = requests.get(f"{base}/bars", params={
        "symbols": OCC_SYMBOL,
        "timeframe": "1Day",
        "start": start.strftime("%Y-%m-%dT00:00:00Z"),
        "end": end.strftime("%Y-%m-%dT23:59:59Z"),
        "limit": 1000,
        "sort": "desc",
    }, headers=headers, timeout=30)

    bars_data = None
    if resp.status_code == 200:
        bars = resp.json().get("bars", {}).get(OCC_SYMBOL, [])
        if bars:
            print(f"[alpaca] Got {len(bars)} daily bars")
            bars_data = bars

    print(f"[alpaca] Fetching snapshot for {OCC_SYMBOL}...")
    resp = requests.get(
        f"{base}/snapshots?symbols={OCC_SYMBOL}&feed=indicative",
        headers=headers, timeout=30,
    )

    snapshot_data = None
    if resp.status_code == 200:
        snapshot = resp.json().get("snapshots", {}).get(OCC_SYMBOL)
        if snapshot:
            snapshot_data = snapshot

    if bars_data or snapshot_data:
        return {"source": "alpaca", "bars": bars_data, "snapshot": snapshot_data}
    return None


# ── Display ─────────────────────────────────────────

def display_results(result):
    source = result.get("source", "unknown")

    print(f"\n{'=' * 65}")
    print(f"  IWN {EXPIRATION} ${STRIKE:.0f} PUT  |  Source: {source}")
    print(f"{'=' * 65}")

    # CBOE contract data
    if "contract" in result:
        c = result["contract"]
        underlying_px = result.get("underlying_price")

        print(f"\n  Underlying (IWN):  ${underlying_px}" if underlying_px else "")
        print(f"  Last Trade:        ${c.get('last_trade_price', 'N/A')}  @ {c.get('last_trade_time', 'N/A')}")
        print(f"  Prev Close:        ${c.get('prev_day_close', 'N/A')}")
        print(f"  Bid:               ${c.get('bid', 'N/A')} x {c.get('bid_size', 'N/A')}")
        print(f"  Ask:               ${c.get('ask', 'N/A')} x {c.get('ask_size', 'N/A')}")
        mid = None
        if c.get("bid") and c.get("ask"):
            mid = (c["bid"] + c["ask"]) / 2
            print(f"  Mid:               ${mid:.2f}")
        print(f"  Theo:              ${c.get('theo', 'N/A')}")
        print(f"  Day OHLC:          O={c.get('open', 'N/A')} H={c.get('high', 'N/A')} L={c.get('low', 'N/A')}")
        print(f"  Volume:            {c.get('volume', 'N/A')}")
        print(f"  Open Interest:     {c.get('open_interest', 'N/A')}")
        print()
        print(f"  Greeks:")
        print(f"    IV:     {c.get('iv', 'N/A'):<10}  ({float(c['iv'])*100:.1f}%)" if c.get("iv") else "    IV:     N/A")
        print(f"    Delta:  {c.get('delta', 'N/A')}")
        print(f"    Gamma:  {c.get('gamma', 'N/A')}")
        print(f"    Theta:  {c.get('theta', 'N/A')}")
        print(f"    Vega:   {c.get('vega', 'N/A')}")
        print(f"    Rho:    {c.get('rho', 'N/A')}")

    # Nearby chain
    if "nearby_chain" in result and result["nearby_chain"]:
        chain = result["nearby_chain"]
        print(f"\n  Nearby 4/17 Puts Chain:")
        print(f"  {'Strike':>8} {'Bid':>8} {'Ask':>8} {'Mid':>8} {'Last':>8} {'Vol':>6} {'OI':>6} {'IV':>8} {'Delta':>8}")
        print(f"  {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*6} {'-'*6} {'-'*8} {'-'*8}")
        for o in chain:
            strike = int(o["option"][-8:]) / 1000
            bid = o.get("bid", 0)
            ask = o.get("ask", 0)
            mid_px = (bid + ask) / 2 if bid and ask else 0
            marker = " <--" if abs(strike - STRIKE) < 0.01 else ""
            print(f"  ${strike:>7.0f} {bid:>8.2f} {ask:>8.2f} {mid_px:>8.2f} "
                  f"{o.get('last_trade_price', 0):>8.2f} {int(o.get('volume', 0)):>6} "
                  f"{int(o.get('open_interest', 0)):>6} {o.get('iv', 0):>8.4f} "
                  f"{o.get('delta', 0):>8.4f}{marker}")

    # Twelve Data / Alpaca bars
    if "bars" in result and result["bars"]:
        bars = result["bars"]
        print(f"\n  Historical Bars ({len(bars)} entries):")
        print(f"  {'Date':<22} {'Open':>8} {'High':>8} {'Low':>8} {'Close':>8} {'Volume':>8}")
        print(f"  {'-'*22} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
        for bar in bars[:30]:
            ts = bar.get("t", bar.get("datetime", ""))[:19]
            o = float(bar.get("o", bar.get("open", 0)))
            h = float(bar.get("h", bar.get("high", 0)))
            lo = float(bar.get("l", bar.get("low", 0)))
            cl = float(bar.get("c", bar.get("close", 0)))
            v = int(bar.get("v", bar.get("volume", 0)))
            print(f"  {ts:<22} {o:>8.2f} {h:>8.2f} {lo:>8.2f} {cl:>8.2f} {v:>8}")

    # Alpaca snapshot
    if "snapshot" in result and result["snapshot"]:
        snap = result["snapshot"]
        trade = snap.get("latestTrade", {})
        quote = snap.get("latestQuote", {})
        greeks = snap.get("greeks", {})
        iv = snap.get("impliedVolatility", greeks.get("implied_volatility"))

        print(f"\n  Live Snapshot:")
        if trade:
            print(f"    Last: ${trade.get('p')} x {trade.get('s')} @ {str(trade.get('t', ''))[:19]}")
        if quote:
            print(f"    Bid:  ${quote.get('bp')} x {quote.get('bs')}")
            print(f"    Ask:  ${quote.get('ap')} x {quote.get('as')}")
        if greeks:
            print(f"    Delta={greeks.get('delta')} Gamma={greeks.get('gamma')} "
                  f"Theta={greeks.get('theta')} Vega={greeks.get('vega')}")
        if iv:
            print(f"    IV: {iv}")


# ── Main ────────────────────────────────────────────

def main():
    print("=" * 65)
    print(f"  IWN {EXPIRATION} ${STRIKE:.0f} PUT — Historical Data Fetch")
    print(f"  OCC Symbol: {OCC_SYMBOL}")
    print("=" * 65)
    print()

    result = None

    # 1. Try Twelve Data (best for historical bars if you have a key)
    result = fetch_twelve_data()

    # 2. Try CBOE delayed quotes (free, always works for current snapshot + greeks)
    if not result:
        result = fetch_cboe()

    # 3. Fall back to Alpaca
    if not result:
        result = fetch_alpaca()

    if not result:
        print("\n[!] No data available. For historical bars, set:")
        print("    export TWELVE_DATA_API_KEY=your_key    (twelvedata.com - free tier)")
        print("  Or for Alpaca:")
        print("    export ALPACA_API_KEY=your_key")
        print("    export ALPACA_SECRET_KEY=your_secret")
        sys.exit(1)

    display_results(result)
    return result


if __name__ == "__main__":
    main()
