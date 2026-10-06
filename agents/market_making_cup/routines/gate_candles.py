"""Fetch historical OHLCV candles directly from Gate's public REST API.

The Hummingbot gate_io candle feed 500s, but Gate's raw /spot/candlesticks endpoint works
(no auth). This lets us build the backtest bench OFFLINE here and ship it inside the agent,
so the competition run never has to backtest (or reach any candle feed) at all.

Gate candle row shape: [ts, quote_volume, close, high, low, open, base_volume, closed]
"""
from __future__ import annotations
import json
import time
import urllib.request

_BASE = "https://api.gateio.ws/api/v4/spot/candlesticks"


def _gate_pair(pair: str) -> str:
    return pair.replace("-", "_").upper()


def fetch_candles(pair: str, days: int = 3, interval: str = "5m") -> list[dict]:
    """Return OHLCV rows [{timestamp, open, high, low, close, volume}], oldest-first, over up to
    the last `days`. Uses a single limit=1000 call (Gate's reliable max-depth per interval:
    5m≈3.5d, 15m≈10d, 1h≈41d), then trims to `days`. (Gate's from/to pagination is unreliable.)"""
    cp = _gate_pair(pair)
    url = f"{_BASE}?currency_pair={cp}&interval={interval}&limit=1000"
    try:
        data = json.load(urllib.request.urlopen(
            urllib.request.Request(url, headers={"Accept": "application/json"}), timeout=20))
    except Exception:
        return []
    cutoff = time.time() - days * 86400
    rows = []
    for c in data:
        ts = int(c[0])
        if ts < cutoff:
            continue
        rows.append({"timestamp": float(ts), "open": float(c[5]), "high": float(c[3]),
                     "low": float(c[4]), "close": float(c[2]), "volume": float(c[6])})
    return sorted(rows, key=lambda r: r["timestamp"])


if __name__ == "__main__":
    import sys
    pair = sys.argv[1] if len(sys.argv) > 1 else "ATH-USDT"
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
    rows = fetch_candles(pair, days)
    if rows:
        span_h = (rows[-1]["timestamp"] - rows[0]["timestamp"]) / 3600
        import datetime
        f = datetime.datetime.utcfromtimestamp(rows[0]["timestamp"]).strftime("%m-%d %H:%M")
        l = datetime.datetime.utcfromtimestamp(rows[-1]["timestamp"]).strftime("%m-%d %H:%M")
        closes = [r["close"] for r in rows]
        print(f"{pair}: {len(rows)} candles, {span_h:.1f}h ({f} → {l} UTC)")
        print(f"  price range {min(closes):.6f} – {max(closes):.6f}, last {closes[-1]:.6f}")
    else:
        print(f"{pair}: no candles")
