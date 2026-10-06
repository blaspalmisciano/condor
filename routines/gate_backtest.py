"""Self-contained candle-cross backtest for a pmm_mister-style maker on Gate data.

The Hummingbot backtest engine can't fetch Gate candles (broken feed), so we build the bench
OFFLINE with this simulator fed by Gate's REST candles, then ship the ranked bench inside the
agent — the competition run never backtests. It's a RANKING tool (relative PnL across configs),
not a precise P&L predictor: coarse-candle fills are optimistic but uniformly so.

Model (directional maker, position_side=BUY): each candle, quote a buy ladder at mid*(1-spread);
a level fills if the bar LOW reaches it (buy to open), then a take-profit sell is placed at
entry*(1+take_profit) and fills when a later bar HIGH reaches it. Maker fee charged on every
fill, so a round trip must clear 2*fee to profit. Leftover inventory is marked at the last close.
"""
from __future__ import annotations
from statistics import median
from typing import Any


def _f(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def simulate(candles: list[dict], cfg: dict, fee: float, candle_sec: int = 300) -> dict:
    """Run the candle-cross maker over `candles`. Returns pnl/volume/realized/unrealized/fills.

    Models EFFECTIVIZATION: a lot that hasn't hit take-profit within `effectivization_time` is
    *worked out* (closed at the bar close) — this is the core pmm_mister mechanic that cycles
    inventory instead of hoarding it. Without it the model vastly overstates inventory drag.
    """
    spreads = cfg.get("buy_spreads") or [0.001]
    if not isinstance(spreads, (list, tuple)):
        spreads = [spreads]
    spreads = [_f(s) for s in spreads]
    tp = _f(cfg.get("take_profit"), 0.001)
    cap = _f(cfg.get("total_amount_quote"), 100.0)
    alloc = _f(cfg.get("portfolio_allocation"), 0.3)
    max_base_pct = _f(cfg.get("max_base_pct"), 0.8)
    eff_sec = _f(cfg.get("buy_position_effectivization_time"), 90.0)
    eff_candles = max(1, round(eff_sec / max(1, candle_sec)))  # how many bars before a lot is worked out
    order_notional = max(1e-9, cap * alloc)
    max_inv_value = cap * max_base_pct

    cash = cap
    inv = 0.0
    realized = 0.0
    volume = 0.0
    fills = 0
    lots: list[dict] = []  # {entry, size, age}

    for c in candles:
        o, h, l, cl = _f(c["open"]), _f(c["high"]), _f(c["low"]), _f(c["close"])
        if o <= 0:
            continue
        # 1) take-profit exits (sell at TP if the bar high reaches it)
        for lot in lots[:]:
            tp_price = lot["entry"] * (1 + tp)
            if h >= tp_price:
                size = lot["size"]
                realized += size * (tp_price - lot["entry"]) - fee * size * (tp_price + lot["entry"])
                cash += size * tp_price
                inv -= size
                volume += size * tp_price
                fills += 1
                lots.remove(lot)
        # 2) effectivization — work out lots older than eff_candles at the bar close
        for lot in lots[:]:
            lot["age"] += 1
            if lot["age"] >= eff_candles:
                size = lot["size"]
                realized += size * (cl - lot["entry"]) - fee * size * (cl + lot["entry"])
                cash += size * cl
                inv -= size
                volume += size * cl
                fills += 1
                lots.remove(lot)
        # 3) buy fills — each ladder level fills if the bar low reaches it and there's room
        mid = o
        for s in spreads:
            bp = mid * (1 - s)
            if l <= bp and inv * mid < max_inv_value and cash >= order_notional:
                size = order_notional / bp
                inv += size
                cash -= size * bp
                realized -= fee * size * bp
                volume += size * bp
                fills += 1
                lots.append({"entry": bp, "size": size, "age": 0})

    last = _f(candles[-1]["close"]) if candles else 0.0
    unrealized = sum(lot["size"] * (last - lot["entry"]) for lot in lots)
    return {"pnl": round(realized + unrealized, 4), "realized": round(realized, 4),
            "unrealized": round(unrealized, 4), "volume": round(volume, 2), "fills": fills}


def split_windows(candles: list[dict], n: int) -> list[list[dict]]:
    """Split the candle series into n disjoint, contiguous time windows (oldest→newest)."""
    if n <= 1 or len(candles) < n:
        return [candles]
    size = len(candles) // n
    return [candles[i * size:(i + 1) * size] for i in range(n)]


def score_over_windows(candles: list[dict], cfg: dict, fee: float, n_windows: int,
                       candle_sec: int = 300) -> dict:
    """Simulate per disjoint window; return per-window PnL + volume + robustness stats."""
    pnls, vols = [], []
    for w in split_windows(candles, n_windows):
        if len(w) < 10:
            continue
        r = simulate(w, cfg, fee, candle_sec)
        pnls.append(r["pnl"])
        vols.append(r["volume"])
    return {"per_window_pnl": pnls,
            "median_pnl": round(median(pnls), 4) if pnls else float("-inf"),
            "n_positive": sum(1 for p in pnls if p > 0),
            "n_windows": len(pnls),
            "total_volume": round(sum(vols), 2)}
