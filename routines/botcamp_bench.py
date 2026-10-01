"""
Botcamp competition — bench + fleet-manager core (infra-independent, unit-testable).

The autonomous agent (Agent Builders Cup) is two cooperating loops sharing a BENCH:

  BENCH BUILDER (continuous): sweep random pmm_mister param variants, backtest each
    over K disjoint daily windows, rank by a ROBUST cross-window statistic (median
    daily PnL), keep only configs that are positive in >= min_positive windows.
    -> antidote to single-window noise (we watched identical configs go +170 / -81
       across two windows; median-of-daily-windows + a "positive in most" gate kills
       that failure mode).

  FLEET MANAGER (every tick): any live controller at <= sub_upnl unrealized PnL, OR
    with no trade for >= sub_no_trade_h hours, is SUBSTITUTED with the top ELIGIBLE
    bench config (excluding the one that just died / recently-failed). No warmup grace
    (triggers are far longer/larger than any startup transient), no global kill
    (continuous rotation IS the risk management on a bounded stake).

This module is pure logic: scoring, windowing, bench upsert/select, trigger detection.
No network, no clients -> fully testable offline. The continuous routine wires it to
the live/backtest clients.
"""
from __future__ import annotations

import time
import hashlib
from statistics import median
from typing import Any, Optional


# --------------------------------------------------------------------------- #
# Scoring — the competition scores actual P&L, so rank on realized+unrealized  #
# (market PnL). No rebate inflation: Gate spot has no maker rebate, and a PnL  #
# competition rewards real money made, not turnover-times-rebate.              #
# --------------------------------------------------------------------------- #
def trial_pnl(trial: dict) -> float:
    """Actual PnL of one backtest trial = realized + unrealized (market PnL)."""
    return float(trial.get("realized", 0.0)) + float(trial.get("unrealized", 0.0))


def robust_score(per_window_pnls: list[float]) -> float:
    """Robust cross-window ranking stat. Median so one lucky window can't inflate a
    config to the top of the bench."""
    vals = [float(p) for p in per_window_pnls if p is not None]
    return float(median(vals)) if vals else float("-inf")


def n_positive(per_window_pnls: list[float]) -> int:
    return sum(1 for p in per_window_pnls if p is not None and p > 0)


# --------------------------------------------------------------------------- #
# K-window sampling — disjoint daily windows over a multi-day lookback.        #
# Each 24h window is its own regime sample (Asia/EU/US sessions); "good across #
# windows" == "robust across regimes".                                        #
# --------------------------------------------------------------------------- #
def k_windows(now: Optional[int] = None, n: int = 7, window_h: int = 24,
              gap_h: int = 0, tail_pad_s: int = 300) -> list[tuple[int, int]]:
    """Return n disjoint [w0, w1] epoch windows going back from `now`.

    Newest window ends `tail_pad_s` before now (avoid the still-forming last candle).
    Optional `gap_h` embargo between windows to reduce leakage. Returned newest-first.
    """
    now = int(now if now is not None else time.time())
    win = window_h * 3600
    gap = gap_h * 3600
    out: list[tuple[int, int]] = []
    end = now - tail_pad_s
    for _ in range(n):
        w0 = end - win
        out.append((w0, end))
        end = w0 - gap
    return out


# --------------------------------------------------------------------------- #
# Bench store — sorted PnL-desc, deduped by param signature, eligibility gate. #
# --------------------------------------------------------------------------- #
def param_sig(config: dict, keys: Optional[list[str]] = None) -> str:
    """Stable signature of the tuning params, so re-swept duplicates collapse to one
    bench row (keep the freshest score)."""
    keys = keys or [
        "buy_spreads", "sell_spreads", "take_profit",
        "min_base_pct", "target_base_pct", "max_base_pct",
        "executor_refresh_time", "buy_cooldown_time", "sell_cooldown_time",
        "buy_position_effectivization_time", "sell_position_effectivization_time",
        "max_active_executors",
    ]
    parts = [f"{k}={config.get(k)}" for k in keys]
    return hashlib.sha1("|".join(parts).encode()).hexdigest()[:16]


def make_bench_entry(config: dict, per_window_pnls: list[float], ts: Optional[int] = None) -> dict:
    return {
        "sig": param_sig(config),
        "config": config,
        "per_window_pnl": [round(float(p), 4) for p in per_window_pnls],
        "median_pnl": round(robust_score(per_window_pnls), 4),
        "n_positive": n_positive(per_window_pnls),
        "n_windows": len(per_window_pnls),
        "updated": int(ts if ts is not None else time.time()),
    }


def bench_upsert(bench: list[dict], entry: dict, max_size: int = 200) -> list[dict]:
    """Insert/replace by signature (freshest wins), re-sort by median PnL desc, cap size."""
    by_sig = {e["sig"]: e for e in bench}
    by_sig[entry["sig"]] = entry  # freshest overwrites
    ranked = sorted(by_sig.values(), key=lambda e: -e["median_pnl"])
    return ranked[:max_size]


def bench_eligible(bench: list[dict], min_positive: int,
                   exclude_sigs: Optional[set[str]] = None) -> list[dict]:
    """Configs allowed to go live: positive in >= min_positive windows, not excluded.
    Returned best-first (already sorted by median PnL)."""
    exclude_sigs = exclude_sigs or set()
    return [e for e in bench
            if e["n_positive"] >= min_positive and e["sig"] not in exclude_sigs]


def select_substitute(bench: list[dict], min_positive: int,
                      exclude_sigs: Optional[set[str]] = None) -> Optional[dict]:
    """Top eligible bench config to deploy in place of a failed controller, or None
    if the bench has nothing that clears the gate."""
    elig = bench_eligible(bench, min_positive, exclude_sigs)
    return elig[0] if elig else None


# --------------------------------------------------------------------------- #
# Fleet-manager triggers — the two hard substitution rules.                    #
# --------------------------------------------------------------------------- #
def needs_substitution(unrealized_pnl: float, last_trade_ts: Optional[float],
                       now: Optional[float] = None,
                       sub_upnl: float = -20.0, sub_no_trade_h: float = 4.0) -> tuple[bool, str]:
    """Return (should_substitute, reason). Fires on EITHER rule."""
    now = float(now if now is not None else time.time())
    if unrealized_pnl is not None and float(unrealized_pnl) <= sub_upnl:
        return True, f"unrealized_pnl {float(unrealized_pnl):.2f} <= {sub_upnl}"
    if last_trade_ts is not None:
        idle_h = (now - float(last_trade_ts)) / 3600.0
        if idle_h >= sub_no_trade_h:
            return True, f"no trade for {idle_h:.1f}h >= {sub_no_trade_h}h"
    return False, ""
