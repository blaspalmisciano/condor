"""PMM Mister level preview: paste a controller config (YAML/JSON), get a candle chart with the spread levels as dotted lines, per-level activation stats for the last N days, and an optional server-side backtest."""

CATEGORY = "Bot Analysis"

import ast
import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field
from telegram import Update
from telegram.ext import ContextTypes

from config_manager import get_client
from routines.base import RoutineResult

logger = logging.getLogger(__name__)

# State key for the Telegram paste flow: run() sets it, handle_message consumes it.
AWAITING_CONFIG_STATE = "pmm_level_preview_config"
MESSAGE_STATES = [AWAITING_CONFIG_STATE]
_PENDING_KEY = "plp_pending_cfg"

BUY_COLORS = ["#58a6ff", "#79c0ff", "#56d4dd", "#7ee787", "#3fb950", "#a5d6ff"]
SELL_COLORS = ["#f85149", "#ff7b72", "#ffa657", "#e3b341", "#d29922", "#ff9492"]

DARK_LAYOUT = dict(
    paper_bgcolor="#0d1117",
    plot_bgcolor="#161b22",
    font=dict(color="#c9d1d9", size=11),
)


class Config(BaseModel):
    """Paste a pmm_mister config; see which spread levels would have been hit over the last N days (candles + dotted levels + %-of-time-active per level), plus an optional real backtest via the Hummingbot backtesting engine."""

    config_text: str = Field(
        default="",
        description="The controller config, pasted as YAML or JSON (dict). Leave empty when running from Telegram to be prompted for a paste.",
    )
    config_name: str = Field(
        default="",
        description="Alternatively: name of a config in the server's controller-config library (e.g. 'usdt_mm_live'). Ignored if config_text is set.",
    )
    days: int = Field(default=7, description="Lookback window in days")
    candle_interval: str = Field(
        default="1m",
        description="Candle interval for the level-touch analysis. Smaller = more accurate 'time active' numbers.",
    )
    candle_connector: str = Field(
        default="",
        description="Connector for candles. Empty = the config's connector_name.",
    )
    run_backtest: bool = Field(
        default=True,
        description="Also run the config through the server-side Hummingbot backtesting engine over the same window.",
    )
    backtest_resolution: str = Field(
        default="3m",
        description="Backtesting engine resolution. 1m is most accurate but slower for long windows. "
                    "'1s' is supported over any window via chunking (see chunk_seconds).",
    )
    chunk_seconds: int = Field(
        default=0,
        description=(
            "Split the backtest window into sub-windows of this many seconds and stitch "
            "them into one continuous result. Each sub-run is short, so no single run "
            "freezes the live API for more than a few seconds. 0 = auto: 3600 (1h "
            "sub-windows) for 1s resolution, single call otherwise."
        ),
    )
    chunk_warmup_seconds: int = Field(
        default=0,
        description=(
            "Warmup overlap prepended to each chunk so executors spanning a boundary are "
            "re-established, not lost. 0 = auto (the config's effectivization_time)."
        ),
    )
    chunk_pause_s: float = Field(
        default=1.0,
        description="Pause between chunk sub-runs so the live API / event loop recovers.",
    )
    trade_cost: float = Field(
        default=0.0,
        description="Fee fraction passed to the backtester (0 = maker with rebates handled separately).",
    )
    rebate_rate: float = Field(
        default=0.00015,
        description="Maker rebate as a fraction of volume, added on top of backtest PnL (0.00015 = 0.015%).",
    )
    hold_candles: int = Field(
        default=1,
        description=(
            "How many candles a quote is held anchored to the same reference before "
            "re-centering. 1 = re-anchor every candle (matches an aggressive "
            "price_distance_tolerance). Higher values simulate a slower-trailing maker: "
            "a level placed at candle t's open can fill any time in the next N candles."
        ),
    )


# ---------------------------------------------------------------------------
# Config parsing
# ---------------------------------------------------------------------------


def parse_config_text(text: str) -> dict:
    """Parse a pasted controller config: JSON, YAML, or a python dict literal."""
    t = text.strip()
    # Tolerate ``` fences and a leading language tag.
    if t.startswith("```"):
        t = t.strip("`")
        first_nl = t.find("\n")
        if first_nl > 0 and " " not in t[:first_nl].strip():
            t = t[first_nl:]
        t = t.strip()
    for loader in (json.loads, yaml.safe_load, ast.literal_eval):
        try:
            d = loader(t)
            if isinstance(d, dict) and d:
                return d
        except Exception:
            continue
    raise ValueError("Could not parse the config as JSON, YAML, or a dict literal.")


def parse_spreads(v) -> list[float]:
    """Normalize spreads: '0.0004,0.0010' | [0.0004, 0.001] | 0.0004 → list[float]."""
    if v is None:
        return []
    if isinstance(v, (int, float)):
        return [float(v)]
    if isinstance(v, str):
        return [float(x) for x in v.split(",") if x.strip()]
    if isinstance(v, list):
        return [float(x) for x in v]
    return []


def _extract_rows(resp) -> list:
    if isinstance(resp, dict):
        for k in ("data", "candles", "rows"):
            if isinstance(resp.get(k), list):
                return resp[k]
        return []
    return resp if isinstance(resp, list) else []


# ---------------------------------------------------------------------------
# Level-touch analysis
# ---------------------------------------------------------------------------


def analyze_levels(
    candles: list[dict],
    buy_spreads: list[float],
    sell_spreads: list[float],
    take_profit: float,
    hold_candles: int = 1,
) -> dict:
    """Per-level activation stats from candles.

    Model: at each candle the maker quotes at open×(1∓spread) (open ≈ the mid /
    reference price at quote time). With hold_candles=1 a BUY level 'activates'
    (would fill) when that same candle's low reaches its price; hold_candles=N
    keeps the quote anchored for N candles, so the fill can happen in any of
    them (slower-trailing maker). take_profit follow-through walks forward
    until fill×(1±tp) prints.
    """
    ts = [float(c["timestamp"]) for c in candles]
    op = [float(c["open"]) for c in candles]
    hi = [float(c["high"]) for c in candles]
    lo = [float(c["low"]) for c in candles]
    n = len(candles)
    span_days = (ts[-1] - ts[0]) / 86400.0 if n > 1 else 1.0
    interval_s = (ts[-1] - ts[0]) / (n - 1) if n > 1 else 60.0
    hold = max(1, int(hold_candles))

    levels: list[dict] = []
    for side, spreads, s_mult in (("buy", buy_spreads, -1.0), ("sell", sell_spreads, 1.0)):
        for i, s in enumerate(spreads):
            prices = [o * (1.0 + s_mult * s) for o in op]
            # touched[t] = quote anchored at t's open fills within its hold window
            # (candles t..t+hold-1).
            if side == "buy":
                touched = [min(lo[t:t + hold]) <= prices[t] for t in range(n)]
            else:
                touched = [max(hi[t:t + hold]) >= prices[t] for t in range(n)]
            n_touch = sum(touched)

            # Longest untouched gap (hours).
            max_gap = gap = 0
            for t in range(n):
                gap = 0 if touched[t] else gap + 1
                max_gap = max(max_gap, gap)

            # TP follow-through: from each fill, candles until fill×(1±tp) prints.
            # Budget-capped forward walk (fills are usually resolved quickly).
            tp_hits = 0
            tp_waits: list[int] = []
            budget = 500_000
            if take_profit > 0 and n_touch:
                for t in range(n):
                    if not touched[t] or budget <= 0:
                        continue
                    fill = prices[t]
                    tgt = fill * (1.0 + take_profit) if side == "buy" else fill * (1.0 - take_profit)
                    for j in range(t, min(n, t + 20_000)):
                        budget -= 1
                        hit = hi[j] >= tgt if side == "buy" else lo[j] <= tgt
                        if hit:
                            tp_hits += 1
                            tp_waits.append(j - t)
                            break
                        if budget <= 0:
                            break
            tp_waits.sort()
            levels.append({
                "level_id": f"{side}_{i}",
                "side": side,
                "index": i,
                "spread": s,
                "prices": prices,
                "touched": touched,
                "n_touch": n_touch,
                "touch_pct": n_touch / n * 100.0 if n else 0.0,
                "touches_per_day": n_touch / span_days if span_days > 0 else 0.0,
                "max_gap_hours": max_gap * interval_s / 3600.0,
                "tp_hit_pct": (tp_hits / n_touch * 100.0) if n_touch else None,
                "tp_median_min": (tp_waits[len(tp_waits) // 2] * interval_s / 60.0) if tp_waits else None,
            })

    # Depth distribution per side: % of candles whose deepest activated level is i
    # (exclusive attribution — "price went this far and no further").
    depth: dict[str, dict[int, float]] = {}
    for side in ("buy", "sell"):
        side_lvls = sorted([l for l in levels if l["side"] == side], key=lambda l: l["spread"])
        counts: dict[int, int] = defaultdict(int)
        for t in range(n):
            deepest = -1
            for k, l in enumerate(side_lvls):
                if l["touched"][t]:
                    deepest = k
            if deepest >= 0:
                counts[side_lvls[deepest]["index"]] += 1
        depth[side] = {i: c / n * 100.0 for i, c in counts.items()} if n else {}

    return {"levels": levels, "depth": depth, "ts": ts, "interval_s": interval_s, "n": n}


def aggregate_candles(candles: list[dict], factor: int) -> list[dict]:
    """OHLC-aggregate consecutive candles for a lighter display chart."""
    if factor <= 1:
        return candles
    out = []
    for i in range(0, len(candles), factor):
        chunk = candles[i:i + factor]
        out.append({
            "timestamp": chunk[0]["timestamp"],
            "open": float(chunk[0]["open"]),
            "high": max(float(c["high"]) for c in chunk),
            "low": min(float(c["low"]) for c in chunk),
            "close": float(chunk[-1]["close"]),
        })
    return out


def _dt(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def _lvl_color(side: str, i: int) -> str:
    pal = BUY_COLORS if side == "buy" else SELL_COLORS
    return pal[i % len(pal)]


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------


def _fmt_pct(p: float, n_touch: int) -> str:
    """'0.0%' hides rare-but-real touches — show '<0.1%' instead."""
    if n_touch and p < 0.05:
        return "<0.1%"
    return f"{p:.1f}%"


def build_levels_figure(candles: list[dict], analysis: dict, pair: str, interval: str):
    """Candles + dotted level lines (top) and per-bucket activation % (bottom)."""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    # Display candles: cap around ~2500 for a readable/exportable chart.
    factor = max(1, len(candles) // 2500)
    disp = aggregate_candles(candles, factor)
    dx = [_dt(float(c["timestamp"])) for c in disp]
    agg_note = f" (display aggregated {factor}×{interval}; stats at {interval})" if factor > 1 else ""

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, row_heights=[0.68, 0.32], vertical_spacing=0.05,
        subplot_titles=(
            f"{pair} {interval} candles + quote levels (dotted; ⨯ = touch){agg_note}",
            "% of candles activating each level (per bucket)",
        ),
    )
    fig.add_trace(go.Candlestick(
        x=dx,
        open=[c["open"] for c in disp], high=[c["high"] for c in disp],
        low=[c["low"] for c in disp], close=[c["close"] for c in disp],
        name="Price", increasing_line_color="#3fb950", decreasing_line_color="#f85149",
        showlegend=False,
    ), row=1, col=1)

    raw_ts = analysis["ts"]
    for l in analysis["levels"]:
        color = _lvl_color(l["side"], l["index"])
        y = [float(c["open"]) * (1.0 + (-1.0 if l["side"] == "buy" else 1.0) * l["spread"]) for c in disp]
        arrow = "▼" if l["side"] == "buy" else "▲"
        # Step shape: a quote rests at a fixed price until re-anchored — the
        # smooth "parallel curve" look understated that the line is many
        # discrete resting quotes.
        fig.add_trace(go.Scatter(
            x=dx, y=y, mode="lines",
            name=f"{arrow} {l['level_id']} ({l['spread'] * 10000:.1f}bps · {_fmt_pct(l['touch_pct'], l['n_touch'])})",
            line=dict(width=1.2, color=color, dash="dot", shape="hv"),
            hovertemplate=f"<b>{l['level_id']}</b> %{{y:,.5f}}<br>%{{x}}<extra></extra>",
        ), row=1, col=1)
        # Explicit touch markers at raw resolution — the display aggregation
        # smooths wicks, so without these, rare fills are invisible.
        touch_idx = [t for t, hit in enumerate(l["touched"]) if hit]
        if touch_idx:
            if len(touch_idx) > 1500:  # keep the figure light for busy ladders
                stride = len(touch_idx) // 1500 + 1
                touch_idx = touch_idx[::stride]
            fig.add_trace(go.Scatter(
                x=[_dt(raw_ts[t]) for t in touch_idx],
                y=[l["prices"][t] for t in touch_idx],
                mode="markers", showlegend=False,
                marker=dict(symbol="x", size=9, color=color, line=dict(width=1, color="#ffffff")),
                hovertemplate=f"<b>{l['level_id']} TOUCH</b> %{{y:,.5f}}<br>%{{x}}<extra></extra>",
            ), row=1, col=1)

    # Bottom: bucketed activation share per level.
    ts = analysis["ts"]
    span = ts[-1] - ts[0] if len(ts) > 1 else 3600.0
    bucket_s = max(3600.0, span / 84)  # ~84 points max
    first_b = int(ts[0] // bucket_s) * bucket_s
    buckets = defaultdict(lambda: defaultdict(int))
    totals: dict[int, int] = defaultdict(int)
    for t_i, t in enumerate(ts):
        b = int((t - first_b) // bucket_s)
        totals[b] += 1
        for l in analysis["levels"]:
            if l["touched"][t_i]:
                buckets[l["level_id"]][b] += 1
    bxs = sorted(totals)
    bx_dt = [_dt(first_b + b * bucket_s + bucket_s / 2) for b in bxs]
    for l in analysis["levels"]:
        color = _lvl_color(l["side"], l["index"])
        y = [buckets[l["level_id"]].get(b, 0) / totals[b] * 100.0 for b in bxs]
        fig.add_trace(go.Scatter(
            x=bx_dt, y=y, mode="lines", name=l["level_id"], showlegend=False,
            line=dict(width=1.4, color=color),
            hovertemplate=f"<b>{l['level_id']}</b> %{{y:.1f}}%%<br>%{{x}}<extra></extra>",
        ), row=2, col=1)

    fig.update_layout(
        **DARK_LAYOUT, margin=dict(l=55, r=25, t=55, b=30), height=760,
        legend=dict(orientation="h", yanchor="bottom", y=1.045, xanchor="right", x=1, font=dict(size=9)),
    )
    fig.update_xaxes(rangeslider_visible=False, row=1, col=1)
    fig.update_yaxes(title_text="Price", gridcolor="#21262d", row=1, col=1)
    fig.update_yaxes(title_text="% active", gridcolor="#21262d", range=[0, 100], row=2, col=1)
    fig.update_xaxes(gridcolor="#21262d", row=2, col=1)
    for ann in fig.layout.annotations:
        ann.font = dict(size=12, color="#c9d1d9")
    return fig


def build_touch_bars(analysis: dict):
    """Horizontal bars: % of candles activating each level."""
    import plotly.graph_objects as go

    lvls = sorted(analysis["levels"], key=lambda l: (l["side"], l["spread"]))
    if not lvls:
        return None
    fig = go.Figure(go.Bar(
        y=[f"{l['level_id']} ({l['spread'] * 10000:.1f}bps)" for l in lvls],
        x=[l["touch_pct"] for l in lvls],
        orientation="h",
        marker_color=[_lvl_color(l["side"], l["index"]) for l in lvls],
        text=[f"{l['touch_pct']:.1f}%  ·  {l['touches_per_day']:.0f}/day" for l in lvls],
        textposition="auto",
        hovertemplate="%{y}: %{x:.1f}%<extra></extra>",
    ))
    fig.update_layout(**DARK_LAYOUT, title="% of time (candles) each level would have filled",
                      margin=dict(l=160, r=30, t=55, b=35), height=max(240, len(lvls) * 42 + 110),
                      xaxis=dict(title="% of candles", gridcolor="#21262d"))
    return fig


def build_backtest_equity(bt: dict):
    """Equity curve from the backtester's pnl_timeseries, if present."""
    import plotly.graph_objects as go

    series = bt.get("pnl_timeseries") or []
    xs, ys = [], []
    if isinstance(series, list):
        for p in series:
            if isinstance(p, dict):
                t = p.get("timestamp") or p.get("ts") or p.get("time")
                # Engine emits {timestamp, total_pnl, executor_realized_pnl,
                # position_unrealized_pnl, cumulative_volume, ...}
                v = p.get("total_pnl")
                if v is None:
                    v = p.get("pnl") or p.get("net_pnl_quote") or p.get("cum_pnl") or p.get("value")
                if t is not None and v is not None:
                    xs.append(_dt(float(t)))
                    ys.append(float(v))
    if not xs:
        return None
    fig = go.Figure(go.Scatter(x=xs, y=ys, mode="lines", line=dict(width=1.6, color="#58a6ff"),
                               hovertemplate="PnL %{y:,.2f}<br>%{x}<extra></extra>"))
    fig.add_hline(y=0, line_dash="dot", line_color="white", opacity=0.3)
    fig.update_layout(**DARK_LAYOUT, title="Backtest cumulative PnL (quote)",
                      margin=dict(l=55, r=25, t=55, b=35), height=300,
                      yaxis=dict(gridcolor="#21262d"), xaxis=dict(gridcolor="#21262d"))
    return fig


# ---------------------------------------------------------------------------
# Backtest
# ---------------------------------------------------------------------------


async def _convert_tick_spreads(
    client, connector: str, pair: str,
    buy_raw: list[float], sell_raw: list[float],
    candles: list[dict],
) -> tuple[list[float], list[float]]:
    """Convert tick counts → spread fractions: tick_count × (min_price_increment / ref_price)."""
    ref_price = float(candles[-1]["close"])
    min_tick = 0.0
    try:
        rules = await client.connectors.get_trading_rules(connector_name=connector)
        pair_rules = (rules or {}).get(pair) or {}
        min_tick = float(pair_rules.get("min_price_increment", 0))
    except Exception as e:
        logger.warning("tick_mode: could not fetch trading rules for %s/%s: %s", connector, pair, e)

    if min_tick <= 0 or ref_price <= 0:
        logger.warning(
            "tick_mode=True but min_price_increment unavailable for %s/%s; "
            "tick counts treated as raw fractions — display will be inaccurate",
            connector, pair,
        )
        return buy_raw, sell_raw

    mult = min_tick / ref_price
    return [s * mult for s in buy_raw], [s * mult for s in sell_raw]


# ---------------------------------------------------------------------------
# Chunked + stitched backtesting (fine resolutions over long windows)
# ---------------------------------------------------------------------------
# The server backtest engine runs a per-bar Python loop ON the single uvicorn
# event loop with no offloading: a long run pins one CPU to 100% and freezes the
# WHOLE API — every endpoint and all live-bot monitoring — for its entire
# duration. A 1s/10-day run is 864,000 bars and would freeze the API for tens of
# minutes. So a 1s backtest cannot be run as one call against a LIVE server.
#
# Instead we split the window into short back-to-back sub-windows (default 1h of
# 1s ≈ 3600 bars, which completes in ~5s — measured), run them sequentially via
# the async task API (submit → poll → delete, so we never hold one long HTTP
# connection and never leave a task grinding), pause briefly between runs so the
# event loop / live monitoring recovers, and stitch the sub-results back into one
# continuous curve. Each sub-run freezes the API for only single-digit seconds.
#
# A warmup overlap (~the controller's effectivization_time) is prepended to each
# sub-window so executors that would already be open at the chunk boundary get
# re-established; the warmup portion is then dropped and executors are attributed
# to exactly one chunk (creation timestamp in [c0, c1)) so nothing double-counts.

CHUNK_TERMINAL = ("completed", "failed", "error", "cancelled")


def _bt_payload(task: dict) -> dict:
    """Return the result payload from a task envelope or a bare result dict."""
    if not isinstance(task, dict):
        return {}
    res = task.get("result")
    return res if isinstance(res, dict) else task


def _auto_warmup_seconds(cfg: dict) -> int:
    """Warmup overlap = the controller's effectivization time (how long a position
    takes to become 'effective'), so executors spanning a chunk boundary are
    re-established inside the warmup rather than lost. Min 300s."""
    effs = []
    for k in ("buy_position_effectivization_time", "sell_position_effectivization_time"):
        try:
            effs.append(int(float(cfg.get(k) or 0)))
        except (TypeError, ValueError):
            pass
    return max(300, max(effs) if effs else 0)


async def _run_one_task(
    client, cfg: dict, w0: int, w1: int, resolution: str, trade_cost: float,
    poll_interval: float, poll_timeout: float,
) -> dict:
    """Submit ONE backtest sub-window via the task API, poll to completion, and
    ALWAYS delete the task. Returns the result payload. Raises on failure/timeout."""
    from condor.backtesting import coerce_controller_config

    submitted = await client.backtesting.submit_task(
        start_time=int(w0), end_time=int(w1),
        backtesting_resolution=resolution, trade_cost=trade_cost,
        config=coerce_controller_config(cfg),
    )
    task_id = submitted.get("task_id") if isinstance(submitted, dict) else None
    if not task_id:
        raise RuntimeError(f"backtest sub-window not accepted: {submitted}")
    deadline = time.monotonic() + poll_timeout
    try:
        while True:
            task = await client.backtesting.get_task(task_id)
            status = task.get("status") if isinstance(task, dict) else None
            if status in CHUNK_TERMINAL:
                if status != "completed":
                    raise RuntimeError(
                        f"sub-window {status}: {task.get('error') or _bt_payload(task).get('error')}"
                    )
                return _bt_payload(task)
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"sub-window still {status} after {int(poll_timeout)}s (task {task_id})"
                )
            await asyncio.sleep(poll_interval)
    finally:
        # Never leave a task grinding on the live server (that is what froze the
        # API for 86 min before) — delete regardless of how we exit.
        try:
            await client.backtesting.delete_task(task_id)
        except Exception as e:
            logger.warning("delete_task(%s) failed: %s", task_id, e)


async def run_chunked_backtest(
    client, cfg: dict, start: int, end: int, resolution: str, trade_cost: float,
    chunk_s: int, warmup_s: int, pause_s: float = 1.0,
    poll_interval: float = 0.5, poll_timeout: float = 120.0,
    health_every: int = 25, log=lambda m: None,
) -> dict:
    """Backtest [start, end] at a fine resolution by running short sub-windows and
    stitching them into ONE continuous result shaped like a single backtest
    (``{results, pnl_timeseries, executors, _chunks}``).

    Each sub-window is ``[c0 - warmup_s, c1]``; only points/executors at/after c0
    are kept. PnL and cumulative-volume curves are rebased per chunk (so warmup
    P&L is excluded) and offset by the running total, giving a continuous curve.
    """
    boundaries = list(range(int(start), int(end), int(chunk_s)))
    stitched_pnl: list[dict] = []
    stitched_execs: list[dict] = []
    pnl_offset = 0.0
    vol_offset = 0.0
    peak = float("-inf")
    max_dd = 0.0
    close_types: dict[str, int] = defaultdict(int)
    chunk_meta: list[dict] = []
    total_wall = 0.0

    for i, c0 in enumerate(boundaries):
        c1 = min(c0 + int(chunk_s), int(end))
        sub_start = c0 - int(warmup_s)
        t0 = time.monotonic()
        try:
            res = await _run_one_task(
                client, cfg, sub_start, c1, resolution, trade_cost,
                poll_interval, poll_timeout,
            )
        except Exception as e:
            dt = time.monotonic() - t0
            total_wall += dt
            chunk_meta.append({"i": i, "c0": c0, "c1": c1, "wall_s": dt, "error": str(e)[:200]})
            log(f"chunk {i+1}/{len(boundaries)} FAILED in {dt:.2f}s: {str(e)[:160]}")
            continue
        dt = time.monotonic() - t0
        total_wall += dt

        pts = _bt_payload_series(res)
        execs = res.get("executors") or []
        kept = [p for p in pts if float(p.get("timestamp", 0)) >= c0]
        kept_execs = [
            e for e in execs
            if c0 <= float(e.get("timestamp", 0)) < c1
        ]
        n_added_pts = 0
        if kept:
            base_pnl = float(kept[0].get("total_pnl") or 0.0)
            base_vol = float(kept[0].get("cumulative_volume") or 0.0)
            for p in kept:
                ts = float(p["timestamp"])
                # The boundary second (ts == previous chunk's c1 == this chunk's c0)
                # is produced by both chunks; keep only the first so the curve has
                # no duplicate timestamps.
                if stitched_pnl and ts <= stitched_pnl[-1]["timestamp"]:
                    continue
                tp = pnl_offset + (float(p.get("total_pnl") or 0.0) - base_pnl)
                cv = vol_offset + (float(p.get("cumulative_volume") or 0.0) - base_vol)
                peak = max(peak, tp)
                max_dd = min(max_dd, tp - peak)
                stitched_pnl.append({
                    "timestamp": ts,
                    "total_pnl": tp,
                    "cumulative_volume": cv,
                    "active_executors": p.get("active_executors"),
                })
            pnl_offset += float(kept[-1].get("total_pnl") or 0.0) - base_pnl
            vol_offset += float(kept[-1].get("cumulative_volume") or 0.0) - base_vol
            n_added_pts = len(kept)
        for e in kept_execs:
            ct = e.get("close_type")
            if ct is not None:
                close_types[str(ct)] += 1
        stitched_execs.extend(kept_execs)

        chunk_meta.append({
            "i": i, "c0": c0, "c1": c1, "wall_s": dt,
            "kept_pts": n_added_pts, "kept_execs": len(kept_execs),
        })
        log(f"chunk {i+1}/{len(boundaries)} [{_dt(c0):%m-%d %H:%M}→{_dt(c1):%H:%M}] "
            f"{dt:.2f}s +{len(kept_execs)} execs, pnl→{pnl_offset:+.2f}")

        if health_every and (i + 1) % health_every == 0:
            try:
                th = time.monotonic()
                await asyncio.wait_for(client.backtesting.list_tasks(), timeout=10)
                log(f"  API health OK ({time.monotonic()-th:.2f}s) after {i+1} chunks")
            except Exception as e:
                log(f"  API HEALTH DEGRADED after {i+1} chunks: {e!r} — stopping early")
                break
        if pause_s and i + 1 < len(boundaries):
            await asyncio.sleep(pause_s)

    filled = sum(1 for e in stitched_execs if float(e.get("filled_amount_quote") or 0) > 0)
    total_volume = sum(float(e.get("filled_amount_quote") or 0) for e in stitched_execs)
    results = {
        "net_pnl_quote": pnl_offset,
        "total_executors": len(stitched_execs),
        "total_executors_with_position": filled,
        "total_volume": total_volume,
        "close_types": dict(close_types),
        "max_drawdown_usd": max_dd,
    }
    return {
        "results": results,
        "pnl_timeseries": stitched_pnl,
        "executors": stitched_execs,
        "_chunks": chunk_meta,
        "_chunk_total_wall_s": total_wall,
    }


def _bt_payload_series(res: dict) -> list:
    """The pnl_timeseries list from a result payload (tolerant of missing keys)."""
    pts = res.get("pnl_timeseries")
    return pts if isinstance(pts, list) else []


async def run_server_backtest(
    client, ctrl_cfg: dict, days: int, resolution: str, trade_cost: float,
    tick_converted_spreads: tuple[list[float], list[float]] | None = None,
    chunk_seconds: int = 0, chunk_warmup_seconds: int = 0, chunk_pause_s: float = 1.0,
    start: int | None = None, end: int | None = None, log=lambda m: None,
) -> dict:
    """Run the config through the Hummingbot backtesting engine on the server.

    For fine resolutions over a long window the run is chunked+stitched (see
    ``run_chunked_backtest``) so no single sub-run freezes the live API for more
    than a few seconds. ``chunk_seconds`` <= 0 auto-selects: 3600s (1h) sub-windows
    for 1s resolution, single call otherwise.
    """
    cfg = dict(ctrl_cfg)
    cfg.setdefault("controller_name", "pmm_mister")
    cfg.setdefault("controller_type", "generic")
    cfg.setdefault("id", "pmm_level_preview")
    if tick_converted_spreads is not None:
        # The backtesting engine runs the controller in simulated mode and cannot fetch
        # live trading rules for min_price_increment, so tick_mode would use
        # spread_multiplier=0 → orders placed at 0% spread → 0 fills. Pre-convert the
        # tick counts to plain fractions and disable tick_mode to avoid double-conversion.
        cfg["buy_spreads"] = list(tick_converted_spreads[0])
        cfg["sell_spreads"] = list(tick_converted_spreads[1])
        cfg["tick_mode"] = False
    else:
        # pmm_mister's Pydantic model expects List[Decimal] for spreads; passing a
        # comma-string causes validation failure → 0 fills. Normalize to list[float].
        for key in ("buy_spreads", "sell_spreads"):
            if key in cfg:
                cfg[key] = parse_spreads(cfg[key])
    if end is None:
        end = int(time.time()) - 300
    if start is None:
        start = end - days * 86400

    # Decide chunk size: explicit, else auto (1s → 1h sub-windows).
    chunk_s = int(chunk_seconds)
    if chunk_s <= 0:
        chunk_s = 3600 if resolution == "1s" else 0

    if chunk_s > 0 and (end - start) > chunk_s:
        warmup = int(chunk_warmup_seconds) or _auto_warmup_seconds(cfg)
        log(f"chunked backtest: {(end-start)/3600:.1f}h @ {resolution} in "
            f"{chunk_s//60}m chunks (warmup {warmup}s)")
        return await run_chunked_backtest(
            client, cfg, start, end, resolution, trade_cost,
            chunk_s=chunk_s, warmup_s=warmup, pause_s=chunk_pause_s, log=log,
        )

    return await client.backtesting.run(
        start_time=start, end_time=end,
        backtesting_resolution=resolution, trade_cost=trade_cost, config=cfg,
    )


def summarize_backtest(bt: dict, rebate_rate: float) -> tuple[dict, list[str]]:
    """Flatten backtester output into KPIs + Telegram lines."""
    res = bt.get("results") or {}
    vol = float(res.get("total_volume") or 0.0)
    pnl = float(res.get("net_pnl_quote") or 0.0)
    rebates = vol * rebate_rate
    n_exec = int(res.get("total_executors") or 0)
    n_filled = int(res.get("total_executors_with_position") or 0)
    close_types = res.get("close_types") or {}
    kpis = {
        "pnl": pnl, "volume": vol, "rebates": rebates, "pnl_reb": pnl + rebates,
        "n_executors": n_exec, "n_filled": n_filled,
        "max_dd": float(res.get("max_drawdown_usd") or 0.0),
        "sharpe": res.get("sharpe_ratio"),
        "close_types": close_types,
    }
    lines = [
        f"Backtest: PnL {pnl:+,.2f} + rebates {rebates:+,.2f} = *{pnl + rebates:+,.2f}* quote",
        f"Volume {vol:,.0f} | executors {n_exec} ({n_filled} filled) | maxDD {kpis['max_dd']:,.2f}",
    ]
    if close_types:
        lines.append("Close types: " + ", ".join(f"{k}:{v}" for k, v in sorted(close_types.items())))
    return kpis, lines


# ---------------------------------------------------------------------------
# Core execution
# ---------------------------------------------------------------------------


async def _resolve_controller_config(client, config: Config) -> dict:
    if config.config_text.strip():
        return parse_config_text(config.config_text)
    if config.config_name.strip():
        raw = await client.controllers.get_controller_config(config.config_name.strip())
        inner = raw.get("config") if isinstance(raw.get("config"), dict) else raw
        if not isinstance(inner, dict) or not inner:
            raise ValueError(f"Config '{config.config_name}' not found in the server library.")
        return inner
    raise ValueError("No config provided.")


async def _execute(config: Config, client, ctrl_cfg: dict) -> RoutineResult:
    pair = (ctrl_cfg.get("trading_pair") or "").upper()
    connector = config.candle_connector.strip() or ctrl_cfg.get("connector_name") or "binance"
    if not pair:
        raise ValueError("Config has no trading_pair.")

    tick_mode = bool(ctrl_cfg.get("tick_mode", False))
    buy_spreads_raw = parse_spreads(ctrl_cfg.get("buy_spreads"))
    sell_spreads_raw = parse_spreads(ctrl_cfg.get("sell_spreads"))
    if not buy_spreads_raw and not sell_spreads_raw:
        raise ValueError("Config has no buy_spreads / sell_spreads.")
    take_profit = float(ctrl_cfg.get("take_profit") or 0.0)

    candles = _extract_rows(await client.market_data.get_candles_last_days(
        connector, pair, max(1, config.days), config.candle_interval
    ))
    if len(candles) < 10:
        raise ValueError(f"Not enough candle data for {pair} {config.candle_interval} ({len(candles)} rows).")

    if tick_mode:
        buy_spreads, sell_spreads = await _convert_tick_spreads(
            client, connector, pair, buy_spreads_raw, sell_spreads_raw, candles
        )
    else:
        buy_spreads, sell_spreads = buy_spreads_raw, sell_spreads_raw

    analysis = analyze_levels(candles, buy_spreads, sell_spreads, take_profit,
                              hold_candles=config.hold_candles)

    bt = None
    bt_kpis: dict = {}
    bt_lines: list[str] = []
    if config.run_backtest:
        try:
            bt = await run_server_backtest(
                client, ctrl_cfg, config.days, config.backtest_resolution, config.trade_cost,
                tick_converted_spreads=(buy_spreads, sell_spreads) if tick_mode else None,
                chunk_seconds=config.chunk_seconds,
                chunk_warmup_seconds=config.chunk_warmup_seconds,
                chunk_pause_s=config.chunk_pause_s,
                log=lambda m: logger.info("pmm_level_preview backtest: %s", m),
            )
            bt_kpis, bt_lines = summarize_backtest(bt, config.rebate_rate)
        except Exception as e:
            logger.warning("server backtest failed: %r", e)
            bt_lines = [f"⚠ Backtest failed: {str(e)[:160]}"]

    fig_levels = build_levels_figure(candles, analysis, pair, config.candle_interval)
    fig_bars = build_touch_bars(analysis)

    # ----- Web report -----
    table_rows = []
    for l in sorted(analysis["levels"], key=lambda l: (l["side"], l["spread"])):
        table_rows.append({
            "Level": l["level_id"],
            "Spread": f"{l['spread'] * 10000:.1f} bps",
            "Touch %": _fmt_pct(l["touch_pct"], l["n_touch"]),
            "Fills/day": f"{l['touches_per_day']:.0f}",
            "Deepest hit %": f"{analysis['depth'][l['side']].get(l['index'], 0.0):.1f}%",
            "Max quiet gap": f"{l['max_gap_hours']:.1f}h",
            "TP hit": (f"{l['tp_hit_pct']:.0f}%" if l["tp_hit_pct"] is not None else "—"),
            "TP median wait": (f"{l['tp_median_min']:.0f}m" if l["tp_median_min"] is not None else "—"),
        })
    columns = ["Level", "Spread", "Touch %", "Fills/day", "Deepest hit %", "Max quiet gap", "TP hit", "TP median wait"]

    try:
        from condor.reports import ReportBuilder

        builder = ReportBuilder(f"PMM Level Preview — {pair} ({config.days}d)")
        builder.source("routine", "pmm_level_preview").tags(["pmm_mister", "levels", "backtest", pair])
        builder.manual_order()
        builder.kpi("Pair", pair)
        builder.kpi("Window", f"{config.days}d @ {config.candle_interval}")
        builder.kpi("Buy levels", str(len(buy_spreads)))
        builder.kpi("Sell levels", str(len(sell_spreads)))
        if bt_kpis:
            builder.kpi("Backtest PnL+Reb", f"{bt_kpis['pnl_reb']:+,.2f}",
                        trend="up" if bt_kpis["pnl_reb"] >= 0 else "down")
            builder.kpi("Backtest Volume", f"{bt_kpis['volume']:,.0f}")
        hold_note = (
            f" Quotes are held anchored for **{config.hold_candles} candles** before re-centering "
            f"(hold_candles); with overlapping hold windows, Touch % is the probability a quote "
            f"fills within its lifetime and Fills/day is an upper bound."
            if config.hold_candles > 1 else
            " Quotes re-anchor **every candle** (hold_candles=1) — levels trail the mid, so a fill "
            "needs the full spread to be crossed within one candle."
        )
        builder.markdown(
            f"## Level activation — {pair}, last {config.days} days\n"
            f"_Model: at each {config.candle_interval} candle the maker quotes at open × (1∓spread) "
            f"(open ≈ mid at quote time). A level **activates** when the candle range reaches its price."
            f"{hold_note} "
            f"'Deepest hit %' attributes each candle only to the deepest level reached — that's the "
            f"'% of time spent at each level'. TP follow-through uses take_profit = "
            f"{take_profit * 10000:.1f} bps. Cooldowns/inventory caps are NOT modeled here — "
            f"the backtest section below includes them._"
        )
        builder.table(table_rows, columns=columns)
        builder.plotly(fig_levels)
        if fig_bars is not None:
            builder.plotly(fig_bars)
        if bt is not None:
            builder.markdown(
                "## Backtest (Hummingbot engine, server-side)\n"
                f"_Same config, last {config.days} days at {config.backtest_resolution} resolution, "
                f"trade_cost {config.trade_cost}. This simulates the real controller: refresh, "
                f"cooldowns, TP/SL barriers._\n\n" + "\n".join("- " + ln.replace("*", "**") for ln in bt_lines)
            )
            fig_eq = build_backtest_equity(bt)
            if fig_eq is not None:
                builder.plotly(fig_eq)
        await builder.save()
    except Exception as e:
        logger.warning("report generation failed: %s", e, exc_info=True)

    # ----- Telegram text -----
    lines = [
        f"*PMM Level Preview* — {pair}, last {config.days}d @ {config.candle_interval}",
        f"{len(buy_spreads)} buy / {len(sell_spreads)} sell levels"
        + (f", TP {take_profit * 10000:.1f}bps" if take_profit else ""),
        "",
    ]
    for l in sorted(analysis["levels"], key=lambda l: (l["side"], l["spread"])):
        arrow = "🟦" if l["side"] == "buy" else "🟥"
        tp_txt = f" | TP {l['tp_hit_pct']:.0f}%~{l['tp_median_min']:.0f}m" if l["tp_hit_pct"] is not None else ""
        lines.append(
            f"{arrow} `{l['level_id']}` {l['spread'] * 10000:.1f}bps: "
            f"active *{_fmt_pct(l['touch_pct'], l['n_touch'])}* (~{l['touches_per_day']:.0f}/d), "
            f"deepest {analysis['depth'][l['side']].get(l['index'], 0.0):.1f}%{tp_txt}"
        )
    if bt_lines:
        lines.append("")
        lines.extend(bt_lines)
    lines.append("\nFull charts in the web report (Reports page).")
    text = "\n".join(lines)

    chart_bytes = None
    try:
        import io
        buf = io.BytesIO()
        fig_levels.write_image(buf, format="png", scale=2)
        chart_bytes = buf.getvalue()
    except Exception as e:
        logger.debug("chart export failed: %s", e)

    return RoutineResult(text=text, table_data=table_rows, table_columns=columns, chart_image=chart_bytes)


async def _run_and_send(config: Config, client, ctrl_cfg: dict, bot, chat_id) -> RoutineResult:
    """Execute and push chart photo + summary to Telegram (best-effort)."""
    result = await _execute(config, client, ctrl_cfg)
    if bot and chat_id:
        try:
            if result.chart_image:
                import io
                await bot.send_photo(chat_id=chat_id, photo=io.BytesIO(result.chart_image),
                                     caption=f"PMM Level Preview — {(ctrl_cfg.get('trading_pair') or '').upper()}")
            await bot.send_message(chat_id=chat_id, text=result.text, parse_mode="Markdown")
        except Exception as e:
            logger.warning("telegram delivery failed: %s", e)
    return result


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> RoutineResult:
    chat_id = getattr(context, "_chat_id", None)
    client = await get_client(chat_id, context=context)
    if not client:
        return RoutineResult(text="No server available. Configure servers in /config.")

    user_data = getattr(context, "_user_data", None)
    if not config.config_text.strip() and not config.config_name.strip():
        # Interactive Telegram flow: ask for a paste, remember this run's settings.
        if user_data is not None and getattr(context, "bot", None) and chat_id:
            user_data["routines_state"] = AWAITING_CONFIG_STATE
            user_data[_PENDING_KEY] = config.model_dump()
            return RoutineResult(
                text="📋 Paste your pmm_mister config now (YAML or JSON dict — the same text "
                     "you'd put in the controller config). I'll analyze the last "
                     f"{config.days} days and reply with the level chart."
            )
        return RoutineResult(text="Provide config_text (pasted YAML/JSON) or config_name.")

    ctrl_cfg = await _resolve_controller_config(client, config)
    return await _run_and_send(config, client, ctrl_cfg, getattr(context, "bot", None), chat_id)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Receive the pasted config while in AWAITING_CONFIG_STATE."""
    if context.user_data.get("routines_state") != AWAITING_CONFIG_STATE:
        return False
    text = (update.message.text or "").strip()
    chat_id = update.effective_chat.id

    try:
        ctrl_cfg = parse_config_text(text)
    except ValueError as e:
        await update.message.reply_text(
            f"❌ {e}\nPaste the config again (YAML or JSON), or send /routines to cancel."
        )
        return True  # keep the state, let them retry

    context.user_data["routines_state"] = None
    pending = context.user_data.pop(_PENDING_KEY, None) or {}
    pending["config_text"] = ""  # config comes via ctrl_cfg
    try:
        config = Config(**{k: v for k, v in pending.items() if k in Config.model_fields})
    except Exception:
        config = Config()

    client = await get_client(chat_id, context=context)
    if not client:
        await update.message.reply_text("No server available. Configure servers in /config.")
        return True

    status = await update.message.reply_text(
        f"⏳ Analyzing {(ctrl_cfg.get('trading_pair') or '?').upper()} — last {config.days}d "
        f"@ {config.candle_interval}" + (", + backtest…" if config.run_backtest else "…")
    )
    try:
        await _run_and_send(config, client, ctrl_cfg, context.bot, chat_id)
        await status.delete()
    except Exception as e:
        logger.error("pmm_level_preview failed: %s", e, exc_info=True)
        await status.edit_text(f"❌ Analysis failed: {str(e)[:300]}")
    return True
