"""PMM parameter sweep: generate N variants of a pmm_mister config (staged OAT +
mini-grid), backtest each over one window via the Hummingbot engine, rank them
(business PnL / volume / total PnL), and render one report: shared-X curves for
base + REAL + top-5, plus a parallel-coordinates chart of every trial."""

CATEGORY = "Bot Analysis"

import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field
from telegram.ext import ContextTypes

from config_manager import get_client
from routines.base import RoutineResult
from routines.pmm_level_preview import _extract_rows, parse_config_text, parse_spreads

logger = logging.getLogger(__name__)

TICK = 0.0001  # min_price_increment for the BRL pairs this targets; used for tick_mode conversion
SWEEP_STORE = Path("data") / "pmm_sweep_store.json"

DARK = dict(paper_bgcolor="#0d1117", plot_bgcolor="#161b22", font=dict(color="#c9d1d9", size=11))
TOP_COLORS = ["#58a6ff", "#a371f7", "#56d4dd", "#f0883e", "#e3b341"]


class Config(BaseModel):
    """Sweep N parameter variants of a pmm_mister config through the server backtester over one window; report top-5 candidates (business PnL / volume / PnL) with shared-axis curves + parallel-coordinates overview."""

    config_text: str = Field(default="", description="Base controller config, pasted YAML/JSON.")
    config_name: str = Field(default="", description="Or: config name in the server library.")
    days: int = Field(default=7, description="Window = last N days (ignored if window_start/end set)")
    window_start: str = Field(default="", description="Explicit window start (ISO UTC or epoch). Use a real run's deploy time to compare vs reality.")
    window_end: str = Field(default="", description="Explicit window end (ISO UTC or epoch).")
    resolution: str = Field(default="1m", description="Backtesting resolution")
    trade_cost: float = Field(default=0.0, description="Fee fraction for the backtester")
    rebate_rate: float = Field(default=0.00015, description="Maker rebate fraction; business PnL = total PnL + volume × this")
    max_trials: int = Field(default=40, description="Hard cap on backtests per sweep (server protection)")
    pause_s: float = Field(default=2.0, description="Pause between backtests (the API server also runs live bots)")
    real_db_path: str = Field(default="", description="Optional archived-bot DB path: overlays REAL fills + mark-to-market PnL from its trades")
    candle_interval: str = Field(default="1m", description="Candle interval for the price panel")


# ---------------------------------------------------------------------------
# Sweep axes
# ---------------------------------------------------------------------------
# Each axis: key, OAT values (base excluded), apply(cfg, v), numeric encoding
# for parcoords. Bands set target_base_pct to the band midpoint (a band is one
# coherent inventory policy, not an independent min/max pair).

def _apply_tp(cfg, m):
    cfg["take_profit"] = float(cfg["take_profit"]) * m

def _apply_spread(cfg, m):
    cfg["buy_spreads"] = [s * m for s in parse_spreads(cfg["buy_spreads"])]
    cfg["sell_spreads"] = [s * m for s in parse_spreads(cfg["sell_spreads"])]

def _apply_band(cfg, band):
    lo, hi = band
    cfg["min_base_pct"], cfg["max_base_pct"] = lo, hi
    cfg["target_base_pct"] = round((lo + hi) / 2, 3)

def _apply_eff(cfg, m):
    for k in ("buy_position_effectivization_time", "sell_position_effectivization_time"):
        if cfg.get(k):
            cfg[k] = int(float(cfg[k]) * m)

AXES = [
    {"key": "tp_mult", "label": "TP ×", "values": [0.75, 1.25, 1.5], "base": 1.0,
     "apply": _apply_tp, "encode": lambda v: float(v)},
    {"key": "spread_mult", "label": "Spread ×", "values": [0.75, 1.5, 2.0], "base": 1.0,
     "apply": _apply_spread, "encode": lambda v: float(v)},
    {"key": "band", "label": "Inventory band", "values": [(0.3, 0.55), (0.35, 0.5), (0.4, 0.5)], "base": None,
     "apply": _apply_band, "encode": lambda v: round(v[1] - v[0], 3) if v else None},
    {"key": "eff_mult", "label": "Effectivization ×", "values": [0.5, 2.0], "base": 1.0,
     "apply": _apply_eff, "encode": lambda v: float(v)},
]


# --- pmm_king axes: min/max spread band, ladder depth, refresh, shift aggression ---

def _king_spread(cfg, m):
    cfg["min_spread"] = float(cfg["min_spread"]) * m
    cfg["max_spread"] = float(cfg["max_spread"]) * m

def _king_norders(cfg, n):
    cfg["n_orders_per_side"] = int(n)

def _king_refresh(cfg, m):
    cfg["executor_refresh_time"] = max(5, int(float(cfg["executor_refresh_time"]) * m))

def _king_shift(cfg, m):
    cfg["shift_intensity"] = float(cfg["shift_intensity"]) * m

PMM_KING_AXES = [
    {"key": "spread_mult", "label": "Spread ×", "values": [0.75, 1.5, 2.0], "base": 1.0,
     "apply": _king_spread, "encode": lambda v: float(v)},
    {"key": "n_orders", "label": "Orders/side", "values": [2, 4, 5], "base": 3,
     "apply": _king_norders, "encode": lambda v: float(v)},
    {"key": "refresh_mult", "label": "Refresh ×", "values": [0.5, 2.0], "base": 1.0,
     "apply": _king_refresh, "encode": lambda v: float(v)},
    {"key": "shift_mult", "label": "Shift ×", "values": [0.5, 2.0], "base": 1.0,
     "apply": _king_shift, "encode": lambda v: float(v)},
]

AXES_BY_CONTROLLER = {"pmm_mister": AXES, "pmm_king": PMM_KING_AXES}


def get_axes(base_cfg: dict) -> list[dict]:
    return AXES_BY_CONTROLLER.get(base_cfg.get("controller_name") or "", AXES)


def trial_id(params: dict) -> str:
    return json.dumps({k: params[k] for k in sorted(params)}, default=str)


def apply_params(base_cfg: dict, params: dict, axes: list | None = None) -> dict:
    cfg = json.loads(json.dumps(base_cfg, default=str))  # deep copy, str-safe
    for ax in (axes if axes is not None else get_axes(base_cfg)):
        v = params.get(ax["key"])
        if v is not None and v != ax["base"]:
            ax["apply"](cfg, v)
    return cfg


# ---------------------------------------------------------------------------
# Store (resumable)
# ---------------------------------------------------------------------------


def _sweep_key(base_cfg: dict, w0: int, w1: int, resolution: str) -> str:
    sig = json.dumps(base_cfg, sort_keys=True, default=str) + f"|{w0}|{w1}|{resolution}"
    return hashlib.sha1(sig.encode()).hexdigest()[:12]


def load_store() -> dict:
    try:
        return json.loads(SWEEP_STORE.read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def save_store(store: dict) -> None:
    SWEEP_STORE.parent.mkdir(parents=True, exist_ok=True)
    tmp = SWEEP_STORE.with_suffix(".tmp")
    tmp.write_text(json.dumps(store, default=str))
    tmp.replace(SWEEP_STORE)


# ---------------------------------------------------------------------------
# Backtest one trial
# ---------------------------------------------------------------------------


def _downsample(series: list, max_pts: int = 700) -> list:
    if len(series) <= max_pts:
        return series
    step = len(series) // max_pts + 1
    out = series[::step]
    if series and out[-1] != series[-1]:
        out.append(series[-1])
    return out


async def run_trial(client, base_cfg: dict, params: dict, w0: int, w1: int,
                    resolution: str, trade_cost: float, ref_price: float) -> dict:
    cfg = apply_params(base_cfg, params)
    c = dict(cfg)
    # Engine can't fetch trading rules → pre-convert tick spreads, disable tick_mode.
    # Ladder keys exist on pmm_mister-style configs only (pmm_king uses min/max_spread).
    if "buy_spreads" in cfg:
        mult = (TICK / ref_price) if cfg.get("tick_mode") else 1.0
        c["buy_spreads"] = [s * mult for s in parse_spreads(cfg["buy_spreads"])]
        c["sell_spreads"] = [s * mult for s in parse_spreads(cfg["sell_spreads"])]
        c["tick_mode"] = False
    c["id"] = "sweep_trial"

    last_err = None
    for attempt in range(3):
        try:
            bt = await asyncio.wait_for(client.backtesting.run(
                start_time=w0, end_time=w1, backtesting_resolution=resolution,
                trade_cost=trade_cost, config=c,
            ), timeout=300)
            break
        except Exception as e:
            last_err = e
            logger.warning("trial %s attempt %d failed: %r", params, attempt + 1, e)
            await asyncio.sleep(3 * (attempt + 1))
    else:
        return {"params": params, "error": f"{type(last_err).__name__}: {str(last_err)[:160]}"}

    res = bt.get("results") or {}
    # pmm_king reports total_volume=0 even with fills — fall back to summing
    # filled_amount_quote across the simulated executors.
    vol_res = float(res.get("total_volume") or 0.0)
    if vol_res <= 0:
        vol_res = sum(float(e.get("filled_amount_quote") or 0) for e in (bt.get("executors") or []))
    pts = bt.get("pnl_timeseries") or []
    pnl_curve = [(float(p["timestamp"]), float(p["total_pnl"])) for p in pts if isinstance(p, dict)]
    vol_curve = [(float(p["timestamp"]), float(p.get("cumulative_volume") or 0)) for p in pts if isinstance(p, dict)]
    # Max drawdown on the total-PnL curve.
    peak, maxdd = float("-inf"), 0.0
    for _, v in pnl_curve:
        peak = max(peak, v)
        maxdd = min(maxdd, v - peak)
    return {
        "params": params,
        "realized": float(res.get("net_pnl_quote") or 0.0),
        "unrealized": float(res.get("unrealized_pnl_quote") or 0.0),
        "volume": vol_res,
        "fills": int(res.get("total_executors_with_position") or 0),
        "n_executors": int(res.get("total_executors") or 0),
        "close_types": res.get("close_types") or {},
        "max_dd": maxdd,
        "pnl_curve": _downsample(pnl_curve),
        "vol_curve": _downsample(vol_curve),
    }


def score(t: dict, rebate_rate: float) -> dict:
    total = t["realized"] + t["unrealized"]
    return {
        "total_pnl": total,
        "rebates": t["volume"] * rebate_rate,
        "business_pnl": total + t["volume"] * rebate_rate,
    }


# ---------------------------------------------------------------------------
# Staged sweep: base + OAT, then mini-grid on the 2 highest-impact axes
# ---------------------------------------------------------------------------


async def run_sweep(client, base_cfg: dict, w0: int, w1: int, cfg: Config,
                    ref_price: float, log=lambda m: None) -> list[dict]:
    axes = get_axes(base_cfg)
    store = load_store()
    skey = _sweep_key(base_cfg, w0, w1, cfg.resolution)
    done: dict = store.setdefault(skey, {})

    async def _run(params: dict) -> Optional[dict]:
        tid = trial_id(params)
        if tid in done and "error" not in done[tid]:
            log(f"cached: {tid}")
            return done[tid]
        t = await run_trial(client, base_cfg, params, w0, w1, cfg.resolution, cfg.trade_cost, ref_price)
        done[tid] = t
        save_store(store)
        if "error" in t:
            log(f"FAILED {tid}: {t['error']}")
            return None
        s = score(t, cfg.rebate_rate)
        log(f"done {tid}: vol {t['volume']:,.0f} pnl {s['total_pnl']:+,.0f} biz {s['business_pnl']:+,.0f}")
        await asyncio.sleep(cfg.pause_s)
        return t

    n_run = 0
    trials: list[dict] = []

    # Stage 0: base
    base_t = await _run({})
    if base_t:
        trials.append(base_t)
    n_run += 1

    # Stage 1: one-at-a-time
    oat_by_axis: dict[str, list[dict]] = {}
    for ax in axes:
        for v in ax["values"]:
            if n_run >= cfg.max_trials:
                break
            t = await _run({ax["key"]: v})
            n_run += 1
            if t:
                trials.append(t)
                oat_by_axis.setdefault(ax["key"], []).append(t)

    # Stage 2: mini-grid on the two axes with largest business-PnL impact.
    if base_t:
        base_biz = score(base_t, cfg.rebate_rate)["business_pnl"]
        impact = {
            k: max(abs(score(t, cfg.rebate_rate)["business_pnl"] - base_biz) for t in ts)
            for k, ts in oat_by_axis.items() if ts
        }
        top2 = sorted(impact, key=impact.get, reverse=True)[:2]
        log(f"axis impact: { {k: round(v,1) for k,v in impact.items()} } → grid on {top2}")
        if len(top2) == 2:
            ax_a = next(a for a in axes if a["key"] == top2[0])
            ax_b = next(a for a in axes if a["key"] == top2[1])
            for va in [ax_a["base"]] + ax_a["values"]:
                for vb in [ax_b["base"]] + ax_b["values"]:
                    params = {}
                    if va is not None and va != ax_a["base"]:
                        params[ax_a["key"]] = va
                    if vb is not None and vb != ax_b["base"]:
                        params[ax_b["key"]] = vb
                    if len(params) < 2:
                        continue  # base/OAT already covered
                    if n_run >= cfg.max_trials:
                        break
                    t = await _run(params)
                    n_run += 1
                    if t:
                        trials.append(t)

    log(f"sweep complete: {len(trials)} ok / {n_run} attempted")
    return trials


# ---------------------------------------------------------------------------
# REAL overlay from an archived bot DB
# ---------------------------------------------------------------------------


async def fetch_real(client, db_path: str, candles: list[dict]) -> Optional[dict]:
    trades = []
    off = 0
    try:
        while True:
            r = await client.archived_bots.get_database_trades(db_path, limit=1000, offset=off)
            rows = r.get("trades") or r.get("data") or []
            if not rows:
                break
            trades.extend(rows)
            total = int((r.get("pagination") or {}).get("total") or 0)
            off += len(rows)
            if off >= total:
                break
    except Exception as e:
        logger.warning("real trades fetch failed for %s: %s", db_path, e)
        return None
    if not trades:
        return None
    evs = sorted(({"ts": float(t["timestamp"]) / 1000, "px": float(t["price"]),
                   "amt": float(t["amount"]), "buy": t.get("trade_type") == "BUY"} for t in trades),
                 key=lambda e: e["ts"])
    curve, cash, pos, ei = [], 0.0, 0.0, 0
    vol_curve, cum_vol = [], 0.0
    for c in candles:
        ct = float(c["timestamp"])
        while ei < len(evs) and evs[ei]["ts"] <= ct + 60:
            e = evs[ei]
            q = e["amt"] * e["px"]
            cum_vol += q
            cash += -q if e["buy"] else q
            pos += e["amt"] if e["buy"] else -e["amt"]
            ei += 1
        curve.append((ct, cash + pos * float(c["close"])))
        vol_curve.append((ct, cum_vol))
    return {"events": evs, "pnl_curve": curve, "vol_curve": vol_curve,
            "volume": cum_vol, "total_pnl": curve[-1][1] if curve else 0.0}


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def _dt(ts):
    return datetime.fromtimestamp(float(ts), tz=timezone.utc)


def _tlabel(t: dict, axes: list | None = None) -> str:
    p = t["params"]
    if not p:
        return "BASE"
    bits = []
    for ax in (axes if axes is not None else AXES):
        v = p.get(ax["key"])
        if v is None:
            continue
        if ax["key"] == "band":
            bits.append(f"band {v[0]}-{v[1]}")
        else:
            bits.append(f"{ax['label'].strip()}{v}".replace(" ", ""))
    return " ".join(bits)


def build_curves_figure(candles, real, top, base_t, pair, interval, axes=None):
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True, row_heights=[0.42, 0.28, 0.30], vertical_spacing=0.045,
        subplot_titles=(f"{pair} {interval} candles" + (" + REAL fills" if real else ""),
                        "Cumulative volume (BRL)", "PnL, mark-to-market (BRL)"))
    factor = max(1, len(candles) // 2000)
    disp = candles[::factor]
    fig.add_trace(go.Candlestick(
        x=[_dt(c["timestamp"]) for c in disp], open=[c["open"] for c in disp],
        high=[c["high"] for c in disp], low=[c["low"] for c in disp],
        close=[c["close"] for c in disp], name="Price", showlegend=False,
        increasing_line_color="#3fb950", decreasing_line_color="#f85149"), row=1, col=1)

    if real:
        buys = [e for e in real["events"] if e["buy"]]
        sells = [e for e in real["events"] if not e["buy"]]
        for evs_, sym, col, nm in ((buys, "triangle-up", "#3fb950", "REAL buys"),
                                   (sells, "triangle-down", "#f85149", "REAL sells")):
            if evs_:
                fig.add_trace(go.Scatter(
                    x=[_dt(e["ts"]) for e in evs_], y=[e["px"] for e in evs_], mode="markers",
                    name=f"{nm} ({len(evs_)})",
                    marker=dict(symbol=sym, size=8, color=col, line=dict(width=1, color="#fff"))), row=1, col=1)
        fig.add_trace(go.Scatter(x=[_dt(t) for t, v in real["vol_curve"]], y=[v for _, v in real["vol_curve"]],
                                 name="REAL", legendgroup="REAL", line=dict(width=2.4, color="#3fb950")), row=2, col=1)
        fig.add_trace(go.Scatter(x=[_dt(t) for t, v in real["pnl_curve"]], y=[v for _, v in real["pnl_curve"]],
                                 name="REAL", legendgroup="REAL", showlegend=False,
                                 line=dict(width=2.4, color="#3fb950")), row=3, col=1)

    series = []
    if base_t:
        series.append((base_t, "#8b949e", "BASE (backtest)"))
    for i, item in enumerate(top):
        # `top` entries may be (trial, label) tuples (pre-labeled by build_report)
        # or bare trials (legacy callers).
        if isinstance(item, tuple):
            t, nm = item
        else:
            t, nm = item, f"#{i+1} {_tlabel(item, axes)}"
        series.append((t, TOP_COLORS[i % len(TOP_COLORS)], nm))
    for t, col, nm in series:
        fig.add_trace(go.Scatter(x=[_dt(a) for a, b in t["vol_curve"]], y=[b for a, b in t["vol_curve"]],
                                 name=nm, legendgroup=nm, line=dict(width=1.5, color=col, dash="dash")), row=2, col=1)
        fig.add_trace(go.Scatter(x=[_dt(a) for a, b in t["pnl_curve"]], y=[b for a, b in t["pnl_curve"]],
                                 name=nm, legendgroup=nm, showlegend=False,
                                 line=dict(width=1.5, color=col, dash="dash")), row=3, col=1)
    fig.add_hline(y=0, line_dash="dot", line_color="white", opacity=0.3, row=3, col=1)
    fig.update_layout(**DARK, margin=dict(l=55, r=25, t=60, b=30), height=1050,
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=9)))
    fig.update_xaxes(rangeslider_visible=False, gridcolor="#21262d")
    fig.update_yaxes(gridcolor="#21262d")
    for ann in fig.layout.annotations:
        ann.font = dict(size=12, color="#c9d1d9")
    return fig


def build_parcoords(trials: list[dict], rebate_rate: float, axes: list | None = None,
                    highlight: dict | None = None):
    """Hoverable parallel coordinates: one polyline per trial across the parameter
    axes plus Volume / Total PnL / Business PnL. go.Parcoords cannot label lines,
    so this is built from Scatter traces — hover names the variant on every axis;
    `highlight` = {trial_id: (label, color)} for the ranked models (others gray)."""
    import plotly.graph_objects as go

    axes = axes if axes is not None else AXES
    highlight = highlight or {}

    # Collect raw values per dimension.
    dims: list[tuple[str, list[float]]] = []
    for ax in axes:
        vals = []
        for t in trials:
            v = t["params"].get(ax["key"])
            enc = ax["encode"](v) if v is not None else (
                ax["encode"](ax["base"]) if ax["base"] is not None else None)
            vals.append(0.4 if enc is None else enc)  # band-base fallback (0.2–0.6 width)
        dims.append((ax["label"], vals))
    dims.append(("Volume (K)", [t["volume"] / 1000 for t in trials]))
    dims.append(("Total PnL", [score(t, rebate_rate)["total_pnl"] for t in trials]))
    dims.append(("Business PnL", [score(t, rebate_rate)["business_pnl"] for t in trials]))

    # Normalize each dimension to [0,1] for a shared y scale.
    lo_hi = []
    for name, vals in dims:
        lo, hi = min(vals), max(vals)
        lo_hi.append((lo, hi if hi > lo else lo + 1))
    xs = list(range(len(dims)))

    fig = go.Figure()
    order = sorted(range(len(trials)), key=lambda i: trial_id(trials[i]["params"]) in highlight)
    for i in order:
        t = trials[i]
        tid = trial_id(t["params"])
        label = _tlabel(t, axes) or "BASE"
        hl = highlight.get(tid)
        y = [(dims[d][1][i] - lo_hi[d][0]) / (lo_hi[d][1] - lo_hi[d][0]) for d in xs]
        hover = "<br>".join(f"{dims[d][0]}: {dims[d][1][i]:,.4g}" for d in xs)
        fig.add_trace(go.Scatter(
            x=xs, y=y, mode="lines+markers",
            name=(hl[0] if hl else label),
            showlegend=bool(hl),
            line=dict(width=2.4 if hl else 1.0,
                      color=hl[1] if hl else "rgba(139,148,158,0.35)"),
            marker=dict(size=5 if hl else 3),
            hovertemplate=f"<b>{hl[0] if hl else label}</b><br>{hover}<extra></extra>",
        ))
    # Axis min/max annotations so the normalized scale stays readable.
    for d in xs:
        fig.add_annotation(x=d, y=-0.06, text=f"{lo_hi[d][0]:,.4g}", showarrow=False,
                           font=dict(size=9, color="#8b949e"), yref="y")
        fig.add_annotation(x=d, y=1.06, text=f"{lo_hi[d][1]:,.4g}", showarrow=False,
                           font=dict(size=9, color="#8b949e"), yref="y")
    fig.update_layout(**DARK, title=f"All {len(trials)} trials — parameters vs outcome (hover names each line)",
                      margin=dict(l=40, r=40, t=80, b=60), height=520,
                      legend=dict(orientation="h", yanchor="bottom", y=-0.22, xanchor="center", x=0.5, font=dict(size=9)),
                      xaxis=dict(tickmode="array", tickvals=xs, ticktext=[n for n, _ in dims],
                                 tickfont=dict(size=11, color="#c9d1d9"), showgrid=True, gridcolor="#21262d"),
                      yaxis=dict(visible=False, range=[-0.12, 1.12]))
    return fig


def _fmt_axis_value(ax: dict, v) -> str:
    """Human-readable value for one swept axis (band → range, multiplier → ×N)."""
    if v is None:
        v = ax.get("base")
    if isinstance(v, (list, tuple)) and len(v) == 2:
        return f"{v[0]:.2f}–{v[1]:.2f}"
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"×{v:g}" if ax.get("base") == 1.0 else f"{v:g}"
    return str(v)


def build_param_table(models: list, axes: list):
    """Heatmap comparing each candidate model's swept-parameter values + outcomes —
    mirrors controller_performance's per-type parameter heatmap. Rows = models,
    cols = each axis + Business PnL + Volume; cell color = column-normalized value
    (green = highest in its column), cell text = the actual value."""
    import numpy as np
    import plotly.graph_objects as go
    if not models:
        return None
    cols = [ax["label"] for ax in axes] + ["Business PnL", "Volume"]
    ylabels, matrix, text = [], [], []
    for t, label in models:
        ylabels.append(label or "BASE")
        row, trow = [], []
        for ax in axes:
            v = t["params"].get(ax["key"], ax.get("base"))
            try:
                enc = ax["encode"](v) if v is not None else None
            except Exception:
                enc = None
            row.append(float(enc) if enc is not None else float("nan"))
            trow.append(_fmt_axis_value(ax, v))
        s = t.get("_score") or {}
        biz = float(s.get("business_pnl", 0.0)); vol = float(t.get("volume", 0.0))
        row += [biz, vol]; trow += [f"{biz:+,.0f}", f"{vol:,.0f}"]
        matrix.append(row); text.append(trow)
    arr = np.array(matrix, dtype=float)
    norm = np.full_like(arr, 0.5)
    for j in range(arr.shape[1]):
        col = arr[:, j]; fin = col[np.isfinite(col)]
        if fin.size:
            lo, hi = fin.min(), fin.max(); rng = (hi - lo) or 1.0
            norm[:, j] = np.where(np.isfinite(col), (col - lo) / rng, 0.5)
    fig = go.Figure(go.Heatmap(
        z=norm, x=cols, y=ylabels, text=text, texttemplate="%{text}",
        textfont=dict(size=10, color="#0d1117"), xgap=1, ygap=1,
        colorscale=[[0, "#161b22"], [0.5, "#2f81f7"], [1, "#3fb950"]],
        showscale=False, hovertemplate="%{y}<br>%{x}: %{text}<extra></extra>"))
    fig.update_layout(**DARK, title="Parameters by Model — candidates side-by-side",
                      margin=dict(l=220, r=30, t=60, b=40),
                      height=max(220, len(ylabels) * 34 + 90))
    fig.update_yaxes(autorange="reversed")  # best (first) row on top
    return fig


async def build_report(cfg: Config, base_cfg: dict, trials: list[dict], candles: list[dict],
                       real: Optional[dict], w0: int, w1: int) -> tuple[str, list[dict], list[str]]:
    from condor.reports import ReportBuilder

    pair = (base_cfg.get("trading_pair") or "?").upper()
    axes = get_axes(base_cfg)
    ok = [t for t in trials if "error" not in t]
    for t in ok:
        t["_score"] = score(t, cfg.rebate_rate)
    base_t = next((t for t in ok if not t["params"]), None)
    by_biz = sorted(ok, key=lambda t: -t["_score"]["business_pnl"])[:5]
    by_vol = sorted(ok, key=lambda t: -t["volume"])[:5]
    by_pnl = sorted(ok, key=lambda t: -t["_score"]["total_pnl"])[:5]

    def rank_rows(ranked):
        rows = []
        for i, t in enumerate(ranked, 1):
            s = t["_score"]
            rows.append({
                "#": str(i), "Variant": _tlabel(t, axes),
                "Business PnL": f"{s['business_pnl']:+,.1f}",
                "Total PnL": f"{s['total_pnl']:+,.1f}",
                "Realized": f"{t['realized']:+,.1f}",
                "Volume": f"{t['volume']:,.0f}",
                "Rebates": f"{s['rebates']:+,.1f}",
                "MaxDD": f"{t['max_dd']:,.1f}",
                "Fills": str(t["fills"]),
            })
        return rows
    cols = ["#", "Variant", "Business PnL", "Total PnL", "Realized", "Volume", "Rebates", "MaxDD", "Fills"]

    w_str = f"{_dt(w0).strftime('%b %d %H:%M')} → {_dt(w1).strftime('%b %d %H:%M')} UTC"
    b = ReportBuilder(f"PMM Sweep — {base_cfg.get('id', pair)} ({len(ok)} trials)")
    b.source("routine", "pmm_sweep").tags(["pmm_mister", "sweep", "backtest", pair])
    b.manual_order()
    if base_t:
        b.kpi("BASE business PnL", f"{base_t['_score']['business_pnl']:+,.0f}",
              trend="up" if base_t['_score']['business_pnl'] >= 0 else "down")
    if by_biz:
        best = by_biz[0]
        b.kpi("Best variant", _tlabel(best, axes)[:28] or "BASE")
        b.kpi("Best business PnL", f"{best['_score']['business_pnl']:+,.0f}",
              trend="up" if best['_score']['business_pnl'] >= 0 else "down")
    if real:
        b.kpi("REAL PnL (this window)", f"{real['total_pnl']:+,.0f}", trend="down" if real['total_pnl'] < 0 else "up")
    b.kpi("Trials", str(len(ok)))
    b.markdown(
        f"## Sweep — `{base_cfg.get('id','base')}` on {pair}, {w_str}\n"
        f"_Staged sweep: base + one-at-a-time on {len(AXES)} axes, then a mini-grid on the two "
        f"highest-impact axes. All backtests: Hummingbot engine, {cfg.resolution} resolution, "
        f"trade_cost {cfg.trade_cost}, tick spreads pre-converted. **Business PnL = total PnL + "
        f"volume × {cfg.rebate_rate}** (rebates). Backtest realized PnL is queue-blind and "
        f"optimistic — volume is trustworthy to ~±25%, realized profit is an upper bound; "
        f"rankings weight accordingly._")
    b.markdown("### 🏆 Top 5 — Business PnL (primary: PnL + rebates)")
    b.table(rank_rows(by_biz), columns=cols)
    b.markdown("### 📊 Top 5 — Volume")
    b.table(rank_rows(by_vol), columns=cols)
    b.markdown("### 💰 Top 5 — Total PnL")
    b.table(rank_rows(by_pnl), columns=cols)

    # Curve set: BASE + top-3 by Total PnL + top-3 by Volume (deduped), each
    # labeled with its rank badges (P# = PnL rank, V# = volume rank).
    def _badges(t):
        bits = []
        for i, x in enumerate(by_pnl[:3]):
            if x is t: bits.append(f"P{i+1}")
        for i, x in enumerate(by_vol[:3]):
            if x is t: bits.append(f"V{i+1}")
        return "·".join(bits)
    curve_set, seen_ids = [], set()
    for t in by_pnl[:3] + by_vol[:3]:
        tid = trial_id(t["params"])
        if tid in seen_ids or (base_t and t is base_t):
            continue
        seen_ids.add(tid)
        curve_set.append((t, f"{_badges(t)} {_tlabel(t, axes) or 'BASE'}"))
    fig_curves = build_curves_figure(candles, real, curve_set, base_t, pair, cfg.candle_interval, axes=axes)
    b.markdown("### Curves — REAL + BASE backtest + best-PnL (P#) + best-volume (V#) models, one shared X axis\n"
               "_REAL (solid green) vs the BASE config's backtest (gray dashed) is the "
               "reality-calibration reference; P#/V# variants are the candidates. Legend "
               "toggles a variant's volume + PnL together._")
    b.plotly(fig_curves)
    highlight = {}
    if base_t:
        highlight[trial_id(base_t["params"])] = ("BASE", "#ffffff")
    for i, (t, lbl) in enumerate(curve_set):
        highlight[trial_id(t["params"])] = (lbl, TOP_COLORS[i % len(TOP_COLORS)])
    fig_pc = build_parcoords(ok, cfg.rebate_rate, axes=axes, highlight=highlight)
    b.markdown("### All trials — parallel coordinates\n"
               "_Each line is one trial crossing its parameter values; drag along any axis to "
               "filter. Color = business PnL._")
    b.plotly(fig_pc)

    # ── Parameters by Model + copy-to-deploy configs ──
    # Candidate set = BASE + top-5 business PnL + top-3 volume (deduped).
    cand, seen = [], set()
    if base_t:
        cand.append((base_t, "BASE")); seen.add(trial_id(base_t["params"]))
    for t in by_biz[:5] + by_vol[:3]:
        tid = trial_id(t["params"])
        if tid in seen:
            continue
        seen.add(tid); cand.append((t, _tlabel(t, axes) or "BASE"))
    fig_pt = build_param_table(cand, axes)
    if fig_pt is not None:
        b.markdown("### Parameters by Model\n_Each candidate's swept-parameter values side by "
                   "side (like the controller-performance heatmap), with Business PnL & Volume. "
                   "Green = highest in its column._")
        b.plotly(fig_pt)

    # Copy-to-deploy: each candidate's EXACT full controller config, ready to paste into
    # Condor's deploy flow. apply_params() reconstructs the complete config (base + this
    # variant's overrides); `id` is uniquified so variants don't collide on deploy.
    import yaml as _yaml, re as _re
    b.markdown("### Deploy configs — copy an exact backtested config\n"
               "_Select-all inside a block and copy. Each is a **complete, runnable** controller "
               "config (base + this variant's swept params) with a unique `id` — paste it straight "
               "into your deploy flow, no editing needed._")
    base_id = str(base_cfg.get("id") or base_cfg.get("controller_name") or "cfg")
    for t, label in cand:
        full = apply_params(base_cfg, t["params"])
        slug = _re.sub(r"[^a-z0-9]+", "_", (label or "base").lower()).strip("_")[:36] or "base"
        full["id"] = base_id if label == "BASE" else f"{base_id}__{slug}"
        s = t.get("_score") or {}
        ytext = _yaml.safe_dump(full, sort_keys=False, default_flow_style=False, allow_unicode=True)
        b.markdown(f"**{label}** — `id: {full['id']}` · biz {s.get('business_pnl', 0.0):+,.0f} · "
                   f"vol {t.get('volume', 0.0):,.0f}\n```yaml\n{ytext}```")

    failed = [t for t in trials if "error" in t]
    if failed:
        b.markdown("_⚠ Failed trials: " + ", ".join(trial_id(t["params"]) for t in failed) + "_")
    b.markdown(
        "### Caveats\n"
        "- Single-window ranking: winners are in-sample; re-validate on a second window before "
        "trusting order (Phase 4).\n"
        "- The engine fills TPs on any candle cross (no queue) — tight-TP variants get flattered.\n"
        "- Totals are mark-to-market at window end; check MaxDD, not just the endpoint.\n"
        "- Nothing here auto-deploys; winners need a live validation run.")
    await b.save()

    lines = [f"*PMM Sweep* — {base_cfg.get('id', pair)}, {len(ok)} trials, {w_str}"]
    for i, t in enumerate(by_biz, 1):
        s = t["_score"]
        lines.append(f"{i}. `{_tlabel(t, axes) or 'BASE'}` biz {s['business_pnl']:+,.0f} "
                     f"(pnl {s['total_pnl']:+,.0f}, vol {t['volume']:,.0f})")
    return "\n".join(lines), rank_rows(by_biz), cols


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _parse_ts(s: str) -> Optional[int]:
    s = s.strip()
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        pass
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> RoutineResult:
    chat_id = getattr(context, "_chat_id", None)
    client = await get_client(chat_id, context=context)
    if not client:
        return RoutineResult(text="No server available. Configure servers in /config.")

    if config.config_text.strip():
        base_cfg = parse_config_text(config.config_text)
    elif config.config_name.strip():
        raw = await client.controllers.get_controller_config(config.config_name.strip())
        base_cfg = raw.get("config") if isinstance(raw.get("config"), dict) else raw
    else:
        return RoutineResult(text="Provide config_text or config_name.")

    w1 = _parse_ts(config.window_end) or int(time.time()) - 300
    w0 = _parse_ts(config.window_start) or (w1 - config.days * 86400)

    pair = (base_cfg.get("trading_pair") or "").upper()
    connector = base_cfg.get("connector_name") or "binance"
    days_back = max(1, int((time.time() - w0) / 86400) + 1)
    candles_all = _extract_rows(await client.market_data.get_candles_last_days(
        connector, pair, days_back, config.candle_interval))
    candles = [c for c in candles_all if w0 - 60 <= float(c["timestamp"]) <= w1 + 60]
    if len(candles) < 50:
        return RoutineResult(text=f"Not enough candles for the window ({len(candles)}) — window too old for {config.candle_interval} history?")
    ref_price = sum(float(c["close"]) for c in candles) / len(candles)

    def _log(m):
        logger.info("pmm_sweep: %s", m)

    trials = await run_sweep(client, base_cfg, w0, w1, config, ref_price, log=_log)
    real = await fetch_real(client, config.real_db_path, candles) if config.real_db_path else None
    text, rows, cols = await build_report(config, base_cfg, trials, candles, real, w0, w1)
    return RoutineResult(text=text, table_data=rows, table_columns=cols)
