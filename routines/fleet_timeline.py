"""Fleet timeline: every controller that ran at any point on a shared timeline — y-axis = each
controller (grouped by cohort), x-axis = hours since the fleet started, colour = cumulative PnL,
blanks = when the controller was NOT live. Makes substitution churn legible: a slot's old config
ends its band and the new config starts a new row. Data: the fleet manager's per-cycle snapshots.
"""
CATEGORY = "Bot Analysis"

import io
import json
import logging
import os
import time

import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pydantic import BaseModel, Field
from telegram.ext import ContextTypes

from routines.base import RoutineResult

logger = logging.getLogger(__name__)

DARK = dict(paper_bgcolor="#0d1117", plot_bgcolor="#161b22", font=dict(color="#c9d1d9"))


class Config(BaseModel):
    """Timeline of every BTC-BRL fleet controller (active + substituted-out) since the fleet
    started: controllers on the y-axis, hours-since-start on the x-axis, cumulative PnL as colour,
    gaps where a controller was not live. Built from the fleet manager's snapshot store."""

    snapshot_file: str = Field(
        default="data/btcbrl_fleet_yield_snapshots.json",
        description="Fleet-manager snapshot store: JSON list of {ts, cum:{controller_id: cum_pnl_brl}}.",
    )
    lookback_hours: float = Field(
        default=0.0, description="Only show the last N hours (0 = since the first snapshot = fleet start)."
    )
    send_photo: bool = Field(default=True, description="Also send the chart as a Telegram photo.")


def _cohort(cid: str) -> str:
    s = cid.rsplit("-", 1)[-1].lower()
    return s if s in ("vol", "pnl") else "other"


def _sort_key(cid: str):
    order = {"vol": 0, "pnl": 1, "other": 2}
    import re
    m = re.search(r"(\d+)", cid)
    return (order.get(_cohort(cid), 2), int(m.group(1)) if m else 0, cid)


def _load(path: str) -> list[dict]:
    with open(path) as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> RoutineResult:
    if not os.path.exists(config.snapshot_file):
        return RoutineResult(text=f"No snapshot store at {config.snapshot_file} yet — the fleet manager writes it each cycle.")
    snaps = _load(config.snapshot_file)
    snaps = [s for s in snaps if isinstance(s, dict) and s.get("cum")]
    snaps.sort(key=lambda s: s.get("ts", 0))
    if len(snaps) < 2:
        return RoutineResult(text=f"Only {len(snaps)} snapshot(s) so far — need ≥2 to draw a timeline. Check back after a few cycles.")

    if config.lookback_hours and config.lookback_hours > 0:
        cut = time.time() - config.lookback_hours * 3600
        kept = [s for s in snaps if s["ts"] >= cut]
        if len(kept) >= 2:
            snaps = kept

    t0 = snaps[0]["ts"]
    xs = [(s["ts"] - t0) / 3600.0 for s in snaps]  # hours since fleet start
    span_h = xs[-1]

    # every controller that appeared at any point, grouped by cohort
    all_ids = sorted({cid for s in snaps for cid in s["cum"]}, key=_sort_key)

    # z-matrix: cum pnl where present, None (gap) where absent
    z = [[(s["cum"].get(cid) if cid in s["cum"] else None) for s in snaps] for cid in all_ids]

    # fleet total per snapshot (sum of present controllers)
    totals = [sum(v for v in s["cum"].values()) for s in snaps]

    # symmetric diverging range centred on 0
    flat = [v for row in z for v in row if v is not None]
    amax = max((abs(v) for v in flat), default=1.0) or 1.0

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, row_heights=[0.24, 0.76], vertical_spacing=0.05,
        subplot_titles=("Fleet total cumulative PnL (BRL)", "Per-controller cumulative PnL — blank = not live"),
    )
    fig.add_trace(
        go.Scatter(x=xs, y=totals, mode="lines+markers", line=dict(color="#58a6ff", width=2),
                   name="fleet total", hovertemplate="%{x:.1f}h · %{y:+,.0f} BRL<extra></extra>"),
        row=1, col=1,
    )
    fig.add_hline(y=0, line_dash="dot", line_color="white", opacity=0.3, row=1, col=1)

    fig.add_trace(
        go.Heatmap(
            z=z, x=xs, y=all_ids, zmid=0, zmin=-amax, zmax=amax,
            colorscale="RdYlGn", hoverongaps=False, xgap=1, ygap=1,
            colorbar=dict(title="cum PnL", len=0.72, y=0.38),
            hovertemplate="%{y} · %{x:.1f}h · %{z:+,.1f} BRL<extra></extra>",
        ),
        row=2, col=1,
    )

    # cohort separator line between vol and pnl groups
    cohorts = [_cohort(c) for c in all_ids]
    if "vol" in cohorts and "pnl" in cohorts:
        boundary = cohorts.index("pnl") - 0.5
        fig.add_hline(y=boundary, line_dash="dash", line_color="#8b949e", opacity=0.6, row=2, col=1)

    fig.update_layout(**DARK, height=max(420, 60 + 26 * len(all_ids)), showlegend=False,
                      margin=dict(l=160, r=20, t=50, b=50),
                      title_text=f"BTC-BRL fleet timeline — {len(all_ids)} controllers over {span_h:.1f}h")
    fig.update_xaxes(title_text="hours since fleet start", gridcolor="#21262d", row=2, col=1)
    fig.update_yaxes(gridcolor="#21262d", row=1, col=1)
    fig.update_yaxes(autorange="reversed", row=2, col=1)  # first controller on top

    chart_bytes = None
    try:
        buf = io.BytesIO()
        fig.write_image(buf, format="png", scale=2)
        chart_bytes = buf.getvalue()
        if config.send_photo and getattr(context, "bot", None) and getattr(context, "_chat_id", None):
            buf.seek(0)
            await context.bot.send_photo(chat_id=context._chat_id, photo=buf, caption="BTC-BRL fleet timeline")
    except Exception as e:
        logger.warning("fleet_timeline chart render failed: %s", e)

    # churn + summary
    first_ids, last_ids = set(snaps[0]["cum"]), set(snaps[-1]["cum"])
    gone = first_ids - last_ids
    added = last_ids - first_ids
    latest = snaps[-1]["cum"]
    rows = sorted(((cid, latest.get(cid)) for cid in all_ids if cid in latest),
                  key=lambda r: (r[1] if r[1] is not None else 0))
    vol_tot = sum(v for c, v in latest.items() if _cohort(c) == "vol")
    pnl_tot = sum(v for c, v in latest.items() if _cohort(c) == "pnl")
    lines = [
        f"BTC-BRL fleet timeline — {len(all_ids)} distinct controllers over {span_h:.1f}h ({len(snaps)} snapshots).",
        f"Latest cum PnL: total {sum(latest.values()):+,.0f} BRL  (vol cohort {vol_tot:+,.0f} · pnl cohort {pnl_tot:+,.0f}).",
        f"Churn: {len(added)} added, {len(gone)} substituted-out since start." if (added or gone) else "Churn: none yet — all original controllers still live.",
        "Worst: " + ", ".join(f"{c}={v:+.0f}" for c, v in rows[:3]),
        "Best:  " + ", ".join(f"{c}={v:+.0f}" for c, v in rows[-3:][::-1]),
    ]
    return RoutineResult(text="\n".join(lines), chart_image=chart_bytes)
