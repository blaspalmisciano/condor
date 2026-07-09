"""Offline iteration harness for controller_performance — NO NETWORK.

Loads the frozen API fixture (tests/fixtures/controller_perf_fixture.json, captured
by capture_controller_perf_fixture.py) plus the local VDA snapshot store, then runs
the routine's pure logic (summary table + every chart + the ReportBuilder) exactly
as run() would. Writes a real HTML report you can open.

    python3 tests/offline_controller_perf.py

Edit chart/table code in routines/controller_performance.py, re-run this, refresh
the HTML. No brigado, no internet, no bots touched.
"""

import importlib
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import routines.controller_performance as cp  # noqa: E402

FIXTURE = ROOT / "tests" / "fixtures" / "controller_perf_fixture.json"


def main():
    importlib.reload(cp)  # pick up edits without restarting anything
    if not FIXTURE.exists():
        sys.exit(f"No fixture at {FIXTURE}.\nRun (while online): python3 tests/capture_controller_perf_fixture.py")

    fx = json.loads(FIXTURE.read_text())
    config = cp.Config()  # tweak fields here to test config combinations offline

    live = fx["live"]
    configs = fx["configs"]
    events = fx["events"]
    drops = fx.get("drops", {})
    skipped = fx.get("skipped", [])
    candle_pair = fx.get("candle_pair", "BTC-BRL")
    candles = fx.get("candles", [])
    rates = fx.get("rates", {})
    verify = fx.get("verify", {})
    bot_health = fx.get("bot_health", {})
    cid_bot = fx.get("cid_bot", {})
    capture_ts = fx.get("capture_ts")
    active_ids = set(live)
    cur = (config.quote_display.strip().upper() or "USDT")

    type_lookup, pair_lookup = {}, {}
    for cid, cfg in configs.items():
        type_lookup[cid] = cfg.get("controller_name") or cp.infer_type(cid, None)
        pair_lookup[cid] = (cfg.get("trading_pair") or cp.infer_pair(cid, None)).upper()

    # Apply the same USDT conversion run() does, using the captured rates.
    for e in events:
        r = rates.get(cp._quote_of(e["pair"]), 1.0)
        e["volume"] *= r; e["pnl"] *= r; e["fees"] = e.get("fees", 0.0) * r
    for cid, lv in live.items():
        r = rates.get(cp._quote_of(pair_lookup.get(cid, "") or cp.infer_pair(cid, None)), 1.0)
        for k in ("volume_traded", "global_pnl_quote", "realized", "unrealized"):
            if k in lv:
                lv[k] = float(lv[k]) * r

    # Per-controller curves from the LOCAL snapshot store; also drives stall detection.
    cutoff = time.time() - config.lookback_days * 86400
    snap_series = cp.load_snapshot_series(config.snapshot_state_file, cutoff) if config.snapshot_state_file else {}
    for cid, info in snap_series.items():  # convert curves to USDT, as run() does
        r = rates.get(cp._quote_of(pair_lookup.get(cid, "") or cp.infer_pair(cid, None)), 1.0)
        if r != 1.0:
            info["snaps"] = [(ts, v * r, p * r) for (ts, v, p) in info["snaps"]]
    stall_status, store_latest = cp.compute_stall_status(
        snap_series, config.stall_hours, bot_health, cid_bot, now=capture_ts)
    stalled_ids = {cid for cid, s in stall_status.items() if s["stalled"] and cid in active_ids}
    print("STALLED/HUNG:", {c: (f"{stall_status[c].get('log_silent_hours') or stall_status[c]['idle_hours']:.0f}h"
                                 + (" hung" if stall_status[c].get("hung") else ""))
                            for c in stalled_ids} or "none")

    summary_rows, perf_by_ctrl = cp.build_summary(
        events, live, type_lookup, pair_lookup, config.rebate_rate, active_ids, stall_status
    )
    # Fixture/store may be older than the window — widen cutoff if nothing matched.
    if config.snapshot_state_file and not snap_series:
        snap_series = cp.load_snapshot_series(config.snapshot_state_file, 0)
        if snap_series:
            print("note: snapshot store is older than lookback window; using full history for offline test")

    if snap_series:
        ts_all = cp.build_ts_from_snapshots(snap_series, config.bucket_hours)
        ts_source = f"live snapshots ({len(snap_series)} controllers)"
    else:
        ts_all = cp.build_timeseries(events, config.bucket_hours)
        ts_source = "closed executors"
    candle_cids = {c for c in ts_all["by_ctrl"]
                   if (pair_lookup.get(c) or cp.infer_pair(c, None)) == candle_pair}

    print(f"controllers={len(summary_rows)} active={len(active_ids)} events={len(events)} "
          f"ts_source={ts_source} candle_cids={len(candle_cids)} candles={len(candles)}")

    from condor.reports import ReportBuilder
    builder = ReportBuilder(f"Controller Performance OFFLINE ({config.lookback_days}d)")
    builder.source("routine", "controller_performance_offline").tags(["offline", "test"])
    builder.manual_order()

    total_vol = sum(e["volume"] for e in events)
    total_pnl = sum(e["pnl"] for e in events)
    total_reb = total_vol * config.rebate_rate
    builder.kpi("Controllers", str(len(summary_rows)))
    builder.kpi("Trading now", f"{len(active_ids) - len(stalled_ids)}/{len(active_ids)}",
                trend="down" if stalled_ids else "neutral")
    if stalled_ids:
        builder.kpi("🔴 Stalled", str(len(stalled_ids)), trend="down")
    builder.kpi(f"Volume ({cur})", cp._fmt(total_vol))
    builder.kpi(f"Realized PnL + Rebates ({cur})", cp._fmt(total_pnl + total_reb))

    if stalled_ids:
        from datetime import datetime, timezone
        det = []
        for cid in sorted(stalled_ids, key=lambda c: -(stall_status[c].get("log_silent_hours") or stall_status[c]["idle_hours"])):
            s = stall_status[cid]
            bot = cid_bot.get(cid, "") or (snap_series.get(cid) or {}).get("bot", "")
            silent = s.get("log_silent_hours")
            if s.get("hung") and silent is not None:
                reason = f"container **running** but **no log activity for {silent:.0f}h** (strategy loop frozen)"
            else:
                last = datetime.fromtimestamp(s["last_move_ts"], tz=timezone.utc).strftime("%b %d %H:%M")
                reason = f"cumulative volume idle **{s['idle_hours']:.0f}h** (last moved {last})"
            line = f"- **`{cid}`** — {reason}" + (f"  _(bot `{bot}`)_" if bot else "")
            err = (bot_health.get(bot) or {}).get("last_error")
            if s.get("hung") and err:
                line += f"\n    last error: `{err}`"
            v = verify.get(bot)
            if v is not None:
                if v["traded"] is False:
                    line += "  → ✅ **primary source confirms halt** (0 trades since)"
                elif v["traded"]:
                    line += (f"  → ⚠ trade table shows **{v['n_trades']} fills / "
                             f"{cp._fmt(v['volume_usdt'])} {cur}** since — recording lag, not halted")
            det.append(line)
        builder.markdown(
            f"## 🔴 {len(stalled_ids)} active controller(s) STALLED / HUNG\n"
            f"_Flagged on the bot's **container log heartbeat**: container 'running' (so "
            f"orchestration & VDA report alive) but no strategy logs for ≥{config.stall_hours:.0f}h. "
            f"Cross-checked against the live trade table (primary source):_\n\n" + "\n".join(det)
        )

    builder.markdown(f"### Per-Controller Summary (offline fixture + frozen snapshots)")
    builder.table(summary_rows, columns=cp.SUMMARY_COLUMNS)

    fig_bars = cp.chart_controller_bars(perf_by_ctrl, type_lookup, active_ids)
    if fig_bars is not None:
        builder.markdown("### All Controllers — PnL & Volume")
        builder.plotly(fig_bars)

    fig_candle = cp.chart_candles_pnl(candles, ts_all, candle_cids, candle_pair, config.rebate_rate)
    if fig_candle is not None:
        builder.markdown(f"### Price & PnL — {candle_pair} (from {ts_source})")
        builder.plotly(fig_candle)

    fig_share = cp.chart_volume_share(ts_all, config.bucket_hours)
    if fig_share is not None:
        builder.markdown("### Volume Share Over Time")
        builder.plotly(fig_share)

    fig_cumvol = cp.chart_cumulative_volume(ts_all)
    if fig_cumvol is not None:
        builder.markdown("### Cumulative Volume per Controller")
        builder.plotly(fig_cumvol)

    configs_by_type = defaultdict(dict)
    event_cids = {e["controller_id"] for e in events}
    for cid, cfg in configs.items():
        if cid in active_ids or cid in event_cids:
            configs_by_type[cfg.get("controller_name") or cp.infer_type(cid, None)][cid] = cfg
    if configs_by_type:
        builder.markdown("## Parameters by Controller Type")
        for ctype in sorted(configs_by_type):
            fig_p = cp.chart_params_for_type(ctype, configs_by_type[ctype], perf_by_ctrl, config.rebate_rate)
            if fig_p is not None:
                builder.plotly(fig_p)

    path = builder.save()
    print(f"\nwrote report: {path}")
    print("open it with:  open " + str(path))


if __name__ == "__main__":
    main()
