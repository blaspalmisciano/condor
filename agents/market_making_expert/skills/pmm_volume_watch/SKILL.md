---
name: pmm_volume_watch
description: Detect a stalled or halted market-making bot from a trade-volume drop —
  snapshots each controller's cumulative volume and alerts when the latest interval
  collapses versus its running median.
when_to_use: To detect a stalled or halted bot from a trade-volume drop — as a standing
  watch on live pmm_mister controllers, or when the user asks "is the bot still trading"
  and you want an early stall signal.
created: '2026-08-28T00:00:00Z'
source: agent:market_making_expert
references_routine: volume_drop_alert
---

# PMM Volume Watch — stall / halt detection

Run the `volume_drop_alert` (VDA) routine. Each run snapshots every controller's
**cumulative** traded volume, buckets it into intervals, and alerts when the most
recent interval's volume falls to `threshold_pct` (default 20%) of that controller's
running median — after a warm-up of a few intervals. That collapse is the signature
of a bot that has stalled or halted while still showing as "running".

## ⚠️ KNOWN GOTCHA — VDA's "24h vol" is a lifetime average, NOT real 24h

The volume/pace figure VDA prints (labelled like "24h vol" / lifetime pace) is a
**lifetime average** — cumulative volume divided by the controller's age — not the
volume traded in the trailing 24 hours. A bot that traded hard early and then
**halted hours ago** still shows a healthy-looking lifetime average and can read as
fine here. **Never declare a bot healthy on VDA's volume number alone.**

**Always cross-check with `controller_performance`'s trade table** (the
`mm_bot_report` skill) before calling a bot healthy: that trade table shows actual
fills and the last-trade timestamp, which is the ground truth for "is it trading
*now*". Treat VDA as the early-warning trigger and `controller_performance` as the
confirmation.

## How to drive it — one-shot, run it repeatedly

`volume_drop_alert` is a **one-shot** routine (not a continuous start/stop routine).
It needs a *series* of snapshots to compute a median and detect a drop, so a single
`action="run"` on its own only records the first snapshot. Drive it one of two ways:

- **Per tick (Autopilot):** call it every tick and let its persisted state file
  accumulate the snapshot history:
  ```python
  manage_routines(action="run", name="volume_drop_alert",
                  config={"threshold_pct": 20, "expected_interval_minutes": 15})
  ```
- **Standalone standing watch:** schedule it on a fixed interval so it snapshots on
  a steady cadence:
  ```python
  manage_routines(action="schedule", name="volume_drop_alert",
                  config={"threshold_pct": 20, "expected_interval_minutes": 15})
  ```
  Set `expected_interval_minutes` to match the cadence you run/schedule it at — it
  is used for the pace projection.

State persists in the routine's `state_file` between runs, so snapshots survive
restarts. `silent=true` suppresses output when nothing is wrong.

## Reading the output

1. If VDA flags a controller (latest interval collapsed vs its median), treat it as
   a **suspected stall** — do not act on it blindly.
2. Confirm with `controller_performance`'s trade table: check the last-trade time and
   recent fills for that controller. Zero recent fills confirms a real stall.
3. On a confirmed stall, follow the Autopilot MONITOR/tune path — investigate logs
   (`logs_summary`), and tune inventory bands or redeploy per `pmm_mister_deploy`.
