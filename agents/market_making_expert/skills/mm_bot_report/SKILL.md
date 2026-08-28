---
name: mm_bot_report
description: 'Run the MM bot status report: running bots, open/hold-mode positions,
  closed position breakdown (TP/SL/Early/Hold), PnL, volume, and error summary.'
when_to_use: When the user asks for a bot status report, "how is the bot doing", "show
  me the report", "what's the PnL", "any errors", "how are positions", "closed positions
  breakdown", or any general health/status check on the running MM bots. Also use
  after deploying a new bot to verify it's running correctly.
created: '2026-07-02T15:46:08Z'
source: agent:market_making_expert
references_routine: controller_performance
---

## MM Bot Report

Run the `controller_performance` routine — it summarizes every controller that ran
in the lookback window (active + stopped) in one shot:

```
manage_routines(action="run", name="controller_performance", config={"lookback_days": 7})
```

**What it returns:**
- **Per-controller summary** — every controller (active + stopped) in the window
- **Trade table** — the authoritative per-controller trade activity (fills, volume,
  last-trade time) — this is the ground truth for whether a bot is actually trading
- **PnL** — realized + unrealized PnL per controller, including maker rebates
- **Volume share** + cumulative curves, plus a candle+PnL chart
- **Stall detection** — flags controllers that have gone quiet inside the window

**Config overrides** (pass as `config={}` keys):
- `lookback_days` — days of history to include (default: 14; use 7 for a recent check)
- `trading_pair` — filter to one pair (default: all)
- `controllers` — comma-separated controller names to include (default: all)
- `controller_types` — filter by controller type (default: all)
- `include_stopped` — include stopped controllers (default: true)
- `stall_hours` — flag a controller with no trades in this many hours as stalled

**After reading the output:**
1. Surface the KPIs (active controllers, PnL, volume) per controller.
2. Read the **trade table** to confirm each bot is actually filling — a controller
   with zero recent fills is stalled even if it still shows as "running".
3. Flag any stalled controllers (see stall detection); for deeper log analysis run
   `manage_routines(action="run", name="logs_summary")` (global routine).
4. Summarize PnL vs volume to comment on fee efficiency.
