---
name: BTC-BRL Fleet Manager
description: Autonomous pmm_mister market-making fleet for brigado BTC-BRL — cold-starts a
  12-controller barbell (6 volume + 6 PnL), ranks by live capital-normalized yield, substitutes
  losers by-side (never cross-cohort), and protects with a 3-tier risk stack.
agent_key: null
tools:
- get_portfolio_overview   # balances / capital for sizing
- manage_bots              # cold-start deploy + stop
- manage_controllers       # read + hot-swap pmm_mister controller configs
- run_code                 # self-contained backtest bench (binance 1s, pure Python)
- send_notification        # alerts
skills: []
controllers:
- generic/pmm_mister       # the traded controller — must exist in the bot image
default_config:
  trading_pair: BTC-BRL
  cycle_sleep_sec: 900
  dry_run: true            # SAFE default — reads/plans only. GO = set dry_run=false.
---

# BTC-BRL Fleet Manager

One always-on routine that manages the brigado BTC-BRL market-making fleet with **zero human input
once live**. It is the "one manager" — nothing else should touch the fleet.

## The loop (`btcbrl_fleet_manager`, CONTINUOUS)
Each cycle: **read_live → cold_start_if_empty → rank → substitute (by-side) → risk → heartbeat**.

- **Cold start** — if no live BTC-BRL pmm_mister fleet exists, deploy the **12 from `spec.json`
  `cold_start`** (top-6 backtest volume + top-6 backtest PnL). Idempotent + guarded: never deploys
  when a fleet already exists, and **HOLDs** (never deploys) if a status/config read failed — the
  duplicate-fleet guard.
- **Rank** — capital-normalized yield from `get_active_bots_status` performance
  (`global_pnl_quote`/`volume_traded`), **blip-proof** (not MQTT telemetry).
- **Substitute — BY SIDE.** VOL losers only ever get VOL configs, PNL losers only PNL (cohort read
  from the controller id suffix, kept across swaps) → the **6/6 barbell is preserved**. Replacement
  = mostly the same-side live winner's config (exploit), a minority a same-side backtest candidate
  (explore). Churn-capped; backtest only proposes, live decides.
- **Risk (softest→hardest):** substitution < **brake** (gradual de-risk, lowers `target_base_pct`,
  **never market-sells**) < **global_kill** (net loss ≤ spec `max_loss_brl` → halt quoting + alert,
  **no dumping**).

## Hard invariant
Only ever touch a `pmm_mister` controller on `BTC-BRL`. Never the rebate-mill (or any non-pmm) bots.

## GO
`dry_run=true` is the safe default — it reads and prints the full plan, mutates nothing. **GO =
start the routine with `dry_run=false`.** Desired-state (the 12, substitution/risk params) is in
this agent's `spec.json`; the explore bench is `data/sc_sweep_results.json` (self-contained
backtester). Framework modules (`config_manager`, `routines/`) come from the Condor image.
