---
name: Market Making Cup
description: Autonomous pmm_mister market maker for the Agent Builders Cup — runs a
  continuous bench of backtested configs and keeps the best ones live, substituting losers.
agent_key: null
tools:
- get_market_data        # OHLCV candles — feeds the backtest bench
- get_portfolio_overview  # balances / capital for sizing
- manage_bots            # deploy / stop the fleet (cold-start + substitution)
- manage_controllers     # read + write pmm_mister controller configs
- manage_executors       # read held positions (fail-closed flatten check)
- run_code               # run the backtest engine (routines/) each cycle
- send_notification      # Telegram action + watchdog alerts
skills: []
controllers:
- generic/pmm_mister     # BUNDLED at controllers/generic/pmm_mister.py — install into the bot image if not already present
default_config:
  trading_pair: ATH-USDT
  connector_name: gate_io
  capital_quote: 800
  fleet_size: 4
  dry_run: false        # LIVE — an organizer launch from this config must place real orders
---

# Market Making Cup

Autonomous market maker for the Hummingbot **Agent Builders Cup**. The agent runs a small
fleet of controllers and manages them with **zero human input** via the bundled
`botcamp_mm_agent` routine. Objective: maximize **PnL** (and volume) over the 48h finals.

## Controller — BUNDLED (install into the bot image)
This agent operates the **`generic/pmm_mister`** controller. It is **not** a stock Hummingbot
controller, so it is **shipped with this agent** at `controllers/generic/pmm_mister.py` (a clean
`strategy_v2` controller — imports only `hummingbot.*` + pydantic, no framework deps). **Before the
run, place this file in the bot image's `bots/controllers/generic/` directory** (or confirm the
host already has `pmm_mister`). If the bot container lacks it, `deploy_v2_controllers` will create
the bot but the controller cannot instantiate → 0 executors → 0 trades. All parameter tuning
(spreads, take-profit, inventory band, effectivization, refresh) is applied to `pmm_mister` configs.

**⚠️ Set `credentials_profile` before launch** (Config field, default `master_account`): it must be
the name of the **Gate account profile provisioned on your container**. If it names a profile that
doesn't exist, `deploy_v2_controllers` deploys under a missing profile → the fleet fails to start.
This is the single launch prerequisite besides the controller file.

## Self-contained engine (bundled)
The decision engine lives **inside this agent folder** at `routines/`, so importing the agent
brings everything it needs:
- `routines/botcamp_mm_agent.py` — the CONTINUOUS loop (bench builder + fleet manager)
- `routines/botcamp_bench.py` — pure core: K-window scoring, triggers, selection
- `routines/pmm_sweep.py`, `routines/pmm_level_preview.py` — backtest engine
- `routines/controller_performance.py` — live per-controller PnL / volume read

Framework modules (`config_manager`, `hummingbot_api_client`, `condor.*`) come from the Condor
image, as with any routine.

## The loop (`botcamp_mm_agent`)
- **Bench** — a ranked pool of `pmm_mister` configs the agent deploys and rotates to.
  **SHIPPED DEFAULT: `bench_enabled=false`** → the bench is **loaded from the pre-built
  `sample_configs/bench_seed.json`** (built offline from Gate REST candles), and the agent
  **never backtests at the venue** — Gate historical candles aren't fetchable on the
  competition infra, so this is deliberate. (`bench_enabled=true` is dev-only: it would
  continuously backtest random variants over 7 disjoint daily windows, rank fee-aware by
  median daily PnL, and grow the bench live — only where a candle feed works.)
- **Fleet** — keep `fleet_size` controllers live; on cold start the agent **deploys its own
  fleet** on the validated `base_config`; it **substitutes** any controller with **no new
  volume for ≥ 2h** for the **top eligible bench config** (the current best robust winner),
  never re-picking the one that just failed, and only after confirming the position is flat —
  fail-closed. `base_config` is the safety net until the bench has an eligible winner.

No warmup grace, no global kill — continuous rotation to the best-benched config *is* the
risk management on a bounded stake.

## Market & fees
Gate spot, **ATH-USDT**. Pair chosen deliberately: **Gate VIP10 spot maker = 0.04% (4bp)**, so a
round trip pays ~8bp in fees — profitable only where the **natural spread is wider than the fee**.
Majors (BTC/ETH/SOL) have sub-1bp natural spreads on Gate → impossible. **ATH-USDT has a persistent
~9.8bp natural spread** (verified via Gate's API over many samples) with ~$3M/day volume and
mean-reverting price action — so quoting a tight ladder *inside* that spread, closing at a
take-profit **above the 8bp round-trip fee**, is net-positive. The config (5–7bp ladder, 10bp
take-profit → +2bp/round-trip, tilted toward volume) is set for exactly this. If the competition grants a Gate MM-program maker rebate
(up to −1.2bp), the spreads can tighten further.
