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
- generic/pmm_mister     # STOCK Hummingbot controller — this agent operates it; no custom controller is shipped
default_config:
  trading_pair: BTC-USDT
  connector_name: gate_io
  capital_quote: 800
  fleet_size: 2
---

# Market Making Cup

Autonomous market maker for the Hummingbot **Agent Builders Cup**. The agent runs a small
fleet of controllers and manages them with **zero human input** via the bundled
`botcamp_mm_agent` routine. Objective: maximize **PnL** (and volume) over the 48h finals.

## Controller
This agent operates the **stock `generic/pmm_mister`** controller — it ships **no custom
controller**; it expects `pmm_mister` to be available on the Condor/Hummingbot image (it is a
standard controller). All parameter tuning (spreads, take-profit, inventory band,
effectivization, refresh) is applied to `pmm_mister` configs.

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
- **Bench** — continuously backtest random `pmm_mister` param variants over **7 disjoint
  daily windows**; rank by **median daily PnL**; keep only configs **positive in ≥5/7
  windows** (robust across regimes — not a single-window fluke). Ranks **fee-aware**
  (`trade_cost`), so spreads must clear the venue maker fee.
- **Fleet** — keep `fleet_size` controllers live; on cold start the agent **deploys its own
  fleet**; it **substitutes** any controller with **no new volume for ≥ 4h** for the top
  eligible bench config (never the one that just failed, and only after confirming the
  position is flat — fail-closed).

No warmup grace, no global kill — continuous rotation to the best-benched config *is* the
risk management on a bounded stake.

## Market & fees
Gate spot, **BTC-USDT** (ETH-USDT optional). **Gate VIP10 spot maker = 0.04% (4bp)** per the
live rate card, so spreads are set **above the fee** (spread capture must beat 4bp); the bench
optimizes net of the fee. If the competition grants a maker rebate (Gate MM program, up to
−1.2bp), spreads can tighten.
