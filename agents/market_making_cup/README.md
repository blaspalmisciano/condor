# Market Making Cup — self-contained Condor Agent (Agent Builders Cup)

Autonomous `pmm_mister` market maker for the Hummingbot Agent Builders Cup. **This folder is
self-contained** — importing it brings the full decision engine; nothing lives outside it
except framework modules (`config_manager`, `hummingbot_api_client`, `condor.*`) that the
Condor/Hummingbot image already provides.

## Layout
```
market_making_cup/
  AGENT.md                     agent identity + declared tools + controller dependency
  loops/mm_cup_operator/
    loop.md                    per-tick playbook
  sample_configs/
    btcusdt.yml                base pmm_mister config (Gate BTC-USDT, full field set)
    bench_seed.json            cold-start fallback bench
  routines/                    THE ENGINE (bundled here — self-contained)
    botcamp_mm_agent.py        CONTINUOUS loop: bench builder + fleet manager
    botcamp_bench.py           pure core: K-window scoring, triggers, selection
    pmm_sweep.py               backtest sweep engine
    pmm_level_preview.py       candle fetch + chunked backtest
    controller_performance.py  live per-controller PnL / volume read
    base.py                    RoutineResult
  tests/
    test_botcamp_bench.py      unit tests for the core (8)
```

## Controller
Operates the **stock `generic/pmm_mister`** controller — ships **no custom controller**; it
expects `pmm_mister` to be present on the image (a standard Condor/Hummingbot controller).

## What it does
Each cycle: backtest random `pmm_mister` param variants over K disjoint daily windows (ranked
**fee-aware** by median PnL); keep a bench of the robust winners; keep `fleet_size` controllers
live (deploying its own on cold start); substitute any controller with no new volume for ≥4h
for the top eligible bench config — only after confirming the position is flat (fail-closed).
No human gate. Objective: maximize PnL over the 48h finals.

## Fees
Gate VIP10 spot maker = 0.04% (4bp) per the live rate card → spreads are set **above the fee**;
`trade_cost` makes the bench optimize net of it.
