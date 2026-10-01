---
name: MM Cup Operator
description: Each tick — grow the backtest bench and substitute any failing controller.
default_config:
  trading_pair: BTC-USDT
  connector_name: gate_io
  fleet_size: 2
  sub_upnl: -20
  sub_no_trade_h: 4
---

# MM Cup Operator — per-tick playbook

The deterministic engine is the **`botcamp_mm_agent`** routine (zero LLM tokens/cycle); the
preferred way to run this agent is to start that routine. This playbook mirrors its logic for
when the brain drives each tick directly.

## Each tick

1. **BENCH** — backtest a few **random `pmm_mister` variants** over the **7 disjoint 24h
   windows**; upsert into the bench sorted by **median daily PnL**. A config is **eligible**
   only if **PnL-positive in ≥5/7 windows**.
2. **MONITOR** — read each live controller's **unrealized PnL** and **traded volume**.
3. **SUBSTITUTE** — for any controller with **unrealized ≤ −20** OR **no new volume for ≥4h**,
   live-update its config to the **top eligible bench config** — excluding the one that just
   failed (anti A→B→A thrash).
4. **FILL** — if fewer than `fleet_size` controllers are running, deploy the top eligible bench
   configs to fill the fleet.

## Rules
- **No warmup grace** — the triggers (4h / −20) are far longer/larger than any startup
  transient, so a fresh controller is never unfairly culled.
- **No global kill** — on a bounded $800 stake, keep rotating to the best bench config rather
  than cutting; that rotation is the risk management.
- **Objective = PnL** (volume is the tiebreaker metric) — the bench ranks on realized+unrealized
  PnL, not rebate-inflated turnover.
