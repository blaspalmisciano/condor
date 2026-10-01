---
name: Market Making Cup
description: Autonomous pmm_mister market maker for the Agent Builders Cup — runs a
  continuous bench of backtested configs and keeps the best ones live, substituting losers.
agent_key: null
skills: []
default_config:
  trading_pair: BTC-USDT
  connector_name: gate_io
  capital_quote: 800
  fleet_size: 2
---

# Market Making Cup

Autonomous market maker for the Hummingbot **Agent Builders Cup**. The agent runs a small
fleet of `pmm_mister` controllers and manages them with **zero human input** via the
`botcamp_mm_agent` routine. Objective: maximize **PnL** (and volume) over the 48h finals.

## The loop (`botcamp_mm_agent`)
- **Bench** — continuously backtest random `pmm_mister` param variants over **7 disjoint
  daily windows**; rank by **median daily PnL**; keep only configs **positive in ≥5/7
  windows** (robust across regimes — not a single-window fluke).
- **Fleet** — keep `fleet_size` controllers live; **substitute** any controller at
  **≤ −20 unrealized PnL** or **≥ 4h with no new volume** for the top eligible bench config
  (never redeploying the one that just failed).

No warmup grace, no global kill — continuous rotation to the best-benched config *is* the
risk management on a bounded stake.

## Market
Gate spot, **BTC-USDT** (ETH-USDT optional). Tight spreads + patient effectivization, with
the inventory band leaning slightly long. Spreads adapt to Gate's confirmed maker-fee tier.
