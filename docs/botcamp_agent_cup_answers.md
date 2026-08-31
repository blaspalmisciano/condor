# Botcamp Agent Cup — submission answers (draft)

_Saved 2026-08-28. These are the final answers as written, kept here so nothing is lost before submitting. Strategy Type selected on the form: **Agent – AI/autonomous trading agent**._

---

## Summary
_(short summary shown on strategy cards)_

We run market making bots and let AI agents handle the optimization: backtest the configs with different variants, keep the winners by volume and PnL, push them live, repeat so the fleet keeps sharpening itself.

---

## Description

The bots are pure market makers (Hummingbot's pmm_mister controller) operated using agents. Instead of hand tuning configs, agents get the best performing controllers, generate parameter variants, backtest them at 1 second resolution on a local copy of the engine that's identical to what runs live, rank them by PnL and by volume, and deploy the winners. Then they watch the live bots, compare real fills against the backtest, and kick off the next round. So it keeps improving the fleet instead of being a static strategy.

---

## Markets

Binance for now, on BRL pairs, mainly BTC-BRL and USDT-BRL. This kind of market making does best in sideways, ranging markets where price chops around a level, so you keep getting filled on both sides and collecting the rebate or a small PnL. It likes quiet to mildly volatile conditions with steady two sided flow.

---

## Parameters

The main knobs, set per controller, and what the agents sweep over:
- buy_spreads and sell_spreads: how far off mid you quote each side. Tighter gets more fills and volume, wider keeps more edge per fill.
- take_profit: where a filled position closes out.
- portfolio_allocation and total_amount_quote: how much capital the controller runs with.
- min, max and target_base_pct: the inventory band, keeps you from getting too long or short.
- executor_refresh_time, buy_cooldown_time and sell_cooldown_time: how often quotes refresh and how long to wait after a fill.
- buy_position_effectivization_time and sell_position_effectivization_time: how hard it works a position over time. It's in seconds, which is why a 1 minute backtest misses it and we run 1 second.
- max_active_executors: cap on how many orders are live at once.

The sweep mostly moves spread, take profit, the inventory band, effectivization and capital, optimizing business PnL, which is market PnL plus volume times the rebate.

---

## Status

Per controller we track volume traded, PnL, rebates, fill count, and whether the bot is actually trading or just sitting there (there's stall detection for when a bot is up but frozen, which does happen). We also look at it per unit of capital: 24h yield, what the controller earned on its allocated capital over the last day, and volume turnover, how many times it churns that capital in volume. Two routines surface all this: a controller performance report with the per controller numbers, and a volume drop alert that pings us on Telegram when a controller's volume falls off. It shows up in a dashboard and over Telegram.

---

## Events

controller stalling, losing its MQTT connection, memory pressure on the host, and volume dropping off. Those fire alerts, and a stall or a big miss also feeds into what we re optimize next round.

---

## Supporting artifacts (for when you resume)
- Concept flowchart: `reports/agent_loop_concept.html` (visual) and `docs/agent_architecture.md` (GitHub-native mermaid).
- Agent build branch: `feat/pmm-autopilot-agent` (the pmm_autopilot agent under `agents/market_making_expert/`).
- 1s fleet sweep results: in `reports/` (leaderboards + unified viz).
