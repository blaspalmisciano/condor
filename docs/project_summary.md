# PMM Autopilot — Project Summary

*A self-improving market-making fleet, operated by an AI agent.*
*Hummingbot Agent Builders Cup submission — Condor. Built on the `feat/pmm-autopilot-agent` branch.*

---

## 1. What we built (in one paragraph)

We run a fleet of pure market-making bots — Hummingbot's `pmm_mister` controller on
Binance BRL pairs (mainly BTC-BRL) — and instead of hand-tuning their configs, we let an
AI agent operate them on a **closed, self-improving loop**. Each cycle the agent reads the
best-performing live controllers, generates parameter variants, backtests every variant at
**1-second resolution on a local copy of the engine that is bit-identical to production**,
ranks them by profit and volume, gates the winners on out-of-sample validation and available
wallet budget, and — **on a single human approval** — reshapes the live fleet to the best
set. Then it watches the live bots, compares their real fills against the backtest, and turns
that divergence into a trust score that sharpens the next round. So the fleet keeps improving
itself rather than being a static strategy.

The loop is deterministic code (a Condor `CONTINUOUS` routine), not an LLM-per-tick agent —
so it costs **zero tokens per lap**, auto-restores after a reboot, and nothing reaches live
capital without a human pressing a button.

---

## 2. The loop at a glance

```
   ┌─────────────────────────────────────────────────────────────────┐
   │                                                                   │
   ▼                                                                   │
 READ ──▶ SWEEP ──▶ RANK ──▶ GATE ──▶ PROPOSE ──▶ [human: Apply] ──▶ DEPLOY
 live     variants  biz PnL   OOS +    Telegram      button           new
 configs  @ 1s      + volume  wallet   msg + HTML                     fleet
 (read-   (local              fit                                      │
  only)    parity)                                                     │
   ▲                                                                   ▼
   │                                                                MONITOR
   └────────────── CALIBRATE (live vs backtest trust haircut) ◀──── (live)
```

| Phase | When | Code |
|---|---|---|
| **Read** live controllers + configs (read-only) | every lap | `controller_performance.gather_active` |
| **Sweep** variants on the local parity engine @ 1s | every lap | `pmm_sweep.run_sweep` |
| **Rank** by business PnL + volume, with guardrails | every lap | `pmm_sweep.score` / `select_topN` |
| **Gate** on out-of-sample + wallet budget | every lap | `wf_split` / `promotion_gate` / `fits_budget` |
| **Propose** to Telegram (summary + HTML + Apply button) | every lap | `pmm_autopilot._reshape_lap` |
| **Deploy** the top-N fleet | **human approval only** | `_execute_reshape` |
| **Monitor** live fills, stalls, volume drops | continuous | `controller_performance` + `volume_drop_alert` |
| **Calibrate** live vs backtest into a trust score | every lap | `calibration_trust` |

Runs on a schedule (currently twice a day, `02:00` / `14:00`). `OPTIMIZE` is expensive so it
doesn't run every tick; `MONITOR` is cheap and runs continuously.

---

## 3. How the agent OPTIMIZES parameters

This is the heart of the system. The agent doesn't guess — it **measures**.

### 3.1 The parameters it sweeps
Each `pmm_mister` controller exposes the knobs a market maker cares about. The agent sweeps
the highest-impact ones:

- **`buy_spreads` / `sell_spreads`** — how far off mid we quote each side. Tighter → more
  fills and volume; wider → more edge per fill.
- **`take_profit`** — where a filled position closes out.
- **`min` / `target` / `max_base_pct`** — the inventory band; keeps the bot from getting too
  long or too short.
- **`buy_position_effectivization_time` / `sell_position_effectivization_time`** — how hard
  it works a position over time. **These are in seconds** — which is exactly why a 1-minute
  backtest is blind to them and we run at 1 second.
- **`portfolio_allocation` / `total_amount_quote`** — how much capital the controller runs.
- **`executor_refresh_time`, `buy_cooldown_time`, `sell_cooldown_time`** — quote refresh
  cadence and post-fill wait.
- **`max_active_executors`** — cap on concurrent live orders.

### 3.2 How the sweep is structured (staged, not brute force)
A full grid over all knobs is combinatorially hopeless. Instead the sweep is **staged**:

1. **Base** — backtest the current live config exactly as it runs, to get a reference.
2. **One-at-a-time (OAT)** — vary each axis independently (TP ×, spread ×, inventory band,
   effectivization ×) to find which knobs actually move the needle.
3. **Mini-grid** — a small combinatorial grid on the **two highest-impact axes** only.

This finds most of the gain at a fraction of the cost of a full grid.

### 3.3 Why 1-second backtesting matters (the key technical idea)
Every backtest runs on a **local parity engine** (`hummingbot-api-local`) that is a
byte-for-byte copy of the production `brigado` server — so heavy runs **never touch live
trading** and can't freeze the live bots. Crucially it runs at **1-second fidelity**:
`pmm_mister`'s sub-minute parameters (cooldowns, effectivization, refresh) are simply
*invisible* at 1-minute resolution, and the ranking comes out wrong at coarse resolution.

A long 1-second run would pin the single-threaded engine at 100% CPU, so the sweep **chunks**
each run into short back-to-back sub-windows (with a warmup overlap equal to the controller's
effectivization time) and **stitches** them back into one continuous curve.

### 3.4 What "best" means — the ranking objective
Variants are scored on:

- **Business PnL** = **market PnL + (volume × maker rebate)** — the true objective for a
  rebate market maker (this is the formula that drives selection).
- **Volume** — turnover; more volume → more rebate.
- **Risk-adjusted PnL** — PnL penalized for **max drawdown** and **time spent underwater**,
  and only crediting rebates on *fundable* volume.

Winners must also clear **guardrails**: business PnL may not fall more than a floor below the
base config, and max drawdown may be no worse than ~1.6× the base's. This keeps the optimizer
from chasing toxic, high-turnover-but-loss-making configs.

---

## 4. How the agent PROPOSES new runs (and reshapes the fleet)

Optimizing individual configs isn't enough — the agent proposes a whole **new generation of
the fleet** and asks a human to approve it.

### 4.1 Global top-N fleet reshape
Rather than tuning each controller in isolation, the agent **pools every variant across all
live controllers** and picks the **global top-N**, where **N = the current live controller
count** so fleet size stays stable. Multiple winners can come from the same origin controller;
some controllers go down; some new ones come up. It then:

- **Caps winners per strategy family** for diversity (no over-concentration in one recipe).
- **Autosizes capital** — splits the capital budget across the N winners so the fleet deploys
  the full budget (`total_amount_quote ≈ budget / N` per winner).
- Produces a **keep / deploy / retire** plan versus the current fleet.

### 4.2 The promotion gate — propose nothing if nothing survives
Before anything is proposed, candidates must survive:

- **Out-of-sample validation** — select on a training window, validate on a held-out tail
  (walk-forward). Configs that only look good in-sample are dropped.
- **Wallet two-sided fit** — the selected set must actually fund on *both* legs of every pair
  given the shared account; the scarcer leg binds. Over-committed sets are blocked.
- **Single-quote partition** — never pool or sum capital across different quote currencies.

If nothing survives, the correct outcome is **to propose nothing** that lap.

### 4.3 Human-in-the-loop delivery
Each lap the agent sends to Telegram:

1. A **plaintext summary** — N controllers, capital, keep/deploy/retire counts, family mix,
   and the top candidates by estimated volume.
2. An **HTML report** — volume & PnL curves (winners vs current live), a PnL-vs-volume
   frontier scatter, and leaderboards by volume and by business PnL.
3. A **single "🔄 Apply reshape" button** carrying that exact proposal's ID.

**Nothing goes live until that button is pressed.** The button executes the *exact snapshot*
the human saw (not a recomputation), it's idempotent (a second press can't double-deploy), and
if a fresh lap produces a better generation first, the old proposal is simply superseded.

### 4.4 The hard safety invariant
The reshape is scoped to `pmm_mister` **only**. It will **never** stop, archive, resize, or
mutate any controller or bot that isn't *exclusively* `pmm_mister` and in the approved plan —
if a bot hosts any other controller type (`rebate_mill`, `pmm_king`, anything co-hosted), the
reshape **refuses that bot** and leaves it running. This rule outranks every optimization
objective and is enforced at the top of `_execute_reshape`.

---

## 5. How the agent MONITORS and learns (calibration)

- **`controller_performance`** — per-controller volume, PnL, rebates, fill count, 24h yield
  (earnings per unit of allocated capital), and volume turnover. It reads the **trade table**
  — the authoritative record of whether a controller is actually filling. A "running"
  controller with no recent fills is **stalled**, and stall detection flags it.
- **`volume_drop_alert`** (continuous) — pings Telegram when a controller's volume falls below
  its running median or a bot goes stale/zombie. *(Gotcha we account for: its "24h volume" is
  a lifetime average, not a true 24h — so a bot that halted hours ago can still look healthy on
  that number. We never call a bot healthy on VDA alone; we confirm with the trade table's
  last-fill time.)*
- **Events that feed back:** controller stalling, lost MQTT connection, host memory pressure,
  volume dropping off — these fire alerts, and a stall or a big miss also informs what we
  re-optimize next round.
- **Calibration** — real fills are overlaid against the backtest for the same window and scored
  with `calibration_trust`. The divergence becomes a **haircut**: configs whose live fills
  chronically underperform their backtest get down-weighted next round, so each cycle is more
  predictive than the last.

**Why live and backtest never match (by design):** the backtest is a candle-cross model — it
has no order-queue position, no adverse selection, no balance contention. So it's an *upper
bound*: healthy live volume runs ~40–70% of backtest. The signals we act on are a live/backtest
volume ratio below ~30% (queue/balance-starved) or a **PnL sign flip** (backtest says profit,
live loses money → adverse selection).

---

## 6. Markets & regime fit

Binance, BRL pairs — mainly **BTC-BRL** and USDT-BRL. This kind of market making does best in
**sideways, ranging markets** where price chops around a level: you keep getting filled on both
sides and collect the rebate plus a small PnL. It likes quiet to mildly volatile conditions
with steady two-sided flow, and the agent's regime read maps conditions (quiet / ranging /
trending / volatile) to appropriate spread, refresh, and inventory-band profiles.

---

## 7. Repository map

```
routines/
  pmm_autopilot.py          THE LOOP — orchestrator: read → sweep → rank → gate → reshape
                            (human-gated). select_topN / plan_reshape / _execute_reshape /
                            handle_callback (Apply button) / _build_gen_report.
  pmm_sweep.py              staged 1s backtest sweep engine (base → OAT → mini-grid),
                            scoring (business PnL, risk-adjusted), gates, wallet fit.
  pmm_level_preview.py      single-config backtest / spread-level preview; chunked 1s backtest.
  controller_performance.py live per-controller vol / PnL / rebates / yield / turnover + stalls.
  volume_drop_alert.py      continuous volume-drop + zombie/stale alerting to Telegram.

agents/market_making_expert/
  AGENT.md                              agent identity + when to consult + the loop
  strategies/pmm_autopilot/strategy.md  the closed-loop playbook (+ the hard safety invariant)
  skills/  pmm_optimize · pmm_mister_deploy · pmm_config_playbook · mm_bot_report ·
           pmm_volume_watch · capital_allocation
  routines/fleet_btcbrl_30m_check.py    agent-scoped health/heartbeat routine

docs/
  project_summary.md               this file
  agent_architecture.md            technical architecture + mermaid diagram
  botcamp_agent_cup_answers.md      the submission form answers
  pmm_autopilot_improvement_plan.md phased roadmap of known improvements
```

---

## 8. What makes it different (for judges)

- **1-second backtesting on a bit-identical local engine** — sub-minute knobs are actually
  resolved, unlike a 1-minute sweep, and heavy runs never risk the live bots.
- **Live-vs-backtest calibration** — the loop measures how well each config's real fills track
  its backtest and feeds that back, getting more predictive every lap.
- **Capital-aware fleet reshape** — reads the whole account, keeps fleet count and capital
  budget stable, autosizes winners, and caps per-family concentration.
- **Human-in-the-loop by design** — the agent explores, ranks, and proposes fully
  autonomously, but deploying real capital is always a single human approval.
- **Deterministic and cheap** — the loop is code, not an LLM per tick: zero tokens per lap,
  auto-restoring, with a watchdog, heartbeat, and a hard "never touch non-`pmm_mister`" safety
  invariant.
