---
name: pmm_optimize
description: Sweep pmm_mister config variants through the backtester and rank the top
  candidates by business PnL and volume — pick the winning spreads/params before deploying.
when_to_use: Before deploying a pmm_mister bot, or when asked to tune / find the best
  spreads or parameters for a pair. Run it to turn one base config into a ranked shortlist
  of parameter variants, then hand the winner to the deploy step.
created: '2026-08-28T00:00:00Z'
source: agent:market_making_expert
references_routine: pmm_sweep
---

# PMM Optimize — parameter sweep for pmm_mister

Run the `pmm_sweep` routine. It takes ONE base `pmm_mister` config, generates a
mini-grid of parameter variants, backtests each over a single window via the
Hummingbot engine, and ranks them by business PnL (PnL + maker rebates), total
PnL, and volume. Output is a ranked table plus curve/parallel-coordinates charts.

## Provide the base config

Pass the base config one of two ways (exactly one is required):

- `config_text` — paste the full pmm_mister config as YAML or JSON (e.g. a
  `pmm_mister` config the user sent in chat).
- `config_name` — the name of a config already in the server library; the routine
  fetches it.

## Window

- `days` — window = last N days (default 7). Ignored if you set an explicit window.
- `window_start` / `window_end` — explicit ISO-UTC or epoch bounds. Use a real
  deployed bot's actual runtime here to sweep against the exact conditions it saw.

## Resolution — this matters for pmm_mister

- `resolution` (default `1m`). **For pmm_mister, `1s` is the meaningful resolution.**
  pmm_mister has sub-minute parameters (refresh time, cooldowns, effectivization,
  price-distance re-entry gates) that a `1m` backtest steps right over — a
  minute-resolution sweep silently misprices those levers and will rank variants
  wrong. Use `resolution="1s"` when the answer depends on spreads/cooldowns/refresh.
  `1s` runs are chunked and much slower — budget for it.
- Keep `resolution`, window, and `trade_cost` **constant** across the whole sweep
  so variants stay comparable.

Other knobs: `max_trials` (hard cap on backtests, default 40), `trade_cost`
(fee fraction), `rebate_rate` (maker rebate credited on funded volume).

## Run it async — pmm_sweep is long

pmm_sweep runs many backtests back-to-back and can take minutes (much longer at
`1s`). Submit it with `run_async` and read the instance back, do not block on `run`:

```python
manage_routines(
    action="run_async",
    name="pmm_sweep",
    config={
        "config_text": "<pasted pmm_mister YAML/JSON>",   # or "config_name": "<lib name>"
        "days": 7,
        "resolution": "1s",
        "max_trials": 40,
    },
)
# → returns an instance_id
manage_routines(action="get_instance", name="<instance_id>")   # poll until it completes
```

## Read the ranked candidates

The completed instance's `result.table_data` is the ranking, already sorted best-first.
Columns: `# | Variant | Business PnL | Total PnL | Realized | Volume | Rebates | MaxDD | Fills`.

- Row `#1` is the top candidate by business PnL. Read the numbers from
  `table_data`, never by parsing `result.text`.
- Prefer a candidate that is strong on **both** business PnL and Volume with a
  tolerable `MaxDD` — a high-PnL variant with near-zero fills is not a real winner.
- The `Variant` label tells you which params changed. Take the winning variant's
  spreads / cooldowns / refresh / inventory params and feed them into the deploy
  step (`pmm_mister_deploy`) as the config to upsert and deploy.
