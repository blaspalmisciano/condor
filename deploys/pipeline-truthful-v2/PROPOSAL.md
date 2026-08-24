# Truthful Pipeline v2 — Proposal

Branch: `feat/pipeline-truthful-v2` (off `feat/controller-performance`). Reversible.
Implements the pipeline-audit fixes and re-scores the cached ganjahro sweeps against the
**real shared-wallet budget**. READ-ONLY against brigado — nothing deployed or changed live.

## 1. Changes made (all in `routines/pmm_sweep.py`)

| Audit item | Change |
|---|---|
| P0.3 fee realism | `Config.trade_cost` default `0.0 → 0.0001` (fills aren't free); rebates now credited on **funded** volume only |
| P0.2 inventory risk | `risk_score()` = `business_pnl − w_dd·\|maxDD\| − w_uw·time_underwater`; `_time_underwater()` from the PnL curve. `score()` kept for comparison. New `Config` weights `risk_w_dd`, `risk_w_uw` |
| P0.1 funding gate | `two_sided_quote_need()`, `fits_budget()`, `autosize_amount()` — model the shared base+quote budget and reject/size candidate sets to it |
| P1.4 out-of-sample | `wf_split()` (train/test window split) + `promotion_gate()` (promote only if edge survives OOS **and** fits budget). `Config.oos_fraction` (0 = legacy single-window) |
| P1.5 calibration | `calibration_trust()` — turns `fetch_real` from display-only into a live-vs-backtest trust/haircut score |
| report | `build_report()` gains a **🛡️ Top-5 Risk-adjusted** table (the deploy-selection metric) |

All compile-checked; existing routine imports unchanged. Re-score/proposal generator:
`deploys/pipeline-truthful-v2/rescore_proposal.py` (+ `proposal_data.json`).

## 2. The revised proposal vs the current 6 champions

### 2a. The blocking finding — the account can't fund its current fleet
Shared `master_account`/binance, measured live:
- **BRL cash (quote) available: ~57.6k** · BTC (base, in BRL): ~492k (abundant)
- Committed by the **other** live controllers (test-tp 3×10,340 + test-alloc 3×10,340 + USDT-BRL rebate-mill 102,400): **~82.2k BRL quote need**
- **FREE quote = 57.6k − 82.2k = −24.6k BRL** → already short before the champions.
- Current 6 champions need **88.2k BRL quote vs 57.6k** → `FITS=False`. **Auto-size for new controllers = 0** (no free BRL).

This is the root cause of the live result (net −259, ~49k INSUFFICIENT_BALANCE, all stuck long): **the wallet is over-committed on BRL cash across both pairs.** The old pipeline could never see it (solo, infinite-capital backtests). ⇒ The correct move is **not "add more bots"** — it's to fit the fleet to the cash.

### 2b. Risk-adjusted re-ranking (from cached trials, fee- + risk-aware)
| Base | OLD deployed pick(s) | NEW risk-adjusted top | Why it changed |
|---|---|---|---|
| g1 (1.0bp) | band 0.2–0.8 + spread×0.75 (biz +789) | **same** (risk-adj +618, underwater 14%) | already robust |
| g2 (1.5bp) | toppnl band 0.2–0.8+eff×2; topvol band 0.3–0.55+eff×4 | **eff×4** (risk-adj +547 vs deployed +408–538) | demotes the high-drawdown volume pick |
| g3 (2.0bp) | toppnl eff×0.25+spread×0.5; topvol eff×4+spread×0.5 | **spread×0.75 + eff×4** (risk-adj +547) | converges to the ~1.5bp/high-eff region |
g2 and g3 optima **converge** to one robust region (~1.5bp, wide inventory, high effectivization) — the truthful pipeline says deploy **fewer bots in one good region**, not 6 diverse capital-hungry ones.

### 2c. Concrete fundable proposal
Because free BRL is negative, deploying anything new requires freeing cash first. Options, in order of preference:
1. **Shrink to fit (recommended):** replace the 6 champions with **3** risk-adjusted controllers (g1 band0.2–0.8+spread×0.75; one ~1.5bp/eff×4; g-tight variant), and **cut the other BTC-BRL test fleet** (test-tp/test-alloc = 62k notional) so free quote turns positive. To fund 3×2000 champions you need ~+27.6k BRL freed → e.g. retire test-alloc (−31k notional ≈ +15.5k BRL) and halve test-tp (≈ +7.75k BRL) and/or trim the rebate mill.
2. **Add BRL capital:** deposit ~30k BRL and the current sizing becomes fundable; then re-rank picks per 2b.
3. **Re-target inventory:** lower `target_base_pct` (e.g. 0.3) so controllers hold less BRL cash each — reduces quote need per controller (`fits_budget` will reflect it).

Exact per-config params for the 3 risk-adjusted picks are in `proposal_data.json`; deployable via the same verified flow as the 2026-08-21 deploy (`deploys/ganjahro-champions-20260821/`).

> Caveat: exact same-window live-vs-backtest calibration (P1.5) needs a `fetch_real` re-run over the live window — not computable from the single-window cache. The `calibration_trust()` machinery is in place for that next cycle.

## 3. Keep / modify / revert

**KEEP (adopt v2 pipeline):**
```
git checkout feat/controller-performance && git merge feat/pipeline-truthful-v2
git push blaspalmisciano feat/controller-performance   # (personal remote)
```
Then future `/routines pmm_sweep` runs are fee-aware, risk-ranked, and budget-gated. (Does NOT change any live bot.)

**MODIFY:** tell me which — e.g. tune `risk_w_dd`/`risk_w_uw`, set `oos_fraction=0.3`, pick a different `target_base_pct`, or choose 2 vs 3 controllers — and I'll adjust on this branch.

**REVERT (discard v2 entirely):**
```
git checkout feat/controller-performance
git branch -D feat/pipeline-truthful-v2      # local
git push blaspalmisciano :feat/pipeline-truthful-v2   # delete remote copy, if pushed
```
The live bots and the original pipeline are untouched either way.
