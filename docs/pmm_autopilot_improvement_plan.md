# PMM Autopilot — Unified Improvement Plan

_Synthesis of a bug hunt (A) + a SOTA quant review (B) into one phased, implementable roadmap.
Grounded in `routines/pmm_autopilot.py`, `routines/pmm_sweep.py`, `routines/controller_performance.py`._

## Executive framing
`run()` only calls `_reshape_lap`. Everything that reaches the live server flows through three
functions: `_reshape_lap` (selection) → `handle_callback` action `"reshape"` (the button) →
`_execute_reshape` (the mutation). The entire M2/proposal path (`_one_lap`,
`_windowed_calibration`, `PROPOSAL_STORE`, deploy/deploy_all/reject) is **dead code**. So are the
"rigorous" helpers in `pmm_sweep.py` (`risk_score`, `wf_split`, `promotion_gate`, `fits_budget`,
`autosize_amount`, `calibration_trust`) — defined but **called by nothing in the live path**.
Net: the live path is a thin, unguarded in-sample turnover-maximizer; the rigor exists but is unwired.
**Most high-impact fixes are "connect code you already have," not new research.**

## PHASE 0 — MUST-FIX before ANY live reshape is pressed
_Interim mitigation: set `dry_run=True` (suppresses the Deploy button) until all land._

- **P0-U1 [H][M] Co-hosted controller safety.** Reshape stops whole bots but redeploys only
  pmm_mister → silently archives `rebate_mill`/`pmm_king` sharing a bot. Fix: build per-bot full
  controller set in `_reshape_lap`; in `_execute_reshape` refuse (or carry configs of) any bot
  hosting non-selected controllers. Unit-testable with a mock `live`.
- **P0-U2 [H][M] Real idempotency + FLEET rewrite.** `reshape_status=="applied"` is never set →
  second press double-deploys a full-capital fleet. Fix: per-proposal `applied` flag read before
  executing; rewrite `FLEET_STORE`/`POOL_STORE` to the new fleet after success.
- **P0-U3 [H][S-M] Proposal identity in the button.** `callback_data` is constant `…reshape:go`;
  press recomputes `select_topN` from the latest stores → deploys something never shown. Fix:
  embed `proposal_id`, snapshot that exact selection, execute the snapshot (don't recompute).
  (P0-U2 + P0-U3 are one change — the snapshot store IS the idempotency store.)
- **P0-U14/U11 [H][S] Empty/undersized guard.** Empty selection still stops fleet + deploys empty
  bot; diversity/dedup can drop below N and over-capitalize survivors. Fix: bail if empty; backfill
  to N or size on true count; assert `sum(autosized) ≤ budget`.
- **P0-U4 [H][M] Rollback + pinned deploy.** Stop-then-deploy leaves fleet DOWN on deploy failure;
  `image=":latest"`, hardcoded `master_account`. Fix: capture archived configs pre-stop, auto-
  rollback on failure; pin image digest; make profile a config field.
- **P0-U7 [H][S] Quote-currency partition.** `only_pair=""` can pool BTC-BRL + ETH-USDT and sum
  capital across quotes. Fix: assert single quote or partition per quote before autosizing.
- **P0-U6 (gate) [H][M] Wallet two-sided fit.** Wire `fits_budget`/`autosize_amount`: block deploy
  if the selected set doesn't fund on both legs (account is over-committed; scarcer leg binds).

**Exit criterion:** a scripted "double-press + co-hosted bot + mixed quote + base-poor wallet +
empty pool" dry-run touches the live API zero times destructively.

## PHASE 1 — Make the optimizer optimize something real (mostly wiring)
- **P1-U5a [H][S] Risk-adjusted selection.** Pool already has `max_dd`; `_sweep_controller` computes
  `t["_rs"]`. In `select_topN`, rank on `risk_adj/capital` (not `volume/capital`) with a `max_dd`
  floor. Closes A5 + B-A2.
- **P1-U5b [H][M] Out-of-sample gate.** Wire `wf_split(w0,w1,0.3)` + `promotion_gate`: select on
  train, validate on test, drop failures; propose nothing if none survive (correct outcome).
- **P1-U8b [H][S] Taker-fee on TP crosses.** `score` credits `volume*rebate` on ALL volume incl.
  taker TP exits. Split maker vs taker (from `close_types`) → rebate only on maker, `taker_fee` on
  taker.
- **P1-U13 [M][M] Calibration feedback.** Wire `_windowed_calibration`/`calibration_trust` into
  `_reshape_lap` as a per-origin haircut (chronic live-underperformers get down-weighted).
- **P1-U10 [H][M] Honor keep/deploy/retire.** `_execute_reshape` ignores `plan_reshape` and stops+
  redeploys everything (every order to back of queue = the maker's edge destroyed). Fix: only stop
  `plan["retire"]`, deploy `plan["deploy"]`, leave `plan["keep"]` running. **Do with P0-U1.**

## PHASE 2 — Quant validity & portfolio
- **P2-U8a [H][M-L] Fill realism.** Candle-cross engine has no queue/adverse-selection → rewards
  toxic tight-spread configs. Add fill-probability × adverse-selection haircut (later: queue model).
- **P2-U8c [H][M] Saturating capital→volume.** `est_volume=_eff*per` assumes linear scaling (false
  for a maker). Use a saturating curve; rank on absolute risk-adjusted rebate at deployed size.
- **P2-U9 [H][M] Portfolio correlation/VaR.** All N quote BTC-BRL (~perfectly correlated) — family
  diversity ≠ diversification. Add aggregate net-delta/VaR constraint; size by risk contribution.
- **P2-U5c [H][M] Multiple-testing correction.** Deflated Sharpe / PBO (CSCV) over the trial pool;
  propose nothing if the champion ≈ median trial.
- **P2-U5d [M][M] Regime windows.** Validate on ≥2-3 disjoint / purged-embargoed windows.
- **P2-U10b [H][M] Champion-challenger.** Run challengers on a capital slice vs the champion; promote
  only on sustained OOS+live win; slow the cadence.

## PHASE 3 — Ops / MLOps hardening
- **P3 kill-switch/auto-rollback** (alpha-decay auto-revert); **generation lineage/experiment ledger**
  (attribute live PnL to a deploy decision); **secrets/config out of code** (hardcoded chat_id, `.env`
  path, image digest); `_next_target_seconds` return None not 0 on malformed input; widen dedup key;
  drop `_save(default=str)` numeric-as-string risk; report cleanups (trial-id curve join, raw-HTML
  section type, sweep-store write lock).

## Dead M2 path — delete vs wire
**Delete** `_one_lap`, `_apply_proposal`, deploy/deploy_all/reject, `PROPOSAL_STORE`,
`_proposal_keyboard` (an in-place-update deploy model superseded by, and contradicting, the reshape
model — two live-mutation paths, one unreachable, is a footgun). **Salvage** `_windowed_calibration`
+ `calibration_trust` + `SNAP_STORE` (the only correct way to compare lifetime-cumulative live perf
to a windowed backtest) → move into `_reshape_lap` as the P1-U13 haircut.

## If you only do 5 things
1. **Arm the button safely:** proposal-id snapshot + real idempotency (P0-U3+U2) — else an old/double
   press deploys a second full-capital fleet.
2. **Stop archiving co-hosted controllers (P0-U1) + only stop/deploy the changed set (P1-U10)** — else
   reshape silently kills every rebate_mill/pmm_king sharing a bot.
3. **Gate on wallet two-sided fit + single quote (P0-U6+U7).**
4. **Select on risk-adjusted return with an OOS gate (P1-U5a+U5b)** — stop ranking on raw in-sample
   volume; propose nothing when nothing survives OOS.
5. **Rollback + pin the image digest (P0-U4).**

All five are wiring-and-guarding existing code, not new research.
