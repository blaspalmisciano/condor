# Ganjahro Champion Deployment — 2026-08-21

Live deployment of 6 champion variants of the `ganjahro` BTC-BRL `pmm_mister` sweep,
selected from `reports/20260821_154935_ganjahro_real_by_controller_backtests_*.html`
(3 top-PnL + 3 top-volume). Backtests: Hummingbot engine, **1-minute** candles, window Aug 12→20.

> Secrets (API host, credentials profile) are intentionally omitted — supply via env/placeholders.

## Instances (deployed as siblings; existing bots untouched)
- `ganjahro-toppnl-btcbrl-<ts>` → `btcbrl-ganjahro-{1,2,3}__toppnl`
- `ganjahro-topvol-btcbrl-<ts>` → `btcbrl-ganjahro-{1,2,3}__topvol`
- credentials_profile: `<ACCOUNT>` · connector: binance · `total_amount_quote: 2000` BRL each

## Champion deltas vs live base `btcbrl-ganjahro-{1,2,3}` (1.0/1.5/2.0 bp)
Perturbations use the exact functions in `routines/pmm_sweep.py` (`_apply_spread`, `_apply_band`, `_apply_eff`):

| Controller | spread (buy/sell) | band (min–max, target) | effectivization (s) |
|---|---|---|---|
| g1 toppnl | ×0.75 → 7.5e-5 | 0.2–0.8, tgt 0.5 | 300 |
| g1 topvol | 1.0e-4 | 0.3–0.7, tgt 0.5 | ×4 → 1200 |
| g2 toppnl | ×0.5 → 7.5e-5 | 0.3–0.7, tgt 0.5 | ×0.25 → 75 |
| g2 topvol | ×0.5 → 7.5e-5 | 0.3–0.7, tgt 0.5 | ×4 → 1200 |
| g3 toppnl | 2.0e-4 | 0.2–0.8, tgt 0.5 | ×2 → 600 |
| g3 topvol | 2.0e-4 | 0.3–0.55, tgt 0.425 | ×4 → 1200 |

Full resolved configs: `champion_configs.json` (this dir). Each differs from its base ONLY in the
fields above + `total_amount_quote=2000`, verified pre- and post-deploy.

## Deploy mechanics (Hummingbot API client)
```
controllers.create_or_update_controller_config(name, cfg)       # per config, then read-back verify
bot_orchestration.deploy_v2_controllers(instance, "<ACCOUNT>", [name+".yml", ...])
```

## Live snapshot @ ~2.6 days (2026-08-24)
Volume ~523,737 BRL (~101,583 USDT); realized +172 BRL; unrealized -510 BRL; rebates +79 BRL (@0.00015);
**net ~-259 BRL**. Health issue: ~49,324 `INSUFFICIENT_BALANCE` closes; all controllers stuck long
(base% 0.56–0.80 vs 0.50 target) — shared-wallet capital contention. See pipeline audit for remediation.
