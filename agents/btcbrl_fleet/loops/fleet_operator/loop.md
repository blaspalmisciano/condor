# Fleet operator loop

Run `btcbrl_fleet_manager` as a CONTINUOUS routine on the server pointed at brigado.

- Keep **exactly one** instance running — it is the sole manager of the BTC-BRL fleet.
- Start in `dry_run=true`, read the plan, then **GO = restart with `dry_run=false`**.
- It cold-starts the 12-controller barbell, then self-manages: by-side substitution of losers +
  the 3-tier risk stack. No other process should touch the fleet.
- Heartbeat: `data/btcbrl_fleet_hb.json` (watchdog can read it). Explore bench kept fresh by the
  self-contained sweep (`scripts/sc_sweep.py`).
