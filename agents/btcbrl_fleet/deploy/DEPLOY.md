# btcbrl_fleet — brigado deployment (live)

Hosted on brigado (always-on) as an isolated condor instance, so it doesn't depend on the laptop.

- Isolated clone: `~/condor-fleet` on brigado (branch `feat/autonomous-fleet-manager`), separate from the
  stale `~/condor` checkout. Uses `~/condor/.venv`.
- condor runs via systemd unit `condor-fleet.service` (this folder) → web/routine API on :8088,
  brigado's own TELEGRAM_TOKEN (different from the Mac → no poller clash), `EnvironmentFile=.env`
  (carries WEB_JWT_SECRET + HB creds). `Restart=always`.
- The routine `btcbrl_fleet_manager` is started via the routine API (server_name=`local` → brigado's
  hummingbot-api at localhost:8000). Continuous → auto-restores on condor restart.
- GO was: start the routine with `dry_run=False`. It cold-started the 12 (6 VOL + 6 PNL) as bot
  `btcbrl-fleet-*` and manages it (by-side substitution, brake, −6000 kill). rebate-mill never touched.

## Operate (on brigado)
- status: `systemctl status condor-fleet`  · logs: `~/condor-fleet/condor.systemd.log`
- routine heartbeat: `~/condor-fleet/data/btcbrl_fleet_hb.json`
- stop the fleet manager: stop the routine instance via the API, or `systemctl stop condor-fleet`
- to halt trading: stop the `btcbrl-fleet-*` bot via the hummingbot-api
