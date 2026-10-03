#!/usr/bin/env python
"""External watchdog for botcamp_mm_agent — runs every 5 min via launchd, INDEPENDENT of any
Claude session. Enforces the only two valid states: the agent is RUNNING, or we're FIXING it.

Each run it checks (a) an instance is registered running on localhost:8088, and (b) the agent's
heartbeat (data/botcamp_mm_heartbeat.json) is fresh. If either fails -> Telegram alert + restart
the instance. A dead/wedged agent therefore self-heals within a few minutes, and you get pinged.
"""
import os, re, sys, json, time, urllib.request

ROOT = "/Users/blaspalmisciano/condor"
sys.path.insert(0, ROOT)
os.chdir(ROOT)

HEARTBEAT = "data/botcamp_mm_heartbeat.json"
STALE_SEC = 2400  # 40 min — a cycle (incl. slow bench build) can run several minutes; only a
                  # truly dead/wedged agent exceeds this. Clean stops are caught by the status check.
_BASE = {"connector_name": "binance", "fleet_size": 4, "capital_quote": 400, "cold_start": True,
         "dry_run": False, "variants_per_cycle": 3, "n_windows": 3, "window_hours": 12,
         "cycle_sleep_sec": 300, "max_subs_per_hour": 0, "use_unrealized_trigger": False}
# one config per pair being tested — the watchdog guards EACH independently
CFGS = [dict(_BASE, trading_pair="SOL-USDT"), dict(_BASE, trading_pair="PEPE-USDT")]


def _hb_path(pair):
    return f"data/botcamp_hb_{pair.replace('-', '').lower()}.json"
CHAT = 6310433268


def _jwt():
    admin = [int(m.group(1)) for line in open(".env")
             if (m := re.match(r'\s*ADMIN_USER_ID\s*=\s*"?([0-9]+)', line))][0]
    from condor.web.auth import create_jwt
    return create_jwt(admin, role="admin")


def _api(method, path, tok, body=None):
    req = urllib.request.Request(
        f"http://localhost:8088/api/v1/routines{path}",
        data=json.dumps(body).encode() if body else None,
        headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}, method=method)
    return json.load(urllib.request.urlopen(req, timeout=30))


def _tg(msg):
    try:
        from routines.pmm_autopilot import _tg_send_message
        _tg_send_message(CHAT, msg)
    except Exception:
        pass


def _pair_of(inst):
    return (inst.get("config") or {}).get("trading_pair")


def main():
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        tok = _jwt()
        insts = [i for i in _api("GET", "/instances", tok) if i.get("routine_name") == "botcamp_mm_agent"]
        for cfg in CFGS:  # guard EACH pair independently
            pair = cfg["trading_pair"]
            running = [i for i in insts if i.get("status") == "running" and _pair_of(i) == pair]
            hb_path = _hb_path(pair)
            hb_age = (time.time() - json.load(open(hb_path)).get("ts", 0)) if os.path.exists(hb_path) else None
            if running and hb_age is not None and hb_age < STALE_SEC:
                print(f"{stamp} OK {pair} — running, heartbeat {hb_age:.0f}s old")
                continue
            why = []
            if not running:
                why.append("no running instance")
            if hb_age is None:
                why.append("no heartbeat")
            elif hb_age >= STALE_SEC:
                why.append(f"heartbeat stale {hb_age/60:.0f}min")
            # DIAGNOSE this pair's failure before restarting
            diag = []
            mine = [i for i in insts if _pair_of(i) == pair]
            latest = sorted(mine, key=lambda i: str(i.get("created_at", "")))[-1] if mine else None
            if latest:
                diag.append(f"last {latest.get('instance_id')} status={latest.get('status')}")
                if latest.get("error"):
                    diag.append(f"error={str(latest['error'])[:120]}")
            if os.path.exists(hb_path):
                try:
                    diag.append(f"hb_status={json.load(open(hb_path)).get('status')}")
                except Exception:
                    pass
            diag_s = " | ".join(diag) or "no diagnostics"
            for i in running:
                try:
                    _api("POST", f"/instances/{i['instance_id']}/stop", tok)
                except Exception:
                    pass
            r = _api("POST", "/start", tok, {"routine_name": "botcamp_mm_agent",
                                             "server_name": "brigado", "config": cfg})
            print(f"{stamp} RESTARTED {pair} ({', '.join(why)}) — {diag_s} -> {r.get('instance_id')}")
            _tg(f"🔧 watchdog: {pair} unhealthy ({', '.join(why)})\nWHY: {diag_s}\n→ restarted {r.get('instance_id')}")
    except Exception as e:
        print(f"{stamp} watchdog ERROR: {e}")
        _tg(f"⚠️ botcamp watchdog error: {str(e)[:120]}")


if __name__ == "__main__":
    main()
