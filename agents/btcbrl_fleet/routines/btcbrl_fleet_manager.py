"""btcbrl_fleet_manager — condor CONTINUOUS routine: the single declarative control loop for the
brigado BTC-BRL market-making fleet. Wrapped as an agent/routine (like botcamp_mm_agent) so it
starts/stops/heartbeats through the condor system; GO = flip the `dry_run` config field to False.

Each cycle:  read_live → cold_start_if_empty → rank → substitute (by-side) → risk → heartbeat

DESIGN (full desired-state in this agent's spec.json):
  • COLD START = if NO live BTC-BRL pmm_mister fleet exists, deploy the 12 from spec.cold_start
    (top-6 backtest volume + top-6 backtest PnL). Idempotent + guarded: never deploy if one exists,
    and never deploy when UNSURE one exists (a failed status read ⇒ HOLD — duplicate-fleet guard).
  • EXPLOIT = converge a loser's CONFIG onto the LIVE winner (capital-normalized yield). Never a
    backtest winner — backtest != live.
  • EXPLORE = a minority of replacements come from a fresh backtest candidate (bench =
    data/sc_sweep_results.json), which must prove out LIVE before it's trusted.
  • BY-SIDE substitution: VOL losers only get VOL configs, PNL losers only PNL — the 6/6 barbell is
    preserved (cohort read from the controller id suffix, kept across swaps).
  • Risk, softest→hardest: substitution < brake (gradual de-risk, never sells) < global_kill
    (max-loss → halt quoting + alert, no dumping).

YIELD is blip-proof: bot_orchestration.get_active_bots_status() → [bot]['performance'][cid]
['performance'] → global_pnl_quote / volume_traded (NOT MQTT telemetry).

SAFETY: dry_run=True (default) ⇒ READS and PLANS only; every mutation is written but guarded inert.
Never touches a non-pmm_mister / wrong-pair controller.
"""
from __future__ import annotations
import os, json, time, asyncio, urllib.request
from collections import Counter

from pydantic import BaseModel, Field
from telegram.ext import ContextTypes
from config_manager import get_client
from routines.controller_performance import gather_active

CONTINUOUS = True

_HERE = os.path.dirname(os.path.abspath(__file__))
SPEC_PATH = os.path.join(_HERE, "..", "spec.json")          # desired-state ships with the agent
BENCH_PATH = "data/sc_sweep_results.json"                   # EXPLORE pool (self-contained backtest)
SNAP_PATH = "data/btcbrl_fleet_yield_snapshots.json"       # rolling cumulative PnL → windowed yield
SUBS_PATH = "data/btcbrl_fleet_sub_times.json"            # churn-cap timestamps
HB_PATH = "data/btcbrl_fleet_hb.json"                     # heartbeat (watchdog reads this)
PAIR = "BTC-BRL"
DEPLOY_IMAGE = "hummingbot/hummingbot:latest"


class Config(BaseModel):
    """Autonomous BTC-BRL fleet manager — cold-start, by-side substitution, 3-tier risk.
    The detailed params (cold-start 12, substitution rules, risk thresholds) live in the agent's
    spec.json; this Config holds the operational knobs."""
    trading_pair: str = Field(default="BTC-BRL", description="Pair to manage")
    cycle_sleep_sec: int = Field(default=900, description="Pause between cycles (15 min)")
    dry_run: bool = Field(default=True, description="TRUE = read + plan only, zero live changes. GO = set this to False.")
    deploy_image: str = Field(default=DEPLOY_IMAGE, description="Container image for a cold-start deploy")


# --------------------------------------------------------------- json helpers
def _load(path, default):
    try:
        return json.load(open(path))
    except Exception:
        return default


def _save(path, obj):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    json.dump(obj, open(tmp, "w"), default=str)
    os.replace(tmp, path)


def _norm(cfg: dict) -> dict:
    c = dict(cfg)
    for k in ("buy_spreads", "sell_spreads", "buy_amounts_pct", "sell_amounts_pct"):
        v = c.get(k)
        if v is not None and not isinstance(v, (list, tuple)):
            c[k] = [v]
    bs, ba = c.get("buy_spreads") or [], c.get("buy_amounts_pct") or []
    if bs and len(bs) != len(ba):
        c["buy_amounts_pct"] = [round(1 / len(bs), 4)] * len(bs)
    ss, sa = c.get("sell_spreads") or [], c.get("sell_amounts_pct") or []
    if ss and len(ss) != len(sa):
        c["sell_amounts_pct"] = [round(1 / len(ss), 4)] * len(ss)
    return c


def invariant_ok(cfg: dict) -> bool:
    """Hard invariant: only ever touch a pmm_mister controller on BTC-BRL."""
    return cfg.get("controller_name") == "pmm_mister" and (cfg.get("trading_pair") or "").upper() == PAIR


def cohort_of(cid: str):
    """VOL / PNL cohort from the controller id suffix (btcbrl-cs{i}-vol / -pnl). Swaps keep the id,
    so the cohort — and the 6/6 barbell — is preserved forever."""
    s = (cid or "").rsplit("-", 1)[-1].lower()
    return s if s in ("vol", "pnl") else None


def load_spec() -> dict:
    raw = json.load(open(SPEC_PATH))
    def strip(d):
        return {k: (strip(v) if isinstance(v, dict) else v)
                for k, v in d.items() if not k.startswith("_comment")}
    return strip(raw)


# --------------------------------------------------------------- read_live (blip-proof)
async def read_live(c, log):
    """(fleet, status_ok, cfg_ok). Per-controller PnL/volume from get_active_bots_status's nested
    performance (survives MQTT blips); configs from gather_active. Both reads must succeed before we
    dare conclude 'no fleet exists' and cold-start."""
    perf_by, bot_by, status_ok = {}, {}, False
    try:
        status = await c.bot_orchestration.get_active_bots_status()
        bots = status.get("data", status) if isinstance(status, dict) else status
        if isinstance(bots, dict):
            status_ok = True
            for bot, info in bots.items():
                perf = (info or {}).get("performance", {})
                if not isinstance(perf, dict):
                    continue
                for cid, p in perf.items():
                    inner = (p or {}).get("performance", {}) if isinstance(p, dict) else {}
                    if not isinstance(inner, dict):
                        continue
                    perf_by[cid] = {"pnl": float(inner.get("global_pnl_quote", 0) or 0),
                                    "vol": float(inner.get("volume_traded", 0) or 0)}
                    bot_by[cid] = bot
    except Exception as e:
        log(f"  (get_active_bots_status failed: {str(e)[:80]})")

    cfg_by, cid_bot, cfg_ok = {}, {}, False
    try:
        _lp, cfg_by, _bn, _h, cid_bot = await gather_active(c)
        cfg_ok = True
    except Exception as e:
        log(f"  (gather_active failed: {str(e)[:60]})")

    fleet, unclassified = {}, 0
    for cid, p in perf_by.items():
        cfg = cfg_by.get(cid)
        if cfg is None:
            unclassified += 1
            continue
        if not invariant_ok(cfg):            # rebate-mill / wrong pair → ignored
            continue
        tq = float(cfg.get("total_amount_quote") or 0)
        al = float(cfg.get("portfolio_allocation") or 0)
        fleet[cid] = {"cfg": cfg, "bot": bot_by.get(cid) or cid_bot.get(cid),
                      "tgtB": round(float(cfg.get("target_base_pct") or 0), 3),
                      "alloc": cfg.get("portfolio_allocation"), "wc": tq * al,
                      "pnl": p["pnl"], "vol": p["vol"], "has_perf": True}
    if unclassified:
        log(f"  ({unclassified} running controller(s) had no config in the store — not classified)")
    return fleet, status_ok, cfg_ok


# --------------------------------------------------------------- cold start
def plan_cold_start(ps: dict, log) -> list:
    out = []
    for item in ps.get("cold_start", []):
        cfg = _norm(item.get("config", {}))
        if invariant_ok(cfg):
            out.append(cfg)
        else:
            log(f"    cold-start config {cfg.get('id')} fails invariant — skipped")
    return out


async def apply_cold_start(c, ps, configs, dry, image, log):
    if dry or not configs:
        return None
    ts = time.strftime("%Y%m%d%H%M%S")
    instance = f"btcbrl-fleet-{ts}"
    names = []
    for cfg in configs:
        await c.controllers.create_or_update_controller_config(cfg["id"], cfg)
        names.append(cfg["id"])
    budget = sum(float(x.get("total_amount_quote", 0) or 0) * float(x.get("portfolio_allocation", 0) or 0) for x in configs)
    maxloss = abs(float(ps["risk"]["global_kill"].get("max_loss_brl") or budget))
    res = await c.bot_orchestration.deploy_v2_controllers(
        instance_name=instance, credentials_profile=ps.get("credentials_profile", "master_account"),
        controllers_config=names, image=image,
        max_global_drawdown_quote=round(maxloss),
        max_controller_drawdown_quote=round(maxloss / max(1, len(configs))))
    log(f"🚀 cold-start deployed {len(names)} controllers as {instance}: {str(res)[:80]}")
    return instance


# --------------------------------------------------------------- rank + by-side substitution
def windowed_yield(fleet: dict, window_h: float) -> dict:
    now = time.time()
    snaps = _load(SNAP_PATH, [])
    snaps.append({"ts": now, "cum": {cid: i["pnl"] for cid, i in fleet.items()}})
    snaps = [s for s in snaps if now - s["ts"] <= max(window_h, 48) * 3600]
    _save(SNAP_PATH, snaps)
    past = [s for s in snaps if now - s["ts"] >= window_h * 3600]
    ref = past[-1] if past else None
    out = {}
    for cid, i in fleet.items():
        wc = i["wc"] or 1e-9
        if ref and cid in ref["cum"]:
            out[cid] = ((i["pnl"] - ref["cum"][cid]) / wc, "window")
        else:
            out[cid] = (i["pnl"] / wc, "lifetime")
    return out


def load_bench_sides() -> dict:
    data = _load(BENCH_PATH, {})
    rows = [r for r in (data.get("rows", []) if isinstance(data, dict) else [])
            if isinstance(r, dict) and r.get("full_config")]
    vol = [_norm(r["full_config"]) for r in sorted(rows, key=lambda r: r.get("volume", 0), reverse=True)]
    pnl = [_norm(r["full_config"]) for r in sorted(rows, key=lambda r: r.get("pnl", 0), reverse=True)]
    return {"vol": vol, "pnl": pnl}


def plan_substitution(fleet: dict, yields: dict, ps: dict, bench_sides: dict) -> dict:
    """COHORT-AWARE: ranks + substitutes WITHIN each side (vol/pnl). A VOL loser only ever gets a
    VOL config (its cohort's live winner, or a vol-side bench candidate); same for PNL. The slot id
    is kept so the cohort tag — and the 6/6 split — survive every swap."""
    sub = ps["substitution"]
    thresh = float(sub["yield_threshold"])
    explore_ratio = float(sub["replacement_pool"]["explore_backtest_candidate_ratio"])

    cohorts = {"vol": [], "pnl": []}
    unclassified = []
    for cid in fleet:
        co = cohort_of(cid)
        (cohorts[co].append(cid) if co in cohorts else unclassified.append(cid))

    winners, loser_items = {}, []
    for co, members in cohorts.items():
        if not members:
            continue
        ranked_c = sorted(members, key=lambda cid: yields[cid][0], reverse=True)
        winners[co] = ranked_c[0]
        for cid in members:
            if yields[cid][0] < thresh and cid != winners[co]:
                loser_items.append((cid, co, yields[cid][0]))

    loser_items.sort(key=lambda t: t[2])
    now = time.time()
    recent = [t for t in _load(SUBS_PATH, []) if now - t < 3600]
    budget = max(0, int(sub["max_subs_per_hour"]) - len(recent))
    capped = loser_items[:budget]
    n_explore = round(len(capped) * explore_ratio)

    plan = []
    for idx, (cid, co, y) in enumerate(capped):
        loser = fleet[cid]
        if not invariant_ok(loser["cfg"]):
            plan.append({"cid": cid, "cohort": co, "src": "refuse", "why": "invariant"}); continue
        side_bench = bench_sides.get(co, [])
        win = winners.get(co)
        desired, src = None, None
        if idx < n_explore and side_bench:
            desired = _norm({**side_bench[idx % len(side_bench)], "id": cid, "portfolio_allocation": loser["alloc"]})
            src = f"explore({co} backtest candidate)"
        elif win and win != cid:
            desired = _norm({**fleet[win]["cfg"], "id": cid, "portfolio_allocation": loser["alloc"]})
            src = f"exploit({co} live winner {win[:16]})"
        elif side_bench:
            desired = _norm({**side_bench[idx % len(side_bench)], "id": cid, "portfolio_allocation": loser["alloc"]})
            src = f"explore({co} bench — no cohort winner)"
        else:
            plan.append({"cid": cid, "cohort": co, "src": "refuse", "why": "no same-side target"}); continue
        if not invariant_ok(desired):
            plan.append({"cid": cid, "cohort": co, "src": "refuse", "why": "desired fails invariant"}); continue
        plan.append({"cid": cid, "cohort": co, "bot": loser["bot"], "src": src, "desired": desired, "y": y})

    return {"winners": winners, "cohort_counts": {k: len(v) for k, v in cohorts.items()},
            "unclassified": unclassified, "losers": [l[0] for l in loser_items],
            "budget": budget, "n_explore": n_explore, "plan": plan}


async def apply_sub(c, a, dry, log):
    if dry or a.get("src") == "refuse":
        return
    cfg, cid = a["desired"], a["cid"]
    if not invariant_ok(cfg):
        log(f"  INVARIANT BLOCK {cid}"); return
    await c.controllers.create_or_update_controller_config(cid, cfg)
    try:
        await c.controllers.update_bot_controller_config(a["bot"], cid, cfg)
    except Exception as e:
        log(f"  {cid} hot-apply note: {str(e)[:60]}")
    subs = _load(SUBS_PATH, []); subs.append(time.time()); _save(SUBS_PATH, subs)


# --------------------------------------------------------------- risk
def _price_ret():
    url = "https://api.binance.com/api/v3/klines?symbol=BTCBRL&interval=5m&limit=147"
    d = json.loads(urllib.request.urlopen(url, timeout=15).read())
    closes = [(float(k[0]) / 1000.0, float(k[4])) for k in d]
    now = closes[-1][1]
    def ret(h):
        tgt = closes[-1][0] - h * 3600
        return now / min(closes, key=lambda x: abs(x[0] - tgt))[1] - 1.0
    return now, ret(2), ret(12)


def risk_check(fleet: dict, ps: dict, log):
    brake = ps["risk"]["brake"]; gk = ps["risk"]["global_kill"]
    try:
        px, r2, r12 = _price_ret()
        would = "HOLD" if (r2 >= -0.015 and r12 >= -0.03) else f"DE-RISK (lower tgtB → floor {brake['floor_tgtB']}; never sells)"
        log(f"  brake      : price {px:,.0f} | 2h {r2:+.2%} | 12h {r12:+.2%} → WOULD {would}")
    except Exception as e:
        log(f"  brake      : price read failed ({str(e)[:40]})")
    maxloss = gk.get("max_loss_brl")
    if maxloss is None:
        log("  global_kill: DISABLED (max_loss_brl not set)")
    elif fleet:
        net = sum(i["pnl"] for i in fleet.values())
        if net <= -abs(float(maxloss)):
            log(f"  global_kill: *** WOULD HALT + ALERT *** net {net:+,.0f} ≤ -{abs(float(maxloss)):,.0f} (no dump)")
        else:
            log(f"  global_kill: ok (net {net:+,.0f} vs limit -{abs(float(maxloss)):,.0f})")
    else:
        log(f"  global_kill: armed at -{abs(float(maxloss)):,.0f} (no fleet yet)")


def _write_heartbeat(status: str, n_fleet: int, dry: bool, log):
    try:
        _save(HB_PATH, {"ts": int(time.time()), "status": status, "pair": PAIR,
                        "n_fleet": n_fleet, "dry_run": dry})
    except Exception as e:
        log(f"heartbeat write err: {str(e)[:50]}")


# --------------------------------------------------------------- one cycle
async def tick(config: Config, live, log):
    ps = load_spec()[PAIR]
    dry = config.dry_run
    fleet, status_ok, cfg_ok = await read_live(live, log)
    log("=" * 74)
    log(f"BTCBRL FLEET MANAGER — {'DRY-RUN (read-only)' if dry else 'LIVE'} — {time.strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"READS: status_ok={status_ok} cfg_ok={cfg_ok}")
    tb = Counter(i["tgtB"] for i in fleet.values())
    log(f"LIVE : {len(fleet)} BTC-BRL pmm_mister controller(s)"
        + (f" | tgtB {dict(sorted(tb.items()))}" if fleet else "")
        + f" | target {ps['controllers']}")
    status = "ok"

    if len(fleet) == 0:
        if not (status_ok and cfg_ok):
            log("\n*** HOLD: no fleet seen BUT a status/config read failed → will NOT cold-start (dup-fleet guard) ***")
            status = "hold_read_fail"
        else:
            configs = plan_cold_start(ps, log)
            budget = sum(float(x.get("total_amount_quote", 0) or 0) * float(x.get("portfolio_allocation", 0) or 0) for x in configs)
            log(f"\nCOLD START: no live fleet → WOULD deploy {len(configs)} controllers (working ≈ {budget:,.0f} BRL, "
                f"profile {ps.get('credentials_profile')})")
            for item in ps.get("cold_start", []):
                cfg = item.get("config", {})
                log(f"    [{item.get('source'):3}] {cfg.get('id'):26} spread {item.get('spread_bp')}bp tp {item.get('tp_bp')}bp "
                    f"eff {item.get('eff')}s tgtB {cfg.get('target_base_pct')}")
            await apply_cold_start(live, ps, configs, dry, config.deploy_image, log)
            status = "cold_start" + ("_planned" if dry else "_deployed")
    else:
        if not any(i["has_perf"] for i in fleet.values()):
            log("\n*** HOLD: fleet exists but no per-controller performance → no ranking/substitution ***")
            status = "hold_no_perf"
        else:
            yields = windowed_yield(fleet, float(ps["convergence"]["rank_window_hours"]))
            bench_sides = load_bench_sides()
            basis = "window" if any(v[1] == "window" for v in yields.values()) else "lifetime (accruing)"
            log(f"\nRANK by capital-normalized yield [{basis}] (bench {len(bench_sides['vol'])} vol / {len(bench_sides['pnl'])} pnl):")
            for co in ("vol", "pnl"):
                members = sorted((cid for cid in fleet if cohort_of(cid) == co), key=lambda x: yields[x][0], reverse=True)
                if members:
                    log(f"  [{co.upper()} cohort]")
                    for cid in members:
                        log(f"    {yields[cid][0]:+8.4f}  {cid[:40]:40} (pnl {fleet[cid]['pnl']:+.0f} / wc {fleet[cid]['wc']:.0f})")
            res = plan_substitution(fleet, yields, ps, bench_sides)
            log("\nCOHORT WINNERS: " + (", ".join(f"{co}={w[:20]}" for co, w in res["winners"].items()) or "none"))
            log(f"SUB PLAN (by-side): {len(res['losers'])} loser(s), churn budget {res['budget']}/h, "
                f"~{res['n_explore']} explore | cohort counts {res['cohort_counts']}")
            if not res["plan"]:
                log("    ✓ no substitutions")
            for a in res["plan"]:
                if a["src"] == "refuse":
                    log(f"    REFUSE [{a.get('cohort')}] {a['cid'][:30]} — {a['why']}")
                else:
                    log(f"    WOULD swap [{a['cohort']}] {a['cid'][:28]:28} (y {a['y']:+.4f}) → {a['src']}")
            for a in res["plan"]:
                await apply_sub(live, a, dry, log)
            status = "managing"

    log("\nRISK (substitution < brake < global_kill):")
    risk_check(fleet, ps, log)
    log("=" * 74)
    _write_heartbeat(status, len(fleet), dry, log)
    return status


# --------------------------------------------------------------- condor routine entrypoint
async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> str:
    logs: list[str] = []
    def log(m):
        logs.append(str(m)); print(m, flush=True)
    chat_id = getattr(context, "_chat_id", None)
    live = await get_client(chat_id, context=context)
    mode = "DRY-RUN" if config.dry_run else "LIVE"
    log(f"🟢 btcbrl_fleet_manager started [{mode}] — {PAIR}, cold-start 12, by-side substitution, "
        f"global-kill from spec. GO = set dry_run=False.")
    try:
        while True:
            try:
                await asyncio.wait_for(tick(config, live, log), timeout=1800)
            except Exception as e:
                log(f"cycle error: {type(e).__name__} {str(e)[:100]}")
            await asyncio.sleep(config.cycle_sleep_sec)
    except asyncio.CancelledError:
        return "btcbrl_fleet_manager stopped\n" + "\n".join(logs[-25:])
