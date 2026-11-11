#!/usr/bin/env python
"""fleet_manager.py — the single declarative control loop for the brigado BTC-BRL MM fleet.

ONE manager, ONE loop. Each tick:
    load_spec → read_live → cold_start_if_empty → rank → substitute → risk → report

FINALIZED design (fleet/spec.json):
  • COLD START = if NO live BTC-BRL pmm_mister fleet exists, deploy the 12 from spec.cold_start
    (top-6 backtest volume + top-6 backtest PnL). Idempotent: never deploy if one already exists,
    and never deploy when we're UNSURE a fleet exists (a failed status read ⇒ HOLD, not deploy —
    that is the duplicate-fleet bug class, guarded).
  • EXPLOIT = converge losers' CONFIG onto the LIVE winner (ranked by capital-normalized yield).
    Never a backtest winner — backtest != live.
  • EXPLORE = a minority of replacements come from a fresh backtest candidate (bench =
    data/sc_sweep_results.json), which must prove out LIVE before it's trusted as an exploit target.
  • Substitution trigger = persistent negative normalized yield over a patience window + churn cap.
  • Risk, softest→hardest: substitution < brake (gradual de-risk, never sells) < global_kill
    (max-loss → halt quoting + alert, no dumping).

YIELD SOURCE is blip-proof: bot_orchestration.get_active_bots_status() →
    [bot]['performance'][cid]['performance'] → global_pnl_quote / volume_traded (NOT MQTT telemetry).

SAFETY: DRY_RUN=True → READS and PLANS only; every mutation is written but guarded inert.
Never touches a non-pmm_mister / wrong-pair controller.
"""
from __future__ import annotations
import os as _os; from dotenv import load_dotenv as _ld; _ld(_os.path.join(_os.path.dirname(_os.path.abspath(__file__)),"..",".env"))
import sys, os, json, time, asyncio, urllib.request

sys.path.insert(0, "/Users/blaspalmisciano/condor")
os.chdir("/Users/blaspalmisciano/condor")
from hummingbot_api_client import HummingbotAPIClient
from aiohttp import ClientTimeout
from routines.controller_performance import gather_active

# ---------------------------------------------------------------- config
DRY_RUN = True                       # <-- ONE-LINE flip to go live. Keep True until validated.
SPEC_PATH = "fleet/spec.json"
BENCH_PATH = "data/sc_sweep_results.json"   # EXPLORE pool — the self-contained backtest output
SNAP_PATH = "fleet/yield_snapshots.json"    # rolling per-controller cumulative PnL (windowed yield)
SUBS_PATH = "fleet/sub_times.json"          # recent substitution timestamps (churn cap)
BRIGADO = "http://100.94.144.46:8000"
USER, PW = _os.getenv("HB_USER"), _os.getenv("HB_PASS")
DEPLOY_IMAGE = "hummingbot/hummingbot:latest"
PAIR = "BTC-BRL"


def log(m): print(m, flush=True)


# ---------------------------------------------------------------- helpers
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
    """VOL / PNL cohort from the controller id suffix (ids are btcbrl-cs{i}-vol / -pnl).
    Substitution keeps the id, so the cohort — and the 6/6 barbell — is preserved forever."""
    s = (cid or "").rsplit("-", 1)[-1].lower()
    return s if s in ("vol", "pnl") else None


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


async def client():
    c = HummingbotAPIClient(base_url=BRIGADO, username=USER, password=PW,
                            timeout=ClientTimeout(total=300, connect=20))
    await c.init()
    return c


# ---------------------------------------------------------------- 1. load_spec
def load_spec() -> dict:
    raw = json.load(open(SPEC_PATH))
    def strip(d):
        return {k: (strip(v) if isinstance(v, dict) else v)
                for k, v in d.items() if not k.startswith("_comment")}
    return strip(raw)


# ---------------------------------------------------------------- 2. read_live (blip-proof)
async def read_live(c, pair=PAIR):
    """Returns (fleet, status_ok, cfg_ok).

    Per-controller PnL/volume come from get_active_bots_status()'s nested `performance`
    (survives MQTT blips). Controller CONFIG (to filter pmm_mister/BTC-BRL + size) comes from
    gather_active's config store. `status_ok`/`cfg_ok` tell the caller whether the two reads
    actually succeeded — both must be true before we dare conclude "no fleet exists" and cold-start.
    """
    # --- robust per-controller perf ---
    perf_by, bot_by = {}, {}
    status_ok = False
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
                    perf_by[cid] = {
                        "pnl": float(inner.get("global_pnl_quote", 0) or 0),
                        "realized": float(inner.get("realized_pnl_quote", 0) or 0),
                        "unrealized": float(inner.get("unrealized_pnl_quote", 0) or 0),
                        "vol": float(inner.get("volume_traded", 0) or 0)}
                    bot_by[cid] = bot
    except Exception as e:
        log(f"  (get_active_bots_status failed: {str(e)[:80]})")

    # --- configs (reliable config store, not MQTT) ---
    cfg_by, cid_bot, cfg_ok = {}, {}, False
    try:
        _lp, cfg_by, _bn, _h, cid_bot = await gather_active(c)
        cfg_ok = True
    except Exception as e:
        log(f"  (gather_active failed: {str(e)[:60]})")

    # The fleet = controllers ACTUALLY RUNNING (present in get_active_bots_status perf), that are
    # pmm_mister/BTC-BRL. cfg_by holds ALL saved configs (incl. retired ones) so it can't define
    # "live" by itself — intersect with perf_by. If perf read failed (status not ok) fall back to
    # cfg_by∩live heuristics is unsafe, so the caller holds on cold-start when status_ok is False.
    fleet = {}
    unclassified = 0
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


# ---------------------------------------------------------------- 3. cold start (plan; inert in dry)
def plan_cold_start(ps: dict) -> list:
    out = []
    for item in ps.get("cold_start", []):
        cfg = _norm(item.get("config", {}))
        if invariant_ok(cfg):
            out.append(cfg)
        else:
            log(f"    cold-start config {cfg.get('id')} fails invariant — skipped")
    return out


async def apply_cold_start(c, ps: dict, configs: list):
    if DRY_RUN or not configs:
        return None
    ts = time.strftime("%Y%m%d%H%M%S")
    instance = f"btcbrl-fleet-{ts}"
    names = []
    for cfg in configs:
        await c.controllers.create_or_update_controller_config(cfg["id"], cfg)
        names.append(cfg["id"])
    budget = sum(float(x.get("total_amount_quote", 0) or 0) * float(x.get("portfolio_allocation", 0) or 0)
                 for x in configs)
    maxloss = abs(float(ps["risk"]["global_kill"].get("max_loss_brl") or budget))
    res = await c.bot_orchestration.deploy_v2_controllers(
        instance_name=instance, credentials_profile=ps.get("credentials_profile", "master_account"),
        controllers_config=names, image=DEPLOY_IMAGE,
        max_global_drawdown_quote=round(maxloss),
        max_controller_drawdown_quote=round(maxloss / max(1, len(configs))))
    log(f"🚀 cold-start deployed {len(names)} controllers as {instance}: {str(res)[:80]}")
    return instance


# ---------------------------------------------------------------- 4. rank + substitution (plan only)
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


def plan_substitution(fleet: dict, yields: dict, ps: dict, bench_sides: dict) -> dict:
    """COHORT-AWARE substitution. Ranks + substitutes WITHIN each side (vol / pnl) so the
    6-volume / 6-PnL barbell is preserved: a VOL loser only ever gets a VOL config (its cohort's
    live winner, or a volume-side bench candidate); same for PNL. The slot id is kept, so the
    cohort tag — and the split — survive every swap. Prevents the all-converge-to-one-winner drift.
    """
    sub = ps["substitution"]
    thresh = float(sub["yield_threshold"])
    explore_ratio = float(sub["replacement_pool"]["explore_backtest_candidate_ratio"])

    # split the live fleet by cohort
    cohorts = {"vol": [], "pnl": []}
    unclassified = []
    for cid in fleet:
        co = cohort_of(cid)
        (cohorts[co].append(cid) if co in cohorts else unclassified.append(cid))

    # per-cohort winner (best live yield in that side) + losers (below threshold, not the winner)
    winners, loser_items = {}, []
    for co, members in cohorts.items():
        if not members:
            continue
        ranked_c = sorted(members, key=lambda cid: yields[cid][0], reverse=True)
        winners[co] = ranked_c[0]
        for cid in members:
            if yields[cid][0] < thresh and cid != winners[co]:
                loser_items.append((cid, co, yields[cid][0]))

    # worst-first globally, shared churn budget
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
        if idx < n_explore and side_bench:                       # EXPLORE within the same side
            desired = _norm({**side_bench[idx % len(side_bench)], "id": cid, "portfolio_allocation": loser["alloc"]})
            src = f"explore({co} backtest candidate)"
        elif win and win != cid:                                 # EXPLOIT the same-side live winner
            desired = _norm({**fleet[win]["cfg"], "id": cid, "portfolio_allocation": loser["alloc"]})
            src = f"exploit({co} live winner {win[:16]})"
        elif side_bench:                                         # no same-side winner → fall back to side bench
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


async def apply_sub(c, a):
    if DRY_RUN or a.get("src") == "refuse":
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


# ---------------------------------------------------------------- 5. risk (report only in dry)
def _price_ret(hours=2):
    url = f"https://api.binance.com/api/v3/klines?symbol=BTCBRL&interval=5m&limit={int(hours*12)+3}"
    d = json.loads(urllib.request.urlopen(url, timeout=15).read())
    closes = [(float(k[0]) / 1000.0, float(k[4])) for k in d]
    now = closes[-1][1]
    def ret(h):
        tgt = closes[-1][0] - h * 3600
        return now / min(closes, key=lambda x: abs(x[0] - tgt))[1] - 1.0
    return now, ret(2), ret(12)


def risk_check(fleet: dict, ps: dict):
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


# ---------------------------------------------------------------- 6. bench + tick
def load_bench() -> list:
    """EXPLORE pool = the self-contained backtest output (data/sc_sweep_results.json)."""
    data = _load(BENCH_PATH, {})
    rows = data.get("rows", []) if isinstance(data, dict) else []
    return [_norm(r["full_config"]) for r in rows if isinstance(r, dict) and r.get("full_config")]


def load_bench_sides() -> dict:
    """Split the bench into side-matched explore pools: vol-side = highest backtest VOLUME
    (tight-spread) candidates, pnl-side = highest backtest PnL (wide-spread) candidates. A VOL
    loser only draws from the vol side, a PNL loser only from the pnl side."""
    data = _load(BENCH_PATH, {})
    rows = [r for r in (data.get("rows", []) if isinstance(data, dict) else [])
            if isinstance(r, dict) and r.get("full_config")]
    vol = [_norm(r["full_config"]) for r in sorted(rows, key=lambda r: r.get("volume", 0), reverse=True)]
    pnl = [_norm(r["full_config"]) for r in sorted(rows, key=lambda r: r.get("pnl", 0), reverse=True)]
    return {"vol": vol, "pnl": pnl}


async def tick():
    spec = load_spec()
    ps = spec[PAIR]
    c = await client()
    try:
        fleet, status_ok, cfg_ok = await read_live(c, PAIR)
        log("=" * 74)
        log(f"FLEET MANAGER — {'DRY-RUN (read-only, no mutations)' if DRY_RUN else 'LIVE'} — {time.strftime('%Y-%m-%d %H:%M:%S')}")
        log("=" * 74)
        from collections import Counter
        reads = f"status_ok={status_ok} cfg_ok={cfg_ok}"
        log(f"READS: {reads}")
        tb = Counter(i["tgtB"] for i in fleet.values())
        log(f"LIVE : {len(fleet)} BTC-BRL pmm_mister controller(s)"
            + (f" | tgtB {dict(sorted(tb.items()))}" if fleet else "")
            + f" | target fleet {ps['controllers']}")

        # ---- COLD START (guarded) ----
        if len(fleet) == 0:
            if not (status_ok and cfg_ok):
                log("\n*** HOLD: no BTC-BRL fleet seen BUT a status/config read failed → will NOT cold-start ***")
                log("    (guards the duplicate-fleet bug: we only cold-start when we're SURE none exists.)")
            else:
                configs = plan_cold_start(ps)
                budget = sum(float(x.get("total_amount_quote",0) or 0)*float(x.get("portfolio_allocation",0) or 0) for x in configs)
                log(f"\nCOLD START: no live BTC-BRL fleet exists → WOULD deploy {len(configs)} controllers as a new bot")
                log(f"    working capital ≈ {budget:,.0f} BRL | credentials_profile={ps.get('credentials_profile')}")
                for item in ps.get("cold_start", []):
                    cfg = item.get("config", {})
                    log(f"    [{item.get('source'):3}] {cfg.get('id'):26} spread {item.get('spread_bp')}bp tp {item.get('tp_bp')}bp "
                        f"eff {item.get('eff')}s tgtB {cfg.get('target_base_pct')}")
                await apply_cold_start(c, ps, configs)   # inert in dry
        else:
            # ---- MANAGE existing fleet ----
            have_perf = any(i["has_perf"] for i in fleet.values())
            if not have_perf:
                log("\n*** HOLD: fleet exists but no per-controller performance available → no ranking/substitution ***")
            else:
                yields = windowed_yield(fleet, float(ps["convergence"]["rank_window_hours"]))
                bench_sides = load_bench_sides()
                basis = "window" if any(v[1] == "window" for v in yields.values()) else "lifetime (history still accruing)"
                log(f"\nRANK by capital-normalized yield [{basis}] (bench: {len(bench_sides['vol'])} vol / {len(bench_sides['pnl'])} pnl candidates):")
                for co in ("vol", "pnl"):
                    members = sorted((cid for cid in fleet if cohort_of(cid) == co), key=lambda x: yields[x][0], reverse=True)
                    if members:
                        log(f"  [{co.upper()} cohort]")
                        for cid in members:
                            log(f"    {yields[cid][0]:+8.4f}  {cid[:40]:40} (pnl {fleet[cid]['pnl']:+.0f} / wc {fleet[cid]['wc']:.0f})")
                res = plan_substitution(fleet, yields, ps, bench_sides)
                log("\nCOHORT WINNERS (exploit targets): " + (", ".join(f"{co}={w[:20]}" for co, w in res["winners"].items()) or "none"))
                log(f"SUBSTITUTION PLAN (by-side): {len(res['losers'])} loser(s) < threshold, churn budget {res['budget']}/h, "
                    f"~{res['n_explore']} explore / rest exploit | live cohort counts {res['cohort_counts']}")
                if res.get("unclassified"):
                    log(f"    ({len(res['unclassified'])} unclassified controller(s) skipped — no vol/pnl tag)")
                if not res["plan"]:
                    log("    ✓ no substitutions (no persistent losers under threshold, or budget 0)")
                for a in res["plan"]:
                    if a["src"] == "refuse":
                        log(f"    REFUSE [{a.get('cohort')}] {a['cid'][:30]} — {a['why']}")
                    else:
                        log(f"    WOULD swap [{a['cohort']}] {a['cid'][:28]:28} (y {a['y']:+.4f}) → {a['src']}")
                for a in res["plan"]:
                    await apply_sub(c, a)

        log("\nRISK (3 tiers: substitution < brake < global_kill):")
        risk_check(fleet, ps)
        log("=" * 74)
    finally:
        try:
            await c.close()
        except Exception:
            pass


async def main(loop=False, interval=900):
    if not loop:
        await tick(); return
    while True:
        try:
            await tick()
        except Exception as e:
            log(f"tick error: {type(e).__name__} {str(e)[:100]}")
        await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(main(loop="--loop" in sys.argv))
