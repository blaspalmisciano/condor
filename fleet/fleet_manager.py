#!/usr/bin/env python
"""fleet_manager.py — the single declarative control loop for the brigado BTC-BRL MM fleet.

ONE manager, ONE loop. Each tick:  load_spec → read_live → rank → substitute → risk → report.

FINALIZED design (fleet/spec.json):
  • EXPLOIT = converge losers' CONFIG onto the LIVE winner (ranked by capital-normalized yield).
    Never a backtest winner — backtest != live.
  • EXPLORE = a minority of replacements come from a fresh backtest candidate (bench), which must
    prove out LIVE before it's trusted as an exploit target. (Bench wired in step 7; stubbed here.)
  • Substitution trigger = persistent negative normalized yield over a patience window + churn cap,
    so the fleet self-sizes its own exploration and converges at the config level.
  • Risk, softest→hardest: substitution < brake (gradual de-risk, never sells) < global_kill
    (max-loss → halt quoting + alert, no dumping).

SAFETY: DRY_RUN=True → READS and PLANS only; every mutation is written but guarded inert.
Liveness is MQTT-robust. A telemetry blip (all-zero PnL AND volume) → HOLD, plan nothing
(phantom zeros must never be read as "everyone's a loser"). Never touches a non-pmm_mister/
wrong-pair controller.
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
SNAP_PATH = "fleet/yield_snapshots.json"   # rolling per-controller cumulative PnL (for windowed yield)
SUBS_PATH = "fleet/sub_times.json"         # recent substitution timestamps (churn cap)
BRIGADO = "http://100.94.144.46:8000"
USER, PW = _os.getenv("HB_USER"), _os.getenv("HB_PASS")


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
    return cfg.get("controller_name") == "pmm_mister" and (cfg.get("trading_pair") or "").upper() == "BTC-BRL"


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


# ---------------------------------------------------------------- 2. read_live (MQTT-robust)
async def read_live(c, pair="BTC-BRL") -> dict:
    """Liveness from active-bots status (survives MQTT blips); configs + perf from gather_active."""
    try:
        status = await c.bot_orchestration.get_active_bots_status()
        active = set((status.get("data") or status or {}).keys())
    except Exception as e:
        log(f"  (active-bots status unavailable: {str(e)[:60]}; using telemetry presence)")
        active = None
    live_perf, cfg_by, bot_names, health, cid_bot = await gather_active(c)
    fleet = {}
    for cid, cfg in cfg_by.items():
        if (cfg.get("trading_pair") or "").upper() != pair:
            continue
        bot = cid_bot.get(cid)
        on_active = (active is None) or (bot in active)
        if not (on_active and cid in live_perf):
            continue
        lp = live_perf.get(cid, {})
        tq = float(cfg.get("total_amount_quote") or 0)
        al = float(cfg.get("portfolio_allocation") or 0)
        wc = tq * al  # working capital
        pnl = float(lp.get("realized", 0) or 0) + float(lp.get("unrealized", 0) or 0)
        fleet[cid] = {"cfg": cfg, "bot": bot,
                      "tgtB": round(float(cfg.get("target_base_pct") or 0), 3),
                      "alloc": cfg.get("portfolio_allocation"), "wc": wc,
                      "pnl": pnl, "vol": float(lp.get("volume_traded", 0) or 0)}
    return fleet


def telemetry_reliable(fleet: dict) -> bool:
    """MQTT fail-safe: if the WHOLE fleet reads 0 PnL AND 0 volume, telemetry is blipping."""
    if not fleet:
        return False
    total_abs_pnl = sum(abs(i["pnl"]) for i in fleet.values())
    total_vol = sum(i["vol"] for i in fleet.values())
    return not (total_abs_pnl == 0 and total_vol == 0)


# ---------------------------------------------------------------- 3. rank + substitution (plan only)
def windowed_yield(fleet: dict, window_h: float) -> dict:
    """Per-controller normalized yield over the window, using the snapshot store.
    Returns {cid: (yield, basis)} where basis = 'window' or 'lifetime' (not enough history yet)."""
    now = time.time()
    snaps = _load(SNAP_PATH, [])
    # record this tick
    snaps.append({"ts": now, "cum": {cid: i["pnl"] for cid, i in fleet.items()}})
    snaps = [s for s in snaps if now - s["ts"] <= max(window_h, 48) * 3600]  # keep ≤48h
    _save(SNAP_PATH, snaps)
    # find the snapshot closest to now-window
    past = [s for s in snaps if now - s["ts"] >= window_h * 3600]
    ref = past[-1] if past else None
    out = {}
    for cid, i in fleet.items():
        wc = i["wc"] or 1e-9
        if ref and cid in ref["cum"]:
            dpnl = i["pnl"] - ref["cum"][cid]
            out[cid] = (dpnl / wc, "window")
        else:
            out[cid] = (i["pnl"] / wc, "lifetime")  # fallback until history accrues
    return out


def plan_substitution(fleet: dict, yields: dict, pair_spec: dict, bench: list) -> dict:
    """PLAN ONLY. Identify persistent losers, pick the live winner (exploit source),
    assign replacements from the two-source pool (exploit=winner config / explore=bench candidate)."""
    sub = pair_spec["substitution"]
    thresh = float(sub["yield_threshold"])
    pool = sub["replacement_pool"]
    explore_ratio = float(pool["explore_backtest_candidate_ratio"])

    # live winner = best normalized yield (exploit source). Never a backtest winner.
    ranked = sorted(fleet.keys(), key=lambda cid: yields[cid][0], reverse=True)
    winner = ranked[0]
    winner_cfg = fleet[winner]["cfg"]

    # persistent losers = yield below threshold, WORST FIRST (so a tight churn budget fixes the
    # biggest bleeders first). basis='window' is the real signal; 'lifetime' is a weak proxy
    # until history accrues.
    losers = sorted((cid for cid in fleet if yields[cid][0] < thresh and cid != winner),
                    key=lambda cid: yields[cid][0])

    # churn cap
    now = time.time()
    recent = [t for t in _load(SUBS_PATH, []) if now - t < 3600]
    budget = max(0, int(sub["max_subs_per_hour"]) - len(recent))
    capped = losers[:budget]

    # two-source assignment: a minority → explore (bench), rest → exploit (winner config)
    n_explore = round(len(capped) * explore_ratio)
    plan = []
    for idx, cid in enumerate(capped):
        loser = fleet[cid]
        if not invariant_ok(loser["cfg"]):
            plan.append({"cid": cid, "src": "refuse", "why": "invariant"}); continue
        use_explore = idx < n_explore and len(bench) > 0
        if use_explore:
            cand = bench[idx % len(bench)]
            desired = _norm({**cand, "id": cid, "portfolio_allocation": loser["alloc"]})
            src = "explore(backtest candidate)"
        else:
            # EXPLOIT: adopt the LIVE winner's config (params), keep own id + allocation
            desired = _norm({**winner_cfg, "id": cid, "portfolio_allocation": loser["alloc"]})
            src = f"exploit(live winner {winner[:18]})"
        if not invariant_ok(desired):
            plan.append({"cid": cid, "src": "refuse", "why": "desired fails invariant"}); continue
        plan.append({"cid": cid, "bot": loser["bot"], "src": src, "desired": desired,
                     "y": yields[cid][0]})
    return {"winner": winner, "winner_y": yields[winner][0], "losers": losers,
            "budget": budget, "n_explore": n_explore, "plan": plan, "ranked": ranked}


# live mutation — WRITTEN but INERT under DRY_RUN
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


# ---------------------------------------------------------------- 4. risk (report only)
def _price_ret(hours):
    url = f"https://api.binance.com/api/v3/klines?symbol=BTCBRL&interval=5m&limit={int(hours*12)+3}"
    d = json.loads(urllib.request.urlopen(url, timeout=15).read())
    closes = [(float(k[0]) / 1000.0, float(k[4])) for k in d]
    now = closes[-1][1]
    def ret(h):
        tgt = closes[-1][0] - h * 3600
        return now / min(closes, key=lambda x: abs(x[0] - tgt))[1] - 1.0
    return now, ret(2), ret(12)


def risk_check(fleet: dict, yields_reliable: bool, pair_spec: dict):
    brake = pair_spec["risk"]["brake"]
    gk = pair_spec["risk"]["global_kill"]
    # TIER 2 — brake (gradual de-risk on drawdown/drift; never sells)
    try:
        px, r2, r12 = _price_ret(2)
        would = "HOLD"
        if r2 < -0.015 or r12 < -0.03:
            would = f"DE-RISK (lower tgtB toward floor {brake['floor_tgtB']}; never sells)"
        log(f"  brake      : price {px:,.0f} | 2h {r2:+.2%} | 12h {r12:+.2%} → WOULD {would}")
    except Exception as e:
        log(f"  brake      : price read failed ({str(e)[:40]})")
    # TIER 3 — global kill (max-loss → halt + alert, no dump)
    maxloss = gk.get("max_loss_brl")
    if maxloss is None:
        log("  global_kill: DISABLED (max_loss_brl not set — tune it in spec)")
    elif yields_reliable:
        net = sum(i["pnl"] for i in fleet.values())
        if net <= -abs(float(maxloss)):
            log(f"  global_kill: *** WOULD HALT + ALERT *** fleet net {net:+,.0f} ≤ -{abs(float(maxloss)):,.0f} (no dump)")
        else:
            log(f"  global_kill: ok (net {net:+,.0f} vs limit -{abs(float(maxloss)):,.0f})")
    else:
        log("  global_kill: can't evaluate (PnL unreliable — MQTT blip)")


# ---------------------------------------------------------------- 5. tick / report
async def load_bench() -> list:
    """EXPLORE pool — fresh backtest candidates. Wired in step 7 (continuous brigado backtest).
    Stubbed empty for now; the consumer falls back to the exploit source when empty."""
    return _load("fleet/bench.json", [])


async def tick():
    spec = load_spec()
    ps = spec["BTC-BRL"]
    c = await client()
    try:
        fleet = await read_live(c, "BTC-BRL")
        log("=" * 72)
        log(f"FLEET MANAGER — {'DRY-RUN (read-only, no mutations)' if DRY_RUN else 'LIVE'} — {time.strftime('%Y-%m-%d %H:%M:%S')}")
        log("=" * 72)
        from collections import Counter
        tb = Counter(i["tgtB"] for i in fleet.values())
        log(f"LIVE : {len(fleet)}/{ps['controllers']} controllers | tgtB {dict(sorted(tb.items()))} | "
            f"alloc {dict(Counter(str(i['alloc']) for i in fleet.values()))}")

        reliable = telemetry_reliable(fleet)
        if not reliable:
            log("\n*** YIELD DATA UNRELIABLE (MQTT telemetry blip: whole fleet reads 0 PnL AND 0 vol) ***")
            log("    → HOLD: planning NO substitutions. (Phantom zeros must not be read as 'all losers'.)")
        else:
            yields = windowed_yield(fleet, float(ps["convergence"]["rank_window_hours"]))
            bench = await load_bench()
            basis = "window" if any(v[1] == "window" for v in yields.values()) else "lifetime (history still accruing)"
            log(f"\nRANK by capital-normalized yield [{basis}]:")
            for cid in sorted(fleet, key=lambda x: yields[x][0], reverse=True):
                log(f"    {yields[cid][0]:+7.3f}  {cid[:40]:40} (wc {fleet[cid]['wc']:.0f} BRL)")
            res = plan_substitution(fleet, yields, ps, bench)
            log(f"\nEXPLOIT target (live winner): {res['winner'][:40]}  yield {res['winner_y']:+.3f}")
            log(f"SUBSTITUTION PLAN: {len(res['losers'])} loser(s) < threshold, "
                f"churn budget {res['budget']}/h, ~{res['n_explore']} explore / rest exploit")
            if not res["plan"]:
                log("    ✓ no substitutions (no persistent losers under threshold, or budget 0)")
            for a in res["plan"]:
                if a["src"] == "refuse":
                    log(f"    REFUSE {a['cid'][:34]} — {a['why']}")
                else:
                    nt = a["desired"].get("target_base_pct")
                    log(f"    WOULD swap {a['cid'][:30]:30} (y {a['y']:+.3f}) → {a['src']}  [tgtB→{nt}]")
            for a in res["plan"]:
                await apply_sub(c, a)

        log("\nRISK (3 tiers: substitution < brake < global_kill):")
        risk_check(fleet, reliable, ps)
        log("=" * 72)
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
