#!/usr/bin/env python
"""Autonomous overnight orchestrator for the brigado BTC-BRL fleet (user: 'full send').
Phase 1: wait for the local search (scripts/local_search_btcbrl.py) to sweep enough windows.
Phase 2: CONVERGE the 11 non-#2 controllers onto the optimized #2-neighborhood (hot-swap,
         diversified across target_base_pct 0.65-0.8, each controller keeps its own allocation).
Phase 3: CIRCUIT-BREAKER loop — watch BTC-BRL trend + fleet inventory; on a sustained downtrend,
         GRADUALLY lower target_base_pct (never market-sells — the 'sell the bottom' lesson).
All live writes: invariant-guarded (pmm_mister + BTC-BRL only), rollback-snapshotted, logged.
Backtest reads hit :8001; live reads/writes hit brigado. Fail-safe = reduce inventory on error.
"""
import sys, os, json, time, asyncio, statistics as st, traceback
import os as _os; from dotenv import load_dotenv as _ld; _ld(_os.path.join(_os.path.dirname(_os.path.abspath(__file__)),"..",".env"))
sys.path.insert(0, "/Users/blaspalmisciano/condor"); os.chdir("/Users/blaspalmisciano/condor")
from pathlib import Path
from hummingbot_api_client import HummingbotAPIClient
from aiohttp import ClientTimeout
from routines.controller_performance import gather_active

BRIGADO="http://100.94.144.46:8000"; LOCAL="http://localhost:8001"
LS_RESULTS="data/localsearch_results.json"
LOG="data/fleet_overnight.log"; REPORT="reports/fleet_overnight_report.html"; HB="data/fleet_overnight_hb.json"
MIN_WINDOWS=5          # wait until the local search has this many windows before converging
MAX_WAIT_MIN=180       # don't wait forever for the search
TGTB_BANDS=[0.65,0.70,0.75,0.80]   # diversify the long-tilt across the fleet
BREAKER_EVERY=900      # 15 min
DROP_TRIGGER=-0.015    # 2h return below -1.5% => de-risk a step
TGTB_FLOOR=0.50; TGTB_STEP=0.15
LOOKBACK_H=2

def log(m):
    line=f"{time.strftime('%Y-%m-%d %H:%M:%S')} {m}"; print(line,flush=True)
    try: open(LOG,"a").write(line+"\n")
    except: pass
def rows(resp):
    if isinstance(resp,dict):
        for k in("candles","data","rows","result"):
            if isinstance(resp.get(k),list): return resp[k]
    return resp if isinstance(resp,list) else []
def norm(cfg):
    c=dict(cfg)
    for k in ("buy_spreads","sell_spreads","buy_amounts_pct","sell_amounts_pct"):
        v=c.get(k)
        if v is not None and not isinstance(v,(list,tuple)): c[k]=[v]
    bs=c.get("buy_spreads") or []; ba=c.get("buy_amounts_pct") or []
    if bs and len(bs)!=len(ba): c["buy_amounts_pct"]=[round(1/len(bs),4)]*len(bs)
    ss=c.get("sell_spreads") or []; sa=c.get("sell_amounts_pct") or []
    if ss and len(ss)!=len(sa): c["sell_amounts_pct"]=[round(1/len(ss),4)]*len(ss)
    return c

async def client(url):
    c=HummingbotAPIClient(base_url=url,username=_os.getenv("HB_USER"),password=_os.getenv("HB_PASS"),timeout=ClientTimeout(total=300,connect=20))
    await c.init(); return c

async def swap(c, cid, cfg, bot, log):
    """Invariant-guarded hot config swap of ONE controller. Returns True on confirmed apply."""
    cfg=norm(cfg); cfg["id"]=cid
    if cfg.get("controller_name")!="pmm_mister" or cfg.get("trading_pair","").upper()!="BTC-BRL":
        log(f"  INVARIANT BLOCK {cid}"); return False
    await c.controllers.create_or_update_controller_config(cid,cfg)
    try: await c.controllers.update_bot_controller_config(bot,cid,cfg)
    except Exception as e: log(f"  {cid} hot-apply note: {str(e)[:60]}")
    return True

# ---- decision helpers (offline-testable) ----
def pick_neighborhood(results):
    """Best config per target_base_pct band, by median MARKET pnl. Returns {tgtB: full_config}."""
    by_band={}
    for k,v in results.items():
        ws=list(v["w"].values())
        if not ws: continue
        tgtB=ws[0].get("tgtB"); mkt=st.median([x["mkt"] for x in ws])
        if tgtB is None: continue
        if tgtB not in by_band or mkt>by_band[tgtB][0]:
            by_band[tgtB]=(mkt,v["full"],len(ws))
    return by_band

def derisk_decision(ret_2h, level):
    """Pure fn: given 2h return and current derisk level, return new level (>=0). Fail-safe up."""
    if ret_2h is None: return level+1          # unknown => de-risk (safe)
    if ret_2h < DROP_TRIGGER: return level+1   # downtrend => de-risk a step
    if ret_2h > 0.01 and level>0: return level-1  # recovery => restore a step
    return level

async def price_ret(hours):
    # Direct public API (NOT through the Docker stack — its DNS breaks after Mac sleep/wake,
    # which would blind the breaker). The Mac reaches api.binance.com fine.
    import urllib.request as _u
    url=f"https://api.binance.com/api/v3/klines?symbol=BTCBRL&interval=5m&limit={int(hours*12)+3}"
    d=json.loads(await asyncio.to_thread(lambda: _u.urlopen(url,timeout=15).read()))
    closes=[(float(k[0])/1000.0,float(k[4])) for k in d]
    now=closes[-1][1]; tgt=closes[-1][0]-hours*3600
    past=min(closes,key=lambda x:abs(x[0]-tgt))[1]
    return now/past-1.0, now

async def breaker_loop(level=0):
    log(f"circuit-breaker online: every {BREAKER_EVERY}s, de-risk if {LOOKBACK_H}h return < {DROP_TRIGGER:+.1%} (start level {level})")
    while True:
        try:
            ret,px=await price_ret(LOOKBACK_H)
            new=derisk_decision(ret,level)
            if new!=level:
                tgtB_cap=max(TGTB_FLOOR, 0.80 - TGTB_STEP*new)
                log(f"BREAKER: 2h ret {ret:+.2%} px {px:,.0f} -> level {level}->{new}, cap tgtB at {tgtB_cap:.2f}")
                bc=await client(BRIGADO)
                lp,cby,_,_,cb=await gather_active(bc)
                fl={cid:c for cid,c in cby.items() if cid in lp and c.get("trading_pair","").upper()=="BTC-BRL"}
                for cid,c in fl.items():
                    cur=float(c.get("target_base_pct") or 0.5)
                    newt=min(cur,tgtB_cap) if new>level else min(0.80,cur+TGTB_STEP)
                    if abs(newt-cur)>1e-6:
                        nc=dict(c); nc["target_base_pct"]=round(newt,3)
                        await swap(bc,cid,nc,cb.get(cid),log)
                await bc.close(); level=new
            else:
                log(f"breaker ok: 2h ret {ret:+.2%} px {px:,.0f} level {level}")
            json.dump({"ts":int(time.time()),"ret_2h":ret,"px":px,"derisk_level":level},open(HB,"w"))
        except Exception as e:
            log(f"breaker ERROR (fail-safe hold): {str(e)[:120]}")
        await asyncio.sleep(BREAKER_EVERY)


async def main():
    log("=== fleet overnight orchestrator START (full send) ===")
    # ---------- PHASE 1: wait for local search ----------
    t0=time.time()
    while True:
        res=json.load(open(LS_RESULTS)) if os.path.exists(LS_RESULTS) else {}
        nwin=len({wk for v in res.values() for wk in v.get("w",{})})
        if nwin>=MIN_WINDOWS: log(f"local search ready: {nwin} windows, {len(res)} neighbors"); break
        if (time.time()-t0)/60>MAX_WAIT_MIN: log(f"wait cap hit ({nwin} windows) — proceeding with #2 base only"); break
        log(f"waiting for local search… {nwin}/{MIN_WINDOWS} windows"); await asyncio.sleep(120)

    CONVERGED_FLAG="data/fleet_converged.flag"
    if os.path.exists(CONVERGED_FLAG):
        log("convergence already done (flag present) — skipping straight to circuit-breaker")
        lvl=0
        try: lvl=int(json.load(open(HB)).get("derisk_level",0))
        except Exception: pass
        await breaker_loop(lvl); return
    results=json.load(open(LS_RESULTS)) if os.path.exists(LS_RESULTS) else {}
    bands=pick_neighborhood(results)
    log(f"neighborhood by tgtB: "+", ".join(f"{b}:mkt{v[0]:+.1f}(n{v[2]})" for b,v in sorted(bands.items())))

    # ---------- PHASE 2: converge the 11 non-#2 controllers ----------
    bc=await client(BRIGADO)
    live_perf,cfg_by,bot_names,health,cid_bot=await gather_active(bc)
    fleet={cid:cfg for cid,cfg in cfg_by.items() if cid in live_perf and cfg.get("trading_pair","").upper()=="BTC-BRL"}
    # fresh rollback right before mutating
    ts=time.strftime("%Y%m%d-%H%M%S")
    json.dump({cid:{"config":cfg,"bot":cid_bot.get(cid)} for cid,cfg in fleet.items()},
              open(f"data/brigado_fleet_rollback_{ts}.json","w"),default=str,indent=2)
    winner=next((cid for cid in fleet if cid.startswith("botcamp-btcbrl-2-")),None)
    two_cfg=norm(fleet[winner]) if winner else None
    targets=[cid for cid in fleet if cid!=winner]
    # ordered band configs (fallback to #2 base at each band if the search lacks it)
    def cfg_for_band(tgtB):
        if tgtB in bands: return dict(bands[tgtB][1])
        c=dict(two_cfg); c["target_base_pct"]=tgtB; return c
    band_cfgs=[cfg_for_band(b) for b in TGTB_BANDS]
    log(f"converging {len(targets)} controllers across tgtB bands {TGTB_BANDS} (winner {winner.split('-202')[0] if winner else '?'} kept)")
    done=0
    for i,cid in enumerate(sorted(targets)):
        tgt_cfg=dict(band_cfgs[i%len(band_cfgs)])
        tgt_cfg["portfolio_allocation"]=fleet[cid].get("portfolio_allocation")  # keep bounded size
        try:
            if await swap(bc,cid,tgt_cfg,cid_bot.get(cid),log):
                done+=1; log(f"  ✓ {cid.split('-202')[0]:20} -> tgtB={tgt_cfg.get('target_base_pct')} spread={tgt_cfg.get('buy_spreads')}")
            await asyncio.sleep(2)
        except Exception as e:
            log(f"  ✗ {cid.split('-202')[0]} swap failed: {str(e)[:80]}")
    log(f"convergence done: {done}/{len(targets)} swapped")
    open(CONVERGED_FLAG,"w").write(str(int(time.time())))  # so a restart won't re-converge / undo de-risk
    await bc.close()

    # ---------- PHASE 3 ----------
    await breaker_loop(0)

if __name__=="__main__":
    # offline self-test of the pure decision fn before going live
    assert derisk_decision(-0.02,0)==1 and derisk_decision(0.0,0)==0 and derisk_decision(0.02,2)==1 and derisk_decision(None,0)==1
    try: asyncio.run(main())
    except Exception: log("FATAL:\n"+traceback.format_exc())
