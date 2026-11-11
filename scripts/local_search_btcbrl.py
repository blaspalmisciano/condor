#!/usr/bin/env python
"""Tight LOCAL search around the live winner (botcamp-btcbrl-2): sweep its neighborhood on the
LOCAL stack (:8001, never brigado) over recent windows, ranked by MARKET PnL (realized+unrealized)
— because live, #2's edge is inventory/swing capture, not volume/rebate. Output: the optimized
neighborhood to converge the fleet toward. Runs until done; writes a ranked report + JSON."""
import sys, os, json, time, asyncio, statistics as st
import os as _os; from dotenv import load_dotenv as _ld; _ld(_os.path.join(_os.path.dirname(_os.path.abspath(__file__)),"..",".env"))
sys.path.insert(0, "/Users/blaspalmisciano/condor"); os.chdir("/Users/blaspalmisciano/condor")
from pathlib import Path
import routines.pmm_sweep as S
S.SWEEP_STORE = Path("data/localsearch_store.json")
from routines.pmm_sweep import run_sweep, score, risk_score, apply_params, Config as SweepConfig
from hummingbot_api_client import HummingbotAPIClient
from aiohttp import ClientTimeout

RESULTS="data/localsearch_results.json"; REPORT="reports/btcbrl_local_search.html"; LOG="data/localsearch.log"
WIN_H=4; N_WINDOWS=8; REBATE=0.00015

def log(m):
    line=f"{time.strftime('%H:%M:%S')} {m}"; print(line,flush=True)
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
    return c

async def main():
    log("=== local search around botcamp-btcbrl-2 START ===")
    # load #2's exact live config from the freshest rollback snapshot
    snaps=sorted(Path("data").glob("brigado_fleet_rollback_*.json"))
    snap=json.load(open(snaps[-1]))
    base=next(v["config"] for k,v in snap.items() if k.startswith("botcamp-btcbrl-2-"))
    base=norm(base); base["id"]="localsearch_base"
    log(f"base(#2): tgtB={base.get('target_base_pct')} spread={base.get('buy_spreads')} tp={base.get('take_profit')} eff={base.get('buy_position_effectivization_time')}")
    scfg=SweepConfig(resolution="1s",trade_cost=0.0001,rebate_rate=REBATE,max_trials=40,pause_s=1.0)
    c=HummingbotAPIClient(base_url="http://localhost:8001",username=_os.getenv("HB_USER"),password=_os.getenv("HB_PASS"),timeout=ClientTimeout(total=1800,connect=15))
    await c.init(); log("client -> :8001")
    results=json.load(open(RESULTS)) if os.path.exists(RESULTS) else {}

    pass_n=0
    while True:
        pass_n+=1
        r=rows(await c.market_data.get_candles_last_days("binance","BTC-BRL",10,"1m"))
        ts=sorted(float(x["timestamp"]) for x in r)
        # recent disjoint windows (newest-first) — agile, recent regime
        wins=[]; w1=int(ts[-1])
        while w1-WIN_H*3600>=ts[0] and len(wins)<N_WINDOWS:
            wins.append((w1-WIN_H*3600,w1)); w1-=WIN_H*3600
        log(f"pass {pass_n}: {len(r)} candles -> {len(wins)} recent {WIN_H}h windows")
        for wi,(w0,w1) in enumerate(wins):
            cw=[x for x in r if w0-60<=float(x["timestamp"])<=w1+60]
            if len(cw)<50: continue
            ref=sum(float(x["close"]) for x in cw)/len(cw)
            wid=time.strftime("%m%d-%H%M",time.localtime(w0))
            try:
                trials=await asyncio.wait_for(run_sweep(c,base,w0,w1,scfg,ref,log=lambda m:None),timeout=2400)
            except Exception as e:
                log(f"  win {wid} err {str(e)[:70]}"); continue
            for t in trials:
                if "error" in t: continue
                sc=score(t,REBATE); rs=risk_score(t,REBATE)
                full=apply_params(base,t.get("params") or {})
                k=json.dumps(t.get("params") or {},sort_keys=True)
                row=results.setdefault(k,{"params":t.get("params") or {},"full":full,"w":{}})
                row["w"][wid]={"mkt":round(t["realized"]+t["unrealized"],2),"vol":round(t["volume"]),
                               "biz":round(sc["business_pnl"],2),"dd":round(t["max_dd"],1),
                               "tgtB":full.get("target_base_pct")}
            json.dump(results,open(RESULTS,"w"),default=str)
            # report: rank by median MARKET pnl
            summ=[]
            for k,v in results.items():
                ws=list(v["w"].values())
                if not ws: continue
                summ.append({"params":v["params"],"tgtB":ws[0].get("tgtB"),"nw":len(ws),
                    "mkt":st.median([x["mkt"] for x in ws]),"vol":st.median([x["vol"] for x in ws]),
                    "dd":min(x["dd"] for x in ws),"npos":sum(1 for x in ws if x["mkt"]>0)})
            summ.sort(key=lambda z:z["mkt"],reverse=True)
            head="<tr><th>#</th><th>params (Δ vs #2)</th><th>tgtB</th><th>nw</th><th>med MARKET pnl</th><th>med vol</th><th>worst dd</th><th>+win</th></tr>"
            body=""
            for i,s in enumerate(summ[:20],1):
                pstr=", ".join(f"{kk}={vv}" for kk,vv in (s["params"] or {}).items()) or "<i>=#2 base</i>"
                mcls="pos" if s["mkt"]>0 else "neg"
                body+=(f"<tr><td>{i}</td><td>{pstr}</td><td>{s['tgtB']}</td><td>{s['nw']}</td>"
                       f"<td class='{mcls}'>{s['mkt']:+.2f}</td><td>{s['vol']:,.0f}</td>"
                       f"<td class='neg'>{s['dd']:+.1f}</td><td>{s['npos']}/{s['nw']}</td></tr>")
            open(REPORT,"w").write(f"""<!doctype html><meta charset=utf-8><title>BTC-BRL Local Search (#2 neighborhood)</title>
<style>body{{font-family:system-ui;margin:24px;background:#0f1115;color:#e6e6e6}}table{{border-collapse:collapse;width:100%;font-size:12.5px}}
th,td{{border:1px solid #2a2f3a;padding:5px 7px;text-align:right}}th{{background:#1b1f28;color:#9aa}}td:nth-child(2),th:nth-child(2){{text-align:left}}
.pos{{color:#4ade80}}.neg{{color:#f87171}}.m{{color:#889;font-size:12px}}</style>
<h2>Local search around botcamp-btcbrl-2 — ranked by MARKET PnL</h2>
<p class=m>Updated {time.strftime('%H:%M:%S')} · pass {pass_n} · {len(summ)} neighbors · windows {len(set(wk for v in results.values() for wk in v['w']))} · local backtest 1s/{WIN_H}h</p>
<table>{head}{body}</table>""")
            log(f"  [{wid}] {len([t for t in trials if 'error' not in t])} ok, {len(results)} neighbors; top mkt {summ[0]['mkt']:+.2f} tgtB={summ[0]['tgtB']}")
        log(f"=== pass {pass_n} done: {len(results)} neighbors ===")

if __name__=="__main__":
    try: asyncio.run(main())
    except Exception as e:
        import traceback; log("FATAL:\n"+traceback.format_exc())
