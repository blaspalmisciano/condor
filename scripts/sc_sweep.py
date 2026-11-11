#!/usr/bin/env python
"""Self-contained BTC-BRL backtest sweep — NO Docker (container DNS breaks on sleep). Pulls 1s
candles from binance public REST (paginated) and simulates in pure Python (routines.gate_backtest).
Sweeps a real GRID over the params the sim can actually evaluate — spread x take-profit x
effectivization — ranks every config by VOLUME and by PnL separately, and regenerates a clear HTML
proposal each pass. Runs continuously (re-fetch + re-sweep every INTERVAL).

NOTE: the candle-cross sim does NOT model target_base_pct (inventory tilt) — that was the LIVE edge
and must be chosen live, not from backtest. So tilt is held fixed here and is NOT a sweep axis
(sweeping it would produce identical rows). This sweep ranks the microstructure params only.
"""
import sys, os, json, time, urllib.request, traceback
sys.path.insert(0, "/Users/blaspalmisciano/condor"); os.chdir("/Users/blaspalmisciano/condor")
from routines.gate_backtest import simulate, score_over_windows

RESULTS="data/sc_sweep_results.json"; REPORT="reports/btcbrl_backtest_proposal.html"; LOG="data/sc_sweep.log"
FEE=0.0001; CANDLE_SEC=1; HOURS=8; N_WINDOWS=8; INTERVAL=1800; TOPN=15

# --- sim-meaningful grid (real units) ---
SPREADS_BP=[0.5,0.75,1.0,1.5,2.0,3.0]      # buy=sell spread, basis points
TPS_BP=[1.0,1.5,2.0]                        # take-profit, bp
EFFS=[150,300,600,900]                      # effectivization work-out, seconds
FIXED_TGTB=0.65                             # tilt is a LIVE decision, not swept (sim ignores it)

def log(m):
    line=f"{time.strftime('%Y-%m-%d %H:%M:%S')} {m}"; print(line,flush=True)
    try: open(LOG,"a").write(line+"\n")
    except Exception: pass

def fetch_1s(hours=HOURS):
    end=int(time.time()*1000); cur=end-hours*3600*1000; got=[]; calls=0
    while cur<end and calls<hours*4+5:
        url=f"https://api.binance.com/api/v3/klines?symbol=BTCBRL&interval=1s&startTime={cur}&limit=1000"
        batch=json.load(urllib.request.urlopen(url,timeout=20)); calls+=1
        if not batch: break
        got+=batch; cur=batch[-1][0]+1000; time.sleep(0.08)
    return [{"open":float(k[1]),"high":float(k[2]),"low":float(k[3]),"close":float(k[4]),
             "volume":float(k[5]),"timestamp":k[0]/1000} for k in got]

def canonical_base():
    pool=json.load(open("data/pmm_autopilot_pool.json"))
    for e in pool:
        fc=e.get("full_config",{})
        if e.get("is_base") and fc.get("trading_pair","").upper()=="BTC-BRL":
            c=dict(fc)
            for k in ("buy_spreads","sell_spreads","buy_amounts_pct","sell_amounts_pct"):
                v=c.get(k)
                if v is not None and not isinstance(v,(list,tuple)): c[k]=[v]
            return c
    raise RuntimeError("no BTC-BRL base in pool")

def build_grid(base):
    out=[]
    for sp in SPREADS_BP:
        for tp in TPS_BP:
            for eff in EFFS:
                c=dict(base)
                c["buy_spreads"]=[round(sp/1e4,8)]; c["sell_spreads"]=[round(sp/1e4,8)]
                c["buy_amounts_pct"]=["1"]; c["sell_amounts_pct"]=["1"]
                c["take_profit"]=round(tp/1e4,8)
                c["buy_position_effectivization_time"]=eff; c["sell_position_effectivization_time"]=eff
                c["target_base_pct"]=FIXED_TGTB
                out.append({"spread_bp":sp,"tp_bp":tp,"eff":eff,"cfg":c})
    return out

def sweep(grid,candles):
    rows=[]
    for g in grid:
        try:
            full=simulate(candles,g["cfg"],FEE,CANDLE_SEC)
            sw=score_over_windows(candles,g["cfg"],FEE,N_WINDOWS,CANDLE_SEC)
            rows.append({"spread_bp":g["spread_bp"],"tp_bp":g["tp_bp"],"eff":g["eff"],
                         "full_config":g["cfg"],"volume":round(full["volume"]),"pnl":round(full["pnl"],2),
                         "fills":full["fills"],"median_pnl":sw["median_pnl"],"n_pos":sw["n_positive"],"n_win":sw["n_windows"]})
        except Exception as e:
            log(f"  cfg sp{g['spread_bp']}/tp{g['tp_bp']}/eff{g['eff']} err: {str(e)[:60]}")
    return rows

def write_html(rows,meta):
    byvol=sorted(rows,key=lambda r:r["volume"],reverse=True)[:TOPN]
    bypnl=sorted(rows,key=lambda r:r["pnl"],reverse=True)[:TOPN]
    def tbl(rs,hl):
        h="<tr><th>#</th><th>spread</th><th>take-profit</th><th>effectiv.</th><th>volume</th><th>PnL (net)</th><th>median/win</th><th>+win</th></tr>"
        body=""
        for i,r in enumerate(rs,1):
            pc="pos" if r["pnl"]>0 else "neg"; mc="pos" if r["median_pnl"]>0 else "neg"
            body+=(f"<tr><td>{i}</td><td>{r['spread_bp']}bp</td><td>{r['tp_bp']}bp</td><td>{r['eff']}s</td>"
                   f"<td class='{'hl' if hl=='v' else ''}'>{r['volume']:,}</td>"
                   f"<td class='{pc} {'hl' if hl=='p' else ''}'>{r['pnl']:+,.2f}</td>"
                   f"<td class='{mc}'>{r['median_pnl']:+,.1f}</td><td>{r['n_pos']}/{r['n_win']}</td></tr>")
        return f"<table>{h}{body}</table>"
    html=f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BTC-BRL Backtest Proposal</title><style>
body{{font-family:-apple-system,system-ui,sans-serif;background:#0f1115;color:#e6e8ec;margin:0;padding:26px 16px;line-height:1.45}}
.wrap{{max-width:880px;margin:0 auto}} h1{{font-size:20px;margin:0 0 4px}} h2{{font-size:15px;color:#8ab4ff;margin:24px 0 6px}}
.m{{color:#858e9c;font-size:12.5px}} table{{border-collapse:collapse;width:100%;font-size:12.5px;margin-top:6px}}
th,td{{border:1px solid #262b36;padding:5px 9px;text-align:right}} th{{background:#1b1f28;color:#9aa}}
.pos{{color:#4ade80}} .neg{{color:#f87171}} .hl{{background:#17202e;font-weight:700}}
.box{{background:#141821;border:1px solid #262b36;border-radius:8px;padding:12px 15px;margin:10px 0;font-size:12.8px}}
.warn{{background:#2a1f1a;border-left:3px solid #f0a060}} code{{background:#0c0e13;border:1px solid #262b36;padding:1px 5px;border-radius:4px;font-size:11px}}</style></head><body><div class="wrap">
<h1>BTC-BRL — Backtest Proposal</h1>
<p class="m">Updated {meta['ts']} · pass {meta['pass']} · <b>{meta['n']} distinct configs</b> (grid: {len(SPREADS_BP)} spreads × {len(TPS_BP)} TPs × {len(EFFS)} effectiv.) · candles: binance BTCBRL <b>1s</b>, {meta['span']:.1f}h · fee {FEE*1e4:.0f}bp · {N_WINDOWS} windows · pure-Python sim.</p>
<div class="box warn"><b>Read first:</b> every row is a <b>distinct, deployable</b> config (its exact spread / take-profit / effectivization shown). The sim does <b>not</b> model inventory tilt (<code>target_base_pct</code>, held at {FIXED_TGTB}) — that was the <i>live</i> edge and is a separate live choice, not a backtest axis. Backtest PnL runs near/below zero (real edge is live inventory+rebate) → treat <b>volume as the robust signal</b>, PnL as relative ranking, and live-validate. All {meta['n']} full configs: <code>data/sc_sweep_results.json</code>.</div>
<h2>🔵 Best for VOLUME (top {TOPN})</h2>{tbl(byvol,'v')}
<h2>🟢 Best for PnL (top {TOPN})</h2>{tbl(bypnl,'p')}
</div></body></html>"""
    os.makedirs("reports",exist_ok=True); tmp=REPORT+".tmp"; open(tmp,"w").write(html); os.replace(tmp,REPORT)

def main():
    log("=== self-contained BTC-BRL 1s sweep START ===")
    base=canonical_base(); grid=build_grid(base)
    log(f"grid: {len(grid)} distinct configs (spread×tp×eff)")
    p=0
    while True:
        p+=1
        try:
            candles=fetch_1s()
            if len(candles)<500: log(f"only {len(candles)} candles — retry next pass"); time.sleep(300); continue
            span=(candles[-1]["timestamp"]-candles[0]["timestamp"])/3600
            rows=sweep(grid,candles)
            json.dump({"ts":int(time.time()),"pass":p,"rows":rows},open(RESULTS,"w"),default=str)
            write_html(rows,{"ts":time.strftime("%Y-%m-%d %H:%M:%S"),"pass":p,"n":len(rows),"span":span})
            tv=sorted(rows,key=lambda r:r["volume"],reverse=True)[0]
            log(f"pass {p}: {len(rows)} configs | {len(candles)} 1s candles ({span:.1f}h) | top-vol {tv['spread_bp']}bp/tp{tv['tp_bp']}/eff{tv['eff']} {tv['volume']:,} | report written")
        except Exception as e:
            log(f"pass {p} ERROR: {str(e)[:120]}")
        time.sleep(INTERVAL)

if __name__=="__main__":
    try: main()
    except Exception: log("FATAL:\n"+traceback.format_exc())
