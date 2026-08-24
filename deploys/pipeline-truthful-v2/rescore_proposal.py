"""Re-score cached ganjahro sweep trials with the truthful-v2 scoring and the REAL
shared-wallet budget, to produce a fundable, risk-adjusted bot proposal.
READ-ONLY against brigado. Run: .venv/bin/python deploys/pipeline-truthful-v2/rescore_proposal.py
"""
import asyncio, json, sys
sys.path.insert(0, "/Users/blaspalmisciano/condor")
from hummingbot_api_client import HummingbotAPIClient
from routines.pmm_sweep import (risk_score, fits_budget, autosize_amount,
                                 two_sided_quote_need, calibration_trust, load_store)
BASE="http://<HBOT_API_HOST>:8000"; USER="<USER>"; PW="<PW>  # supply via env"
REBATE=0.00015; TAKER=0.0001; W_DD=1.0; W_UW=200.0
# ganjahro base sweeps identified by unique base-trial volume (matches deploy report)
GANJAHRO={"51893467c482":"g1(1.0bp)","e64c8e9015d0":"g2(1.5bp)","e4672e08f592":"g3(2.0bp)"}
# live champions' actual results (from live pull, 2.59d) for calibration
LIVE={"total_realized":172.1,"total_unreal":-510.1,"total_vol_brl":523737.0}

def tlabel(params):
    return "BASE" if not params else ", ".join(f"{k}={v}" for k,v in sorted(params.items()))

async def main():
    c=HummingbotAPIClient(base_url=BASE,username=USER,password=PW); await c.init()
    bal=(await c.portfolio.get_state())["master_account"]["binance"]
    px={t["token"]:t["price"] for t in bal}; brl_usd=px["BRL"]
    avail_brl=[t for t in bal if t["token"]=="BRL"][0]["available_units"]
    btc=[t for t in bal if t["token"]=="BTC"][0]
    avail_btc_brl=btc["available_units"]*btc["price"]/brl_usd   # BTC valued in BRL
    # committed by OTHER live controllers (exclude ganjahro champions we may replace)
    data=(await c.bot_orchestration.get_active_bots_status())["data"]
    com_q=com_b=0.0; others=[]
    for bot,binfo in data.items():
        for cid in (binfo.get("performance") or {}):
            if "ganjahro" in cid: continue
            cfg=await c.controllers.get_controller_config(cid)
            amt=float(cfg.get("total_amount_quote") or 0)
            tb=cfg.get("target_base_pct")
            q,b=two_sided_quote_need(amt, tb)
            com_q+=q; com_b+=b; others.append((cid,amt,cfg.get("trading_pair")))
    print(f"WALLET (master_account/binance)")
    print(f"  avail quote(BRL cash): {avail_brl:,.0f} BRL")
    print(f"  avail base(BTC in BRL): {avail_btc_brl:,.0f} BRL")
    print(f"  committed by OTHER live controllers ({len(others)}): quote {com_q:,.0f} / base {com_b:,.0f} BRL")
    print(f"  FREE quote: {avail_brl-com_q:,.0f} BRL | FREE base: {avail_btc_brl-com_b:,.0f} BRL\n")

    # re-rank each ganjahro sweep by risk-adjusted score
    store=load_store()
    proposal=[]
    for skey,name in GANJAHRO.items():
        trials=[t for t in store.get(skey,{}).values() if isinstance(t,dict) and "realized" in t and "error" not in t]
        for t in trials:
            t["_r"]=risk_score(t,REBATE,w_dd=W_DD,w_uw=W_UW,taker_fee=TAKER)
        by_risk=sorted(trials,key=lambda t:-t["_r"]["risk_adj_pnl"])
        by_biz=sorted(trials,key=lambda t:-t["_r"]["business_pnl"])
        by_vol=sorted(trials,key=lambda t:-t["volume"])
        top=by_risk[0]
        print(f"== {name} ({len(trials)} trials) ==")
        print(f"  OLD top business_pnl: {tlabel(by_biz[0]['params'])}  biz={by_biz[0]['_r']['business_pnl']:+,.0f}  risk-adj={by_biz[0]['_r']['risk_adj_pnl']:+,.0f}")
        print(f"  OLD top volume:       {tlabel(by_vol[0]['params'])}  vol={by_vol[0]['volume']:,.0f}  risk-adj={by_vol[0]['_r']['risk_adj_pnl']:+,.0f}")
        print(f"  NEW top risk-adj:     {tlabel(top['params'])}  risk-adj={top['_r']['risk_adj_pnl']:+,.0f}  (biz={top['_r']['business_pnl']:+,.0f}, maxDD_pen={top['_r']['pen_dd']:,.0f}, underwater={top['_r']['time_underwater']*100:.0f}%)")
        proposal.append((name,top))
    # autosize a 3-controller replacement set to the FREE budget (target 0.5)
    size=autosize_amount(3, 0.5, avail_brl, avail_btc_brl, com_q, com_b, safety=0.8)
    print(f"\n== FUNDABLE SIZING ==")
    print(f"  Current 6 champions each 2000 -> total 12,000 notional")
    cur=fits_budget([(2000,0.5)]*6, avail_brl, avail_btc_brl, com_q, com_b)
    print(f"  Current-6 fit: quote_need {cur['quote_need']:,.0f} vs {cur['quote_avail']:,.0f} -> {'OK' if cur['quote_ok'] else 'OVER'}; base {'OK' if cur['base_ok'] else 'OVER'}; FITS={cur['fits']}")
    print(f"  Auto-sized max per controller for 3 two-sided @50%: {size:,.0f} BRL")
    prop=fits_budget([(size,0.5)]*3, avail_brl, avail_btc_brl, com_q, com_b)
    print(f"  Proposed-3 @ {size:,.0f}: quote_need {prop['quote_need']:,.0f} vs {prop['quote_avail']:,.0f} -> FITS={prop['fits']}")
    # calibration (aggregate, coarse — windows differ; documented)
    cal=calibration_trust(LIVE["total_realized"], 172.0 or 1, LIVE["total_vol_brl"], LIVE["total_vol_brl"])
    print(f"\n== CALIBRATION (aggregate, coarse) ==")
    print(f"  live realized {LIVE['total_realized']:+.0f} vs live unrealized {LIVE['total_unreal']:+.0f} -> unrealized dominates (inventory risk unpriced in backtest)")
    print(f"  NOTE: exact same-window live-vs-backtest calibration needs the fetch_real overlay on a re-run; not computable from single-window cache.")
    json.dump({"free_quote":avail_brl-com_q,"free_base":avail_btc_brl-com_b,"autosize":size,
               "proposal":[(n,t["params"],t["_r"]) for n,t in proposal]},
              open("/Users/blaspalmisciano/condor/deploys/pipeline-truthful-v2/proposal_data.json","w"),indent=1,default=str)
    await c.close()
asyncio.run(main())
