#!/usr/bin/env python
"""Overnight BTC-BRL fleet generator — runs BACK-TO-BACK on the LOCAL backtest stack (:8001),
NEVER brigado. Sweeps the 5 unique live base configs across many disjoint 4h windows, aggregates
each param-set's volume / business-PnL / risk-adjusted PnL ACROSS windows (K-window robustness),
and regenerates an HTML report with three deployable fleets (volume-max / PnL-max / risk-adjusted)
after every sweep. Loops until killed. Self-heals on local-stack hiccups.
"""
import sys, os, json, time, asyncio, traceback, statistics, subprocess
import os as _os; from dotenv import load_dotenv as _ld; _ld(_os.path.join(_os.path.dirname(_os.path.abspath(__file__)),"..",".env"))
sys.path.insert(0, "/Users/blaspalmisciano/condor")
os.chdir("/Users/blaspalmisciano/condor")
from pathlib import Path
import routines.pmm_sweep as S
S.SWEEP_STORE = Path("data/overnight_sweep_store.json")  # isolate from the 70MB shared store
from routines.pmm_sweep import run_sweep, score, risk_score, apply_params, get_axes, trial_id, Config as SweepConfig
from hummingbot_api_client import HummingbotAPIClient
from aiohttp import ClientTimeout

LOCAL_URL = "http://localhost:8001"
RESULTS = "data/overnight_results.json"
PROGRESS = "data/overnight_progress.json"
REPORT = "reports/overnight_btcbrl_fleets.html"
LOGF = "data/overnight_sweep.log"
WIN_H = 4
STEP_H = 2          # rolling step between windows (overlap ok; more robustness samples)
MAX_TRIALS = 40
RESOLUTION = "1s"
FLEET_N = 12        # match the current live fleet size
REBATE = 0.00015


def log(m):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {m}"
    print(line, flush=True)
    try:
        open(LOGF, "a").write(line + "\n")
    except Exception:
        pass


def _extract_rows(resp):
    if isinstance(resp, dict):
        for k in ("candles", "data", "rows", "result"):
            if isinstance(resp.get(k), list):
                return resp[k]
    return resp if isinstance(resp, list) else []


def load_bases():
    """The 5 unique BTC-BRL base param-sets from the live-fleet pool."""
    pool = json.load(open("data/pmm_autopilot_pool.json"))
    bases = [e for e in pool if e.get("is_base") and e.get("full_config", {}).get("trading_pair", "").upper() == "BTC-BRL"]
    seen = {}
    for e in bases:
        fc = e["full_config"]
        # dedup IGNORING effectivization time — the sweep explores it as an axis, so one base per
        # spread/tp/inventory family is enough (collapses 11 live variants → 5 distinct families)
        k = json.dumps({kk: fc.get(kk) for kk in ("buy_spreads", "sell_spreads", "take_profit",
                        "target_base_pct", "total_amount_quote")}, default=str, sort_keys=True)
        if k not in seen:
            # `label` is uniformly "BASE"; the live controller id in `origin` is the distinct name
            lbl = e.get("origin") or e.get("label") or f"base{len(seen)}"
            seen[k] = {"label": lbl, "cfg": fc}
    return list(seen.values())


def stack_up():
    try:
        import urllib.request
        urllib.request.urlopen(LOCAL_URL + "/docs", timeout=5)
        return True
    except Exception:
        return False


def restart_stack():
    log("local stack unreachable — attempting docker restart")
    try:
        subprocess.run(["docker", "restart", "hummingbot-api-local-8001"], timeout=120,
                       capture_output=True)
    except Exception as e:
        log(f"docker restart failed: {e}")
    for _ in range(24):  # wait up to 2 min for health
        if stack_up():
            log("local stack back up")
            return True
        time.sleep(5)
    return False


# --- aggregation: key -> list of per-window results -------------------------
def _key(base_label, params):
    return base_label + "||" + trial_id(params or {})


def load_results():
    try:
        return json.load(open(RESULTS))
    except Exception:
        return {}


def save_results(r):
    tmp = RESULTS + ".tmp"
    json.dump(r, open(tmp, "w"), default=str)
    os.replace(tmp, RESULTS)


def aggregate(results, base, window_id, trials):
    for t in trials:
        if "error" in t:
            continue
        sc = score(t, REBATE)
        rs = risk_score(t, REBATE)
        k = _key(base["label"], t.get("params"))
        row = results.setdefault(k, {"base_label": base["label"], "params": t.get("params") or {},
                                     "windows": {}})
        row["windows"][str(window_id)] = {
            "vol": round(t["volume"], 1), "biz": round(sc["business_pnl"], 2),
            "pnl": round(sc["total_pnl"], 2), "risk_adj": round(rs["risk_adj_pnl"], 2),
            "max_dd": round(t["max_dd"], 2)}
        # keep a deployable full config
        try:
            row["full_config"] = apply_params(base["cfg"], t.get("params") or {})
        except Exception:
            row["full_config"] = base["cfg"]


def _med(xs):
    return statistics.median(xs) if xs else 0.0


def summarize(results):
    out = []
    for k, row in results.items():
        ws = list(row["windows"].values())
        if not ws:
            continue
        vols = [w["vol"] for w in ws]; bizs = [w["biz"] for w in ws]
        ras = [w["risk_adj"] for w in ws]; dds = [w["max_dd"] for w in ws]
        pnls = [w["pnl"] for w in ws]
        out.append({
            "key": k, "base_label": row["base_label"], "params": row["params"],
            "full_config": row.get("full_config"),
            "n_windows": len(ws), "n_pos_biz": sum(1 for b in bizs if b > 0),
            "med_vol": round(_med(vols), 0), "med_biz": round(_med(bizs), 2),
            "med_pnl": round(_med(pnls), 2), "med_risk_adj": round(_med(ras), 2),
            "worst_dd": round(min(dds), 2),
        })
    return out


def _pick_fleet(summ, sort_key, n=FLEET_N, min_windows=1):
    elig = [s for s in summ if s["n_windows"] >= min_windows]
    return sorted(elig, key=lambda s: s[sort_key], reverse=True)[:n]


def write_report(results, progress):
    summ = summarize(results)
    max_w = max([s["n_windows"] for s in summ], default=0)
    # require robustness once we have enough windows
    min_w = max(1, min(max_w, 3))
    fleets = {
        "VOLUME-max": _pick_fleet(summ, "med_vol", min_windows=min_w),
        "PnL-max (business PnL)": _pick_fleet(summ, "med_biz", min_windows=min_w),
        "RISK-adjusted (PnL − drawdown pen.)": _pick_fleet(summ, "med_risk_adj", min_windows=min_w),
    }

    def tbl(rows):
        h = ("<tr><th>#</th><th>base</th><th>params</th><th>med vol</th><th>med biz</th>"
             "<th>med risk-adj</th><th>worst dd</th><th>+biz windows</th></tr>")
        body = ""
        for i, s in enumerate(rows, 1):
            p = ", ".join(f"{k.split('_')[-1]}={v}" for k, v in (s["params"] or {}).items()) or "<i>base</i>"
            body += (f"<tr><td>{i}</td><td>{s['base_label'][:28]}</td><td>{p}</td>"
                     f"<td>{s['med_vol']:,.0f}</td><td class='{ 'pos' if s['med_biz']>0 else 'neg'}'>{s['med_biz']:+,.2f}</td>"
                     f"<td class='{ 'pos' if s['med_risk_adj']>0 else 'neg'}'>{s['med_risk_adj']:+,.1f}</td>"
                     f"<td class='neg'>{s['worst_dd']:+,.1f}</td><td>{s['n_pos_biz']}/{s['n_windows']}</td></tr>")
        return f"<table>{h}{body}</table>"

    secs = ""
    for name, rows in fleets.items():
        secs += f"<h2>{name}</h2>{tbl(rows)}"

    html = f"""<!doctype html><html><head><meta charset="utf-8"><title>BTC-BRL Overnight Fleets</title>
<style>body{{font-family:-apple-system,system-ui,sans-serif;margin:24px;background:#0f1115;color:#e6e6e6}}
h1{{font-size:20px}} h2{{font-size:16px;margin-top:28px;color:#8ab4ff}}
table{{border-collapse:collapse;width:100%;font-size:13px;margin-top:6px}}
th,td{{border:1px solid #2a2f3a;padding:5px 8px;text-align:right}} th{{background:#1b1f28;color:#9aa}}
td:nth-child(2),td:nth-child(3),th:nth-child(2),th:nth-child(3){{text-align:left}}
.pos{{color:#4ade80}} .neg{{color:#f87171}} .meta{{color:#889;font-size:12px}}</style></head><body>
<h1>BTC-BRL Overnight Fleet Candidates <span class="meta">(local backtest, 1s, {WIN_H}h windows)</span></h1>
<p class="meta">Updated {time.strftime('%Y-%m-%d %H:%M:%S')} · pass {progress.get('pass',0)} ·
sweeps done {progress.get('sweeps_done',0)} · windows {progress.get('windows_total',0)} ·
unique candidates {len(summ)} · bases {progress.get('n_bases',0)} ·
robustness filter: ≥{min_w} windows</p>
<p class="meta">Each fleet = top {FLEET_N} controllers by that objective, median across all windows swept.
"+biz windows" = how many windows the config had positive business PnL (robustness). "worst dd" = deepest drawdown seen.</p>
{secs}
</body></html>"""
    os.makedirs("reports", exist_ok=True)
    tmp = REPORT + ".tmp"
    open(tmp, "w").write(html)
    os.replace(tmp, REPORT)


async def main():
    log("=== overnight BTC-BRL sweep START ===")
    bases = load_bases()
    log(f"loaded {len(bases)} unique base configs: {[b['label'][:24] for b in bases]}")
    scfg = SweepConfig(resolution=RESOLUTION, trade_cost=0.0001, rebate_rate=REBATE,
                       max_trials=MAX_TRIALS, pause_s=1.0)

    client = None
    async def reconnect():
        nonlocal client
        try:
            if client:
                await client.close()
        except Exception:
            pass
        client = HummingbotAPIClient(base_url=LOCAL_URL, username=_os.getenv("HB_USER"), password=_os.getenv("HB_PASS"),
                                     timeout=ClientTimeout(total=1800, connect=15))
        await client.init()
        log("client (re)connected")
    await reconnect()

    conn = bases[0]["cfg"].get("connector_name") or "binance"
    pair = "BTC-BRL"

    results = load_results()
    progress = {"pass": 0, "sweeps_done": 0, "windows_total": 0, "n_bases": len(bases), "started": int(time.time())}

    pass_num = 0
    while True:
        pass_num += 1
        progress["pass"] = pass_num
        # (re)fetch candles each pass → windows shift forward as new data accrues
        try:
            rows = _extract_rows(await client.market_data.get_candles_last_days(conn, pair, 10, "1m"))
        except Exception as e:
            log(f"candle fetch failed: {e}; reconnecting")
            if not stack_up():
                restart_stack()
            await reconnect()
            continue
        if len(rows) < 100:
            log(f"only {len(rows)} candles — wait + retry")
            await asyncio.sleep(60)
            continue
        ts = sorted(float(c["timestamp"]) for c in rows)
        span_h = (ts[-1] - ts[0]) / 3600
        # DISJOINT 4h windows across the whole span (every WIN_H, no overlap)
        raw = []
        w0 = int(ts[0])
        while w0 + WIN_H * 3600 <= ts[-1]:
            raw.append((w0, w0 + WIN_H * 3600))
            w0 += WIN_H * 3600
        # Reorder so early sweeps SPAN all regimes instead of marching oldest→newest:
        # most-recent first, then a bit-reversal-style interleave over the rest. This way,
        # whenever we're stopped, the windows already covered are spread across the 10 days
        # (incl. the recent BTC move that sank the fleet), not clustered at one end.
        if raw:
            recent = raw[-1]
            rest = raw[:-1]
            order = sorted(range(len(rest)),
                           key=lambda i: bin(i)[2:].zfill(16)[::-1])  # bit-reversal spread
            windows = [recent] + [rest[i] for i in order]
        else:
            windows = raw
        log(f"pass {pass_num}: {len(rows)} candles, span {span_h:.1f}h → {len(windows)} disjoint {WIN_H}h windows "
            f"(diverse-first order); {len(bases)} bases")

        for wi, (w0, w1) in enumerate(windows):
            cw = [c for c in rows if w0 - 60 <= float(c["timestamp"]) <= w1 + 60]
            if len(cw) < 50:
                continue
            ref = sum(float(c["close"]) for c in cw) / len(cw)
            win_id = f"{time.strftime('%m%d-%H%M', time.localtime(w0))}"
            for base in bases:
                bc = dict(base["cfg"]); bc["id"] = "sweep_base"
                try:
                    trials = await asyncio.wait_for(
                        run_sweep(client, bc, w0, w1, scfg, ref,
                                  log=lambda m: None),
                        timeout=2400)
                    ok = [t for t in trials if "error" not in t]
                    aggregate(results, base, win_id, trials)
                    save_results(results)
                    progress["sweeps_done"] += 1
                    progress["windows_total"] = len(set(
                        wk for r in results.values() for wk in r["windows"]))
                    write_report(results, progress)
                    json.dump(progress, open(PROGRESS, "w"))
                    log(f"[p{pass_num} w{wi+1}/{len(windows)} {win_id}] {base['label'][:20]}: "
                        f"{len(ok)} ok trials, {len(results)} unique candidates total")
                except Exception as e:
                    log(f"sweep err {win_id}/{base['label'][:16]}: {type(e).__name__} {str(e)[:100]}")
                    if not stack_up():
                        restart_stack()
                    try:
                        await reconnect()
                    except Exception as e2:
                        log(f"reconnect failed: {e2}; sleeping 60s")
                        await asyncio.sleep(60)
        log(f"=== pass {pass_num} complete: {progress['sweeps_done']} sweeps, "
            f"{len(results)} unique candidates, {progress['windows_total']} windows ===")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("stopped by signal")
    except Exception:
        log("FATAL:\n" + traceback.format_exc())
