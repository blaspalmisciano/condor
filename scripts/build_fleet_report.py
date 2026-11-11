#!/usr/bin/env python
"""Build a deployable-fleet HTML from the overnight sweep results: which pmm_mister configs to
deploy, their exact params, and what to expect (backtest medians across windows, rebate-aware)."""
import json, statistics as st, html

R = json.load(open("data/overnight_results.json"))
ASSUMED_REBATE = 0.00015   # 1.5bp, what the sweep assumed
LOW_REBATE = 0.00005       # 0.5bp stress case
WIN_PER_DAY = 6            # 4h windows -> 6/day


def rows():
    out = []
    for k, v in R.items():
        ws = list(v["windows"].values())
        if len(ws) < 20:
            continue
        fc = v.get("full_config") or {}
        vol = [w["vol"] for w in ws]; pnl = [w["pnl"] for w in ws]; dd = [w["max_dd"] for w in ws]
        mvol = st.median(vol); mpnl = st.median(pnl)
        out.append({
            "label": v["base_label"], "params": v.get("params") or {}, "fc": fc,
            "nw": len(ws), "npos_biz": sum(1 for p, vv in zip(pnl, vol) if p + vv * ASSUMED_REBATE > 0),
            "mvol": mvol, "mpnl": mpnl, "wdd": min(dd),
            "mbiz": mpnl + mvol * ASSUMED_REBATE,
            "mbiz_low": mpnl + mvol * LOW_REBATE,
        })
    return out


def dedupe(cands):
    seen, out = set(), []
    for c in cands:
        fc = c["fc"]
        sig = (tuple(fc.get("buy_spreads") or []), fc.get("take_profit"),
               fc.get("buy_position_effectivization_time"), round(c["mvol"]))
        if sig in seen:
            continue
        seen.add(sig); out.append(c)
    return out


allc = rows()
fleet_vol = dedupe(sorted(allc, key=lambda c: c["mvol"], reverse=True))[:6]
# defensive: shallow drawdown + still net-positive at assumed rebate
defset = [c for c in allc if c["mbiz"] > 0]
fleet_def = dedupe(sorted(defset, key=lambda c: c["wdd"], reverse=True))[:6]

breakeven_bp = (-st.median([c["mpnl"] for c in allc]) / st.median([c["mvol"] for c in allc])) * 1e4


def cfg_cells(fc):
    spreads = ", ".join(f"{float(s)*1e4:.2f}bp" for s in (fc.get("buy_spreads") or []))
    tp = float(fc.get("take_profit") or 0) * 1e4
    eff = fc.get("buy_position_effectivization_time")
    band = f"{fc.get('min_base_pct')}/{fc.get('target_base_pct')}/{fc.get('max_base_pct')}"
    alloc = fc.get("portfolio_allocation"); tot = fc.get("total_amount_quote")
    return spreads, f"{tp:.2f}bp", f"{eff}s", band, f"{alloc}", f"{tot:,.0f}"


def fleet_table(fleet):
    head = ("<tr><th>#</th><th>spread</th><th>take-profit</th><th>effectiv.</th>"
            "<th>inv band<br>min/tgt/max</th><th>alloc</th><th>notional</th>"
            "<th>med vol<br>/4h</th><th>trading<br>P&L</th><th>+rebate<br>@1.5bp</th>"
            "<th>net<br>business</th><th>worst<br>DD</th><th>+win%</th></tr>")
    body = ""
    for i, c in enumerate(fleet, 1):
        sp, tp, eff, band, alloc, tot = cfg_cells(c["fc"])
        bcls = "pos" if c["mbiz"] > 0 else "neg"
        body += (f"<tr><td>{i}</td><td>{sp}</td><td>{tp}</td><td>{eff}</td><td>{band}</td>"
                 f"<td>{alloc}</td><td>{tot}</td><td>{c['mvol']:,.0f}</td>"
                 f"<td class='neg'>{c['mpnl']:+.2f}</td><td class='pos'>+{c['mvol']*ASSUMED_REBATE:.2f}</td>"
                 f"<td class='{bcls}'>{c['mbiz']:+.2f}</td><td class='neg'>{c['wdd']:+.1f}</td>"
                 f"<td>{round(100*c['npos_biz']/c['nw'])}%</td></tr>")
    return f"<table>{head}{body}</table>"


def fleet_expect(fleet, name):
    n = len(fleet)
    day_vol = sum(c["mvol"] for c in fleet) * WIN_PER_DAY
    day_biz = sum(c["mbiz"] for c in fleet) * WIN_PER_DAY
    day_biz_low = sum(c["mbiz_low"] for c in fleet) * WIN_PER_DAY
    day_trade = sum(c["mpnl"] for c in fleet) * WIN_PER_DAY
    nominal = sum(float(c["fc"].get("total_amount_quote") or 0) for c in fleet)
    effective = sum(float(c["fc"].get("total_amount_quote") or 0) * float(c["fc"].get("portfolio_allocation") or 0) for c in fleet)
    worst = min(c["wdd"] for c in fleet)
    return f"""<div class="exp">
<h3>{name} — expected daily (assuming behavior repeats)</h3>
<table class="sum">
<tr><td>controllers</td><td>{n}</td></tr>
<tr><td>capital — nominal / actually working</td><td>{nominal:,.0f} BRL / <b>{effective:,.0f} BRL</b> (alloc'd)</td></tr>
<tr><td>daily volume (fleet)</td><td>{day_vol:,.0f} BRL</td></tr>
<tr><td>daily trading P&L (price only, no rebate)</td><td class="neg">{day_trade:+,.1f} BRL</td></tr>
<tr><td>daily net @ 1.5bp rebate (assumed)</td><td class="{ 'pos' if day_biz>0 else 'neg'}"><b>{day_biz:+,.1f} BRL</b></td></tr>
<tr><td>daily net @ 0.5bp rebate (stress)</td><td class="{ 'pos' if day_biz_low>0 else 'neg'}">{day_biz_low:+,.1f} BRL</td></tr>
<tr><td>worst single-window drawdown seen</td><td class="neg">{worst:+,.1f} BRL</td></tr>
</table></div>"""


HTML = f"""<!doctype html><html><head><meta charset="utf-8"><title>BTC-BRL Deployable Fleets</title>
<style>
body{{font-family:-apple-system,system-ui,sans-serif;margin:28px;background:#0f1115;color:#e6e6e6;line-height:1.5;max-width:1100px}}
h1{{font-size:22px}} h2{{font-size:17px;margin-top:34px;color:#8ab4ff;border-bottom:1px solid #2a2f3a;padding-bottom:5px}}
h3{{font-size:14px;color:#9aa;margin:10px 0 4px}}
table{{border-collapse:collapse;width:100%;font-size:12.5px;margin-top:8px}}
th,td{{border:1px solid #2a2f3a;padding:5px 7px;text-align:right}} th{{background:#1b1f28;color:#9aa;font-weight:600}}
td:first-child,th:first-child{{text-align:center}}
.pos{{color:#4ade80}} .neg{{color:#f87171}} .meta{{color:#889;font-size:12px}}
.callout{{background:#1a2230;border-left:3px solid #8ab4ff;padding:12px 16px;margin:16px 0;border-radius:4px}}
.warn{{background:#2a1f1a;border-left:3px solid #f0a060}}
.exp{{background:#141821;border:1px solid #2a2f3a;border-radius:6px;padding:4px 14px 12px;margin-top:10px}}
table.sum td{{text-align:left}} table.sum td:last-child{{text-align:right;font-variant-numeric:tabular-nums}}
code{{background:#1b1f28;padding:1px 5px;border-radius:3px;color:#cbd}}
</style></head><body>

<h1>BTC-BRL — Deployable pmm_mister Fleets</h1>
<p class="meta">From the overnight local backtest · 198 candidates scored across 72 windows over 10 days ·
controller <code>pmm_mister</code> on <code>binance BTC-BRL</code> · showing configs seen in ≥20 windows.
All figures are <b>backtest medians per 4h window</b>.</p>

<div class="callout warn">
<b>Read this first — where the money comes from.</b> These are <b>rebate-harvesting</b> configs: they quote
ultra-tight (~1bp) so the <i>trading itself loses a sliver</i> every round trip (a ~2bp round-trip fee vs ~1.5bp
captured). The profit is the <b>maker rebate</b> paid on volume. So <b>net P&L ≈ trading loss + rebate</b>, and it
lives or dies on the real rebate. Break-even is ≈ <b>{breakeven_bp:.2f}bp</b>; the sweep assumed <b>1.5bp</b>.
<br>→ <b>Confirm brigado's actual binance maker rebate on BTC-BRL before deploying.</b> Below that break-even, every fleet here loses money by design.
</div>

<h2>Fleet A — Volume / rebate-harvest (aggressive)</h2>
<p class="meta">Highest turnover = most rebate income, but rides the deepest drawdowns. The <code>alloc-500</code> family.</p>
{fleet_table(fleet_vol)}
{fleet_expect(fleet_vol, "Fleet A")}

<h2>Fleet B — Defensive / low-drawdown (conservative)</h2>
<p class="meta">A fifth of the volume (less rebate), but drawdowns an order of magnitude shallower — the far safer rebuild
after the fleet went underwater. The <code>tp-1bp spread_1.5</code> family.</p>
{fleet_table(fleet_def)}
{fleet_expect(fleet_def, "Fleet B")}

<h2>How to read the configs</h2>
<ul class="meta">
<li><b>spread</b> — how far inside the mid each buy order sits (bp). Tighter = more fills = more volume + more adverse selection.</li>
<li><b>take-profit</b> — target exit above entry (bp). Must clear the round-trip fee to win on price alone; here it doesn't — rebate makes up the gap.</li>
<li><b>effectiv.</b> — a lot not closed within this many seconds is worked out at market (cycles inventory instead of hoarding it).</li>
<li><b>inv band</b> — min/target/max base inventory fraction; keeps the book from drifting one-sided.</li>
<li><b>alloc / notional</b> — <code>portfolio_allocation</code> × <code>total_amount_quote</code> = capital actually quoting. Note it's <b>5%</b>: only ~500 BRL of each 10k nominal is working. Raising alloc scales volume (and risk) ~linearly.</li>
<li><b>+win%</b> — share of windows the config was net-positive (at 1.5bp rebate). None exceed ~60% — this is thin-edge rebate farming, not a directional winner.</li>
</ul>

<div class="callout">
<b>What to expect — honestly.</b> These are optimistic backtest fills on coarse candles, and they assume the next
period's regime looks like the last 10 days. Treat the <i>volume</i> numbers as reliable (turnover is robust) and
the <i>P&L</i> as a thin, rebate-dependent margin around zero, not a forecast. Deploy still requires the Phase-0
safety fixes (co-hosted-controller guard, idempotent deploy) — see <code>docs/pmm_autopilot_improvement_plan.md</code>.
</div>

<p class="meta">Full deployable configs (every field) for each row are in <code>data/overnight_results.json</code>.</p>
</body></html>"""

open("reports/btcbrl_deployable_fleets.html", "w").write(HTML)
print("wrote reports/btcbrl_deployable_fleets.html")
print(f"Fleet A (volume): {len(fleet_vol)} configs; Fleet B (defensive): {len(fleet_def)} configs")
print(f"break-even rebate: {breakeven_bp:.2f}bp")
print("Fleet A daily net @1.5bp:", round(sum(c['mbiz'] for c in fleet_vol)*WIN_PER_DAY,1),
      "| @0.5bp:", round(sum(c['mbiz_low'] for c in fleet_vol)*WIN_PER_DAY,1))
print("Fleet B daily net @1.5bp:", round(sum(c['mbiz'] for c in fleet_def)*WIN_PER_DAY,1),
      "| worst DD:", round(min(c['wdd'] for c in fleet_def),1))
