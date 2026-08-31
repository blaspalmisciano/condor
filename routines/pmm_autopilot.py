"""PMM Autopilot orchestrator — the self-improving loop, autonomous.

Each lap chains the pieces the flowchart shows, end to end:
  1. read the LIVE running controllers + their current configs   (read-only)
  2. sweep each live config on the LOCAL parity backtest stack     (base -> OAT -> combinations, 1s)
  3. rank the variants by risk-adjusted score                      (risk_score)
  4. calibrate live-vs-backtest over the inter-lap window          (calibration_trust)
  5. gate + propose the winners for human approval                 (promotion_gate; Telegram)
then it sleeps `frequency_sec` and laps again.

Design invariants:
  * Live bots are read ONLY here. Nothing is deployed or mutated on the live server.
  * All backtests run on the LOCAL stack (config.local_url), never on the live API,
    so a 1s sweep can't freeze the live bots.
  * Deploy stays a HUMAN action — winners are proposed, not shipped (M4 wires the button).

Calibration (M2): live perf from the API is lifetime-cumulative, so a single reading
can't be compared to a windowed backtest. Instead each lap snapshots (cum_vol, cum_pnl,
t) per controller; the DELTA vs the previous lap is the real windowed live activity,
scaled onto the backtest window before scoring trust. First lap after a (re)start is a
warm-up (no prior snapshot).

Set as CONTINUOUS: run() loops until stopped. `max_laps` (0 = forever) bounds it for
tests / one-shot use.
"""
import asyncio
import json
import os
import time

from pydantic import BaseModel, Field
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from config_manager import get_client
from routines.base import RoutineResult
from routines.pmm_level_preview import _extract_rows
from routines.pmm_sweep import (
    Config as SweepConfig,
    apply_params,
    calibration_trust,
    get_axes,
    run_sweep,
    risk_score,
    score,
    _tlabel,
)
from routines.controller_performance import gather_active

from hummingbot_api_client import HummingbotAPIClient
from aiohttp import ClientTimeout

CONTINUOUS = True

PROPOSAL_STORE = "data/pmm_autopilot_proposals.json"
CALIB_STORE = "data/pmm_autopilot_calibration.json"
SNAP_STORE = "data/pmm_autopilot_snapshots.json"


class Config(BaseModel):
    """PMM Autopilot — laps the self-improving loop: sweep live controllers, propose winners (human-gated)."""

    live_server: str = Field(default="brigado", description="server running the live bots (read-only)")
    local_url: str = Field(default="http://localhost:8000", description="local backtest API base url")
    local_user: str = Field(default="elamigo", description="local API username")
    local_pass: str = Field(default="barabit", description="local API password")
    resolution: str = Field(default="1s", description="backtest resolution (1s resolves sub-minute knobs)")
    window_hours: int = Field(default=24, description="backtest window length in hours")
    max_controllers: int = Field(default=6, description="max live controllers to sweep per lap")
    only_pair: str = Field(default="", description="filter to one trading pair, e.g. BTC-BRL (blank = all)")
    min_uplift: float = Field(default=0.5, description="min risk-adj uplift over base to propose")
    frequency_sec: int = Field(default=3600, description="seconds between laps (continuous mode)")
    max_laps: int = Field(default=0, description="stop after N laps (0 = run forever)")
    dry_run: bool = Field(default=True, description="M1-3: propose only; the deploy button is wired in M4")


def _load(path):
    try:
        return json.load(open(path))
    except Exception:
        return {}


def _save(path, obj):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    json.dump(obj, open(tmp, "w"), default=str)
    os.replace(tmp, path)


def _clean_live_config(cfg: dict) -> dict:
    """Strip underscore-prefixed metadata the live API attaches (e.g. `_config_name`).
    CRITICAL: `_config_name` present in a config makes the backtest engine create
    ZERO executors (silent 0-volume). Must be removed before backtesting."""
    return {k: v for k, v in cfg.items() if not k.startswith("_")}


async def _sweep_controller(local, cid, base_cfg, w0, w1, scfg, config, log):
    """Sweep one live config on the local stack; return (base_t, best, ok_trials, axes) or None."""
    base_cfg = _clean_live_config(base_cfg)
    pair = (base_cfg.get("trading_pair") or "").upper()
    conn = base_cfg.get("connector_name") or "binance"
    days_back = max(1, config.window_hours // 24 + 1)
    rows = _extract_rows(await local.market_data.get_candles_last_days(conn, pair, days_back, "1m"))
    cw = [c for c in rows if w0 - 60 <= float(c["timestamp"]) <= w1 + 60]
    if len(cw) < 50:
        log(f"{cid}: only {len(cw)} candles for window — skipped")
        return None
    ref = sum(float(c["close"]) for c in cw) / len(cw)
    log(f"{cid} [{pair}] sweeping {config.resolution} over {config.window_hours}h on local…")
    trials = await run_sweep(local, base_cfg, w0, w1, scfg, ref, log=lambda m, c=cid: log(f"  {c}: {m}"))
    ok = [t for t in trials if "error" not in t]
    if not ok:
        log(f"{cid}: no successful trials")
        return None
    axes = get_axes(base_cfg)
    # Objective at 1s = VOLUME (turnover). Profit is rebate-driven, so more volume =
    # more rebate; volume differs by thousands across variants and is the robust
    # differentiator (business PnL and risk_adj collapse to sub-unit noise at 1s on
    # small live capital). Guardrails keep the winner honest: business PnL may not
    # drop more than `biz_floor` below base, and max_dd no worse than 1.6× base's.
    biz_floor = 0.5
    for t in ok:
        t["_biz"] = score(t, scfg.rebate_rate)["business_pnl"]
        t["_rs"] = risk_score(t, scfg.rebate_rate)["risk_adj_pnl"]
        t["_vol"] = t["volume"]
    base_t = next((t for t in ok if not t.get("params")), None)
    base_biz = base_t["_biz"] if base_t else 0.0
    base_dd = base_t["max_dd"] if base_t else -1e9
    cand = [t for t in ok if t.get("params")
            and t["_biz"] >= base_biz - biz_floor and t["max_dd"] >= base_dd * 1.6]
    best = max(cand, key=lambda t: t["_vol"]) if cand else (base_t or max(ok, key=lambda t: t["_vol"]))
    return base_t, best, ok, axes


def _windowed_calibration(cid, lp, base_t, w0, w1, scfg, snaps, now):
    """Delta-vs-previous-snapshot live activity, scaled onto the backtest window."""
    cur_vol = lp.get("volume_traded", 0.0)
    cur_pnl = lp.get("realized", 0.0) + lp.get("unrealized", 0.0)
    prev = snaps.get(cid)
    snaps[cid] = {"t": now, "cum_vol": cur_vol, "cum_pnl": cur_pnl}
    if not prev or now <= prev.get("t", now):
        return {"trust": None, "note": "warmup"}
    dt = now - prev["t"]
    dvol = max(0.0, cur_vol - prev.get("cum_vol", 0.0))
    dpnl = cur_pnl - prev.get("cum_pnl", 0.0)
    bt_win = max(1, w1 - w0)
    scale = dt / bt_win
    bt_vol_s = (base_t["volume"] if base_t else 0.0) * scale
    bt_pnl_s = (score(base_t, scfg.rebate_rate)["total_pnl"] if base_t else 0.0) * scale
    c = calibration_trust(dpnl, bt_pnl_s, dvol, bt_vol_s)
    c["window_s"] = round(dt)
    return c


async def _one_lap(config: Config, live, local, snaps, log) -> dict:
    live_perf, cfg_by_ctrl, bot_names, bot_health, cid_bot = await gather_active(live)

    cands = []
    for cid, cfg in cfg_by_ctrl.items():
        if cid not in live_perf:
            continue
        if (cfg.get("controller_name") or "") != "pmm_mister":
            continue
        if config.only_pair and (cfg.get("trading_pair", "").upper() != config.only_pair.upper()):
            continue
        cands.append((cid, cfg))
    cands.sort(key=lambda kv: -live_perf.get(kv[0], {}).get("volume_traded", 0.0))
    cands = cands[: config.max_controllers]
    if not cands:
        return {"analyses": [], "proposals": [], "summary":
                f"No running pmm_mister controllers to optimize (pair={config.only_pair or 'any'})."}

    w1 = int(time.time()) - 300
    w0 = w1 - config.window_hours * 3600
    scfg = SweepConfig(resolution=config.resolution, days=max(1, config.window_hours // 24 or 1))
    now = int(time.time())

    analyses, proposals = [], []
    for cid, base_cfg in cands:
        base_cfg = _clean_live_config(dict(base_cfg))
        base_cfg.setdefault("id", cid)
        swept = await _sweep_controller(local, cid, base_cfg, w0, w1, scfg, config, log)
        if not swept:
            continue
        base_t, best, ok, axes = swept
        base_vol = base_t["_vol"] if base_t else 0.0
        uplift = best["_vol"] - base_vol  # volume (turnover) uplift, BRL
        calib = _windowed_calibration(cid, live_perf.get(cid, {}), base_t, w0, w1, scfg, snaps, now)
        label = _tlabel(best, axes) or "BASE"
        improved = bool(best.get("params")) and uplift >= config.min_uplift
        log(f"{cid}: base vol {base_vol:,.0f} -> best '{label}' {best['_vol']:,.0f} "
            f"(uplift {uplift:+,.0f}) | trust {calib.get('trust')}")
        analyses.append({
            "controller_id": cid, "pair": (base_cfg.get("trading_pair") or "").upper(),
            "n_trials": len(ok), "base_vol": round(base_vol, 0), "best_label": label,
            "best_vol": round(best["_vol"], 0), "uplift": round(uplift, 0),
            "best_biz": round(best["_biz"], 2), "trust": calib.get("trust"), "proposed": improved,
        })
        if improved:
            full_cfg = apply_params(base_cfg, best["params"], axes)
            full_cfg["id"] = f"{cid}__{label.lower().replace(' ', '_').replace('×', 'x')}"
            pid = f"{cid[:8]}-{now % 100000}"
            proposals.append({
                "proposal_id": pid, "controller_id": cid, "bot": cid_bot.get(cid),
                "pair": (base_cfg.get("trading_pair") or "").upper(), "label": label,
                "params": best["params"], "base_vol": round(base_vol, 0),
                "best_vol": round(best["_vol"], 0), "vol_uplift": round(uplift, 0),
                "base_biz": round(base_t["_biz"] if base_t else 0.0, 2),
                "best_biz": round(best["_biz"], 2), "max_dd": round(best["max_dd"], 1),
                "calibration": calib, "full_config": full_cfg, "window": [w0, w1],
                "resolution": config.resolution, "created": now, "status": "proposed",
            })

    # persist proposals + calibration snapshots
    if proposals:
        store = _load(PROPOSAL_STORE)
        for p in proposals:
            store[p["proposal_id"]] = p
        _save(PROPOSAL_STORE, store)
    _save(SNAP_STORE, snaps)
    calib_hist = _load(CALIB_STORE)
    for a in analyses:
        calib_hist.setdefault(a["controller_id"], []).append({"t": now, "trust": a["trust"]})
    _save(CALIB_STORE, calib_hist)

    # summary
    lines = [f"🤖 *PMM Autopilot* — lap ({len(analyses)} controllers swept, "
             f"{config.resolution}/{config.window_hours}h on local)\n"]
    for a in analyses:
        flag = "✅ propose" if a["proposed"] else "— hold (base optimal)"
        tr = "warm-up" if a["trust"] is None else a["trust"]
        lines.append(f"• `{a['controller_id']}` [{a['pair']}] · {a['n_trials']} variants · "
                     f"vol {a['base_vol']:,.0f}→{a['best_vol']:,.0f} (uplift {a['uplift']:+,.0f}) · "
                     f"trust {tr} → {flag}")
    if proposals:
        lines.append(f"\n*{len(proposals)} proposal(s) awaiting your OK:*")
        for p in proposals:
            lines.append(f"  `{p['proposal_id']}` {p['controller_id']} → *{p['label']}* "
                         f"(vol uplift {p['vol_uplift']:+,.0f} → {p['best_vol']:,.0f})"
                         + ("  _(deploy button: M4)_" if config.dry_run else ""))
    else:
        lines.append("\n_No variant beat the live config — fleet near-optimal this window._")
    return {"analyses": analyses, "proposals": proposals, "summary": "\n".join(lines)}


def _proposal_keyboard(proposals):
    """Inline Deploy/Reject buttons per proposal — the human approval gate.
    callback_data routes to this routine's handle_callback:
    routines:pmm_autopilot:{deploy|reject}:{proposal_id} (<64 bytes)."""
    rows = []
    for p in proposals:
        pid = p["proposal_id"]
        rows.append([
            InlineKeyboardButton(f"✅ Deploy {p['label'][:16]}",
                                 callback_data=f"routines:pmm_autopilot:deploy:{pid}"),
            InlineKeyboardButton("❌", callback_data=f"routines:pmm_autopilot:reject:{pid}"),
        ])
    return InlineKeyboardMarkup(rows) if rows else None


async def handle_callback(update, context, action, params):
    """M4 — human approval gate. Applies a proposal's tuned params to the LIVE
    controller in place (update_bot_controller_config), only on an explicit press."""
    query = update.callback_query
    pid = params[0] if params else None
    store = _load(PROPOSAL_STORE)
    p = store.get(pid)
    if not p:
        await query.answer("Proposal expired or already handled.", show_alert=True)
        return
    base_text = (query.message.text if query.message else "") or ""

    if action == "reject":
        p["status"] = "rejected"
        store[pid] = p
        _save(PROPOSAL_STORE, store)
        await query.answer("Rejected — nothing deployed.")
        try:
            await query.edit_message_text(base_text + f"\n\n❌ Rejected `{pid}`", parse_mode="Markdown")
        except Exception:
            pass
        return

    if action == "deploy":
        if p.get("status") == "deployed":
            await query.answer("Already deployed.", show_alert=True)
            return
        await query.answer("Deploying to the live controller…")
        chat_id = getattr(context, "_chat_id", None) or (query.message.chat_id if query.message else None)
        live = await get_client(chat_id, context=context)
        cid = p["controller_id"]
        bot = p.get("bot")
        cfg = dict(p["full_config"])
        cfg["id"] = cid                       # in-place update keeps the live id
        cfg.setdefault("controller_name", "pmm_mister")
        try:
            res = await live.controllers.update_bot_controller_config(
                bot_name=bot, controller_name=cid, config=cfg)
            ok = (res.get("status") == "success") if isinstance(res, dict) else bool(res)
            p["status"] = "deployed" if ok else "deploy_failed"
            p["deploy_result"] = str(res)[:300]
            store[pid] = p
            _save(PROPOSAL_STORE, store)
            tail = (f"\n\n✅ *Deployed* `{p['label']}` → `{cid}` on `{bot}`"
                    if ok else f"\n\n⚠️ deploy returned: `{str(res)[:150]}`")
            await query.edit_message_text(base_text + tail, parse_mode="Markdown")
        except Exception as e:
            p["status"] = "deploy_error"
            store[pid] = p
            _save(PROPOSAL_STORE, store)
            try:
                await query.edit_message_text(base_text + f"\n\n⚠️ deploy error: `{str(e)[:150]}`",
                                              parse_mode="Markdown")
            except Exception:
                pass


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> RoutineResult:
    chat_id = getattr(context, "_chat_id", None)
    live = await get_client(chat_id, context=context)  # brigado — READ ONLY
    if not live:
        return RoutineResult(text="No live server available. Configure servers in /config.")
    local = HummingbotAPIClient(
        base_url=config.local_url, username=config.local_user, password=config.local_pass,
        timeout=ClientTimeout(total=1800, connect=15))
    await local.init()

    snaps = _load(SNAP_STORE)
    last = {"analyses": [], "proposals": [], "summary": "no lap yet"}
    lap = 0
    try:
        while True:
            lap += 1
            logs = []
            try:
                last = await _one_lap(config, live, local, snaps, lambda m: logs.append(str(m)))
            except Exception as e:
                last = {"analyses": [], "proposals": [], "summary": f"⚠️ lap {lap} error: {e}"}
            # notify the chat if a bot is available; attach approval buttons for proposals
            try:
                if chat_id and getattr(context, "bot", None):
                    kb = None if config.dry_run else _proposal_keyboard(last.get("proposals", []))
                    await context.bot.send_message(chat_id=chat_id, text=last["summary"],
                                                   parse_mode="Markdown", reply_markup=kb)
            except Exception:
                pass
            if config.max_laps and lap >= config.max_laps:
                break
            await asyncio.sleep(config.frequency_sec)
    except asyncio.CancelledError:
        return RoutineResult(text=f"PMM Autopilot stopped after {lap} lap(s).")
    finally:
        try:
            await local.close()
        except Exception:
            pass

    a = last["analyses"]
    rows = [{
        "Controller": x["controller_id"], "Pair": x["pair"], "Variants": x["n_trials"],
        "Base risk-adj": x["base_risk_adj"], "Best": x["best_label"],
        "Best risk-adj": x["best_risk_adj"], "Uplift": x["uplift"],
        "Trust": ("warm-up" if x["trust"] is None else x["trust"]),
        "Action": "propose" if x["proposed"] else "hold",
    } for x in a]
    cols = ["Controller", "Pair", "Variants", "Base risk-adj", "Best", "Best risk-adj", "Uplift", "Trust", "Action"]
    return RoutineResult(text=last["summary"], table_data=rows or None, table_columns=cols if rows else None)
