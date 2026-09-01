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
    frequency_sec: int = Field(default=43200, description="fixed interval between laps when run_at_hours is blank (43200 = 12h); the lap's own runtime is subtracted so spacing stays exact")
    run_at_hours: str = Field(default="02:00,14:00", description="clock times (machine tz) to START each lap; laps finish ~3-4h later. Blank = use frequency_sec instead.")
    sweep_window_hours: int = Field(default=4, description="backtest window per reshape lap (4h ≈ 3-4h/lap)")
    max_laps: int = Field(default=0, description="stop after N laps (0 = run forever)")
    dry_run: bool = Field(default=False, description="if True, propose text only (no Deploy button)")


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


def _next_target_seconds(hhmm_csv: str) -> int:
    """Seconds until the next clock target (machine-local tz). Anchors laps to fixed
    times of day so a proposal lands by a predictable hour, instead of drifting."""
    import datetime as _dt
    now = _dt.datetime.now()
    targets = []
    for hm in (hhmm_csv or "").split(","):
        hm = hm.strip()
        if not hm:
            continue
        try:
            h, m = map(int, hm.split(":"))
        except Exception:
            continue
        t = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if t <= now:
            t += _dt.timedelta(days=1)
        targets.append(t)
    if not targets:
        return 0
    return max(1, int((min(targets) - now).total_seconds()))


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


POOL_STORE = "data/pmm_autopilot_pool.json"
FLEET_STORE = "data/pmm_autopilot_livefleet.json"
RESHAPE_STORE = "data/pmm_autopilot_reshape.json"


def _family(origin: str) -> str:
    """Strategy family of a live controller id (before its optimization suffix)."""
    return (origin or "").split("__")[0]


def select_topN(pool: list, fleet: dict, budget: float = None, max_per_family: int = 4,
                biz_floor: float = -1.0) -> list:
    """Pick the best N candidates GLOBALLY (N = live controller count) for a stable-size
    fleet reshape, then AUTOSIZE their capital to spend `budget` (default = current fleet
    total). Design:
      * Only candidates whose origin is currently LIVE are eligible (defunct controllers
        excluded).
      * Rank by TURNOVER EFFICIENCY (volume / capital) — fair across capital tiers, and
        the right metric once we re-capitalise everyone to an equal share of the budget.
      * Guardrail: business PnL >= biz_floor.
      * Diversity: at most `max_per_family` winners per strategy family, so the fleet
        can't collapse into one origin family.
      * Autosize: each of the N winners gets budget/N capital (writes total_amount_quote),
        so the fleet deploys the whole budget, not a fraction of it.
    """
    N = int(fleet.get("N") or 0)
    budget = float(budget if budget is not None else (fleet.get("total_capital") or 0.0))
    live_ids = set((fleet.get("controllers") or {}).keys())
    elig = [c for c in pool
            if c.get("origin") in live_ids and c.get("capital", 0) > 0 and c.get("biz", 0) >= biz_floor]
    for c in elig:
        c["_eff"] = c["volume"] / c["capital"]
    # dedup byte-identical clones (keep simplest label)
    uniq, dedup = {}, []
    for c in sorted(elig, key=lambda c: (-c["_eff"], len(str(c.get("params") or {})))):
        key = (c.get("origin"), round(c["_eff"], 4), round(c.get("biz", 0), 1))
        if key in uniq:
            continue
        uniq[key] = True
        dedup.append(c)
    selected, fam_count, seen = [], {}, set()
    for c in dedup:
        if len(selected) >= N:
            break
        if c["cand_id"] in seen:
            continue
        fam = _family(c["origin"])
        if fam_count.get(fam, 0) >= max_per_family:
            continue
        selected.append(c)
        seen.add(c["cand_id"])
        fam_count[fam] = fam_count.get(fam, 0) + 1
    # autosize to spend the budget: equal share per controller
    per = round(budget / max(1, len(selected)), 2) if selected else 0.0
    for c in selected:
        c["autosized_capital"] = per
        c["est_volume"] = round(c["_eff"] * per)  # volume scales ~linearly with capital
        c["full_config"] = dict(c["full_config"])
        c["full_config"]["total_amount_quote"] = per
    return selected


def plan_reshape(selected: list, fleet: dict) -> dict:
    """Diff the selected top-N against the current live fleet:
    keep   = live controllers whose exact config (base) is selected (no-op),
    deploy = selected candidates not currently live (new/tweaked → deploy),
    retire = live controllers absent from the selection (stop)."""
    live_ids = set((fleet.get("controllers") or {}).keys())
    sel_ids = {c["cand_id"] for c in selected}
    keep = [c for c in selected if c["cand_id"] in live_ids]
    deploy = [c for c in selected if c["cand_id"] not in live_ids]
    retire = sorted(live_ids - sel_ids)
    return {"keep": keep, "deploy": deploy, "retire": retire,
            "N": int(fleet.get("N") or 0), "n_selected": len(selected)}


async def _execute_reshape(live, selected: list, fleet: dict, ts: str) -> dict:
    """Apply the fleet reshape: save each selected config, deploy ONE new bot running
    the top-N controllers, then stop+archive the old bots. Fleet count stays = N.
    Returns a step-by-step status. High-consequence: stops live bots, redeploys."""
    steps = []
    instance = f"pmm-autopilot-{ts}"
    names = [c["cand_id"] for c in selected]
    budget = float(fleet.get("total_capital") or 0.0)
    # 1) save every selected config
    saved = 0
    for c in selected:
        try:
            await live.controllers.create_or_update_controller_config(c["cand_id"], c["full_config"])
            saved += 1
        except Exception as e:
            steps.append(f"save {c['cand_id']} FAILED: {str(e)[:80]}")
    steps.append(f"saved {saved}/{len(selected)} configs")
    if saved < len(selected):
        return {"ok": False, "steps": steps, "instance": instance, "aborted": "not all configs saved"}
    # 2) STOP the old bots FIRST — frees their capital so the new fleet can fund
    #    (deploy-then-stop would double pmm capital and blow past liquid BRL).
    old_bots = sorted({info.get("bot") for info in (fleet.get("controllers") or {}).values() if info.get("bot")})
    stopped = []
    for b in old_bots:
        try:
            await live.bot_orchestration.stop_and_archive_bot(
                bot_name=b, skip_order_cancellation=True, archive_locally=True)
            stopped.append(b)
        except Exception as e:
            steps.append(f"stop {b} FAILED: {str(e)[:80]}")
    steps.append(f"stopped {len(stopped)}/{len(old_bots)} old bots (capital freed)")
    # 3) deploy the new bot with the top-N controllers
    try:
        res = await live.bot_orchestration.deploy_v2_controllers(
            instance_name=instance, credentials_profile="master_account",
            controllers_config=names,
            max_global_drawdown_quote=round(budget * 0.15),
            max_controller_drawdown_quote=round(budget / max(1, len(names)) * 0.6),
            image="hummingbot/hummingbot:latest")
        steps.append(f"deployed `{instance}` with {len(names)} controllers ({str(res)[:60]})")
    except Exception as e:
        return {"ok": False, "steps": steps + [f"⚠️ DEPLOY FAILED after stopping old bots: {str(e)[:120]}",
                "→ restore: redeploy the archived configs, or re-run reshape"],
                "instance": instance, "aborted": "deploy failed"}
    return {"ok": True, "steps": steps, "instance": instance,
            "deployed": len(names), "stopped": len(stopped)}


def _proposal_keyboard(proposals):
    """Inline Deploy/Reject buttons per proposal — the human approval gate.
    callback_data routes to this routine's handle_callback:
    routines:pmm_autopilot:{deploy|reject}:{proposal_id} (<64 bytes)."""
    rows = []
    if len(proposals) > 1:
        rows.append([InlineKeyboardButton(f"🚀 Deploy ALL ({len(proposals)})",
                                          callback_data="routines:pmm_autopilot:deploy_all:all")])
    for p in proposals:
        pid = p["proposal_id"]
        rows.append([
            InlineKeyboardButton(f"✅ Deploy {p['label'][:16]}",
                                 callback_data=f"routines:pmm_autopilot:deploy:{pid}"),
            InlineKeyboardButton("❌", callback_data=f"routines:pmm_autopilot:reject:{pid}"),
        ])
    return InlineKeyboardMarkup(rows) if rows else None


async def _apply_proposal(live, p) -> tuple[bool, str]:
    """Apply one proposal's tuned params to its live controller in place."""
    cid = p["controller_id"]
    cfg = dict(p["full_config"])
    cfg["id"] = cid
    cfg.setdefault("controller_name", "pmm_mister")
    try:
        res = await live.controllers.update_bot_controller_config(
            bot_name=p.get("bot"), controller_name=cid, config=cfg)
        ok = (res.get("status") == "success") if isinstance(res, dict) else bool(res)
        return ok, (str(res)[:200])
    except Exception as e:
        return False, str(e)[:200]


async def handle_callback(update, context, action, params):
    """M4 — human approval gate. Applies proposals' tuned params to the LIVE
    controllers in place (update_bot_controller_config), only on an explicit press."""
    query = update.callback_query
    store = _load(PROPOSAL_STORE)
    base_text = (query.message.text if query.message else "") or ""
    chat_id = getattr(context, "_chat_id", None) or (query.message.chat_id if query.message else None)

    # ---- Deploy EVERYTHING: apply every still-open proposal at once ----
    if action == "deploy_all":
        todo = [pp for pp in store.values() if pp.get("status") == "proposed"]
        if not todo:
            await query.answer("No open proposals to deploy.", show_alert=True)
            return
        await query.answer(f"Deploying all {len(todo)} proposals…")
        live = await get_client(chat_id, context=context)
        ok_n, fails = 0, []
        for pp in todo:
            ok, info = await _apply_proposal(live, pp)
            pp["status"] = "deployed" if ok else "deploy_failed"
            pp["deploy_result"] = info
            store[pp["proposal_id"]] = pp
            ok_n += 1 if ok else 0
            if not ok:
                fails.append(pp["controller_id"])
        _save(PROPOSAL_STORE, store)
        tail = f"\n\n🚀 *Deployed {ok_n}/{len(todo)}* proposals to the live fleet."
        if fails:
            tail += "\n⚠️ failed: " + ", ".join(f"`{c}`" for c in fails[:8])
        try:
            await query.edit_message_text(base_text + tail, parse_mode="Markdown")
        except Exception:
            if chat_id and getattr(context, "bot", None):
                await context.bot.send_message(chat_id=chat_id, text=tail, parse_mode="Markdown")
        return

    # ---- Fleet RESHAPE: retire losers, promote the global top-N winners ----
    if action == "reshape":
        pool = _load(POOL_STORE)
        fleet = _load(FLEET_STORE)
        if not pool or not fleet:
            await query.answer("No reshape plan available (pool/fleet missing).", show_alert=True)
            return
        selected = select_topN(pool, fleet)
        plan = plan_reshape(selected, fleet)
        if plan.get("reshape_status") == "applied":
            await query.answer("Reshape already applied.", show_alert=True)
            return
        await query.answer(f"Reshaping fleet → top {len(selected)}… (stops old bots, redeploys)")
        live = await get_client(chat_id, context=context)
        ts = time.strftime("%Y%m%d%H%M%S")
        res = await _execute_reshape(live, selected, fleet, ts)
        _save(RESHAPE_STORE, {"applied": res.get("ok"), "instance": res.get("instance"),
                              "selected": [c["cand_id"] for c in selected], "steps": res["steps"]})
        head = ("🔄 *Reshape applied*" if res.get("ok") else "⚠️ *Reshape aborted*")
        tail = "\n".join("• " + s for s in res["steps"])
        try:
            await query.edit_message_text(base_text + f"\n\n{head}\n{tail}", parse_mode="Markdown")
        except Exception:
            if chat_id and getattr(context, "bot", None):
                await context.bot.send_message(chat_id=chat_id, text=f"{head}\n{tail}", parse_mode="Markdown")
        return

    pid = params[0] if params else None
    p = store.get(pid)
    if not p:
        await query.answer("Proposal expired or already handled.", show_alert=True)
        return

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
        live = await get_client(chat_id, context=context)
        ok, info = await _apply_proposal(live, p)
        p["status"] = "deployed" if ok else "deploy_failed"
        p["deploy_result"] = info
        store[pid] = p
        _save(PROPOSAL_STORE, store)
        tail = (f"\n\n✅ *Deployed* `{p['label']}` → `{p['controller_id']}` on `{p.get('bot')}`"
                if ok else f"\n\n⚠️ deploy returned: `{info[:150]}`")
        try:
            await query.edit_message_text(base_text + tail, parse_mode="Markdown")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Reshape lap: sweep the whole live fleet, pool variants, propose the top-N,
# send it to Telegram (proposal + Deploy button + current-generation HTML).
# ---------------------------------------------------------------------------

def _read_token():
    import re
    try:
        for line in open("/Users/blaspalmisciano/condor/.env"):
            m = re.match(r'\s*TELEGRAM_TOKEN\s*=\s*"?([^"\s]+)', line)
            if m:
                return m.group(1)
    except Exception:
        return None


def _reshape_keyboard():
    return {"inline_keyboard": [[{"text": "🔄 Apply reshape",
                                  "callback_data": "routines:pmm_autopilot:reshape:go"}]]}


def _tg_send_message(chat_id, text, reply_markup=None, token=None):
    import json as _j, urllib.request as _u
    token = token or _read_token()
    d = {"chat_id": chat_id, "text": text}       # plaintext — no parse_mode (avoids 400s)
    if reply_markup:
        d["reply_markup"] = _j.dumps(reply_markup)
    req = _u.Request(f"https://api.telegram.org/bot{token}/sendMessage",
                     data=_j.dumps(d).encode(), headers={"Content-Type": "application/json"})
    try:
        return _j.load(_u.urlopen(req, timeout=30)).get("ok")
    except Exception:
        return False


def _tg_send_document(chat_id, path, caption="", token=None):
    import json as _j, urllib.request as _u, os as _o
    token = token or _read_token()
    boundary = "----pmmauto" + str(int(time.time()))
    with open(path, "rb") as f:
        content = f.read()
    fn = _o.path.basename(path)
    parts = []

    def add(name, val):
        parts.append((f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{val}\r\n').encode())
    add("chat_id", str(chat_id))
    if caption:
        add("caption", caption[:1000])
    parts.append((f'--{boundary}\r\nContent-Disposition: form-data; name="document"; filename="{fn}"\r\n'
                  f'Content-Type: text/html\r\n\r\n').encode() + content + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    req = _u.Request(f"https://api.telegram.org/bot{token}/sendDocument", data=b"".join(parts),
                     headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    try:
        return _j.load(_u.urlopen(req, timeout=180)).get("ok")
    except Exception:
        return False


def _reshape_summary_text(selected, plan, fleet):
    from collections import Counter
    if not selected:
        return "🔄 PMM Autopilot — no reshape proposal this lap (fleet already optimal)."
    fam = Counter(_family(c["origin"]) for c in selected)
    top = sorted(selected, key=lambda c: -c.get("est_volume", c["volume"]))[:5]
    cap = sum(c.get("autosized_capital", c["capital"]) for c in selected)
    lines = [f"🔄 PMM Autopilot — reshape proposal ({fleet['N']} live controllers, {cap:,.0f} BRL)",
             f"keep {len(plan['keep'])} · deploy {len(plan['deploy'])} · retire {len(plan['retire'])}",
             f"families: {', '.join(f'{f}×{n}' for f, n in fam.most_common())}", "", "Top 5 by est. volume:"]
    for i, c in enumerate(top, 1):
        lines.append(f"  {i}. {c['cand_id']}  ~{c.get('est_volume', c['volume']):,} vol")
    lines += ["", "Press Apply reshape to stop the old bot and deploy this top-"
              f"{len(selected)} set. Ignore it and the next lap proposes again. Nothing happens until you press."]
    return "\n".join(lines)


async def _build_gen_report(pool, fleet, selected):
    """Current-generation HTML: volume curves + PnL curves (joined from sweep cache)
    + PnL-vs-volume scatter + leaderboards. Returns the saved HTML path (or None)."""
    import glob, json as _j
    from datetime import datetime as _dtc, timezone as _tz
    import plotly.graph_objects as go
    from condor.reports import ReportBuilder
    curve_idx = {}
    for f in glob.glob("data/pmm_sweep_store*.json"):
        try:
            store = _j.load(open(f))
        except Exception:
            continue
        for _sk, trials in store.items():
            for _tid, t in trials.items():
                if isinstance(t, dict) and t.get("pnl_curve"):
                    biz = (t.get("realized", 0) + t.get("unrealized", 0)) + t.get("volume", 0) * 0.00015
                    curve_idx.setdefault((round(t.get("volume", 0)), round(biz, 1), round(t.get("max_dd", 0), 1)), t)
    live_ids = set((fleet.get("controllers") or {}).keys())
    inpool = [c for c in pool if c["origin"] in live_ids]
    win_ids = {c["cand_id"] for c in selected}
    base_ids = {c["cand_id"] for c in inpool if c.get("is_base")}
    for c in inpool:
        t = curve_idx.get((round(c["volume"]), round(c["biz"], 1), round(c["max_dd"], 1)))
        c["pnl_curve"], c["vol_curve"] = (t.get("pnl_curve"), t.get("vol_curve")) if t else (None, None)
    DARK = dict(paper_bgcolor="#0d1117", plot_bgcolor="#161b22", font=dict(color="#c9d1d9", size=12))
    PAL = ["#58a6ff", "#f0883e", "#3fb950", "#a371f7", "#e3b341", "#56d4dd", "#ff7b72", "#7ee787"]
    def dtx(ts):
        return _dtc.fromtimestamp(float(ts), tz=_tz.utc)
    def overlay(metric, title, ylab):
        key = "pnl_curve" if metric == "pnl" else "vol_curve"
        fig = go.Figure(); first = True
        for c in inpool:
            if c["cand_id"] in base_ids and c.get(key):
                fig.add_trace(go.Scatter(x=[dtx(a) for a, _ in c[key]], y=[v for _, v in c[key]],
                    line=dict(width=1, color="#8b949e"), opacity=0.4, name="current live",
                    legendgroup="base", showlegend=first, hovertext=c["cand_id"], hoverinfo="text+y"))
                first = False
        for i, c in enumerate(sorted([c for c in inpool if c["cand_id"] in win_ids and c.get(key)],
                                     key=lambda c: -c["volume"])[:8]):
            fig.add_trace(go.Scatter(x=[dtx(a) for a, _ in c[key]], y=[v for _, v in c[key]],
                line=dict(width=2.2, color=PAL[i % len(PAL)]), name=f"★ {c['cand_id'][:26]}",
                hovertext=c["cand_id"], hoverinfo="text+y"))
        if metric == "pnl":
            fig.add_hline(y=0, line_dash="dot", line_color="white", opacity=0.3)
        fig.update_layout(**DARK, height=520, title=title, margin=dict(l=60, r=20, t=70, b=40),
                          legend=dict(orientation="h", y=1.02, x=0, font=dict(size=9)))
        fig.update_xaxes(gridcolor="#21262d"); fig.update_yaxes(gridcolor="#21262d", title=ylab)
        return fig
    def scatter():
        fig = go.Figure()
        others = [c for c in inpool if c["cand_id"] not in win_ids and c["cand_id"] not in base_ids]
        fig.add_trace(go.Scatter(x=[c["volume"] for c in others], y=[c["biz"] for c in others], mode="markers",
            name=f"all variants ({len(others)})", marker=dict(size=6, color="rgba(139,148,158,0.45)"),
            text=[c["cand_id"] for c in others], hovertemplate="%{text}<br>vol %{x:,.0f}<br>biz %{y:.1f}<extra></extra>"))
        bs = [c for c in inpool if c["cand_id"] in base_ids]
        fig.add_trace(go.Scatter(x=[c["volume"] for c in bs], y=[c["biz"] for c in bs], mode="markers",
            name="current live (base)", marker=dict(size=10, color="#8b949e", symbol="square", line=dict(width=1, color="#fff")),
            text=[c["cand_id"] for c in bs], hovertemplate="%{text}<br>vol %{x:,.0f}<br>biz %{y:.1f}<extra></extra>"))
        ws = [c for c in inpool if c["cand_id"] in win_ids]
        fig.add_trace(go.Scatter(x=[c["volume"] for c in ws], y=[c["biz"] for c in ws], mode="markers",
            name=f"new generation ★ ({len(ws)})", marker=dict(size=13, color="#3fb950", symbol="star", line=dict(width=1, color="#fff")),
            text=[c["cand_id"] for c in ws], hovertemplate="%{text}<br>vol %{x:,.0f}<br>biz %{y:.1f}<extra></extra>"))
        fig.add_hline(y=0, line_dash="dot", line_color="white", opacity=0.3)
        fig.update_layout(**DARK, height=560, title="PnL vs Volume — every variant; ★ = new generation",
                          margin=dict(l=60, r=20, t=70, b=50), legend=dict(orientation="h", y=1.02, x=0))
        fig.update_xaxes(gridcolor="#21262d", title="Cumulative volume (BRL)")
        fig.update_yaxes(gridcolor="#21262d", title="Business PnL (BRL)")
        return fig
    if not inpool:
        return None
    topvol = sorted(inpool, key=lambda c: -c["volume"])[:12]
    topbiz = sorted(inpool, key=lambda c: -c["biz"])[:12]
    b = ReportBuilder("BTC-BRL Fleet — Current Generation Sweep (1s)")
    b.source("routine", "pmm_autopilot").tags(["pmm_mister", "sweep", "btcbrl", "1s", "reshape"])
    b.manual_order()
    b.kpi("Live controllers", str(fleet["N"])); b.kpi("Variants pooled", str(len(inpool)))
    b.kpi("New-gen winners", str(len(selected))); b.kpi("Best volume", f"{max(c['volume'] for c in inpool):,.0f}")
    b.markdown(f"# BTC-BRL fleet — current generation sweep\n_1-second backtests across the **{fleet['N']} live "
               f"pmm controllers**, every variant pooled ({len(inpool)}). Faded gray = current live; ★ = the "
               f"winners the agent selected for the next generation._")
    b.markdown("## Cumulative volume — winners vs current live")
    b.plotly(overlay("vol", "Cumulative volume (BRL) — ★ new generation vs current live", "Cumulative volume (BRL)"))
    b.markdown("## Market PnL — winners vs current live")
    b.plotly(overlay("pnl", "Market PnL, mark-to-market (BRL) — ★ new generation vs current live", "Market PnL (BRL)"))
    b.markdown("## PnL vs Volume frontier")
    b.plotly(scatter())
    b.markdown("## 🏆 Top 12 by volume")
    b.table([{"#": str(i), "Candidate": c["cand_id"], "Volume": f"{c['volume']:,.0f}", "Business PnL": f"{c['biz']:+.1f}",
              "MaxDD": f"{c['max_dd']}", "New gen": "★" if c["cand_id"] in win_ids else ""} for i, c in enumerate(topvol, 1)],
             columns=["#", "Candidate", "Volume", "Business PnL", "MaxDD", "New gen"])
    b.markdown("## 💰 Top 12 by business PnL")
    b.table([{"#": str(i), "Candidate": c["cand_id"], "Business PnL": f"{c['biz']:+.1f}", "Volume": f"{c['volume']:,.0f}",
              "MaxDD": f"{c['max_dd']}", "New gen": "★" if c["cand_id"] in win_ids else ""} for i, c in enumerate(topbiz, 1)],
             columns=["#", "Candidate", "Business PnL", "Volume", "MaxDD", "New gen"])
    rid = await b.save()
    hits = sorted(glob.glob(f"reports/*{rid}*.html"), key=lambda p: -os.path.getmtime(p))
    return hits[0] if hits else None


async def _reshape_lap(config: Config, live, local, log) -> dict:
    """One reshape lap: sweep the whole live pmm fleet on local, pool every variant,
    snapshot the fleet, and select the global top-N. Writes POOL_STORE + FLEET_STORE."""
    live_perf, cfg_by, bot_names, health, cid_bot = await gather_active(live)
    ctrls = [(cid, c) for cid, c in cfg_by.items()
             if cid in live_perf and (_clean_live_config(c).get("controller_name") == "pmm_mister")]
    if config.only_pair:
        ctrls = [(cid, c) for cid, c in ctrls if (c.get("trading_pair", "").upper() == config.only_pair.upper())]
    if not ctrls:
        return {"selected": [], "plan": {}, "fleet": {}, "summary": "No live pmm_mister controllers to optimize."}
    ctrls.sort(key=lambda kv: -live_perf.get(kv[0], {}).get("volume_traded", 0.0))
    fleet = {cid: {"bot": cid_bot.get(cid),
                   "capital": float(_clean_live_config(c).get("total_amount_quote") or 0),
                   "live_vol": live_perf.get(cid, {}).get("volume_traded", 0)} for cid, c in ctrls}
    total_cap = sum(f["capital"] for f in fleet.values())
    fsnap = {"N": len(ctrls), "total_capital": total_cap, "controllers": fleet}
    _save(FLEET_STORE, fsnap)
    w1 = int(time.time()) - 300
    w0 = w1 - config.sweep_window_hours * 3600
    scfg = SweepConfig(resolution=config.resolution, days=1)
    cfgobj = type("SweepCfg", (), {"window_hours": config.sweep_window_hours, "resolution": config.resolution})()
    pool = []
    for cid, base_cfg in ctrls:
        bc = _clean_live_config(dict(base_cfg)); bc.setdefault("id", cid)
        cap = float(bc.get("total_amount_quote") or 0)
        try:
            swept = await _sweep_controller(local, cid, bc, w0, w1, scfg, cfgobj, log)
        except Exception as e:
            log(f"{cid} sweep error: {e}")
            continue
        if not swept:
            continue
        base_t, best, ok, axes = swept
        for t in ok:
            lbl = _tlabel(t, axes) or "BASE"
            params = t.get("params") or {}
            fc = apply_params(bc, params, axes)
            nid = cid if not params else f"{cid}__{lbl.lower().replace(' ', '_').replace('×', 'x')}"
            fc["id"] = nid
            pool.append({"origin": cid, "label": lbl, "params": params, "cand_id": nid,
                         "volume": round(t["volume"]), "biz": round(t["_biz"], 2),
                         "max_dd": round(t["max_dd"], 1), "capital": cap, "bot": cid_bot.get(cid),
                         "full_config": fc, "is_base": not params})
        _save(POOL_STORE, pool)
        log(f"{cid}: pooled +{len(ok)} variants")
    if not pool:
        return {"selected": [], "plan": {}, "fleet": fsnap, "summary": "Sweep produced no candidates."}
    selected = select_topN(pool, fsnap, budget=total_cap)
    plan = plan_reshape(selected, fsnap)
    return {"selected": selected, "plan": plan, "fleet": fsnap, "pool": pool,
            "summary": _reshape_summary_text(selected, plan, fsnap)}


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> RoutineResult:
    chat_id = getattr(context, "_chat_id", None)
    live = await get_client(chat_id, context=context)  # brigado — READ ONLY
    if not live:
        return RoutineResult(text="No live server available. Configure servers in /config.")
    local = HummingbotAPIClient(
        base_url=config.local_url, username=config.local_user, password=config.local_pass,
        timeout=ClientTimeout(total=1800, connect=15))
    await local.init()

    chat_id = chat_id or 6310433268
    token = _read_token()
    last = {"selected": [], "plan": {}, "fleet": {}, "summary": "no lap yet"}
    lap = 0
    try:
        while True:
            if (config.run_at_hours or "").strip():
                await asyncio.sleep(_next_target_seconds(config.run_at_hours))  # anchor to clock time
            lap += 1
            lap_start = time.time()
            logs = []
            try:
                last = await _reshape_lap(config, live, local, lambda m: logs.append(str(m)))
            except Exception as e:
                last = {"selected": [], "plan": {}, "fleet": {}, "summary": f"⚠️ lap {lap} error: {e}"}
            # send the proposal + Deploy button, then the current-generation HTML report
            try:
                rm = _reshape_keyboard() if (last.get("selected") and not config.dry_run) else None
                _tg_send_message(chat_id, last["summary"], reply_markup=rm, token=token)
                if last.get("selected"):
                    html = await _build_gen_report(last.get("pool") or _load(POOL_STORE),
                                                   last["fleet"], last["selected"])
                    if html:
                        _tg_send_document(chat_id, html,
                                          caption="Current generation — ★ winners vs live", token=token)
            except Exception as e:
                logs.append(f"send error: {e}")
            if config.max_laps and lap >= config.max_laps:
                break
            if not (config.run_at_hours or "").strip():
                elapsed = time.time() - lap_start
                await asyncio.sleep(max(60, config.frequency_sec - elapsed))  # exact interval: subtract runtime
    except asyncio.CancelledError:
        return RoutineResult(text=f"PMM Autopilot stopped after {lap} lap(s).")
    finally:
        try:
            await local.close()
        except Exception:
            pass

    live_ids = set((last.get("fleet", {}).get("controllers") or {}).keys())
    rows = [{
        "Candidate": c["cand_id"], "Volume": f"{c['volume']:,.0f}", "Business PnL": f"{c['biz']:+.1f}",
        "Autosized cap": f"{c.get('autosized_capital', c['capital']):,.0f}",
        "Status": "keep" if c["cand_id"] in live_ids else "deploy",
    } for c in last.get("selected", [])]
    cols = ["Candidate", "Volume", "Business PnL", "Autosized cap", "Status"]
    return RoutineResult(text=last["summary"], table_data=rows or None, table_columns=cols if rows else None)
