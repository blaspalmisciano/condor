"""PMM Autopilot orchestrator — one autonomous lap of the self-improving loop.

Chains the pieces the flowchart shows, end to end:
  1. read the LIVE running controllers + their current configs   (read-only)
  2. sweep each live config on the LOCAL parity backtest stack     (base -> OAT -> combinations, 1s)
  3. rank the variants by risk-adjusted score                      (risk_score)
  4. calibrate live-vs-backtest and haircut trust                  (calibration_trust)
  5. gate + propose the winners for human approval                 (promotion_gate; Telegram)

Design invariants:
  * Live bots are read ONLY here (get_active_bots_status / get_bot_controller_configs).
    Nothing is deployed or mutated on the live server by this routine.
  * All backtests run on the LOCAL stack (config.local_url), never on the live API,
    so a 1s sweep can't freeze the live bots.
  * Deploy stays a HUMAN action — winners are proposed, not shipped.

Milestone 1: one lap, dry. CONTINUOUS scheduling (M3) and the deploy-on-approval
button wiring (M4) build on top of this without changing the lap logic.
"""
import json
import os
import time

from pydantic import BaseModel, Field
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

PROPOSAL_STORE = "data/pmm_autopilot_proposals.json"
CALIB_STORE = "data/pmm_autopilot_calibration.json"


class Config(BaseModel):
    """PMM Autopilot — one lap: sweep the live controllers, propose winners (human-gated)."""

    live_server: str = Field(default="brigado", description="server running the live bots (read-only)")
    local_url: str = Field(default="http://localhost:8000", description="local backtest API base url")
    local_user: str = Field(default="elamigo", description="local API username")
    local_pass: str = Field(default="barabit", description="local API password")
    resolution: str = Field(default="1s", description="backtest resolution (1s resolves sub-minute knobs)")
    window_hours: int = Field(default=24, description="backtest window length in hours")
    max_controllers: int = Field(default=3, description="max live controllers to sweep this lap")
    only_pair: str = Field(default="", description="filter to one trading pair, e.g. BTC-BRL (blank = all)")
    min_uplift: float = Field(default=0.0, description="min risk-adj uplift over base to propose")
    dry_run: bool = Field(default=True, description="M1: propose only; the deploy button is wired in M4")


def _load(path):
    try:
        return json.load(open(path))
    except Exception:
        return {}


def _save(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    json.dump(obj, open(tmp, "w"), default=str)
    os.replace(tmp, path)


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> RoutineResult:
    chat_id = getattr(context, "_chat_id", None)
    live = await get_client(chat_id, context=context)  # brigado — READ ONLY
    if not live:
        return RoutineResult(text="No live server available. Configure servers in /config.")

    local = HummingbotAPIClient(
        base_url=config.local_url, username=config.local_user, password=config.local_pass,
        timeout=ClientTimeout(total=1800, connect=15))
    await local.init()

    logs: list[str] = []
    def log(m):
        logs.append(str(m))

    try:
        # 1) read the live fleet (read only)
        live_perf, cfg_by_ctrl, bot_names, bot_health, cid_bot = await gather_active(live)

        cands = []
        for cid, cfg in cfg_by_ctrl.items():
            if cid not in live_perf:  # only actually-running controllers
                continue
            if (cfg.get("controller_name") or "") != "pmm_mister":
                continue
            if config.only_pair and (cfg.get("trading_pair", "").upper() != config.only_pair.upper()):
                continue
            cands.append((cid, cfg))
        cands.sort(key=lambda kv: -live_perf.get(kv[0], {}).get("volume_traded", 0.0))
        cands = cands[: config.max_controllers]

        if not cands:
            return RoutineResult(text="No running pmm_mister controllers found to optimize "
                                      f"(server={config.live_server}, pair={config.only_pair or 'any'}).")

        w1 = int(time.time()) - 300
        w0 = w1 - config.window_hours * 3600
        scfg = SweepConfig(resolution=config.resolution, days=max(1, config.window_hours // 24 or 1))

        proposals = []
        analyses = []
        for cid, base_cfg in cands:
            base_cfg = dict(base_cfg)
            base_cfg.setdefault("id", cid)
            pair = (base_cfg.get("trading_pair") or "").upper()
            conn = base_cfg.get("connector_name") or "binance"

            days_back = max(1, config.window_hours // 24 + 1)
            rows = _extract_rows(await local.market_data.get_candles_last_days(conn, pair, days_back, "1m"))
            cw = [c for c in rows if w0 - 60 <= float(c["timestamp"]) <= w1 + 60]
            if len(cw) < 50:
                log(f"{cid}: only {len(cw)} candles for window — skipped")
                continue
            ref = sum(float(c["close"]) for c in cw) / len(cw)

            log(f"{cid} [{pair}] sweeping {config.resolution} over {config.window_hours}h on local…")
            trials = await run_sweep(local, base_cfg, w0, w1, scfg, ref, log=lambda m, c=cid: log(f"  {c}: {m}"))
            ok = [t for t in trials if "error" not in t]
            if not ok:
                log(f"{cid}: no successful trials")
                continue

            axes = get_axes(base_cfg)
            for t in ok:
                t["_rs"] = risk_score(t, scfg.rebate_rate)["risk_adj_pnl"]
            base_t = next((t for t in ok if not t.get("params")), None)
            best = max(ok, key=lambda t: t["_rs"])
            base_rs = base_t["_rs"] if base_t else 0.0
            uplift = best["_rs"] - base_rs

            # calibration: how well the live controller tracks its own backtest
            lp = live_perf.get(cid, {})
            base_bt_pnl = score(base_t, scfg.rebate_rate)["total_pnl"] if base_t else 0.0
            calib = calibration_trust(
                lp.get("realized", 0.0) + lp.get("unrealized", 0.0), base_bt_pnl,
                lp.get("volume_traded", 0.0), base_t["volume"] if base_t else 0.0)

            label = _tlabel(best, axes) or "BASE"
            log(f"{cid}: base risk-adj {base_rs:+.1f} -> best '{label}' {best['_rs']:+.1f} "
                f"(uplift {uplift:+.1f}) | live-vs-bt trust {calib['trust']}")

            improved = bool(best.get("params")) and uplift >= config.min_uplift
            analyses.append({
                "controller_id": cid, "pair": pair, "n_trials": len(ok),
                "base_risk_adj": round(base_rs, 2), "best_label": label,
                "best_risk_adj": round(best["_rs"], 2), "uplift": round(uplift, 2),
                "trust": calib["trust"], "vol_ratio": round(calib["vol_ratio"], 2),
                "proposed": improved,
            })

            if improved:
                full_cfg = apply_params(base_cfg, best["params"], axes)
                full_cfg["id"] = f"{cid}__{label.lower().replace(' ', '_').replace('×', 'x')}"
                pid = f"{cid[:8]}-{int(time.time())%100000}"
                proposals.append({
                    "proposal_id": pid, "controller_id": cid, "bot": cid_bot.get(cid),
                    "pair": pair, "label": label, "params": best["params"],
                    "base_risk_adj": round(base_rs, 2), "best_risk_adj": round(best["_rs"], 2),
                    "uplift": round(uplift, 2),
                    "base_biz": round(score(base_t, scfg.rebate_rate)["business_pnl"], 2) if base_t else None,
                    "best_biz": round(score(best, scfg.rebate_rate)["business_pnl"], 2),
                    "best_vol": round(best["volume"], 0), "max_dd": round(best["max_dd"], 1),
                    "calibration": calib, "full_config": full_cfg,
                    "window": [w0, w1], "resolution": config.resolution,
                    "created": int(time.time()), "status": "proposed",
                })

        # persist proposals + calibration for the (M4) deploy handler and next lap
        store = _load(PROPOSAL_STORE)
        for p in proposals:
            store[p["proposal_id"]] = p
        _save(PROPOSAL_STORE, store)
        calib_store = _load(CALIB_STORE)
        for p in proposals:
            calib_store.setdefault(p["controller_id"], []).append(
                {"t": p["created"], **p["calibration"]})
        _save(CALIB_STORE, calib_store)

        # summary
        lines = [f"🤖 *PMM Autopilot* — lap complete ({len(analyses)} live controllers swept, "
                 f"{config.resolution}/{config.window_hours}h on local stack)\n"]
        # what the lap examined (transparency, even when nothing is proposed)
        for a in analyses:
            flag = "✅ propose" if a["proposed"] else "— hold (base optimal)"
            lines.append(
                f"• `{a['controller_id']}` [{a['pair']}] · {a['n_trials']} variants\n"
                f"    risk-adj base {a['base_risk_adj']:+} → best '{a['best_label']}' {a['best_risk_adj']:+} "
                f"(uplift {a['uplift']:+}) · live-vs-bt trust {a['trust']} → {flag}")
        if not proposals:
            lines.append("\n_No variant beat the live config — fleet is near-optimal for this window._")
        else:
            lines.append(f"\n*{len(proposals)} proposal(s) awaiting your OK:*")
        for p in proposals:
            c = p["calibration"]
            lines.append(
                f"• `{p['controller_id']}` [{p['pair']}] → *{p['label']}*\n"
                f"    risk-adj {p['base_risk_adj']:+} → {p['best_risk_adj']:+} (uplift {p['uplift']:+}) · "
                f"vol {p['best_vol']:,.0f} · DD {p['max_dd']}\n"
                f"    live-vs-backtest trust {c['trust']} (vol×{c['vol_ratio']:.2f})\n"
                f"    proposal `{p['proposal_id']}` — awaiting your OK" + (" (deploy wiring: M4)" if config.dry_run else ""))
        summary = "\n".join(lines)

        # optional: emit to the chat if a bot is present
        try:
            if chat_id and getattr(context, "bot", None):
                await context.bot.send_message(chat_id=chat_id, text=summary, parse_mode="Markdown")
        except Exception as e:
            log(f"telegram send skipped: {e}")

        rows = [{
            "Controller": a["controller_id"], "Pair": a["pair"], "Variants": a["n_trials"],
            "Base risk-adj": a["base_risk_adj"], "Best": a["best_label"],
            "Best risk-adj": a["best_risk_adj"], "Uplift": a["uplift"], "Trust": a["trust"],
            "Action": "propose" if a["proposed"] else "hold",
        } for a in analyses]
        cols = ["Controller", "Pair", "Variants", "Base risk-adj", "Best", "Best risk-adj", "Uplift", "Trust", "Action"]
        return RoutineResult(text=summary, table_data=rows or None, table_columns=cols if rows else None)
    finally:
        try:
            await local.close()
        except Exception:
            pass
