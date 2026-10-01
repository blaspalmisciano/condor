"""
Botcamp Agent Builders Cup — autonomous market-making agent (CONTINUOUS routine).

Two cooperating loops sharing a bench (see routines/botcamp_bench.py for the pure core):

  1. BENCH BUILDER — each cycle, generate a batch of RANDOM pmm_mister param variants,
     backtest each over K disjoint daily windows, score by MEDIAN daily PnL, and upsert
     into the bench (sorted PnL-desc, deduped, eligibility = positive in >= N windows).
     Runs continuously: when one batch finishes, the next starts.

  2. FLEET MANAGER — each cycle, keep `fleet_size` pmm_mister controllers live. Any
     controller at <= `sub_upnl` unrealized PnL, OR with no new volume for >= `sub_no_trade_h`
     hours, is SUBSTITUTED (live config update) with the top ELIGIBLE bench config
     (excluding the one that just failed). No warmup grace; no global kill.

Autonomous: no human gate. On the competition infra the live server and the backtest
stack are the same Hummingbot API instance (`local_url`). `dry_run=True` runs the full
decision logic (bench + triggers + selection) but performs NO live deploys/updates — used
for offline/integration testing.

Deterministic code (zero LLM tokens per cycle). This routine IS the agent's engine; the
agent (AGENT.md + loops/mm_cup_operator/loop.md) owns and runs it.
"""
from __future__ import annotations

import asyncio
import copy
import random
import time
from typing import Any, Optional

from pydantic import BaseModel, Field
from telegram.ext import ContextTypes

from config_manager import get_client
from routines.botcamp_bench import (
    k_windows, trial_pnl, make_bench_entry, bench_upsert, select_substitute,
    param_sig, bench_eligible,
)

CONTINUOUS = True

BENCH_STORE = "data/botcamp_bench.json"
SEED_BENCH = "agents/market_making_cup/sample_configs/bench_seed.json"  # shipped fallback (egress hedge)
STATE_STORE = "data/botcamp_mm_state.json"


class Config(BaseModel):
    """Autonomous MM competition agent — continuous bench + fleet manager."""
    trading_pair: str = Field(default="BTC-USDT", description="Pair to market-make")
    connector_name: str = Field(default="gate_io", description="Exchange connector")
    fleet_size: int = Field(default=2, description="Live controllers to keep running")
    capital_quote: float = Field(default=800.0, description="Total quote capital (split across fleet)")
    # substitution rules
    sub_upnl: float = Field(default=-20.0, description="Substitute at <= this unrealized PnL")
    sub_no_trade_h: float = Field(default=4.0, description="Substitute after this many hours with no new volume")
    # bench scoring
    lookback_days: int = Field(default=7, description="Backtest lookback window count (disjoint 24h windows)")
    n_windows: int = Field(default=7, description="K disjoint daily windows for robust scoring")
    window_hours: int = Field(default=24, description="Length of each bench window")
    min_positive_windows: int = Field(default=5, description="Config eligible only if positive in >= this many windows")
    resolution: str = Field(default="1s", description="Backtest resolution")
    trade_cost: float = Field(default=0.0, description="Maker fee fraction (set once Gate tier confirmed; 0 = free maker)")
    variants_per_cycle: int = Field(default=2, description="New random variants backtested per cycle")
    # loop timing
    cycle_sleep_sec: int = Field(default=120, description="Pause between cycles")
    # infra
    local_url: str = Field(default="http://localhost:8000", description="Hummingbot API base url (backtest+live)")
    local_user: str = Field(default="elamigo")
    local_pass: str = Field(default="barabit")
    credentials_profile: str = Field(default="master_account")
    deploy_image: str = Field(default="hummingbot/hummingbot:latest")
    bot_name: str = Field(default="botcamp-mm", description="Bot instance that hosts the fleet")
    dry_run: bool = Field(default=True, description="Decision logic only; no live deploys/updates")


# --------------------------------------------------------------------------- #
# small json store helpers (match the autopilot's forgiving style)             #
# --------------------------------------------------------------------------- #
def _load(path, default):
    import json, os
    try:
        if os.path.exists(path):
            return json.load(open(path))
    except Exception:
        pass
    return default


def _save(path, obj):
    import json, os
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    json.dump(obj, open(tmp, "w"), default=str)
    os.replace(tmp, path)


# --------------------------------------------------------------------------- #
# base config + random variant generation (the "random changes" search)        #
# --------------------------------------------------------------------------- #
def base_config(cfg: Config) -> dict:
    """A COMPLETE, known-good pmm_mister config (verified to generate executors in the
    backtest) — modelled on real high-volume MM runs: tight spreads, patient
    effectivization, lean-long inventory band. The full field set matters: a minimal
    config produces 0 executors (missing amounts_pct / order types / tolerances / global
    SL-TP / skew etc.). random_variant() overrides only the tunable params on top of this."""
    return {
        "controller_name": "pmm_mister", "controller_type": "generic",
        "connector_name": cfg.connector_name, "trading_pair": cfg.trading_pair,
        "total_amount_quote": round(cfg.capital_quote / max(1, cfg.fleet_size), 2),
        # tunable (random_variant overrides these)
        "buy_spreads": [0.0005], "sell_spreads": [0.0005], "take_profit": 0.0001,
        "min_base_pct": 0.2, "target_base_pct": 0.5, "max_base_pct": 0.8,
        "executor_refresh_time": 300,
        "buy_position_effectivization_time": 900, "sell_position_effectivization_time": 900,
        # structural (required for the controller to actually quote)
        "buy_amounts_pct": ["1"], "sell_amounts_pct": ["1"],
        "buy_cooldown_time": 10, "sell_cooldown_time": 10,
        "max_active_executors_by_level": 20,
        "open_order_type": 3, "take_profit_order_type": 3,
        "tick_mode": False, "price_distance_tolerance": "0.0003",
        "refresh_tolerance": "0.0012", "tolerance_scaling": "1.2", "min_skew": "1",
        "portfolio_allocation": "0.025", "leverage": 1, "position_mode": "ONEWAY",
        "position_side": "BUY", "position_profit_protection": True,
        "manual_kill_switch": False, "initial_positions": [],
        # global safety (kept conservative; the organizers also impose hard limits)
        "global_pnl_reference": "position",
        "global_sl_enabled": True, "global_stop_loss": "0.01", "global_sl_activation_from": "target_base",
        "global_tp_enabled": True, "global_take_profit": "0.005", "global_tp_activation_from": "min_base",
    }


_SPREADS = [0.0003, 0.0005, 0.0008, 0.0012, 0.002]
_TPS = [0.00005, 0.0001, 0.0002, 0.0004]
_EFF = [600, 900, 1800, 2700]
_REFRESH = [120, 300, 600]
_BANDS = [(0.2, 0.5, 0.8), (0.1, 0.4, 0.7), (0.3, 0.5, 0.7), (0.25, 0.5, 0.75)]


def random_variant(cfg: Config, rng: random.Random) -> dict:
    """Random pmm_mister param combination around the base — the bench's search."""
    c = base_config(cfg)
    s = rng.choice(_SPREADS)
    c["buy_spreads"] = [s]; c["sell_spreads"] = [s]
    c["take_profit"] = rng.choice(_TPS)
    eff = rng.choice(_EFF)
    c["buy_position_effectivization_time"] = eff
    c["sell_position_effectivization_time"] = eff
    c["executor_refresh_time"] = rng.choice(_REFRESH)
    lo, mid, hi = rng.choice(_BANDS)
    c["min_base_pct"], c["target_base_pct"], c["max_base_pct"] = lo, mid, hi
    return c


# --------------------------------------------------------------------------- #
# bench builder — backtest one variant over K windows -> per-window PnL         #
# --------------------------------------------------------------------------- #
async def backtest_over_windows(local, cfg: Config, variant: dict, windows, log) -> Optional[list[float]]:
    """Return per-window PnL list (one float per window), or None if data unavailable."""
    from routines.pmm_sweep import run_trial
    from routines.pmm_level_preview import _extract_rows
    conn, pair = variant["connector_name"], variant["trading_pair"]
    pnls: list[float] = []
    for (w0, w1) in windows:
        try:
            rows = _extract_rows(await local.market_data.get_candles_last_days(conn, pair, cfg.lookback_days + 1, "1m"))
            cw = [c for c in rows if w0 - 60 <= float(c["timestamp"]) <= w1 + 60]
            if len(cw) < 30:
                return None  # candle data not reachable / insufficient (egress issue)
            ref = sum(float(c["close"]) for c in cw) / len(cw)
            r = await run_trial(local, variant, {}, w0, w1, cfg.resolution, cfg.trade_cost, ref)
            if "error" in r:
                return None
            pnls.append(trial_pnl(r))
        except Exception as e:
            log(f"backtest window err: {str(e)[:60]}")
            return None
    return pnls


async def grow_bench(local, cfg: Config, bench: list[dict], log) -> list[dict]:
    """Backtest `variants_per_cycle` fresh random variants and upsert into the bench."""
    rng = random.Random()
    windows = k_windows(n=cfg.n_windows, window_h=cfg.window_hours)
    for _ in range(max(1, cfg.variants_per_cycle)):
        v = random_variant(cfg, rng)
        pnls = await backtest_over_windows(local, cfg, v, windows, log)
        if not pnls:
            log("bench: no candle data this variant (egress?) — skipping")
            continue
        entry = make_bench_entry(v, pnls)
        bench = bench_upsert(bench, entry)
        log(f"bench +1: median {entry['median_pnl']:.2f}, +{entry['n_positive']}/{entry['n_windows']} windows")
    return bench


# --------------------------------------------------------------------------- #
# fleet manager — monitor live controllers, substitute on the two rules        #
# --------------------------------------------------------------------------- #
async def read_fleet(live, cfg: Config) -> dict[str, dict]:
    """Return {controller_id: {unrealized, volume}} for our pair's live pmm_mister controllers."""
    from routines.controller_performance import gather_active
    out: dict[str, dict] = {}
    try:
        live_perf, cfg_by, bot_names, health, cid_bot = await gather_active(live)
    except Exception:
        return out
    for cid, c in cfg_by.items():
        if cid not in live_perf:
            continue  # only ACTUALLY-RUNNING controllers (saved-but-idle configs are not "live")
        if c.get("controller_name") != "pmm_mister":
            continue
        if (c.get("trading_pair", "").upper() != cfg.trading_pair.upper()):
            continue
        lp = live_perf.get(cid, {})
        out[cid] = {
            "unrealized": float(lp.get("unrealized", 0.0)),
            "volume": float(lp.get("volume_traded", 0.0)),
            "bot": cid_bot.get(cid),
        }
    return out


def check_triggers(fleet: dict[str, dict], state: dict, cfg: Config, now: float) -> list[tuple[str, str]]:
    """Return [(controller_id, reason)] for controllers that must be substituted.
    No-trade is detected via volume staying flat across cycles (robust, needs no
    per-fill timestamp): we remember (volume, since_ts) and fire when volume hasn't
    increased for sub_no_trade_h hours."""
    vol_hist = state.setdefault("vol_hist", {})
    to_sub: list[tuple[str, str]] = []
    for cid, m in fleet.items():
        # unrealized rule
        if m["unrealized"] <= cfg.sub_upnl:
            to_sub.append((cid, f"unrealized {m['unrealized']:.2f} <= {cfg.sub_upnl}"))
            continue
        # no-new-volume rule
        prev = vol_hist.get(cid)
        if prev is None or m["volume"] > prev["volume"] + 1e-9:
            vol_hist[cid] = {"volume": m["volume"], "since": now}  # volume advanced -> reset clock
        else:
            idle_h = (now - prev["since"]) / 3600.0
            if idle_h >= cfg.sub_no_trade_h:
                to_sub.append((cid, f"no new volume for {idle_h:.1f}h >= {cfg.sub_no_trade_h}h"))
    # prune history for controllers no longer present
    for cid in list(vol_hist.keys()):
        if cid not in fleet:
            vol_hist.pop(cid, None)
    return to_sub


async def apply_substitution(live, cfg: Config, cid: str, new_cfg: dict, log) -> bool:
    """Live-update a controller's config to the bench winner's params (in-place swap,
    no whole-bot redeploy). Returns True on success."""
    if cfg.dry_run:
        log(f"[dry_run] would substitute {cid} -> {param_sig(new_cfg)}")
        return True
    try:
        payload = dict(new_cfg)
        payload["id"] = cid  # keep the slot id, swap the params
        await live.controllers.create_or_update_controller_config(cid, payload)
        # live-apply to the running bot slot
        try:
            await live.controllers.update_bot_controller_config(cfg.bot_name, cid, payload)
        except Exception:
            pass  # some servers apply on next refresh from the saved config
        log(f"substituted {cid} -> bench config {param_sig(new_cfg)}")
        return True
    except Exception as e:
        log(f"substitution of {cid} FAILED: {str(e)[:80]}")
        return False


# --------------------------------------------------------------------------- #
# the continuous loop                                                           #
# --------------------------------------------------------------------------- #
async def run_cycle(live, local, cfg: Config, bench: list[dict], state: dict, log) -> list[dict]:
    now = time.time()
    # 1) grow the bench
    bench = await grow_bench(local, cfg, bench, log)
    _save(BENCH_STORE, bench)

    # 2) manage the fleet
    fleet = await read_fleet(live, cfg)
    log(f"fleet: {len(fleet)} live / target {cfg.fleet_size}")
    recently_failed = set(state.get("recently_failed", []))
    to_sub = check_triggers(fleet, state, cfg, now)
    for cid, reason in to_sub:
        pick = select_substitute(bench, cfg.min_positive_windows,
                                 exclude_sigs=recently_failed)
        if not pick:
            log(f"{cid} needs sub ({reason}) but bench has no eligible config yet")
            continue
        log(f"SUBSTITUTE {cid}: {reason}")
        if await apply_substitution(live, cfg, cid, pick["config"], log):
            # remember what just failed so we don't redeploy it immediately
            dead_sig = state.get("live_sig", {}).get(cid)
            if dead_sig:
                recently_failed.add(dead_sig)
            state.setdefault("live_sig", {})[cid] = pick["sig"]
            state["vol_hist"].pop(cid, None)  # reset the no-trade clock for the new config
    state["recently_failed"] = list(recently_failed)[-20:]  # bounded memory
    _save(STATE_STORE, state)
    return bench


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> str:
    from aiohttp import ClientTimeout
    from hummingbot_api_client import HummingbotAPIClient

    logs: list[str] = []
    def log(m):
        logs.append(str(m))

    chat_id = getattr(context, "_chat_id", None)
    live = await get_client(chat_id, context=context)
    local = HummingbotAPIClient(base_url=config.local_url, username=config.local_user,
                                password=config.local_pass, timeout=ClientTimeout(total=1800, connect=15))
    await local.init()

    # seed bench from the shipped fallback if we have nothing yet (egress hedge)
    bench = _load(BENCH_STORE, [])
    if not bench:
        seed = _load(SEED_BENCH, [])
        if seed:
            bench = seed
            log(f"seeded bench from fallback: {len(bench)} configs")
    state = _load(STATE_STORE, {"vol_hist": {}, "live_sig": {}, "recently_failed": []})

    try:
        while True:
            try:
                bench = await asyncio.wait_for(
                    run_cycle(live, local, config, bench, state, log), timeout=3600)
            except Exception as e:
                log(f"cycle error: {type(e).__name__} {str(e)[:100]}")
            await asyncio.sleep(config.cycle_sleep_sec)
    except asyncio.CancelledError:
        return "botcamp_mm_agent stopped\n" + "\n".join(logs[-20:])
