"""Per-controller volume monitor: snapshots cumulative volume, alerts on dips."""

CATEGORY = "Monitoring"

import json
import logging
import statistics
import time
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from config_manager import get_client

logger = logging.getLogger(__name__)
DEFAULT_STATE_PATH = Path("data") / "volume_drop_state.json"
STATE_VERSION = 1


class Config(BaseModel):
    """Snapshots each controller's cumulative volume per run; alerts when last interval drops to <=20% of the running median (after a 4-interval warm-up)."""

    threshold_pct: float = Field(
        default=20.0,
        description="Alert when last-interval volume <= this % of median",
    )
    min_intervals: int = Field(
        default=4,
        description="Skip output until at least this many completed intervals (deltas) per controller",
    )
    controller_warmup_minutes: int = Field(
        default=30,
        description="Skip controllers whose earliest executor is younger than this many minutes",
    )
    expected_interval_minutes: int = Field(
        default=15,
        description="Schedule cadence used only for the projection display (volume/min × this). Set to your /routines schedule interval.",
    )
    max_snapshots: int = Field(
        default=200,
        description="Max snapshots retained per controller (rolling window)",
    )
    state_file: str = Field(
        default=str(DEFAULT_STATE_PATH),
        description="Path to JSON state file (relative to Condor working dir)",
    )
    silent: bool = Field(
        default=False,
        description="Compute but suppress all Telegram messages",
    )
    trailing_activation_pct: float = Field(
        default=0.5,
        description="PnL%% on deployed capital required to arm the trailing stop (e.g. 0.5 = 0.5%%)",
    )
    trailing_drawdown_pct: float = Field(
        default=0.3,
        description="If PnL%% falls this far below peak (after arming) → 🚨 trailing trigger",
    )
    target_chat_id: int = Field(
        default=0,
        description="Send messages to this chat/group ID instead of where scheduled (0 = use scheduled chat). Use a negative group ID to post to a group while scheduling from DM.",
    )
    quote_to_usd_rate: float = Field(
        default=0.0,
        description="Manual quote→USD multiplier (e.g. ~0.18 for BRL). 0 = auto-detect from USDT-<quote> price.",
    )
    rebate_rate: float = Field(
        default=0.00015,
        description="Estimated maker rebate as a fraction of traded volume (e.g. 0.00015 = 0.015%%). Used for the 24h yield calc.",
    )
    stale_minutes: int = Field(
        default=30,
        description="Mark a controller/bot STALE (loud alert) when reported volume hasn't moved for at least this many minutes while the bot is RUNNING. This is the zombie-detector: process up, trading dead.",
    )
    recent_pace_minutes: int = Field(
        default=60,
        description="Window used to compute the bot's *current* pace. Smaller = faster reaction to slowdowns; should be ≥ 2× schedule cadence so we have ≥ 2 snapshots.",
    )


def _load_state(path: Path) -> dict:
    try:
        with path.open("r") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("controllers"), dict):
            return data
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    return {"version": STATE_VERSION, "controllers": {}}


def _save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(state, f, indent=2)
    tmp.replace(path)


def _bucket_volumes(snapshots: list, interval_sec: float, now: float) -> list[float]:
    """Compute volume per fixed time-bucket ending at `now` (most recent last).

    Buckets are wall-clock sized at `interval_sec`. Each bucket's volume is
    vol_at(end) − vol_at(start), where vol_at(t) is the total_volume from the
    latest snapshot with ts ≤ t. Returns [] if there isn't enough history for
    one full bucket. Zero-volume buckets are kept — they mean "no trading in
    this window," which is real signal.
    """
    if len(snapshots) < 2:
        return []
    first_ts = snapshots[0]["ts"]
    if now - first_ts < interval_sec:
        return []

    bucket_ends = []
    t = now
    while t - interval_sec >= first_ts:
        bucket_ends.append(t)
        t -= interval_sec
    bucket_ends.reverse()

    def vol_at(ts: float) -> float:
        best = snapshots[0]
        for s in snapshots:
            if s["ts"] <= ts:
                best = s
            else:
                break
        return float(best["total_volume"])

    buckets: list[float] = []
    for end_ts in bucket_ends:
        start_ts = end_ts - interval_sec
        v = vol_at(end_ts) - vol_at(start_ts)
        buckets.append(max(v, 0.0))
    return buckets


def _recent_pace_and_flat(snapshots: list, now: float, recent_window_sec: float) -> tuple[float, float, int]:
    """Compute the controller's *current* pace and how long it's been frozen.

    Returns ``(recent_pace_per_sec, flat_minutes, n_recent_snaps)``:
    - ``recent_pace_per_sec`` — volume delta over the last ``recent_window_sec``
      of snapshots, divided by the actual span those snapshots cover (USD/sec
      in the snapshot's currency). 0 if we don't have ≥ 2 snaps in the window
      or if the volume hasn't moved.
    - ``flat_minutes`` — walking back from the most recent snapshot, the
      longest contiguous stretch where ``total_volume`` equals the latest
      value. This is the zombie-detector signal: a healthy bot churns volume
      every poll, so a long flat tail means trading died.
    - ``n_recent_snaps`` — how many snapshots fell inside the recent window.

    The recent-window pace replaces the old full-rolling-window pace which
    averaged staleness against 50h of healthy history and silently masked the
    "process up, trading dead" failure mode.
    """
    if not snapshots:
        return 0.0, 0.0, 0
    cutoff = now - recent_window_sec
    recent = [s for s in snapshots if s["ts"] >= cutoff]
    recent_pace = 0.0
    if len(recent) >= 2:
        t_span = recent[-1]["ts"] - recent[0]["ts"]
        v_span = max(recent[-1]["total_volume"] - recent[0]["total_volume"], 0.0)
        if t_span > 0:
            recent_pace = v_span / t_span

    latest_vol = snapshots[-1]["total_volume"]
    latest_ts = snapshots[-1]["ts"]
    flat_since_ts = latest_ts
    for s in reversed(snapshots[:-1]):
        if s["total_volume"] == latest_vol:
            flat_since_ts = s["ts"]
        else:
            break
    flat_minutes = max(latest_ts - flat_since_ts, 0.0) / 60.0
    return recent_pace, flat_minutes, len(recent)


def _format_bot_summary(targets: dict, interval_min: int, now: float) -> str:
    """Aggregate volume + PNL per bot, plus lifetime-projected pace per chosen interval."""
    by_bot: dict[str, dict] = {}
    for t in targets.values():
        if t.get("source") != "bot":
            continue
        bn = t.get("bot_name")
        if not bn:
            continue
        b = by_bot.setdefault(
            bn,
            {
                "volume": 0.0,
                "pnl": 0.0,
                "realized": 0.0,
                "unrealized": 0.0,
                "controllers": 0,
                "earliest_ts": None,
            },
        )
        b["volume"] += float(t.get("current_volume") or 0.0)
        b["pnl"] += float(t.get("global_pnl") or 0.0)
        b["realized"] += float(t.get("realized_pnl") or 0.0)
        b["unrealized"] += float(t.get("unrealized_pnl") or 0.0)
        b["controllers"] += 1
        ts = t.get("started_at")
        if ts is not None:
            if b["earliest_ts"] is None or ts < b["earliest_ts"]:
                b["earliest_ts"] = ts
    if not by_bot:
        return ""
    interval_sec = interval_min * 60
    lines = ["*Per-bot performance:*"]
    for bn, b in by_bot.items():
        pnl_pct = (b["pnl"] / b["volume"] * 100.0) if b["volume"] > 0 else 0.0
        pace_str = ""
        if b["earliest_ts"]:
            age_sec = now - b["earliest_ts"]
            if age_sec > 0:
                pace = (b["volume"] / age_sec) * interval_sec
                age_min = age_sec / 60.0
                pace_str = f" | Pace: {pace:.0f}/{interval_min}m (over {age_min:.0f}m life)"
        lines.append(
            f"`{bn}`: Vol {b['volume']:.2f} | "
            f"PNL {b['pnl']:+.2f} ({pnl_pct:+.3f}%) "
            f"[r:{b['realized']:+.2f} u:{b['unrealized']:+.2f}] | "
            f"{b['controllers']} ctrl{pace_str}"
        )
    return "\n".join(lines)


def _resolve_interval_minutes(context, config_default: int) -> int:
    """Auto-detect this routine's schedule cadence; fall back to Config default."""
    try:
        instance_id = getattr(context, "_instance_id", None)
        chat_id = getattr(context, "_chat_id", None)
        if not instance_id or chat_id is None:
            return config_default
        user_data = context.application.user_data.get(chat_id, {}) or {}
        inst = (user_data.get("routine_instances") or {}).get(instance_id) or {}
        sched = inst.get("schedule") or {}
        if sched.get("type") == "interval":
            secs = int(sched.get("interval_sec") or 0)
            if secs > 0:
                return max(1, secs // 60)
        if sched.get("type") == "daily":
            return 24 * 60
    except Exception as e:
        logger.debug("interval auto-detect failed: %s", e)
    return config_default


def _format_context_block(t: dict, snapshots: list, last_window_vol: float, age_min: float, interval_min: int) -> str:
    """Build PNL + %drop context lines. All values are volume per interval_min window."""
    lines = []
    interval_sec = interval_min * 60
    pnl = t.get("global_pnl", 0.0)
    pnl_pct = t.get("global_pnl_pct", 0.0)
    realized = t.get("realized_pnl", 0.0)
    unrealized = t.get("unrealized_pnl", 0.0)
    lines.append(
        f"PNL: {pnl:+.2f} ({pnl_pct:+.3f}%) [r:{realized:+.2f} u:{unrealized:+.2f}]"
    )

    # Drop vs lifetime average — compare last bucket to lifetime avg per same window
    if age_min > 0:
        lifetime_avg_per_window = (t["current_volume"] / (age_min * 60.0)) * interval_sec
        if lifetime_avg_per_window > 0:
            drop_lifetime = (1 - last_window_vol / lifetime_avg_per_window) * 100.0
            lines.append(
                f"Drop vs lifetime avg ({lifetime_avg_per_window:.0f}/{interval_min}m): {drop_lifetime:+.1f}%"
            )

    # Drop vs monitored average — same idea, scoped to since-first-snapshot
    if len(snapshots) >= 2:
        first = snapshots[0]
        time_since_first = snapshots[-1]["ts"] - first.get("ts", snapshots[-1]["ts"])
        vol_since_first = t["current_volume"] - first.get("total_volume", 0.0)
        if time_since_first > 0 and vol_since_first > 0:
            monitored_avg_per_window = (vol_since_first / time_since_first) * interval_sec
            drop_monitored = (1 - last_window_vol / monitored_avg_per_window) * 100.0
            lines.append(
                f"Drop vs monitored avg ({monitored_avg_per_window:.0f}/{interval_min}m): {drop_monitored:+.1f}%"
            )

    return "\n".join(lines)


def _parse_created_at(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        from datetime import datetime as _dt

        return _dt.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


async def _sum_controller_volume(
    client, controller_id: str, max_pages: int = 50
) -> tuple[float, Optional[float], bool]:
    """Sum filled_amount_quote across ALL executors (any status) for a controller.

    Returns (total_volume, earliest_created_at_epoch, complete). complete=False if
    pagination hit max_pages cap, meaning the snapshot is incomplete.
    """
    total = 0.0
    earliest: Optional[float] = None
    cursor: Optional[str] = None
    pages = 0
    while pages < max_pages:
        page = await client.executors.search_executors(
            controller_ids=[controller_id], limit=100, cursor=cursor
        )
        if isinstance(page, dict):
            records = page.get("data") or page.get("executors") or []
            cursor = page.get("next_cursor") or page.get("cursor")
        else:
            records = list(page or [])
            cursor = None
        if not records:
            return total, earliest, True
        for ex in records:
            try:
                total += float(ex.get("filled_amount_quote") or 0.0)
            except (TypeError, ValueError):
                pass
            ts = _parse_created_at(ex.get("created_at"))
            if ts is not None and (earliest is None or ts < earliest):
                earliest = ts
        pages += 1
        if not cursor:
            return total, earliest, True
    return total, earliest, False


import re as _re

# Bot names from Hummingbot deployments encode their deploy timestamp.
# Known formats:
#   - bot_YYYYMMDDHHMMSS-…              (e.g. "bot_20260605222718-…")
#   - <name>-…-YYYYMMDD-HHMMSS          (e.g. "chessboard-btc-brl-1-20260606-163913")
# Parsing this is the only reliable source of true bot age:
# - general_logs is trimmed to the last 100 entries, so min(log.ts) rolls
#   forward and underreports age on verbose bots.
# - first_seen_ts (pinned by this routine on first observation) is "now" for
#   any controller we've just started tracking.
_BOT_DEPLOY_PATTERNS = (
    _re.compile(r"bot_(\d{14})"),          # compact: 14 contiguous digits
    _re.compile(r"(\d{8})-(\d{6})"),       # split:   YYYYMMDD-HHMMSS
)


def _bot_deploy_ts(bot_name: Optional[str]) -> Optional[float]:
    """Parse a UTC deploy timestamp out of a bot name. Returns None if absent."""
    if not isinstance(bot_name, str):
        return None
    from datetime import datetime, timezone

    for pat in _BOT_DEPLOY_PATTERNS:
        m = pat.search(bot_name)
        if not m:
            continue
        try:
            stamp = "".join(m.groups())  # "20260606" + "163913" → "20260606163913"
            return (
                datetime.strptime(stamp, "%Y%m%d%H%M%S")
                .replace(tzinfo=timezone.utc)
                .timestamp()
            )
        except Exception:
            continue
    return None


# Map a raw executor `type` (from API/config) to a coarse "kind" used for
# rendering. PMM controllers ship position_executors; grid controllers ship
# grid_executors; both flow through the same RUNNING-executor scan.
_KIND_BY_RAW_TYPE = {
    "grid": "grid",
    "grid_executor": "grid",
    "position": "position",
    "position_executor": "position",
    "dca": "dca",
    "dca_executor": "dca",
    "order": "order",
    "order_executor": "order",
}


def _executor_kind(raw_type: str) -> str:
    """Coarse kind ('grid' | 'position' | 'dca' | 'order' | 'other')."""
    t = (raw_type or "").strip().lower()
    return _KIND_BY_RAW_TYPE.get(t, _KIND_BY_RAW_TYPE.get(t.replace("_executor", ""), "other"))


def _kind_from_controller_name(name: str) -> Optional[str]:
    """Infer controller kind from its name (e.g. 'pmm_mister' → position)."""
    n = (name or "").lower()
    if "grid" in n:
        return "grid"
    if "pmm" in n or "market_making" in n or "market-making" in n:
        return "position"
    if "rebate" in n:
        # rebate_mill is PMM-style: no persistent running executors, driven by
        # bot orchestration volume_traded. Mapping to "position" ensures
        # is_pmm_style=True even when current volume is 0 (between clips).
        return "position"
    if "dca" in n:
        return "dca"
    return None


def _summarize_running_ex(ex: dict) -> dict:
    """Flatten a raw RUNNING executor record into the dict the renderer wants.

    Handles both grid and position (PMM) executors so the downstream code
    doesn't have to special-case shapes. Missing fields stay None — the
    renderer treats them as "—".
    """
    cfg = ex.get("config") or {}
    ci = ex.get("custom_info") or {}
    raw_type = ex.get("executor_type") or cfg.get("type") or cfg.get("executor_type") or ""
    try:
        filled = float(ex.get("filled_amount_quote") or 0.0)
    except (TypeError, ValueError):
        filled = 0.0
    try:
        net_pnl_pct = float(ex.get("net_pnl_pct") or 0.0) * 100.0
    except (TypeError, ValueError):
        net_pnl_pct = 0.0
    # Position executors keep entry/current price in config or custom_info,
    # not at the top level. Try each source, take the first one that's a real
    # number (not None/0/junk-string).
    def _num(*candidates) -> Optional[float]:
        for v in candidates:
            if v is None or v == "":
                continue
            try:
                f = float(v)
            except (TypeError, ValueError):
                continue
            if f > 0:
                return f
        return None

    entry_price = _num(
        cfg.get("entry_price"),
        ex.get("entry_price"),
        ci.get("current_position_average_price"),
    )
    current_price = _num(ex.get("current_price"), ci.get("close_price"))
    pos_quote = _num(
        ci.get("position_size_quote"),
        ci.get("position_quote"),
        cfg.get("amount_quote"),
    ) or 0.0

    try:
        capital_quote = float(cfg.get("total_amount_quote") or 0.0)
    except (TypeError, ValueError):
        capital_quote = 0.0

    return {
        "id": ex.get("executor_id") or ex.get("id"),
        "type": raw_type or "grid",
        "kind": _executor_kind(raw_type),
        "side": ci.get("side") or cfg.get("side") or ex.get("side"),
        "pair": ex.get("trading_pair") or cfg.get("trading_pair") or "?",
        "filled_quote": filled,
        "net_pnl_quote": float(ex.get("net_pnl_quote") or 0.0),
        "net_pnl_pct": net_pnl_pct,
        # Grid-specific
        "start_price": cfg.get("start_price"),
        "end_price": cfg.get("end_price"),
        # Position-specific
        "entry_price": entry_price,
        "current_price": current_price,
        "take_profit": cfg.get("take_profit"),
        "stop_loss": cfg.get("stop_loss"),
        # Common
        "position_quote": pos_quote,
        "capital_quote": capital_quote,
        "created_at": ex.get("created_at"),
    }


async def _discover_targets(client) -> dict:
    """Discover controllers to monitor.

    Step 1 — `bot_orchestration.get_active_bots_status()` gives per-controller
             cumulative `volume_traded` + PnL for everything managed by a bot.
             We seed one target per controller, whatever its strategy
             (grids, PMM, DCA, …).
    Step 2 — One paginated scan of `search_executors(status=RUNNING)`. Each
             RUNNING executor is attached to its controller's target. If the
             controller isn't owned by any orchestrated bot, we create an
             "orphan" target and backfill total volume from a per-controller
             sweep.

    Returns {state_key: target_dict}.
    """
    targets: dict = {}

    # --- 1. Bot orchestration path ---
    bots: dict = {}
    try:
        bots_data = await client.bot_orchestration.get_active_bots_status()
        if isinstance(bots_data, dict):
            bots = bots_data.get("data") or {}
    except Exception as e:
        logger.warning("get_active_bots_status failed: %s", e)

    # controller_id → state_key, so the RUNNING-executor scan below can attach
    # executors to whichever bot owns the controller.
    bot_ctrl_key: dict[str, str] = {}

    # Pre-fetch per-bot controller configs in parallel. This is the only place
    # we learn that "btcbrl-v4" is really a `pmm_mister` controller — the bot
    # orchestration perf dict keys are controller *ids*, which often don't
    # encode the strategy name. Config also gives us trading_pair / connector
    # for PMMs (which never surface in the running-executor scan).
    ctrl_configs: dict[str, dict] = {}  # cid → config
    if bots:
        import asyncio as _asyncio

        async def _fetch_one(bn: str) -> dict:
            try:
                return await client.controllers.get_bot_controller_configs(bn) or []
            except Exception as e:
                logger.debug("controller configs fetch failed for %s: %s", bn, e)
                return []

        results = await _asyncio.gather(
            *[_fetch_one(bn) for bn in bots.keys()], return_exceptions=False
        )
        for cfgs in results:
            for cfg in cfgs or []:
                if not isinstance(cfg, dict):
                    continue
                cid = cfg.get("id") or cfg.get("controller_id")
                if cid:
                    ctrl_configs[cid] = cfg

    for bot_name, bot_info in bots.items():
        if not isinstance(bot_info, dict):
            continue
        # Prefer the deploy timestamp embedded in the bot name — it survives
        # log rotation and restarts. Fall back to the oldest general_log entry.
        started_at: Optional[float] = _bot_deploy_ts(bot_name)
        if started_at is None:
            logs = bot_info.get("general_logs") or []
            ts_values = [
                l.get("timestamp")
                for l in logs
                if isinstance(l, dict) and isinstance(l.get("timestamp"), (int, float))
            ]
            if ts_values:
                started_at = float(min(ts_values))

        perf_dict = bot_info.get("performance") or {}
        if not isinstance(perf_dict, dict):
            continue
        for cid, ctrl_info in perf_dict.items():
            ctrl_perf = (
                ctrl_info.get("performance") if isinstance(ctrl_info, dict) else None
            ) or {}
            try:
                volume = float(ctrl_perf.get("volume_traded") or 0.0)
            except (TypeError, ValueError):
                volume = 0.0
            try:
                realized_pnl = float(ctrl_perf.get("realized_pnl_quote") or 0.0)
                unrealized_pnl = float(ctrl_perf.get("unrealized_pnl_quote") or 0.0)
                global_pnl = float(ctrl_perf.get("global_pnl_quote") or (realized_pnl + unrealized_pnl))
                global_pnl_pct = float(ctrl_perf.get("global_pnl_pct") or 0.0) * 100.0
            except (TypeError, ValueError):
                realized_pnl = unrealized_pnl = global_pnl = global_pnl_pct = 0.0
            # PMMs expose open quotes via positions_summary — keep as a hint so
            # the renderer can still show "N quotes live" even on the rare run
            # where the RUNNING-executor scan returns nothing for this ctrl.
            positions_summary = ctrl_perf.get("positions_summary")
            if not isinstance(positions_summary, list):
                positions_summary = []
            # Seed pair / connector / kind / capital from controller config
            # when we have it — for PMMs this is the only reliable source.
            # Falls back to parsing the cid if config is missing.
            cfg = ctrl_configs.get(cid) or {}
            ctrl_name = cfg.get("controller_name") or ""
            kind = (
                _kind_from_controller_name(ctrl_name)
                or _kind_from_controller_name(cid)
            )
            pairs: set[str] = set()
            conns: set[str] = set()
            if cfg.get("trading_pair"):
                pairs.add(cfg["trading_pair"])
            if cfg.get("connector_name"):
                conns.add(cfg["connector_name"])
            # Capital exposed: total quote currency assigned to this controller.
            # Same field name on both PMM and grid configs.
            try:
                capital_quote = float(cfg.get("total_amount_quote") or 0.0)
            except (TypeError, ValueError):
                capital_quote = 0.0

            key = f"bot/{bot_name}/{cid}"
            bot_ctrl_key[cid] = key
            targets[key] = {
                "source": "bot",
                "bot_name": bot_name,
                "controller_id": cid,
                "display_id": cid,
                # Inferred from controller_name (e.g. "pmm_mister" → position).
                # Overridden once we see actual executors below.
                "controller_kind": kind,
                "controller_name": ctrl_name,
                "trading_pairs": pairs,
                "connectors": conns,
                "accounts": set(),
                "current_volume": volume,
                "started_at": started_at,
                "n_running_executors": 0,
                "realized_pnl": realized_pnl,
                "unrealized_pnl": unrealized_pnl,
                "global_pnl": global_pnl,
                "global_pnl_pct": global_pnl_pct,
                "capital_quote": capital_quote,
                "running_execs": [],
                "positions_summary": positions_summary,
            }

    # --- 2. Unified RUNNING-executor scan ---
    # ONE pass over all running executors, bucketed by controller_id. If we own
    # the controller via a bot, attach the executor there; otherwise it goes
    # into orphan_meta to become its own target.
    orphan_meta: dict = {}
    cursor: Optional[str] = None
    while True:
        try:
            page = await client.executors.search_executors(
                status="RUNNING", limit=100, cursor=cursor
            )
        except Exception as e:
            logger.warning("running executors search failed: %s", e)
            break
        if isinstance(page, dict):
            records = page.get("data") or page.get("executors") or []
            cursor = page.get("next_cursor") or page.get("cursor")
        else:
            records = list(page or [])
            cursor = None
        if not records:
            break
        for ex in records:
            cid = ex.get("controller_id") or "<unknown>"
            summary = _summarize_running_ex(ex)
            pair = ex.get("trading_pair")
            conn = ex.get("connector_name")
            acct = ex.get("account_name")
            key = bot_ctrl_key.get(cid)
            if key:
                t = targets[key]
                t["running_execs"].append(summary)
                t["n_running_executors"] += 1
                if pair:
                    t["trading_pairs"].add(pair)
                if conn:
                    t["connectors"].add(conn)
                if acct:
                    t["accounts"].add(acct)
            else:
                entry = orphan_meta.setdefault(
                    cid,
                    {
                        "trading_pairs": set(),
                        "connectors": set(),
                        "accounts": set(),
                        "executor_ids": [],
                        "running_execs": [],
                    },
                )
                if pair:
                    entry["trading_pairs"].add(pair)
                if conn:
                    entry["connectors"].add(conn)
                if acct:
                    entry["accounts"].add(acct)
                if summary["id"]:
                    entry["executor_ids"].append(summary["id"])
                entry["running_execs"].append(summary)
        if not cursor:
            break

    for cid, info in orphan_meta.items():
        try:
            total_vol, earliest_ts, complete = await _sum_controller_volume(client, cid)
        except Exception as e:
            logger.warning("orphan volume sum failed for %s: %s", cid, e)
            continue
        if not complete:
            logger.warning("orphan controller %s: too many executors, skipping", cid)
            continue
        # Best-effort PNL fetch for orphans
        realized_pnl = unrealized_pnl = global_pnl = global_pnl_pct = 0.0
        try:
            perf = await client.executors.get_performance_report(controller_id=cid)
            realized_pnl = float(perf.get("pnl_total_quote") or 0.0)
            unrealized_pnl = float(perf.get("unrealized_pnl_quote") or 0.0)
            global_pnl = float(perf.get("global_pnl_quote") or (realized_pnl + unrealized_pnl))
            if total_vol > 0:
                global_pnl_pct = (global_pnl / total_vol) * 100.0
        except Exception as e:
            logger.warning("orphan PNL fetch failed for %s: %s", cid, e)

        # Dominant kind across this orphan's running executors decides labeling.
        kind_counts: dict[str, int] = {}
        for e in info["running_execs"]:
            k = e.get("kind") or "other"
            kind_counts[k] = kind_counts.get(k, 0) + 1
        dominant_kind = max(kind_counts, key=kind_counts.get) if kind_counts else None
        # Bot label adapts: standalone PMMs read as such, not "grids".
        if dominant_kind == "position":
            bot_label = "Standalone PMMs"
        elif dominant_kind in ("grid", None):
            bot_label = "Standalone grids"
        else:
            bot_label = f"Standalone {dominant_kind}s"

        # Orphan capital = sum of each running executor's allotted total_amount_quote
        capital_quote = sum(
            float(e.get("capital_quote") or 0.0) for e in info.get("running_execs", [])
        )

        key = f"exec/{cid}"
        targets[key] = {
            "source": "orphan",
            "bot_name": bot_label,
            "controller_id": cid,
            "display_id": cid,
            "controller_kind": dominant_kind,
            "trading_pairs": info["trading_pairs"],
            "connectors": info["connectors"],
            "accounts": info["accounts"],
            "current_volume": total_vol,
            "started_at": earliest_ts,
            "n_running_executors": len(info["executor_ids"]),
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "global_pnl": global_pnl,
            "global_pnl_pct": global_pnl_pct,
            "capital_quote": capital_quote,
            "running_execs": info.get("running_execs", []),
            "positions_summary": [],
        }

    # Backfill controller_kind for bot targets from observed executor kinds.
    for t in targets.values():
        if t.get("source") != "bot":
            continue
        kc: dict[str, int] = {}
        for e in t.get("running_execs") or []:
            k = e.get("kind") or "other"
            kc[k] = kc.get(k, 0) + 1
        if kc:
            t["controller_kind"] = max(kc, key=kc.get)

    return targets


_USD_STABLES = {"USD", "USDT", "USDC", "BUSD", "DAI", "TUSD", "USDD", "FDUSD"}


def _quote_ccy(pairs) -> Optional[str]:
    """Pick the dominant quote currency from a set/list of 'BASE-QUOTE' pairs."""
    counts: dict[str, int] = {}
    for p in pairs:
        if isinstance(p, str) and "-" in p:
            q = p.rsplit("-", 1)[1].upper()
            counts[q] = counts.get(q, 0) + 1
    return max(counts, key=counts.get) if counts else None


async def _usd_rate(client, connector: str, quote: Optional[str], cache: dict, manual: float) -> Optional[float]:
    """Return multiplier to convert `quote` currency amounts to USD. None if undetermined."""
    if manual and manual > 0:
        return manual
    if not quote:
        return None
    if quote in _USD_STABLES:
        return 1.0
    if quote in cache:
        return cache[quote]
    rate = None
    for pair, invert in ((f"{quote}-USDT", False), (f"USDT-{quote}", True)):
        try:
            res = await client.market_data.get_prices(connector, [pair])
            px = None
            if isinstance(res, dict):
                # response may be {pair: px} or {"prices": {pair: px}}
                v = res.get(pair)
                if v is None and isinstance(res.get("prices"), dict):
                    v = res["prices"].get(pair)
                if isinstance(v, (int, float)):
                    px = float(v)
                elif isinstance(v, dict):
                    px = float(v.get("price") or v.get("mid_price") or 0) or None
            if px and px > 0:
                rate = (1.0 / px) if invert else px
                break
        except Exception as e:
            logger.debug("usd rate fetch %s failed: %s", pair, e)
    cache[quote] = rate
    return rate


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> str:
    sched_chat_id = getattr(context, "_chat_id", None)
    bot = getattr(context, "bot", None)
    # Where to deliver messages: explicit target overrides the scheduled chat.
    # Lets you schedule from DM (where the framework works) but post to a group.
    chat_id = config.target_chat_id if config.target_chat_id else sched_chat_id

    client = await get_client(sched_chat_id, context=context)
    if not client:
        return "No server available. Configure /servers."

    state_path = Path(config.state_file)
    state = _load_state(state_path)
    controllers_state = state.setdefault("controllers", {})
    bot_data = getattr(context, "bot_data", None)

    targets = await _discover_targets(client)
    if not targets:
        return "No running controllers found (checked bot orchestration + executor API)."

    now = time.time()
    summary_lines: list[str] = []
    alert_rows: list[list] = []
    counts = {"ok": 0, "alert": 0, "warming": 0, "too_young": 0}
    warmup_sec = config.controller_warmup_minutes * 60
    threshold_ratio = config.threshold_pct / 100.0
    interval_min = _resolve_interval_minutes(context, config.expected_interval_minutes)

    bots_state = state.setdefault("bots", {})
    interval_sec = interval_min * 60
    # Recent window for *current* pace — clamp to ≥ 2× interval so we always
    # have at least 2 snapshots, even when the user lowers recent_pace_minutes.
    recent_pace_sec = max(config.recent_pace_minutes * 60, 2 * interval_sec)
    usd_cache: dict = {}

    # ─── Pass 1: per-controller analysis ───
    # Collect everything we need per controller. Don't render yet.
    per_ctrl: dict[str, dict] = {}
    for key, t in targets.items():
        cid = t["display_id"]
        pair_str = ",".join(sorted(t["trading_pairs"])) or "?"
        connector_str = ",".join(sorted(t["connectors"])) or "?"
        total_vol = t["current_volume"]
        earliest_ts = t["started_at"]

        ctrl = {
            "cid": cid,
            "pair_str": pair_str,
            "connector_str": connector_str,
            "total_vol": total_vol,
            "earliest_ts": earliest_ts,
            "kind": "ok",  # ok | too_young | first | alert | stale
            "last_window_vol": 0.0,
            "baseline": 0.0,
            "baseline_source": "",
            "ratio_pct": None,
            "n_buckets": 0,
            "n_active": 0,
            "triggered": False,
            "stale": False,
            "flat_minutes": 0.0,
        }

        # Pin started_at on first observation. The log-derived value from
        # bot_orchestration rolls forward as logs are rotated, which kept
        # tripping the warmup gate every tick → no snapshots ever appended
        # → pace stuck at "starting…".
        cstate = controllers_state.setdefault(key, {"snapshots": []})
        if "first_seen_ts" not in cstate:
            cstate["first_seen_ts"] = earliest_ts if earliest_ts else now
        earliest_ts = cstate["first_seen_ts"]
        t["started_at"] = earliest_ts
        ctrl["earliest_ts"] = earliest_ts

        # too-young
        if (now - earliest_ts) < warmup_sec:
            ctrl["kind"] = "too_young"
            ctrl["age_min"] = (now - earliest_ts) / 60.0
            per_ctrl[key] = ctrl
            continue

        snapshots = cstate.setdefault("snapshots", [])
        snapshots.append({
            "ts": now,
            "total_volume": total_vol,
            "global_pnl": float(t.get("global_pnl") or 0.0),
        })
        if len(snapshots) > config.max_snapshots:
            del snapshots[: len(snapshots) - config.max_snapshots]

        buckets = _bucket_volumes(snapshots, interval_sec, now)
        ctrl["n_buckets"] = len(buckets)
        ctrl["n_active"] = sum(1 for b in buckets if b > 0)

        if len(snapshots) < 2:
            ctrl["kind"] = "first"
            per_ctrl[key] = ctrl
            continue

        if buckets:
            ctrl["last_window_vol"] = buckets[-1]
            ctrl["last_source"] = f"bucket {ctrl['n_buckets']}"
        else:
            span_sec = snapshots[-1]["ts"] - snapshots[0]["ts"]
            span_vol = max(snapshots[-1]["total_volume"] - snapshots[0]["total_volume"], 0.0)
            ctrl["last_window_vol"] = (span_vol / span_sec * interval_sec) if span_sec > 0 else 0.0
            ctrl["last_source"] = f"extrap. from {span_sec/60:.1f}m"

        # Baseline: bucket median, fallback to monitored avg from our snapshots
        monitored_avg = 0.0
        monitored_span_min = 0.0
        if len(snapshots) >= 2:
            first = snapshots[0]
            t_span = snapshots[-1]["ts"] - first["ts"]
            v_span = max(snapshots[-1]["total_volume"] - first["total_volume"], 0.0)
            if t_span > 0:
                monitored_avg = (v_span / t_span) * interval_sec
                monitored_span_min = t_span / 60.0

        if ctrl["n_buckets"] >= config.min_intervals:
            ctrl["baseline"] = statistics.median(buckets)
            ctrl["baseline_source"] = f"median of {ctrl['n_buckets']} buckets"
        elif monitored_avg > 0:
            ctrl["baseline"] = monitored_avg
            ctrl["baseline_source"] = (
                f"monitored avg over {monitored_span_min:.0f}m"
            )
        else:
            ctrl["baseline_source"] = "insufficient history"

        if ctrl["baseline"] > 0:
            ctrl["ratio_pct"] = ctrl["last_window_vol"] / ctrl["baseline"] * 100.0
            ctrl["triggered"] = ctrl["last_window_vol"] <= ctrl["baseline"] * threshold_ratio

        # ── Zombie / stale detection ──
        # Reported `volume_traded` hasn't moved for ≥ stale_minutes while the
        # controller is supposedly RUNNING → process up, trading dead. This is
        # the case the median-of-buckets rule cannot catch (all buckets are
        # zero, baseline = 0, ratio undefined). Treat as a loud alert.
        _, ctrl["flat_minutes"], _ = _recent_pace_and_flat(snapshots, now, recent_pace_sec)
        snap_span_min = (snapshots[-1]["ts"] - snapshots[0]["ts"]) / 60.0
        if snap_span_min >= config.stale_minutes and ctrl["flat_minutes"] >= config.stale_minutes:
            ctrl["stale"] = True
            ctrl["triggered"] = True
            ctrl["kind"] = "stale"
        elif ctrl["triggered"]:
            ctrl["kind"] = "alert"

        per_ctrl[key] = ctrl

    # ─── Pass 2: group by bot, render ───
    by_bot: dict[Optional[str], list[str]] = {}
    for key, t in targets.items():
        by_bot.setdefault(t.get("bot_name"), []).append(key)

    # Cross-bot totals for the summary header. We sum the per-bot display
    # values *after* USD conversion so the totals match what users see in
    # each bot section.
    total_filled_disp = 0.0
    total_pnl_disp = 0.0
    total_pace_hr_disp = 0.0
    total_capital_disp = 0.0
    total_24h_pnl_disp = 0.0
    total_24h_rebates_disp = 0.0
    total_24h_vol_disp = 0.0
    # If all bots converted to the same currency, use it; otherwise fall back
    # to a neutral label.
    total_currencies: set[str] = set()

    # ─── 24h projection helper ───
    # Returns (pnl_24h, vol_24h, span_sec) projected to a 24h window from the
    # monitored snapshot history. If we have less than 24h, we extrapolate
    # from per-second rates so the user gets a steady-state estimate; if we
    # have more than 24h, we use the actual 24h-ago snapshot.
    def _proj_24h(snapshots: list) -> tuple[float, float, float]:
        if len(snapshots) < 2:
            return 0.0, 0.0, 0.0
        latest = snapshots[-1]
        t_end = latest["ts"]
        v_end = float(latest.get("total_volume") or 0.0)
        p_end = float(latest.get("global_pnl") or 0.0)
        # Pick the snapshot closest to (but not later than) 24h ago.
        cutoff = t_end - 86400.0
        ref = snapshots[0]
        for s in snapshots:
            if s["ts"] <= cutoff:
                ref = s
            else:
                break
        t_span = t_end - ref["ts"]
        if t_span <= 0:
            return 0.0, 0.0, 0.0
        v_span = max(v_end - float(ref.get("total_volume") or 0.0), 0.0)
        p_span = p_end - float(ref.get("global_pnl") or 0.0)
        # Extrapolate to a 24h window when we have less than 24h of history.
        scale = 86400.0 / t_span if t_span < 86400.0 else 1.0
        return p_span * scale, v_span * scale, t_span

    for bot_name, keys in by_bot.items():
        # ── Bot section (orchestrated bots AND synthetic "(direct executors)") ──
        # Aggregate across this bot's controllers
        agg_vol = 0.0          # lifetime cumulative (all executors ever) — used by drop math
        agg_pnl = 0.0
        agg_realized = 0.0
        agg_unrealized = 0.0
        agg_last = 0.0
        agg_baseline = 0.0
        agg_capital = 0.0      # sum of total_amount_quote across the bot's controllers
        agg_pnl_24h = 0.0      # sum of 24h-projected PnL across the bot's controllers
        agg_vol_24h = 0.0      # sum of 24h-projected volume across the bot's controllers
        any_baseline = False
        earliest = None
        run_filled = 0.0       # filled volume of CURRENTLY running execs only
        run_pnl = 0.0          # PnL of currently running execs only
        n_running_grids = 0
        for key in keys:
            t_ = targets[key]
            agg_vol += float(t_.get("current_volume") or 0.0)
            agg_pnl += float(t_.get("global_pnl") or 0.0)
            agg_realized += float(t_.get("realized_pnl") or 0.0)
            agg_unrealized += float(t_.get("unrealized_pnl") or 0.0)
            agg_capital += float(t_.get("capital_quote") or 0.0)
            cs = controllers_state.get(key, {})
            snaps = cs.get("snapshots", [])
            p24, v24, span = _proj_24h(snaps)
            # Lifetime-avg fallback when we don't have 2+ snapshots yet:
            # extrapolate from `current_volume / bot_age × 86400`. Same idea
            # as the lifetime-avg pace fallback, just over 24h.
            if v24 == 0:
                deploy_ts_b = _bot_deploy_ts(bot_name) or t_.get("started_at")
                if deploy_ts_b:
                    age = now - float(deploy_ts_b)
                    lifetime_vol = float(t_.get("current_volume") or 0.0)
                    if age > 0 and lifetime_vol > 0:
                        v24 = lifetime_vol / age * 86400.0
            agg_pnl_24h += p24
            agg_vol_24h += v24
            for ex in (t_.get("running_execs") or []):
                run_filled += float(ex.get("filled_quote") or 0.0)
                run_pnl += float(ex.get("net_pnl_quote") or 0.0)
                n_running_grids += 1
            ctrl = per_ctrl[key]
            agg_last += ctrl["last_window_vol"]
            if ctrl["baseline"] > 0:
                agg_baseline += ctrl["baseline"]
                any_baseline = True
            ts = t_.get("started_at")
            if ts and (earliest is None or ts < earliest):
                earliest = ts

        bot_pnl_pct = (agg_pnl / agg_vol * 100.0) if agg_vol > 0 else 0.0
        agg_ratio_pct = (agg_last / agg_baseline * 100.0) if any_baseline else None

        # USD conversion. PnL% and ratios are currency-agnostic; only absolute
        # amounts need converting. Determine quote ccy + connector for this bot.
        bot_pairs: set = set()
        bot_conns: set = set()
        for key in keys:
            bot_pairs |= set(targets[key].get("trading_pairs") or [])
            bot_conns |= set(targets[key].get("connectors") or [])
        quote = _quote_ccy(bot_pairs)
        conn = next(iter(bot_conns)) if bot_conns else "binance"
        rate = await _usd_rate(client, conn, quote, usd_cache, config.quote_to_usd_rate)
        if rate:
            cur = "USD"
            agg_vol *= rate
            agg_pnl *= rate
            agg_realized *= rate
            agg_unrealized *= rate
            agg_last *= rate
            agg_baseline *= rate
            agg_capital *= rate
            agg_pnl_24h *= rate
            agg_vol_24h *= rate
            run_filled *= rate
            run_pnl *= rate
            bot_money = lambda v: v * rate  # noqa: E731
        else:
            cur = quote or "quote"
            bot_money = lambda v: v  # noqa: E731

        # 24h rebates estimate (display currency)
        agg_rebates_24h = agg_vol_24h * config.rebate_rate
        # Yield: (24h PnL + 24h rebates) / capital. Capital is in display
        # currency now too. Falls back to 0% when no capital is configured
        # (e.g. orphan grids without total_amount_quote on their executors).
        agg_yield_24h_pct = (
            ((agg_pnl_24h + agg_rebates_24h) / agg_capital * 100.0)
            if agg_capital > 0
            else None
        )

        # Bot-level trailing
        bstate = bots_state.setdefault(bot_name, {})
        trailing_active = bool(bstate.get("trailing_active", False))
        peak = bstate.get("peak_pnl_pct")
        if not trailing_active and bot_pnl_pct >= config.trailing_activation_pct:
            trailing_active = True
            peak = bot_pnl_pct
        if trailing_active:
            if peak is None or bot_pnl_pct > peak:
                peak = bot_pnl_pct
        bstate["trailing_active"] = trailing_active
        bstate["peak_pnl_pct"] = peak

        trailing_triggered = False
        if trailing_active and peak is not None:
            stop_level = peak - config.trailing_drawdown_pct
            trailing_triggered = bot_pnl_pct < stop_level
            if trailing_triggered:
                trailing_line = (
                    f"🚨 Profit dropped! Peaked at {peak:+.3f}%, "
                    f"now {bot_pnl_pct:+.3f}% (below stop {stop_level:+.3f}%)"
                )
            else:
                trailing_line = (
                    f"🟢 Trailing on — peak {peak:+.3f}%, "
                    f"alerts below {stop_level:+.3f}% (now {bot_pnl_pct:+.3f}%)"
                )
        else:
            trailing_line = (
                f"⚪ Trailing off — starts when profit ≥ {config.trailing_activation_pct:+.2f}%"
            )

        # ── Recent-window pace + zombie detection ──
        # `bot_recent_pace` is the volume rate over the last
        # `recent_pace_minutes` of snapshots (NOT the full 50h rolling window).
        # A flat last hour drops it to 0 immediately, which is exactly what we
        # want — a healthy bot's pace should reflect *now*, not its lifetime
        # average.
        # `bot_max_flat_min` is the longest contiguous "no movement" tail
        # across the bot's controllers — the zombie signal. We also track
        # `bot_has_long_history` so we only flag stale when the snapshot span
        # is long enough to be meaningful.
        bot_recent_pace = 0.0  # raw quote per second, summed across controllers
        bot_max_flat_min = 0.0
        bot_min_flat_min = float("inf")
        bot_has_long_history = False
        bot_n_snap_ctrls = 0
        for key in keys:
            cs = controllers_state.get(key, {})
            snaps = cs.get("snapshots", [])
            if len(snaps) < 2:
                continue
            bot_n_snap_ctrls += 1
            snap_span_min = (snaps[-1]["ts"] - snaps[0]["ts"]) / 60.0
            if snap_span_min >= config.stale_minutes:
                bot_has_long_history = True
            rp, fm, _ = _recent_pace_and_flat(snaps, now, recent_pace_sec)
            bot_recent_pace += rp
            if fm > bot_max_flat_min:
                bot_max_flat_min = fm
            if fm < bot_min_flat_min:
                bot_min_flat_min = fm
        if bot_min_flat_min == float("inf"):
            bot_min_flat_min = 0.0

        # A bot is STALE when EVERY snapshot-bearing controller has been
        # frozen for ≥ stale_minutes (min_flat_min ≥ threshold). If even one
        # controller is alive, the bot isn't a zombie — it's just partially
        # idle, and the per-controller alerts handle that.
        bot_is_stale = (
            bot_n_snap_ctrls > 0
            and bot_has_long_history
            and bot_min_flat_min >= config.stale_minutes
        )

        # ── Running-executor pace (the ONLY volume signal we trust) ──
        # Each running executor's filled / its own runtime = a clean steady
        # rate, immune to the lifecycle jumps that made cumulative volume
        # explode to 1205%. Works the same for grid and position executors.
        MIN_RT_HR = 5.0 / 60.0  # executors younger than 5m are too noisy to rate
        run_pace_hr = 0.0
        per_grid: list[dict] = []
        kind_counts_bot: dict[str, int] = {}
        for key in keys:
            for ex in (targets[key].get("running_execs") or []):
                ca = _parse_created_at(ex.get("created_at"))
                rt_hr = ((now - ca) / 3600.0) if ca else 0.0
                filled_usd = bot_money(ex.get("filled_quote") or 0.0)
                g_pace = (filled_usd / rt_hr) if rt_hr >= MIN_RT_HR else None
                if g_pace:
                    run_pace_hr += g_pace
                k = ex.get("kind") or "other"
                kind_counts_bot[k] = kind_counts_bot.get(k, 0) + 1
                per_grid.append({"ex": ex, "rt_hr": rt_hr, "filled_usd": filled_usd, "pace_hr": g_pace})
        # Dominant executor kind for this bot, used to label the header
        # ("live grids" vs "live PMM quotes" vs mix). PMM controllers don't
        # expose running executors via the executor API at all — their
        # quotes/positions live inside the strategy. For those bots we have to
        # fall back to the controller_kind hint (parsed from controller name).
        if kind_counts_bot:
            dominant = max(kind_counts_bot, key=kind_counts_bot.get)
            mixed = len(kind_counts_bot) > 1
        else:
            dominant = (
                targets[keys[0]].get("controller_kind")
                if keys else None
            ) or "exec"
            mixed = False
        # "Orchestration-only" controllers — PMM, chessboard, and other
        # strategies whose internal quotes/positions are not exposed by
        # `search_executors(status=RUNNING)`. Detection: no running executors
        # observed, but the bot reports lifetime volume via orchestration.
        # In that case we drive the header from orchestration data + snapshot
        # deltas instead of from running_execs (which are empty).
        is_pmm_style = (not kind_counts_bot) and (
            dominant in ("position", "dca", "order") or agg_vol > 0
        )

        # `bot_pace_src` tags the provenance of the displayed pace:
        #   'snap'    — recent-window snapshot delta (the truth)
        #   'stale'   — recent_pace = 0 and bot is frozen ≥ stale_minutes
        #   'cold'    — we don't have enough recent snapshots yet (just booted)
        #   'none'    — no signal at all
        # We INTENTIONALLY no longer fall back to a lifetime average. That
        # fallback masked the zombie case: a dead bot with 200h of healthy
        # history would show its lifetime avg as "current pace" and the
        # baseline (also lifetime avg) would say "✅ 95%", suppressing the
        # alert. The whole point of this routine is to detect dead volume —
        # so we now surface "0 USD/hr — STALE" instead.
        bot_pace_src = "snap"
        if is_pmm_style:
            # Source the header metrics from bot orchestration + recent
            # snapshots. `agg_vol`/`agg_pnl` are cumulative lifetime values
            # from `volume_traded` and (realized + unrealized), already in
            # display currency. `bot_recent_pace` is in raw quote currency
            # per second — convert to display currency per hour.
            pace_rate = rate if rate else 1.0
            run_pace_hr = bot_recent_pace * pace_rate * 3600.0
            run_filled = agg_vol
            run_pnl = agg_pnl
            n_running_grids = len(keys)
            if run_pace_hr <= 0:
                if bot_is_stale:
                    bot_pace_src = "stale"
                elif bot_n_snap_ctrls == 0 or not bot_has_long_history:
                    bot_pace_src = "cold"
                else:
                    bot_pace_src = "none"
        run_pace_day = run_pace_hr * 24.0

        # Accumulate cross-bot totals (all values already in display currency).
        total_filled_disp += run_filled
        total_pnl_disp += run_pnl
        total_pace_hr_disp += run_pace_hr
        total_capital_disp += agg_capital
        total_24h_pnl_disp += agg_pnl_24h
        total_24h_rebates_disp += agg_rebates_24h
        total_24h_vol_disp += agg_vol_24h
        total_currencies.add(cur)

        if is_pmm_style:
            if dominant == "position":
                live_label = "live PMM controllers"
            elif dominant in ("dca", "order"):
                live_label = f"live {dominant} controllers"
            else:
                # Unknown / generic kind (e.g. chessboard). Use the controller's
                # own name where available — it's the most informative label.
                first_name = (
                    targets[keys[0]].get("controller_name")
                    if keys else ""
                ) or "controllers"
                live_label = f"live {first_name.replace('_', ' ')} controllers"
        elif mixed:
            live_label = "live executors"
        elif dominant == "grid":
            live_label = "live grids"
        elif dominant == "position":
            live_label = "live PMM quotes"
        elif dominant == "dca":
            live_label = "live DCA legs"
        elif dominant == "order":
            live_label = "live orders"
        else:
            live_label = "live executors"

        # Snapshot the running pace for a clean drop alert (rates, no lifecycle noise).
        # PMM-style bots use the snapshot-derived pace; either way this is a
        # per-interval rate, so the median baseline behaves the same.
        psnaps = bstate.setdefault("pace_snaps", [])
        psnaps.append({"ts": now, "pace_hr": run_pace_hr})
        if len(psnaps) > config.max_snapshots:
            del psnaps[: len(psnaps) - config.max_snapshots]
        pace_hist = [p["pace_hr"] for p in psnaps if p["pace_hr"] > 0]
        baseline_pace = (
            statistics.median(pace_hist)
            if len(pace_hist) >= config.min_intervals
            else None
        )
        # Stale always trips, regardless of baseline state — that's the whole
        # point of zombie detection. Otherwise the median-vs-recent rule
        # applies as before.
        bot_pace_triggered = bot_is_stale or (
            baseline_pace is not None
            and baseline_pace > 0
            and run_pace_hr <= baseline_pace * threshold_ratio
        )

        # ── Bot header ──
        # Hierarchy is the only typography Telegram gives us: bold for values,
        # italic for labels, UPPERCASE for category lines, blank lines for
        # vertical weight. Each metric gets its own line so the eye can land
        # on it.
        # 24h yield line — only when we have a configured capital. Shows the
        # warmup state ("est. from Xh") when we have <24h of snapshot history
        # so users know the number is an extrapolation.
        if agg_yield_24h_pct is None:
            yield_line = "📈 _24h yield:_ _no capital configured_"
        else:
            yicon = "📈" if agg_yield_24h_pct >= 0 else "📉"
            # If we don't have 24h of snapshots, hint that this is projected.
            youngest_span = 0.0
            for key in keys:
                cs = controllers_state.get(key, {})
                snaps = cs.get("snapshots", [])
                if len(snaps) >= 2:
                    sp = snaps[-1]["ts"] - snaps[0]["ts"]
                    if sp > youngest_span:
                        youngest_span = sp
            if 0 < youngest_span < 86400:
                yield_prefix = f"📊 _24h yield (projected from {youngest_span/3600:.1f}h):_"
            else:
                yield_prefix = f"{yicon} _24h yield:_"
            yield_line = (
                f"{yield_prefix} *{agg_yield_24h_pct:+.3f}%* "
                f"on *{agg_capital:,.0f} {cur}* capital\n"
                f"     _24h market {agg_pnl_24h:+,.0f} · "
                f"24h rebates {agg_rebates_24h:+,.0f} {cur}_"
            )
        # PnL line for the bot header — same convention as per-row:
        # "PnL (lifetime, incl. rebates)" with a breakdown sub-line.
        bot_reb_lifetime = run_filled * config.rebate_rate
        bot_pnl_incl = run_pnl + bot_reb_lifetime
        bot_pnl_incl_icon = "📈" if bot_pnl_incl >= 0 else "📉"
        header_lines = [
            f"🤖 *{bot_name}*",
            f"▶️ {n_running_grids} {live_label.upper()}",
            "",
            f"💼 _Capital:_ *{agg_capital:,.0f} {cur}*",
            f"💰 _Filled (lifetime):_ *{run_filled:,.0f} {cur}*",
            f"📊 _24h volume:_ *{agg_vol_24h:,.0f} {cur}*",
            f"{bot_pnl_incl_icon} _PnL (lifetime, incl. rebates):_ "
            f"*{bot_pnl_incl:+,.0f} {cur}*",
            f"     _market {run_pnl:+,.0f} · rebates {bot_reb_lifetime:+,.0f} {cur}_",
            f"⚡ _Pace:_ *{run_pace_hr:,.0f} {cur}/hr*  ·  *{run_pace_day:,.0f} {cur}/day*"
            + (f"  ·  🚨 *STALE — frozen {bot_max_flat_min:.0f} min*"
               if bot_pace_src == "stale"
               else (" _(no recent snapshots — warming up)_" if bot_pace_src == "cold"
                     else (" _(no signal)_" if bot_pace_src == "none" else ""))),
            yield_line,
        ]
        if bot_is_stale:
            # Explicit zombie line — bypass the median-vs-current framing
            # entirely. A frozen bot has no "normal" to compare against; the
            # only relevant fact is that trading died while the process is up.
            header_lines.append(
                f"🚨 _BOT NOT TRADING_ — `volume_traded` hasn't moved for "
                f"≥ *{bot_max_flat_min:.0f} min* "
                f"(threshold {config.stale_minutes}m). Process up, trading dead."
            )
        elif baseline_pace is None:
            n_so_far = len(pace_hist)
            header_lines.append(
                f"📊 _vs normal:_ building history ({n_so_far}/{config.min_intervals})"
            )
        else:
            vs = (run_pace_hr / baseline_pace * 100.0) if baseline_pace else 0.0
            picon = "🚨" if bot_pace_triggered else ("⚠️" if vs < 80 else "✅")
            tail = f" — LOW (≤{config.threshold_pct:.0f}%)" if bot_pace_triggered else ""
            header_lines.append(
                f"📊 _vs normal:_ {picon} {vs:.0f}% "
                f"(normal ≈ {baseline_pace:,.0f} {cur}/hr){tail}"
            )
        if rate is None and quote:
            header_lines.append(f"_(amounts in {quote} — USD rate unavailable)_")
        header_lines.append(trailing_line)

        # ── Per-executor / per-controller breakdown ──
        # Each executor renders differently depending on kind:
        #   grid     → side · pair · range (start–end)
        #   position → side · pair · entry → TP / SL (PMM quote)
        #   dca/order/other → side · pair · entry price
        # PMM-style bots don't surface running executors via the executor API,
        # so we fall back to a per-controller summary drawn from bot
        # orchestration (volume_traded, realized/unrealized PnL, trade counts).
        # Helper: format hr+day pace consistently across grids and PMMs.
        # Bold via Markdown so the pace numbers pop in Telegram.
        def _fmt_pace(hr: Optional[float]) -> str:
            if not hr or hr <= 0:
                return "starting…"
            return f"*{hr:,.0f} {cur}/hr* · *{hr * 24:,.0f} {cur}/day*"

        # Helper: human-readable price — avoids `:,.4g`'s scientific notation
        # for big prices (e.g. BRL pairs ~370k).
        def _fmt_price(p) -> str:
            if not isinstance(p, (int, float)) or p <= 0:
                return "—"
            if p >= 100:
                return f"{p:,.0f}"
            if p >= 1:
                return f"{p:,.2f}"
            return f"{p:,.6g}"

        # Helper: per-controller pace from snapshot deltas. Used for PMM
        # controllers where there's no running-executor-derived pace.
        rate_factor = rate if rate else 1.0

        def _ctrl_pace_hr(key: str) -> Optional[float]:
            cs = controllers_state.get(key, {})
            snaps = cs.get("snapshots", [])
            if len(snaps) < 2:
                return None
            t_span = snaps[-1]["ts"] - snaps[0]["ts"]
            v_span = max(snaps[-1]["total_volume"] - snaps[0]["total_volume"], 0.0)
            if t_span <= 0 or v_span <= 0:
                return None
            return (v_span / t_span) * 3600.0 * rate_factor

        def _ctrl_pace_with_src(key: str, target: dict) -> tuple[Optional[float], str]:
            """Returns (pace_hr, source) where source is 'snap', 'lifetime', or ''.

            'snap' = computed from the last two state-file snapshots (most
            accurate; reflects the current trading rate).
            'lifetime' = lifetime cumulative volume divided by bot age (a
            steady-state estimate, useful immediately on a fresh state file).
            Empty string means we have nothing to show.
            """
            p = _ctrl_pace_hr(key)
            if p is not None:
                return p, "snap"
            # Fallback: lifetime volume / bot age. Uses the deploy-timestamp
            # age which is more reliable than `started_at` for fresh state.
            deploy_ts = _bot_deploy_ts(bot_name) or target.get("started_at")
            if not deploy_ts:
                return None, ""
            age = now - float(deploy_ts)
            if age <= 0:
                return None, ""
            vol_q = float(target.get("current_volume") or 0.0)
            if vol_q <= 0:
                return None, ""
            return (vol_q / age * 3600.0) * rate_factor, "lifetime"

        # Block-list: each entry is a list of lines for one executor/controller.
        # We join with blank lines between blocks so each item gets breathing
        # room (instead of a wall of stats).
        ctrl_blocks: list[list[str]] = []
        any_ctrl_alert = False

        if is_pmm_style and not per_grid:
            for i, key in enumerate(keys, 1):
                t_ = targets[key]
                cid = t_.get("display_id") or "?"
                pair_set = sorted(t_.get("trading_pairs") or [])
                pair = pair_set[0] if pair_set else (cid.split("_")[-1] if "_" in cid else "?")
                vol = bot_money(float(t_.get("current_volume") or 0.0))
                pnl = bot_money(float(t_.get("global_pnl") or 0.0))
                rpnl = bot_money(float(t_.get("realized_pnl") or 0.0))
                upnl = bot_money(float(t_.get("unrealized_pnl") or 0.0))
                pnl_pct = float(t_.get("global_pnl_pct") or 0.0)
                pnl_icon = "📈" if pnl >= 0 else "📉"
                # Age: prefer the bot's deploy timestamp (parsed from the bot
                # name) over `started_at`, which is unreliable after a fresh
                # state file or log rotation.
                deploy_ts_ctrl = _bot_deploy_ts(bot_name) or t_.get("started_at")
                age = ""
                if deploy_ts_ctrl:
                    age_sec = max(now - float(deploy_ts_ctrl), 0.0)
                    age = (
                        f" · 🕐 {age_sec/3600:.1f}h"
                        if age_sec >= 3600
                        else f" · 🕐 {age_sec/60:.0f}m"
                    )
                ctrl_pace, ctrl_pace_src = _ctrl_pace_with_src(key, t_)
                # Per-controller capital + 24h yield
                cap_q = float(t_.get("capital_quote") or 0.0)
                cap_disp = bot_money(cap_q)
                cs = controllers_state.get(key, {})
                snaps = cs.get("snapshots", [])
                p24q, v24q, _ = _proj_24h(snaps)
                # Same lifetime-avg fallback as the bot-level loop.
                # Note: distinct local name to avoid clobbering `age` (the
                # formatted display string set above).
                if v24q == 0:
                    deploy_ts_c = _bot_deploy_ts(bot_name) or t_.get("started_at")
                    lifetime_vol_q = float(t_.get("current_volume") or 0.0)
                    if deploy_ts_c and lifetime_vol_q > 0:
                        age_secs_c = now - float(deploy_ts_c)
                        if age_secs_c > 0:
                            v24q = lifetime_vol_q / age_secs_c * 86400.0
                p24 = bot_money(p24q)
                reb24 = bot_money(v24q * config.rebate_rate)
                yield_pct = ((p24 + reb24) / cap_disp * 100.0) if cap_disp > 0 else None
                if yield_pct is None:
                    yield_str = "_24h yield:_ —"
                else:
                    yi = "📈" if yield_pct >= 0 else "📉"
                    ctrl_span = (
                        snaps[-1]["ts"] - snaps[0]["ts"] if len(snaps) >= 2 else 0.0
                    )
                    proj_tag = (
                        f" _(proj. from {ctrl_span/3600:.1f}h)_"
                        if 0 < ctrl_span < 86400
                        else ""
                    )
                    yield_str = (
                        f"{yi} _24h yield (on capital):_ *{yield_pct:+.3f}%*{proj_tag}\n"
                        f"     _24h market {p24:+,.0f} · 24h rebates {reb24:+,.0f} {cur}_"
                    )
                # Stale wins over every other tag — and we force the displayed
                # pace to 0 so the eye doesn't skip over a misleading lifetime
                # average. Otherwise we just drop the old "(lifetime avg)" tag
                # entirely; it hid the zombie case and added no value.
                ctrl_meta = per_ctrl.get(key, {})
                if ctrl_meta.get("stale"):
                    pace_tag = f"  ·  🚨 *STALE — frozen {ctrl_meta.get('flat_minutes', 0):.0f}m*"
                    ctrl_pace = 0.0
                else:
                    pace_tag = ""
                # Per-row title uses the actual controller_name so chessboard
                # / pmm_mister / etc. each read as themselves instead of a
                # generic "PMM controller".
                cname = t_.get("controller_name") or ""
                if "pmm" in cname.lower():
                    row_title = "PMM controller"
                elif cname:
                    # Replace underscores with spaces: Telegram legacy Markdown
                    # treats "_" as italic delimiter even inside *bold*, so a
                    # name like "rebate_mill" inside *#N rebate_mill* causes a
                    # parse error that silently drops the whole chunk.
                    row_title = cname.replace("_", " ")
                else:
                    row_title = "controller"
                # PnL we show INCLUDES estimated maker rebates. The two
                # time windows have their own rebate calc, clearly labeled:
                #   "PnL (lifetime)" = market PnL + lifetime volume × rate
                #   "24h yield"      = 24h PnL + 24h volume × rate, ÷ capital
                # Both numerators below are in display currency.
                reb_lifetime = vol * config.rebate_rate
                pnl_incl = pnl + reb_lifetime
                pnl_incl_pct = (pnl_incl / vol * 100.0) if vol > 0 else 0.0
                pnl_incl_icon = "📈" if pnl_incl >= 0 else "📉"
                vol_24h = bot_money(v24q)
                ctrl_blocks.append([
                    f"⚙️ *#{i} {row_title}*",
                    f"  {pair} · `{cid[:18]}`{age}",
                    f"  💼 _Capital:_ *{cap_disp:,.0f} {cur}*",
                    f"  💰 _Lifetime vol:_ *{vol:,.0f} {cur}*",
                    f"  📊 _24h vol:_ *{vol_24h:,.0f} {cur}*",
                    f"  {pnl_incl_icon} _PnL (lifetime, incl. rebates):_ "
                    f"*{pnl_incl:+,.0f} {cur}* ({pnl_incl_pct:+.3f}% _of vol_)",
                    f"     _market {pnl:+,.0f} · rebates {reb_lifetime:+,.0f}_  "
                    f"_[r:{rpnl:+,.0f} u:{upnl:+,.0f}]_",
                    f"  ⚡ _Pace:_ {_fmt_pace(ctrl_pace)}{pace_tag}",
                    f"  {yield_str}",
                ])
            # Skip the per_grid loop entirely for PMM-style bots
            per_grid = []

        for i, g in enumerate(per_grid, 1):
            ex = g["ex"]
            kind = ex.get("kind") or "other"
            side = ex.get("side")
            sicon, slbl = (
                ("🟢", "BUY") if side in (1, "BUY")
                else ("🔴", "SELL") if side in (2, "SELL")
                else ("⚪", str(side) if side else "?")
            )
            kind_label = {
                "grid": "grid",
                "position": "PMM quote",
                "dca": "DCA",
                "order": "order",
            }.get(kind, ex.get("type") or "exec")
            short_id = (ex.get("id") or "?")[:8]
            pnl_usd = bot_money(ex.get("net_pnl_quote") or 0.0)
            pnl_pct = ex.get("net_pnl_pct") or 0.0
            pnl_icon = "📈" if pnl_usd >= 0 else "📉"

            # Price context: grids show range; positions/PMMs show entry → TP/SL
            sp, epx = ex.get("start_price"), ex.get("end_price")
            if kind == "grid" and isinstance(sp, (int, float)) and isinstance(epx, (int, float)):
                price_str = f"📐 {_fmt_price(sp)}–{_fmt_price(epx)}"
            else:
                entry = ex.get("entry_price")
                tp = ex.get("take_profit")
                sl = ex.get("stop_loss")
                bits: list[str] = []
                if isinstance(entry, (int, float)) and entry > 0:
                    bits.append(f"@ {_fmt_price(entry)}")
                # TP/SL come as fractional (0.001 = 0.1%); display as %.
                if isinstance(tp, (int, float)) and tp > 0:
                    bits.append(f"TP {tp * 100:.2f}%")
                if isinstance(sl, (int, float)) and sl > 0:
                    bits.append(f"SL {sl * 100:.2f}%")
                price_str = ("🎯 " + " ".join(bits)) if bits else "🎯 —"

            pos_usd = bot_money(ex.get("position_quote") or 0.0)
            rt = g["rt_hr"]
            age = f"{rt:.1f}h" if rt >= 1 else f"{rt*60:.0f}m"
            # Per-grid capital comes from the executor's own total_amount_quote.
            cap_q = float(ex.get("capital_quote") or 0.0)
            cap_disp = bot_money(cap_q)
            cap_line = (
                f"  💼 _Capital:_ *{cap_disp:,.0f} {cur}*"
                if cap_q > 0
                else "  💼 _Capital:_ —"
            )
            ctrl_blocks.append([
                f"{sicon} *#{i} {slbl} {kind_label}*",
                f"  {ex.get('pair','?')} · 🕐 {age} · `{short_id}`",
                cap_line,
                f"  💰 _Filled:_ *{g['filled_usd']:,.0f} {cur}*",
                f"  {pnl_icon} _PnL:_ {pnl_usd:+,.0f} {cur} ({pnl_pct:+.2f}%)",
                f"  📦 _Position:_ *{pos_usd:,.0f} {cur}*  ·  {price_str}",
                f"  ⚡ _Pace:_ {_fmt_pace(g['pace_hr'])}",
            ])

        # Render: blank line between each block for readability, and a blank
        # line between the bot header and the first block.
        if ctrl_blocks:
            ctrl_lines = [""]
            for j, block in enumerate(ctrl_blocks):
                if j > 0:
                    ctrl_lines.append("")
                ctrl_lines.extend(block)
        else:
            ctrl_lines = []

        if baseline_pace is None:
            counts["warming"] += 1
        elif not (trailing_triggered or bot_pace_triggered):
            counts["ok"] += 1

        # Stash per-bot meta so the rebalance callback can find the right controllers
        # without re-querying. Includes controller_ids, connectors, accounts, pair.
        if bot_data is not None:
            bot_data.setdefault("volalert_bot_meta", {})[bot_name] = {
                "controller_ids": sorted({targets[k]["controller_id"] for k in keys}),
                "connectors": sorted(bot_conns),
                "pairs": sorted(bot_pairs),
                "accounts": sorted(
                    {a for k in keys for a in (targets[k].get("accounts") or set())}
                ),
                "source": targets[keys[0]].get("source") if keys else None,
                "ts": now,
            }

        # Bot-level alert button (one row per bot, even if both triggers fire).
        # Now includes Rebalance — runs the stop-then-replace flow on confirm.
        if trailing_triggered or bot_pace_triggered:
            counts["alert"] += 1
            alert_rows.append(
                [
                    InlineKeyboardButton(
                        f"⛔ Stop {bot_name[:12]}",
                        callback_data=f"volalert:stopbot:{bot_name}",
                    ),
                    InlineKeyboardButton(
                        f"⚖️ Rebal {bot_name[:12]}",
                        callback_data=f"volalert:rebalbot:{bot_name}",
                    ),
                    InlineKeyboardButton(
                        f"✅ Ack {bot_name[:12]}",
                        callback_data=f"volalert:ackbot:{bot_name}",
                    ),
                ]
            )

        bot_section = "\n".join(header_lines + ctrl_lines)
        summary_lines.append(bot_section)

    _save_state(state_path, state)

    if not config.silent and chat_id and bot and summary_lines:
        n_bots = sum(1 for bn in by_bot if bn is not None)
        divider = "━━━━━━━━━━━━━━━━━━━━━━━━━"
        thin = "─────────────────────────"
        # Pick a label for the totals: if every bot rendered in the same
        # currency we use it directly; otherwise fall back to a neutral hint.
        total_cur = next(iter(total_currencies)) if len(total_currencies) == 1 else "mixed"
        total_pnl_pct = (
            (total_pnl_disp / total_filled_disp * 100.0)
            if total_filled_disp > 0
            else 0.0
        )
        total_day_disp = total_pace_hr_disp * 24.0
        # Grand 24h yield: aggregate numerator over aggregate capital.
        total_yield_pct = (
            ((total_24h_pnl_disp + total_24h_rebates_disp) / total_capital_disp * 100.0)
            if total_capital_disp > 0
            else None
        )
        # Leading whitespace + heavy banner gives the title visual weight that
        # font-size would normally give. Title is uppercased so it pops next
        # to the bot names below (which stay lowercase / mixed case).
        banner = (
            "\n"
            f"{divider}\n"
            f"       📊 *VOLUME MONITOR*\n"
            f"{divider}"
        )
        # Count distinct bot sections actually being rendered (each entry of
        # by_bot becomes its own section). When there's only one section,
        # the totals are identical to that bot's header — skip them.
        n_sections = len(by_bot)
        subtitle = f"_{len(targets)} controllers across {n_bots} bot(s)_"
        # PnL at the portfolio level — same convention as per-controller +
        # per-bot. One headline number that *includes* lifetime rebates,
        # with a market/rebates breakdown on the next line.
        total_reb_lifetime_disp = total_filled_disp * config.rebate_rate
        total_pnl_incl_disp = total_pnl_disp + total_reb_lifetime_disp
        total_pnl_incl_pct = (
            (total_pnl_incl_disp / total_filled_disp * 100.0)
            if total_filled_disp > 0
            else 0.0
        )
        total_pnl_incl_icon = "📈" if total_pnl_incl_disp >= 0 else "📉"
        totals_lines = [
            f"💼 _Total capital:_ *{total_capital_disp:,.0f} {total_cur}*",
            f"💰 _Total volume (lifetime):_ *{total_filled_disp:,.0f} {total_cur}*",
            f"📊 _Total 24h volume:_ *{total_24h_vol_disp:,.0f} {total_cur}*",
            f"{total_pnl_incl_icon} _Total PnL (lifetime, incl. rebates):_ "
            f"*{total_pnl_incl_disp:+,.0f} {total_cur}* "
            f"({total_pnl_incl_pct:+.3f}% _of vol_)",
            f"     _market {total_pnl_disp:+,.0f} · "
            f"rebates {total_reb_lifetime_disp:+,.0f} {total_cur}_",
            f"⚡ _Combined pace:_ *{total_pace_hr_disp:,.0f} {total_cur}/hr*  ·  "
            f"*{total_day_disp:,.0f} {total_cur}/day*",
        ]
        if total_yield_pct is not None:
            yi = "📈" if total_yield_pct >= 0 else "📉"
            # Use the longest span across all controllers as the "are we still
            # projecting" hint at the grand-total level.
            longest_span = 0.0
            for _key, _cs in controllers_state.items():
                _snaps = _cs.get("snapshots", []) if isinstance(_cs, dict) else []
                if len(_snaps) >= 2:
                    sp = _snaps[-1]["ts"] - _snaps[0]["ts"]
                    if sp > longest_span:
                        longest_span = sp
            if 0 < longest_span < 86400:
                prefix = f"📊 _24h yield (projected from {longest_span/3600:.1f}h):_"
            else:
                prefix = f"{yi} _24h yield:_"
            totals_lines.append(
                f"{prefix} *{total_yield_pct:+.3f}%* "
                f"on *{total_capital_disp:,.0f} {total_cur}* capital\n"
                f"     _24h market {total_24h_pnl_disp:+,.0f} · "
                f"24h rebates {total_24h_rebates_disp:+,.0f} {total_cur}_"
            )
        totals_block = "\n".join(totals_lines)
        # Skip the grand-total block when there's only one bot section — the
        # numbers would be identical to its header, so showing both is just
        # duplicate noise.
        if n_sections > 1:
            header = (
                banner
                + "\n" + subtitle
                + "\n\n" + totals_block
                + "\n" + thin
            )
        else:
            header = (
                banner
                + "\n" + subtitle
                + "\n" + thin
            )
        kb = InlineKeyboardMarkup(alert_rows) if alert_rows else None
        # Alert → ring. No alert → silent push (message appears, no sound/badge).
        silent_push = not alert_rows

        # Telegram caps messages at 4096 chars. With many controllers the
        # combined message overflows. Fix: send the header once, then each bot
        # section as its own message. This avoids splitting a unified string
        # mid-Markdown-span (e.g. *bold across a chunk boundary), which makes
        # Telegram reject the second chunk silently.
        TG_MAX_CHARS = 3800  # buffer under the 4096 ceiling for safety

        def _chunk_for_telegram(text: str, limit: int = TG_MAX_CHARS) -> list[str]:
            if len(text) <= limit:
                return [text]
            chunks: list[str] = []
            remaining = text
            while len(remaining) > limit:
                # Prefer splitting on a blank line; fall back to a single
                # newline; last resort = hard split.
                cut = remaining.rfind("\n\n", 0, limit)
                if cut <= 0:
                    cut = remaining.rfind("\n", 0, limit)
                if cut <= 0:
                    cut = limit
                chunks.append(remaining[:cut].rstrip())
                remaining = remaining[cut:].lstrip("\n")
            if remaining:
                chunks.append(remaining)
            return chunks

        async def _send(text: str, reply_markup=None) -> None:
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode="Markdown",
                    reply_markup=reply_markup,
                    disable_notification=silent_push,
                )
            except Exception as e:
                logger.warning("Markdown send failed (%s); retrying plain", e)
                try:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        reply_markup=reply_markup,
                        disable_notification=silent_push,
                    )
                except Exception as e2:
                    logger.error("Plain-text send also failed: %s", e2)

        # 1. Header message (banner + grand totals when > 1 bot).
        await _send(header)

        # 2. One Telegram message per bot section; chunk further only when a
        #    single section still exceeds TG_MAX_CHARS (e.g. many executors).
        for i, section in enumerate(summary_lines):
            is_last_bot = i == len(summary_lines) - 1
            chunks = _chunk_for_telegram(section)
            for j, chunk in enumerate(chunks):
                is_last_chunk = is_last_bot and j == len(chunks) - 1
                await _send(chunk, reply_markup=kb if is_last_chunk else None)

    short = (
        f"checked {len(targets)} | "
        f"ok:{counts['ok']} warming:{counts['warming']} "
        f"too_young:{counts['too_young']} alerts:{counts['alert']}"
    )
    return short
