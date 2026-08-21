"""Controller performance: per-controller summary, candle+PnL chart, volume share, cumulative curves, and per-type parameter heatmaps (port of the market-regime dashboard's Controller Performance + Market Regime tabs)."""

CATEGORY = "Bot Analysis"

import asyncio
import json
import logging
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field
from telegram.ext import ContextTypes

from config_manager import get_client
from routines.base import RoutineResult

logger = logging.getLogger(__name__)

PALETTE = [
    "#58a6ff", "#3fb950", "#f0883e", "#a371f7", "#f85149",
    "#56d4dd", "#e3b341", "#ff7b72", "#7ee787", "#d2a8ff", "#8b949e",
    "#ffa657", "#79c0ff", "#d29922", "#bc8cff", "#ff9492",
]

DARK_LAYOUT = dict(
    paper_bgcolor="#0d1117",
    plot_bgcolor="#161b22",
    font=dict(color="#c9d1d9", size=11),
)

# Tokens that mark the end of a controller "type" prefix when inferring from id.
_PAIR_TOKENS = {
    "btc", "eth", "usdt", "usdc", "brl", "eur", "ton", "doge", "sol", "bnb",
    "binance", "perpetual", "kucoin", "okx", "gate", "btcbrl", "ethbrl",
    "usdtbrl", "btcusdt", "ethusdt", "tonusdt", "btceur",
}

# Hummingbot's catch-all controller_id for un-attributed/ad-hoc executors — not a
# real controller, and its lumped totals distort charts. Excluded everywhere.
_SENTINEL_CIDS = {"main", ""}

# Deploy timestamp patterns embedded in bot db names.
_DEPLOY_PATTERNS = (
    re.compile(r"(\d{8})-(\d{6})"),   # YYYYMMDD-HHMMSS
    re.compile(r"bot_(\d{14})"),      # bot_YYYYMMDDHHMMSS
)

# Config keys that are never tunable numeric params worth comparing.
_PARAM_SKIP = {
    "id", "controller_name", "controller_type", "connector_name", "trading_pair",
    "manual_kill_switch", "candles_config", "initial_positions", "position_mode",
    "leverage", "time_limit", "config_name", "total_amount_quote",  # capital handled separately
}


class Config(BaseModel):
    """Summary of every controller that ran in the lookback window (active + stopped), with a candle+PnL chart, volume share, cumulative curves, and one parameter heatmap per controller type. PnL includes maker rebates."""

    lookback_days: int = Field(default=14, description="Days of history to include (active + stopped controllers)")
    trading_pair: str = Field(
        default="",
        description="Filter to specific trading pairs. Empty = all pairs (candle chart uses the highest-volume pair).",
        json_schema_extra={"widget": "multiselect", "options_from": "trading_pairs"},
    )
    controllers: str = Field(
        default="",
        description="Comma-separated controller IDs to INCLUDE (case-insensitive substring). Empty = all.",
    )
    exclude_controllers: str = Field(
        default="",
        description="Comma-separated controller IDs to EXCLUDE (case-insensitive substring).",
    )
    controller_types: str = Field(
        default="",
        description="Filter to specific controller types (e.g. pmm_king, chessboard). Empty = all.",
        json_schema_extra={"widget": "multiselect", "options_from": "controller_types"},
    )
    include_stopped: bool = Field(
        default=True,
        description="Include archived/stopped bots (not just currently-running ones).",
    )
    bucket_hours: int = Field(default=4, description="Time bucket size (hours) for the volume share + cumulative charts")
    candle_connector: str = Field(default="binance", description="Connector for the candle chart OHLC data")
    candle_interval: str = Field(default="1h", description="Candle interval for the candle chart (e.g. 15m, 1h, 4h)")
    rebate_rate: float = Field(
        default=0.00015,
        description="Maker rebate as a fraction of volume (0.00015 = 0.015%). Added to PnL everywhere.",
    )
    max_dbs: int = Field(
        default=0,
        description=(
            "Safety cap on how many archived bot databases to scan (most recent first). "
            "0 (default) or negative = no cap — scan every archived DB. Set a non-zero "
            "value if the SDK is throwing timeouts on very old bots."
        ),
    )
    max_executors_per_db: int = Field(
        default=0,
        description=(
            "Skip per-executor time-series fetch for bot databases larger than this. "
            "0 (default) or negative = no cap — every bot is fetched in full, including "
            "huge PMM bots with 100k+ executors. A non-zero cap is a safety knob if you "
            "hit API timeouts; the executors endpoint does not paginate so very large "
            "bots can take 30-60s to fetch."
        ),
    )
    snapshot_state_file: str = Field(
        default="data/volume_drop_state.json",
        description=(
            "Path to the Volume Drop Alert snapshot store. When present, per-controller "
            "PnL/volume CURVES are built from these continuously-recorded snapshots (real "
            "time-series for every controller, incl. live PMMs). Empty = use executor history only."
        ),
    )
    stall_hours: float = Field(
        default=6.0,
        description=(
            "Flag an active controller as STALLED if its cumulative volume hasn't moved "
            "for this many hours (measured against the freshest snapshot in the store, so a "
            "VDA recording outage doesn't false-flag everyone). Catches bots that orchestration "
            "still reports 'active' but have actually halted."
        ),
    )
    verify_stale_days: float = Field(
        default=2.0,
        description=(
            "When a controller's snapshot data is older than this many days, cross-check the "
            "bot's primary source (the live trade table) to independently confirm real "
            "volume since the freeze — distinguishing a recording outage from a genuinely "
            "halted bot. Set 0 to disable the primary-source check."
        ),
    )
    quote_display: str = Field(
        default="USDT",
        description=(
            "Display currency for ALL volume & PnL. Non-USDT bot quotes (e.g. BRL) are "
            "converted via a live BTC cross-rate (price(BTC-USDT)/price(BTC-<quote>))."
        ),
    )


# ---------------------------------------------------------------------------
# Type / pair inference
# ---------------------------------------------------------------------------


def _parse_csv(s: str) -> list[str]:
    return [t.strip().lower() for t in s.split(",") if t.strip()]


def infer_type(controller_id: str, controller_name: str | None) -> str:
    """Controller 'type' = its controller_name when known, else inferred from the id prefix."""
    if controller_name:
        return controller_name
    s = (controller_id or "").lower().replace("-", "_")
    keep: list[str] = []
    for tok in s.split("_"):
        if tok in _PAIR_TOKENS or tok.isdigit() or (tok.startswith("v") and tok[1:].isdigit()):
            break
        keep.append(tok)
    return "_".join(keep) or (controller_id or "unknown")


_PAIR_RE = re.compile(r"(btc|eth|usdt|usdc|ton|doge|sol|bnb)(brl|usdt|usdc|eur|usd)", re.I)


def infer_pair(controller_id: str, cfg_pair: str | None) -> str:
    """Trading pair from config when known, else parsed out of the controller id."""
    if cfg_pair:
        return cfg_pair.upper()
    m = _PAIR_RE.search((controller_id or "").replace("-", "").replace("_", ""))
    if m:
        return f"{m.group(1).upper()}-{m.group(2).upper()}"
    return ""


def _deploy_ts(db_path: str) -> float | None:
    for pat in _DEPLOY_PATTERNS:
        m = pat.search(db_path)
        if not m:
            continue
        try:
            stamp = "".join(m.groups())
            return datetime.strptime(stamp, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------


def _extract_rows(resp) -> list[dict]:
    if isinstance(resp, dict):
        data = resp.get("data")
        if isinstance(data, list):
            return data
        for k in ("executors", "rows", "results"):
            if isinstance(resp.get(k), list):
                return resp[k]
        return []
    return resp if isinstance(resp, list) else []


async def _db_size(client, db_path: str) -> int:
    """Executor count for a db via the lightweight summary endpoint (-1 on failure)."""
    try:
        s = await client.archived_bots.get_database_summary(db_path)
        if isinstance(s, dict):
            return int(s.get("total_executors") or s.get("total_orders") or 0)
    except Exception as e:
        logger.debug("get_database_summary(%s) failed: %s", db_path, e)
    return -1


async def _fetch_db_executors(client, db_path: str) -> list[dict]:
    try:
        return _extract_rows(await client.archived_bots.get_database_executors(db_path))
    except Exception as e:
        # Was DEBUG — silently ate all fetch errors. WARNING so we see the
        # actual reason (e.g. "Server disconnected" vs a real 5xx).
        logger.warning("get_database_executors(%s) failed: %r", db_path, e)
        return []


async def gather_executor_events(
    client, config: Config, type_lookup: dict[str, str], pair_lookup: dict[str, str]
) -> tuple[list[dict], dict[str, int], list[tuple[str, int]]]:
    """Collect closed-executor events (per controller, timestamped) from archived +
    active-bot databases within the lookback window.

    Databases larger than max_executors_per_db are skipped (they would time out over
    the API); their controllers still surface via live orchestration data. Returns
    (events, drop_stats, skipped[(db_name, n_executors)])."""
    cutoff = time.time() - config.lookback_days * 86400

    db_paths: list[str] = []
    if config.include_stopped:
        try:
            listed = _extract_rows(await client.archived_bots.list_databases()) or []
            archived = []
            for item in listed:
                p = item if isinstance(item, str) else (item.get("db_path") or item.get("path") or item.get("name"))
                if not p:
                    continue
                dts = _deploy_ts(p)
                archived.append((dts or 0.0, p))
            archived.sort(reverse=True)
            # DO NOT filter by deploy_ts. Long-running bots (like pmm_king,
            # deployed weeks ago) still produce close_timestamps INSIDE our
            # lookback window; filtering by deploy time silently drops them.
            # We just cap the total number of DBs scanned via max_dbs.
            for dts, p in archived:
                db_paths.append(p)
            if config.max_dbs and config.max_dbs > 0:
                db_paths = db_paths[: config.max_dbs]
        except Exception as e:
            logger.warning("list_databases failed: %s", e)

    # Active bots → construct their live instances db path. Orchestration
    # sometimes keeps reporting a bot as "active" after its container has been
    # removed and its data moved to bots/archived/ — so we add BOTH candidate
    # paths and let the size-gate below drop the one that doesn't resolve.
    # Without this, the routine silently shows 0 vol/PnL for any active bot
    # whose data has migrated to archived/.
    active_seen = set()
    for bot_name in (type_lookup.get("__active_bots__") or "").split("|"):
        if not bot_name:
            continue
        for prefix in ("bots/instances", "bots/archived"):
            p = f"{prefix}/{bot_name}/data/{bot_name}.sqlite"
            if p not in active_seen:
                db_paths.append(p)
                active_seen.add(p)

    # Size-gate each db (parallel, lightweight) before pulling the executor table —
    # a 144k-executor PMM bot would time out and get silently dropped otherwise.
    sem = asyncio.Semaphore(8)

    async def _sized(p):
        async with sem:
            return p, await _db_size(client, p)

    sizes = await asyncio.gather(*[_sized(p) for p in db_paths], return_exceptions=True)
    fetch_paths: list[str] = []
    skipped: list[tuple[str, int]] = []
    # max_executors_per_db <= 0 means "no cap" — fetch everything regardless of size.
    cap = config.max_executors_per_db
    for r in sizes:
        if isinstance(r, Exception):
            continue
        p, n = r
        if cap <= 0 or n == -1 or 0 <= n <= cap:
            fetch_paths.append(p)
        else:
            skipped.append((p.split("/")[-1], n))

    async def _one(p):
        async with sem:
            return await _fetch_db_executors(client, p)

    results = await asyncio.gather(*[_one(p) for p in fetch_paths], return_exceptions=True)

    include = _parse_csv(config.controllers)
    exclude = _parse_csv(config.exclude_controllers)
    types_filter = _parse_csv(config.controller_types)
    # trading_pair is now a CSV of upper-cased pairs (multiselect widget).
    # Empty = no pair filter.
    want_pairs = {p.upper() for p in _parse_csv(config.trading_pair)}

    events: list[dict] = []
    drops: dict[str, int] = defaultdict(int)
    seen_exec: set[str] = set()
    # Track fetch outcomes so the report can surface which DBs died silently.
    # Previously any exception/timeout was swallowed by asyncio.gather and the
    # bot contributed 0 events invisibly — e.g. king-sized bots would fail and
    # the KPI would show $0 with no clue why. This list feeds into `drops`
    # under 'fetch_failed' so the report can print which bots dropped out.
    fetch_failures: list[tuple[str, str]] = []  # [(db_path, reason)]
    for p, res in zip(fetch_paths, results):
        if isinstance(res, Exception):
            fetch_failures.append((p, f"{type(res).__name__}: {str(res)[:120]}"))
            logger.warning("executors fetch failed for %s: %r", p, res)
            continue
        if not res:
            fetch_failures.append((p, "empty result (timeout / 5xx / not found)"))
            logger.warning("executors fetch returned empty for %s", p)
            continue
        for row in res:
            close_ts = float(row.get("close_timestamp") or 0)
            if close_ts <= 0:
                drops["still_open"] += 1
                continue
            if close_ts < cutoff:
                drops["outside_window"] += 1
                continue
            cid = str(row.get("controller_id") or "")
            if cid in _SENTINEL_CIDS:
                drops["sentinel_main"] += 1
                continue
            # Dedup: key must include the SOURCE PATH, not just row.id. Multiple
            # deployments of the same controller each have their own SQLite with
            # its own auto-increment id column; without the path, id=1 from
            # deployment A collides with id=1 from deployment B and one gets
            # dropped. This was invisibly wiping out volume for
            # chessboard_lite_btcbrl_3 (two deployments, ~2.76M BRL lost).
            row_id = row.get("id") or row.get("executor_id") or f"{cid}:{close_ts}"
            exid = f"{p}::{row_id}"
            if exid in seen_exec:
                continue
            seen_exec.add(exid)

            ctype = type_lookup.get(cid) or infer_type(cid, None)
            pair = pair_lookup.get(cid) or infer_pair(cid, None)
            cl = cid.lower()
            if include and not any(t in cl for t in include):
                drops["not_included"] += 1
                continue
            if exclude and any(t in cl for t in exclude):
                drops["excluded"] += 1
                continue
            if types_filter and ctype.lower() not in types_filter:
                drops["other_type"] += 1
                continue
            if want_pairs and pair and pair.upper() not in want_pairs:
                drops["other_pair"] += 1
                continue
            # Capital allocated to this executor: parsed from the per-row
            # `config` JSON. For chessboards this is per-grid-instance
            # (typically 20k BRL); for PMMs the config-level total. We keep
            # the value here so build_summary can look it up for historical
            # controllers where orchestration doesn't provide configs.
            cap_row = 0.0
            cfg_raw = row.get("config")
            if cfg_raw:
                try:
                    cfg_j = json.loads(cfg_raw) if isinstance(cfg_raw, str) else cfg_raw
                    cap_row = float(cfg_j.get("total_amount_quote") or 0.0)
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass
            price = float(row.get("close_price") or 0.0)
            if price <= 0:
                base_amt = float(row.get("filled_amount") or 0.0)
                quote_amt = float(row.get("filled_amount_quote") or 0.0)
                if base_amt > 0 and quote_amt > 0:
                    price = quote_amt / base_amt
            side_raw = str(row.get("side") or row.get("trade_type") or "").upper()
            if side_raw in ("1", "BUY", "LONG"):
                side = "BUY"
            elif side_raw in ("0", "2", "SELL", "SHORT"):
                side = "SELL"
            else:
                side = side_raw
            events.append({
                "ts": close_ts,
                "controller_id": cid,
                "type": ctype,
                "pair": pair,
                "volume": float(row.get("filled_amount_quote") or 0.0),
                "pnl": float(row.get("net_pnl_quote") or 0.0),
                "fees": float(row.get("cum_fees_quote") or 0.0),
                "capital": cap_row,
                "price": price,
                "side": side,
            })
    events.sort(key=lambda e: e["ts"])
    # If any fetches failed silently, record that fact in drops so the report
    # renderer can see it (and surface in the "Data gaps" section).
    if fetch_failures:
        drops["fetch_failed_dbs"] = len(fetch_failures)
    return events, dict(drops), skipped + [(p.split("/")[-1], -1) for p, _ in fetch_failures]


def _quote_of(pair: str) -> str:
    return pair.split("-")[-1].upper() if pair and "-" in pair else ""


async def get_usdt_rates(client, connector: str, quotes: set[str]) -> dict[str, float]:
    """Map each quote currency -> multiplier into USDT via a BTC cross-rate.

    USDT/USDC/USD -> 1.0. Others use price(BTC-USDT)/price(BTC-<quote>) on the same
    connector. On any failure the rate falls back to 1.0 (values shown in native quote).
    """
    rates: dict[str, float] = {}
    need: set[str] = set()
    for q in quotes:
        if not q:
            continue
        if q in ("USDT", "USDC", "USD"):
            rates[q] = 1.0
        else:
            need.add(q)
    if need:
        pairs = ["BTC-USDT"] + [f"BTC-{q}" for q in need]
        try:
            resp = await client.market_data.get_prices(connector, pairs)
            px = (resp.get("prices") if isinstance(resp, dict) else None) or {}
            btc_usdt = float(px.get("BTC-USDT") or 0)
            for q in need:
                bq = float(px.get(f"BTC-{q}") or 0)
                rates[q] = (btc_usdt / bq) if (bq > 0 and btc_usdt > 0) else 1.0
        except Exception as e:
            logger.warning("USDT rate fetch failed (%s) — showing native quote: %s", connector, e)
            for q in need:
                rates[q] = 1.0
    return rates


def _max_log_ts(rows) -> float:
    """Newest timestamp across a list of log entries (0 if none)."""
    ts = [float(r.get("timestamp") or 0) for r in rows if isinstance(r, dict)]
    return max(ts) if ts else 0.0


async def gather_active(client) -> tuple[dict[str, dict], dict[str, dict], list[str], dict[str, dict], dict[str, str]]:
    """Return (live_perf_by_ctrl, config_by_ctrl, active_bot_names, bot_health, cid_bot).

    live_perf: controller_id -> {volume_traded, global_pnl_quote, realized, unrealized}
    config_by_ctrl: controller_id -> full controller config (name, pair, params)
    bot_health: bot_name -> {status, last_log_ts, n_errors, last_error} — the bot's
        container heartbeat. A 'running' bot whose last_log_ts is hours old is HUNG
        (container alive, strategy loop frozen) — the authoritative liveness signal.
    cid_bot: controller_id -> bot_name
    """
    live: dict[str, dict] = {}
    configs: dict[str, dict] = {}
    bot_names: list[str] = []
    bot_health: dict[str, dict] = {}
    cid_bot: dict[str, str] = {}
    try:
        st = await client.bot_orchestration.get_active_bots_status()
        data = st.get("data") if isinstance(st, dict) else {}
    except Exception as e:
        logger.warning("get_active_bots_status failed: %s", e)
        data = {}

    for bot_name, bi in (data or {}).items():
        if not isinstance(bi, dict):
            continue
        bot_names.append(bot_name)
        err_logs = bi.get("error_logs") or []
        gen_logs = bi.get("general_logs") or []
        bot_health[bot_name] = {
            "status": bi.get("status") or "unknown",
            "last_log_ts": max(_max_log_ts(gen_logs), _max_log_ts(err_logs)),
            "n_errors": len(err_logs),
            "last_error": (err_logs[-1].get("msg", "").splitlines()[0][:160] if err_logs else ""),
        }
        perf = bi.get("performance") or {}
        for cid, info in perf.items():
            cid_bot[cid] = bot_name
            cp = (info.get("performance") if isinstance(info, dict) else None) or {}
            realized = float(cp.get("realized_pnl_quote") or 0.0)
            unreal = float(cp.get("unrealized_pnl_quote") or 0.0)
            live[cid] = {
                "volume_traded": float(cp.get("volume_traded") or 0.0),
                "global_pnl_quote": float(cp.get("global_pnl_quote") or (realized + unreal)),
                "realized": realized,
                "unrealized": unreal,
            }

    # Per-bot controller configs (controller_name = type, trading_pair, params).
    async def _cfgs(bn):
        try:
            return await client.controllers.get_bot_controller_configs(bn) or []
        except Exception:
            return []

    for cfgs in await asyncio.gather(*[_cfgs(bn) for bn in bot_names]):
        for c in cfgs or []:
            cid = c.get("id") or c.get("controller_id")
            if cid:
                configs[cid] = c

    # Augment with the global config library (covers some stopped controllers).
    try:
        for c in (await client.controllers.list_controller_configs()) or []:
            inner = c.get("config") if isinstance(c.get("config"), dict) else c
            cid = inner.get("id") or inner.get("config_name") or c.get("id")
            if cid and cid not in configs:
                configs[cid] = inner
    except Exception as e:
        logger.debug("list_controller_configs failed: %s", e)

    return live, configs, bot_names, bot_health, cid_bot


# ---------------------------------------------------------------------------
# Timeseries (bucketed cumulative, no pandas)
# ---------------------------------------------------------------------------


def build_timeseries(events: list[dict], bucket_hours: int) -> dict[str, Any]:
    if not events:
        return {"buckets": [], "by_ctrl": {}}
    bucket_s = bucket_hours * 3600
    vol: dict[str, dict[int, float]] = defaultdict(lambda: defaultdict(float))
    pnl: dict[str, dict[int, float]] = defaultdict(lambda: defaultdict(float))
    for e in events:
        b = int(e["ts"] // bucket_s) * bucket_s
        vol[e["controller_id"]][b] += e["volume"]
        pnl[e["controller_id"]][b] += e["pnl"]
    first = min(min(d) for d in vol.values())
    last = max(max(d) for d in vol.values())
    buckets = list(range(first, last + bucket_s, bucket_s))
    by_ctrl: dict[str, dict] = {}
    for ctrl in vol:
        cum_v = cum_p = 0.0
        cv, cp = {}, {}
        for b in buckets:
            cum_v += vol[ctrl].get(b, 0.0)
            cum_p += pnl[ctrl].get(b, 0.0)
            cv[b], cp[b] = cum_v, cum_p
        by_ctrl[ctrl] = {"bucket_vol": dict(vol[ctrl]), "cum_vol": cv, "cum_pnl": cp}
    return {"buckets": buckets, "by_ctrl": by_ctrl}


def load_snapshot_series(state_file: str, cutoff: float) -> dict[str, dict]:
    """Read Volume Drop Alert's snapshot store → {controller_id: {"bot", "snaps":[(ts,vol,pnl)]}}.

    VDA records {ts, total_volume, global_pnl} per controller every run, keyed by
    'bot/<bot_name>/<controller_id>' (or 'exec/<cid>'). We pick the most-recent
    deployment per controller_id (avoids cumulative resets across re-deploys) and
    keep snapshots within the window.
    """
    from pathlib import Path

    p = Path(state_file)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text())
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("snapshot state read failed: %s", e)
        return {}

    by_cid: dict[str, tuple[float, str, list]] = {}
    for key, cs in (data.get("controllers") or {}).items():
        snaps = cs.get("snapshots") or []
        if not snaps:
            continue
        parts = key.split("/")
        cid = parts[-1]
        if cid in _SENTINEL_CIDS:
            continue
        bot = parts[1] if key.startswith("bot/") and len(parts) >= 3 else ""
        last_ts = float(snaps[-1].get("ts") or 0)
        if cid not in by_cid or last_ts > by_cid[cid][0]:
            by_cid[cid] = (last_ts, bot, snaps)

    out: dict[str, dict] = {}
    for cid, (_last, bot, snaps) in by_cid.items():
        series = [
            (float(s["ts"]), float(s.get("total_volume") or 0.0), float(s.get("global_pnl") or 0.0))
            for s in snaps if float(s.get("ts") or 0) >= cutoff
        ]
        if series:
            out[cid] = {"bot": bot, "snaps": sorted(series)}
    return out


def build_ts_from_snapshots(series: dict[str, dict], bucket_hours: int) -> dict[str, Any]:
    """Convert cumulative VDA snapshots into the {buckets, by_ctrl} shape the chart
    builders consume. cum_pnl is raw global PnL (rebates added by the chart layer)."""
    bucket_s = bucket_hours * 3600
    all_ts = [s[0] for info in series.values() for s in info["snaps"]]
    if not all_ts:
        return {"buckets": [], "by_ctrl": {}}
    first = int(min(all_ts) // bucket_s) * bucket_s
    last = int(max(all_ts) // bucket_s) * bucket_s
    buckets = list(range(first, last + bucket_s, bucket_s))

    by_ctrl: dict[str, dict] = {}
    for cid, info in series.items():
        snaps = info["snaps"]  # sorted (ts, vol, pnl)
        cum_vol, cum_pnl, bucket_vol = {}, {}, {}
        prev_v = None
        i = 0
        cur_v = cur_p = 0.0
        for b in buckets:
            end = b + bucket_s
            while i < len(snaps) and snaps[i][0] <= end:
                cur_v, cur_p = snaps[i][1], snaps[i][2]
                i += 1
            cum_vol[b], cum_pnl[b] = cur_v, cur_p
            bucket_vol[b] = max(cur_v - (prev_v if prev_v is not None else cur_v), 0.0)
            prev_v = cur_v
        by_ctrl[cid] = {"bucket_vol": bucket_vol, "cum_vol": cum_vol, "cum_pnl": cum_pnl}
    return {"buckets": buckets, "by_ctrl": by_ctrl}


def compute_stall_status(
    snap_series: dict[str, dict],
    stall_hours: float,
    bot_health: dict[str, dict] | None = None,
    cid_bot: dict[str, str] | None = None,
    now: float | None = None,
) -> tuple[dict[str, dict], float]:
    """Per-controller liveness detection.

    AUTHORITATIVE signal = the bot container's log heartbeat (bot_health[bot]
    ['last_log_ts'] from orchestration). A bot whose status is 'running' but whose
    last log entry is ≥`stall_hours` old is HUNG — the container is up but the
    strategy loop is frozen (e.g. stuck on an exchange error). This catches zombies
    that orchestration + VDA still report 'running'.

    SECONDARY signal = cumulative-volume idleness from the snapshot store, used only
    when no log heartbeat is available. Measured against the store's freshest snapshot
    so a recording outage doesn't false-flag bots that traded up to the outage.

    Returns ({cid: {last_move_ts, idle_hours, log_silent_hours, status, hung,
    stalled}}, store_latest_ts).
    """
    bot_health = bot_health or {}
    cid_bot = cid_bot or {}
    store_latest = max(
        (s[0] for info in snap_series.values() for s in info["snaps"]), default=0.0
    )
    ref_now = now if now is not None else (store_latest or 0.0)

    # Volume last-move time per snapshot controller (secondary signal).
    vol_idle: dict[str, float] = {}
    last_move_map: dict[str, float] = {}
    for cid, info in snap_series.items():
        snaps = info["snaps"]
        if not snaps:
            continue
        last_move = snaps[0][0]
        for i in range(1, len(snaps)):
            if snaps[i][1] > snaps[i - 1][1] + 1e-9:
                last_move = snaps[i][0]
        vol_idle[cid] = (store_latest - last_move) / 3600.0
        last_move_map[cid] = last_move

    out: dict[str, dict] = {}
    for cid in set(snap_series) | set(cid_bot):
        health = bot_health.get(cid_bot.get(cid, ""), {})
        status = health.get("status")
        last_log_ts = float(health.get("last_log_ts") or 0)
        log_silent_h = ((ref_now - last_log_ts) / 3600.0) if last_log_ts else None
        idle_h = vol_idle.get(cid)

        if last_log_ts:
            # Authoritative heartbeat: fresh logs = ALIVE regardless of status string
            # ('idle' is a healthy waiting state). Only hung if logs went silent on a
            # container that isn't stopped.
            hung = (log_silent_h >= stall_hours) and (status != "stopped")
            stalled = hung
        else:
            # No heartbeat at all (e.g. archived/stale controller) → volume-idle fallback.
            hung = False
            stalled = idle_h is not None and idle_h >= stall_hours
        out[cid] = {
            "last_move_ts": last_move_map.get(cid) or last_log_ts,
            "idle_hours": idle_h if idle_h is not None else (log_silent_h or 0.0),
            "log_silent_hours": log_silent_h,
            "status": status,
            "hung": hung,
            "stalled": stalled,
        }
    return out, store_latest


async def verify_bot_activity(client, bot_name: str, since_ts: float, quote_rate: float) -> dict:
    """Primary-source cross-check: does the bot's live trade table show real fills
    AFTER `since_ts`? Independent of orchestration's cumulative counters (which can
    freeze while still reporting a bot 'active').

    Cheap by design: reads total + the newest page first; only if the newest trade
    post-dates `since_ts` does it binary-search the window and sum volume.

    Returns {traded: bool, n_trades: int, volume_usdt: float, newest_ts: float|None}.
    """
    db = f"bots/instances/{bot_name}/data/{bot_name}.sqlite"
    since_ms = since_ts * 1000

    async def _page(off, lim):
        r = await client.archived_bots.get_database_trades(db, limit=lim, offset=off)
        return r.get("trades") or [], int((r.get("pagination") or {}).get("total") or 0)

    try:
        _, total = await _page(0, 1)
        if total <= 0:
            return {"traded": False, "n_trades": 0, "volume_usdt": 0.0, "newest_ts": None}
        newest, _ = await _page(max(0, total - 1), 1)
        newest_ts = float(newest[-1]["timestamp"]) if newest else 0.0
        if newest_ts <= since_ms:  # nothing newer than the freeze → confirmed halt
            return {"traded": False, "n_trades": 0, "volume_usdt": 0.0, "newest_ts": newest_ts / 1000}

        # Binary-search the first offset whose trade ts >= since_ms, then sum the tail.
        lo, hi = 0, total
        while lo < hi:
            mid = (lo + hi) // 2
            rows, _ = await _page(mid, 1)
            if rows and float(rows[0]["timestamp"]) < since_ms:
                lo = mid + 1
            else:
                hi = mid
        vol = 0.0
        n = 0
        off = lo
        while off < total:
            lim = min(2000, total - off)
            rows, _ = await _page(off, lim)
            if not rows:
                break
            for r in rows:
                if float(r.get("timestamp") or 0) < since_ms:
                    continue
                vol += float(r.get("amount") or 0) * float(r.get("price") or 0)
                n += 1
            off += lim
        return {"traded": n > 0, "n_trades": n, "volume_usdt": vol * quote_rate, "newest_ts": newest_ts / 1000}
    except Exception as e:
        logger.warning("verify_bot_activity(%s) failed: %s", bot_name, e)
        return {"traded": None, "n_trades": 0, "volume_usdt": 0.0, "newest_ts": None}


def _dt(b: int) -> datetime:
    return datetime.fromtimestamp(b, tz=timezone.utc)


def _colors(keys: list[str]) -> dict[str, str]:
    return {k: PALETTE[i % len(PALETTE)] for i, k in enumerate(sorted(keys))}


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------


def chart_candles_pnl(candles: list[dict], ts: dict, include_cids: set, pair: str, rebate_rate: float):
    """Candles (top) + per-controller cumulative PnL incl. rebates (bottom) from the
    pre-built timeseries (snapshot-sourced), filtered to this pair's controllers."""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    if not candles:
        return None
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, row_heights=[0.62, 0.38], vertical_spacing=0.04,
        subplot_titles=(f"Price — {pair}", "Cumulative PnL + Rebates per Controller"),
    )
    cx = [_dt(int(c["timestamp"])) for c in candles]
    fig.add_trace(go.Candlestick(
        x=cx, open=[c["open"] for c in candles], high=[c["high"] for c in candles],
        low=[c["low"] for c in candles], close=[c["close"] for c in candles],
        name="Price", increasing_line_color="#3fb950", decreasing_line_color="#f85149",
        showlegend=False,
    ), row=1, col=1)

    cids = [c for c in sorted(ts["by_ctrl"]) if not include_cids or c in include_cids]
    colors = _colors(cids)
    for ctrl in cids:
        d = ts["by_ctrl"][ctrl]
        # Trim the curve at the last bucket with actual new volume (bucket_vol
        # > 0). Without this, cum_pnl holds its last value forward for the
        # entire remaining window, making a stopped controller look identical
        # to one that's still flat-trading. Snapshot-based ts doesn't carry
        # bucket_vol reliably (it's derived from a running cumulative delta),
        # so we treat a repeated cum_vol as "not trading."
        buckets = ts["buckets"]
        bvol = d.get("bucket_vol") or {}
        prev_v = None
        last_active = None
        for b in buckets:
            v_now = d["cum_vol"][b]
            delta = bvol.get(b) if b in bvol else (v_now - prev_v if prev_v is not None else v_now)
            if delta and delta > 0:
                last_active = b
            prev_v = v_now
        if last_active is None:
            continue
        cut = buckets[:buckets.index(last_active) + 1]
        x = [_dt(b) for b in cut]
        y = [d["cum_pnl"][b] + d["cum_vol"][b] * rebate_rate for b in cut]
        fig.add_trace(go.Scatter(
            x=x, y=y, name=ctrl, mode="lines", line=dict(width=1.5, color=colors[ctrl]),
            hovertemplate=f"<b>{ctrl}</b><br>PnL+rebate: %{{y:,.2f}}<br>%{{x}}<extra></extra>",
        ), row=2, col=1)
    fig.add_hline(y=0, line_dash="dot", line_color="white", opacity=0.3, row=2, col=1)
    fig.update_layout(
        **DARK_LAYOUT, margin=dict(l=50, r=30, t=50, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.04, xanchor="right", x=1, font=dict(size=9)),
    )
    fig.update_xaxes(rangeslider_visible=False, row=1, col=1)
    fig.update_yaxes(title_text="Price", row=1, col=1, gridcolor="#21262d")
    fig.update_yaxes(title_text="PnL+Reb (USDT)", row=2, col=1, gridcolor="#21262d")
    fig.update_xaxes(gridcolor="#21262d", row=2, col=1)
    for ann in fig.layout.annotations:
        ann.font = dict(size=12, color="#c9d1d9")
    return fig


def chart_volume_share(ts: dict, bucket_hours: int):
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    buckets, by_ctrl = ts["buckets"], ts["by_ctrl"]
    if not buckets:
        return None
    totals = {b: sum(c["bucket_vol"].get(b, 0.0) for c in by_ctrl.values()) for b in buckets}
    active = [b for b in buckets if totals[b] > 0]
    if not active:
        return None
    colors = _colors(list(by_ctrl))
    x = [_dt(b) for b in active]
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    for ctrl in sorted(by_ctrl, key=lambda c: -sum(by_ctrl[c]["bucket_vol"].values())):
        y = [by_ctrl[ctrl]["bucket_vol"].get(b, 0.0) / totals[b] * 100 for b in active]
        fig.add_trace(go.Bar(x=x, y=y, name=ctrl, marker_color=colors[ctrl],
                             hovertemplate=f"<b>{ctrl}</b><br>%{{y:.1f}}%%<br>%{{x}}<extra></extra>"),
                      secondary_y=False)
    fig.add_trace(go.Scatter(x=x, y=[totals[b] for b in active], name="Total Volume", mode="lines",
                             line=dict(color="#c9d1d9", width=2),
                             hovertemplate="<b>Total</b><br>%{y:,.0f}<br>%{x}<extra></extra>"),
                  secondary_y=True)
    fig.update_layout(**DARK_LAYOUT, barmode="stack", bargap=0.1,
                      title=f"Volume Share Over Time ({bucket_hours}h buckets)",
                      margin=dict(l=50, r=50, t=60, b=30),
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=9)))
    fig.update_yaxes(title_text="Volume Share (%)", secondary_y=False, gridcolor="#21262d")
    fig.update_yaxes(title_text="Total Volume (USDT)", secondary_y=True)
    return fig


def chart_cumulative_volume(ts: dict):
    import plotly.graph_objects as go

    buckets, by_ctrl = ts["buckets"], ts["by_ctrl"]
    if not buckets:
        return None
    colors = _colors(list(by_ctrl))
    last = buckets[-1]
    x = [_dt(b) for b in buckets]
    fig = go.Figure()
    for ctrl in sorted(by_ctrl, key=lambda c: -by_ctrl[c]["cum_vol"][last]):
        fig.add_trace(go.Scatter(x=x, y=[by_ctrl[ctrl]["cum_vol"][b] for b in buckets],
                                 name=ctrl, mode="lines", stackgroup="v",
                                 line=dict(width=0.5, color=colors[ctrl]),
                                 hovertemplate=f"<b>{ctrl}</b><br>%{{y:,.0f}}<br>%{{x}}<extra></extra>"))
    fig.update_layout(**DARK_LAYOUT, title="Cumulative Volume per Controller (Stacked)",
                      yaxis_title="Cumulative Volume (USDT)", margin=dict(l=50, r=20, t=60, b=30),
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=9)))
    return fig


def chart_controller_bars(perf_by_ctrl: dict, type_lookup: dict, active_ids: set):
    """Two horizontal bar charts — PnL+rebates and volume per controller — covering
    EVERY controller (active + stopped), colored by type. This is the chart that
    always shows all controllers, regardless of how few executors they close."""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    if not perf_by_ctrl:
        return None
    cids = sorted(perf_by_ctrl, key=lambda c: perf_by_ctrl[c]["_pnl_reb"])
    types = sorted({type_lookup.get(c) or infer_type(c, None) for c in cids})
    tcolor = {t: PALETTE[i % len(PALETTE)] for i, t in enumerate(types)}

    def _label(c):
        return f"{c} {'🟢' if c in active_ids else ''}".strip()

    fig = make_subplots(rows=1, cols=2, subplot_titles=("PnL + Rebates (USDT)", "Volume (USDT)"),
                        horizontal_spacing=0.16)
    labels = [_label(c) for c in cids]
    bar_colors = [tcolor[type_lookup.get(c) or infer_type(c, None)] for c in cids]
    fig.add_trace(go.Bar(
        y=labels, x=[perf_by_ctrl[c]["_pnl_reb"] for c in cids], orientation="h",
        marker_color=bar_colors, showlegend=False,
        hovertemplate="%{y}<br>PnL+reb: %{x:,.2f}<extra></extra>"), row=1, col=1)
    fig.add_trace(go.Bar(
        y=labels, x=[perf_by_ctrl[c]["_volume"] for c in cids], orientation="h",
        marker_color=bar_colors, showlegend=False,
        hovertemplate="%{y}<br>Volume: %{x:,.0f}<extra></extra>"), row=1, col=2)
    # Legend proxies for the type→color mapping.
    for t in types:
        fig.add_trace(go.Bar(y=[None], x=[None], name=t, marker_color=tcolor[t], showlegend=True), row=1, col=1)
    fig.update_layout(**DARK_LAYOUT, title="Per-Controller Comparison (all controllers)",
                      margin=dict(l=200, r=20, t=60, b=40), barmode="overlay",
                      height=max(260, len(cids) * 26 + 120),
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=9)))
    fig.add_vline(x=0, line_color="white", opacity=0.3, row=1, col=1)
    return fig


def _flat_params(cfg: dict) -> dict[str, float]:
    out: dict[str, float] = {}
    for k, v in cfg.items():
        if k in _PARAM_SKIP or k.startswith("_"):
            continue
        if isinstance(v, bool):
            continue
        if isinstance(v, (int, float)):
            out[k] = float(v)
        elif isinstance(v, str):
            try:
                out[k] = float(v)
            except ValueError:
                if "," in v:
                    try:
                        out[k] = float(v.split(",")[0])
                    except ValueError:
                        pass
        elif isinstance(v, list) and v:
            try:
                out[k] = float(v[0])
            except (TypeError, ValueError):
                pass
    return out


def chart_params_for_type(ctype: str, configs: dict[str, dict], perf: dict[str, dict], rebate_rate: float):
    """One heatmap comparing key params across all controllers of a single type.

    Rows = controllers, Cols = params that DIFFER between them + PnL+Rebate & Volume.
    """
    import numpy as np
    import plotly.graph_objects as go

    if not configs:
        return None
    flat = {cid: _flat_params(cfg) for cid, cfg in configs.items()}
    # Keep only params present for ≥1 controller AND that vary (or capital, always shown).
    all_keys = sorted({k for f in flat.values() for k in f})
    varying = [k for k in all_keys if len({round(flat[c].get(k), 10) for c in flat if k in flat[c]}) > 1]
    cols = varying + ["total_amount_quote", "_pnl_reb", "_volume"]

    cids = sorted(configs, key=lambda c: -(perf.get(c, {}).get("_pnl_reb", 0.0)))
    matrix, text = [], []
    for cid in cids:
        cfg = configs[cid]
        row, trow = [], []
        for k in cols:
            if k == "total_amount_quote":
                val = float(cfg.get("total_amount_quote") or 0.0)
                trow.append(f"{val:,.0f}")
            elif k == "_pnl_reb":
                val = perf.get(cid, {}).get("_pnl_reb", 0.0)
                trow.append(f"{val:,.1f}")
            elif k == "_volume":
                val = perf.get(cid, {}).get("_volume", 0.0)
                trow.append(f"{val:,.0f}")
            else:
                val = flat.get(cid, {}).get(k, 0.0)
                trow.append(f"{val * 10000:.1f}bps" if 0 < abs(val) < 0.1 else f"{val:g}")
            row.append(val)
        matrix.append(row)
        text.append(trow)
    if not matrix:
        return None

    arr = np.array(matrix, dtype=float)
    cmin, cmax = arr.min(axis=0), arr.max(axis=0)
    rng = cmax - cmin
    rng[rng == 0] = 1
    norm = (arr - cmin) / rng

    labels = []
    for k in cols:
        labels.append({"total_amount_quote": "Capital", "_pnl_reb": "PnL+Reb", "_volume": "Volume"}.get(k, k))

    # Bright 3-stop ramp that reads on the dark report background — low cells
    # were previously `#161b22`, which is literally the report background so
    # they vanished. Cyan → yellow → magenta keeps every cell visible AND text
    # (`#0d1117` = near-black) legible on top.
    fig = go.Figure(go.Heatmap(
        z=norm, x=labels, y=cids, text=text, texttemplate="%{text}",
        textfont=dict(size=10, color="#0d1117"),
        colorscale=[[0, "#4facfe"], [0.5, "#ffd166"], [1, "#ef476f"]],
        showscale=False, hovertemplate="%{y}<br>%{x}: %{text}<extra></extra>",
    ))
    fig.update_layout(**DARK_LAYOUT, title=f"Parameters — {ctype} ({len(cids)} controllers)",
                      margin=dict(l=180, r=30, t=60, b=40),
                      height=max(200, len(cids) * 34 + 90))
    return fig


def chart_candles_orders(candles: list[dict], events: list[dict], pair: str, rebate_rate: float = 0.0):
    """Candlestick + orders (row 1), cumulative volume (row 2), cumulative PnL+rebates (row 3),
    all sharing the same x-axis. Scatter markers default to legendonly; subplots are visible."""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    if not candles:
        return None
    pair_events = [e for e in events if e.get("pair") == pair and e.get("price", 0) > 0]
    if not pair_events:
        return None

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        row_heights=[0.6, 0.2, 0.2], vertical_spacing=0.04,
        subplot_titles=(f"Candle + Orders — {pair}", "Cumulative Volume", "Cumulative PnL + Rebates"),
    )

    cx = [_dt(int(c["timestamp"])) for c in candles]
    fig.add_trace(go.Candlestick(
        x=cx,
        open=[c["open"] for c in candles],
        high=[c["high"] for c in candles],
        low=[c["low"] for c in candles],
        close=[c["close"] for c in candles],
        name="Price",
        increasing_line_color="#3fb950",
        decreasing_line_color="#f85149",
        showlegend=False,
    ), row=1, col=1)

    by_ctrl: dict[str, list] = defaultdict(list)
    for e in pair_events:
        by_ctrl[e["controller_id"]].append(e)

    colors = _colors(list(by_ctrl))
    for ctrl in sorted(by_ctrl):
        ctrl_evs = by_ctrl[ctrl]
        color = colors[ctrl]
        buys = [e for e in ctrl_evs if e.get("side") == "BUY"]
        sells = [e for e in ctrl_evs if e.get("side") == "SELL"]
        others = [e for e in ctrl_evs if e.get("side") not in ("BUY", "SELL")]

        if buys:
            fig.add_trace(go.Scatter(
                x=[_dt(int(e["ts"])) for e in buys],
                y=[e["price"] for e in buys],
                mode="markers",
                name=f"{ctrl} ▲buy",
                marker=dict(symbol="triangle-up", size=8, color=color, line=dict(width=1, color="#0d1117")),
                visible="legendonly",
                hovertemplate=f"<b>{ctrl}</b><br>BUY @ %{{y:,.4f}}<br>%{{x}}<extra></extra>",
            ), row=1, col=1)
        if sells:
            fig.add_trace(go.Scatter(
                x=[_dt(int(e["ts"])) for e in sells],
                y=[e["price"] for e in sells],
                mode="markers",
                name=f"{ctrl} ▼sell",
                marker=dict(symbol="triangle-down", size=8, color=color, line=dict(width=1, color="#0d1117")),
                visible="legendonly",
                hovertemplate=f"<b>{ctrl}</b><br>SELL @ %{{y:,.4f}}<br>%{{x}}<extra></extra>",
            ), row=1, col=1)
        if others:
            fig.add_trace(go.Scatter(
                x=[_dt(int(e["ts"])) for e in others],
                y=[e["price"] for e in others],
                mode="markers",
                name=ctrl,
                marker=dict(symbol="circle", size=6, color=color, line=dict(width=1, color="#0d1117")),
                visible="legendonly",
                hovertemplate=f"<b>{ctrl}</b><br>@ %{{y:,.4f}}<br>%{{x}}<extra></extra>",
            ), row=1, col=1)

        # Cumulative volume — step line, one trace per controller
        cum_v = 0.0
        xs_v, ys_v = [], []
        for e in ctrl_evs:
            cum_v += e["volume"]
            xs_v.append(_dt(int(e["ts"])))
            ys_v.append(cum_v)
        if xs_v:
            fig.add_trace(go.Scatter(
                x=xs_v, y=ys_v, name=ctrl, mode="lines",
                line=dict(width=1.5, color=color, shape="hv"),
                showlegend=True,
                hovertemplate=f"<b>{ctrl}</b><br>Cum. Vol: %{{y:,.0f}}<br>%{{x}}<extra></extra>",
            ), row=2, col=1)

        # Cumulative PnL + rebates, one trace per controller
        cum_pnl = 0.0
        xs_p, ys_p = [], []
        for e in ctrl_evs:
            cum_pnl += e["pnl"] + e["volume"] * rebate_rate
            xs_p.append(_dt(int(e["ts"])))
            ys_p.append(cum_pnl)
        if xs_p:
            fig.add_trace(go.Scatter(
                x=xs_p, y=ys_p, name=ctrl, mode="lines",
                line=dict(width=1.5, color=color),
                showlegend=False,
                hovertemplate=f"<b>{ctrl}</b><br>PnL+Reb: %{{y:,.2f}}<br>%{{x}}<extra></extra>",
            ), row=3, col=1)

    fig.add_hline(y=0, line_dash="dot", line_color="white", opacity=0.3, row=3, col=1)
    fig.update_layout(
        **DARK_LAYOUT,
        title=f"Candle + Orders by Controller — {pair}",
        margin=dict(l=50, r=30, t=60, b=30),
        legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.01, font=dict(size=9)),
    )
    fig.update_xaxes(rangeslider_visible=False, row=1, col=1)
    fig.update_xaxes(gridcolor="#21262d", row=2, col=1)
    fig.update_xaxes(gridcolor="#21262d", row=3, col=1)
    fig.update_yaxes(title_text="Price", gridcolor="#21262d", row=1, col=1)
    fig.update_yaxes(title_text="Cum. Vol (USDT)", gridcolor="#21262d", row=2, col=1)
    fig.update_yaxes(title_text="PnL+Reb (USDT)", gridcolor="#21262d", row=3, col=1)
    for ann in fig.layout.annotations:
        ann.font = dict(size=11, color="#c9d1d9")
    return fig


# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------


def _fmt(v: float) -> str:
    sign = "-" if v < 0 else ""
    v = abs(v)
    if v >= 1_000_000:
        return f"{sign}{v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"{sign}{v / 1_000:.1f}K"
    return f"{sign}{v:.2f}"


SUMMARY_COLUMNS = [
    "Controller", "Type", "Pair", "Status", "Capital", "24h Yield", "Runs", "Fill %",
    "Window Vol", "Window PnL+Reb",
    "Live Vol", "Live PnL", "Unreal.",
    "Win rate", "Last close",
]


def build_summary(events, live, type_lookup, pair_lookup, rebate_rate, active_ids, stall_status=None, configs=None, snap_series=None):
    """Build the per-controller table + a perf map for the parameter heatmaps.

    Window columns (Vol/PnL+Reb/Runs/Win) are summed from CLOSED executors inside
    the lookback window. Live columns (Vol/PnL/Unreal.) are lifetime-cumulative
    from bot orchestration and only populated for currently-active controllers —
    PMM controllers rarely close executors, so their throughput shows here.

    stall_status (from compute_stall_status): active controllers whose cumulative
    volume has frozen are surfaced as '🔴 stalled Nh' rather than '🟢 active'.
    """
    stall_status = stall_status or {}
    by_ctrl: dict[str, list] = defaultdict(list)
    for e in events:
        by_ctrl[e["controller_id"]].append(e)

    all_ids = set(by_ctrl) | set(active_ids)
    rows = []
    perf_by_ctrl: dict[str, dict] = {}
    snap_series = snap_series or {}
    for cid in all_ids:
        evs = by_ctrl.get(cid, [])
        win_vol = sum(e["volume"] for e in evs)
        # PMM executors record net_pnl_quote = 0 per row (PnL lives on the
        # bot's aggregated position, not per-executor). Summing events
        # therefore reports rebates-only for PMM. When a snapshot series
        # exists we mirror EXACTLY what the cumulative-PnL chart plots at its
        # endpoint — snap[-1].global_pnl + snap[-1].total_volume × rebate —
        # so the chart's last point equals this row's PnL+Reb / Volume.
        # Grid/DCA controllers whose events already carry non-zero pnl keep
        # sum-of-events since that IS their authoritative signal.
        snap_info = snap_series.get(cid) or {}
        snap = snap_info.get("snaps") or []
        if snap and sum(1 for e in evs if e["pnl"] != 0) == 0:
            win_vol = float(snap[-1][1])
            win_pnl = float(snap[-1][2])
        else:
            win_pnl = sum(e["pnl"] for e in evs)
        win_reb = win_vol * rebate_rate
        wins = sum(1 for e in evs if e["pnl"] > 0)
        filled_count = sum(1 for e in evs if e["volume"] > 0)
        fill_pct = (filled_count / len(evs) * 100.0) if evs else None
        ctype = type_lookup.get(cid) or (evs[0]["type"] if evs else infer_type(cid, None))
        pair = pair_lookup.get(cid) or (evs[0]["pair"] if evs else infer_pair(cid, None))
        is_active = cid in active_ids
        ss = stall_status.get(cid)
        is_stalled = is_active and bool(ss and ss["stalled"])
        lv = live.get(cid, {}) if is_active else {}
        live_vol = float(lv.get("volume_traded", 0.0))
        live_pnl = float(lv.get("global_pnl_quote", 0.0))
        unreal = float(lv.get("unrealized", 0.0))
        last_close = max((e["ts"] for e in evs), default=0)

        # Perf for the parameter heatmap: prefer the richer of window vs live so
        # active PMMs (0 closed runs) still score on their real throughput.
        perf_vol = max(win_vol, live_vol)
        perf_pnl = (live_pnl + live_vol * rebate_rate) if is_active else (win_pnl + win_reb)
        perf_by_ctrl[cid] = {"_pnl_reb": perf_pnl, "_volume": perf_vol}

        # Capital allocated to this controller. Priority:
        #   1. live orchestration `capital_quote` (accounts for user resizing)
        #   2. controller config `total_amount_quote` (works while active)
        #   3. per-executor `capital` recorded from each event's config JSON
        #      — this is the ONLY source that survives after a bot stops and
        #      leaves orchestration/configs empty. Executors carry their own
        #      config, so we can back-out the assigned capital from history.
        cfg = (configs or {}).get(cid) or {}
        cap_native = float(cfg.get("total_amount_quote") or 0.0)
        cap_disp = float(lv.get("capital_quote", cap_native)) if is_active else cap_native
        if cap_disp <= 0 and evs:
            caps = [e.get("capital", 0.0) for e in evs if e.get("capital", 0.0) > 0]
            if caps:
                # Grids all share the same total_amount_quote per instance;
                # PMMs likewise. Median is a stable pick that ignores stray
                # zero rows.
                caps.sort()
                cap_disp = caps[len(caps) // 2]
        # Daily yield = (PnL + rebates over the controller's ACTIVE span) /
        # capital / active_days. We anchor the denominator to the actual span
        # the controller traded, not the whole lookback — else a controller
        # active for 2 days in a 14d window looks 7× worse than reality.
        #
        # Prefer strict last-24h if there IS closed-executor activity in that
        # window; otherwise fall back to the active-span average so stopped
        # controllers still get a comparable number instead of "0".
        now_ts = time.time()
        day_pnl = sum(e["pnl"] for e in evs if e["ts"] >= now_ts - 86400)
        day_vol = sum(e["volume"] for e in evs if e["ts"] >= now_ts - 86400)
        if evs and (day_pnl != 0 or day_vol != 0):
            reb_24h = day_vol * rebate_rate
            yield_24h = ((day_pnl + reb_24h) / cap_disp * 100.0) if cap_disp > 0 else None
        elif evs and cap_disp > 0 and (win_vol > 0 or win_pnl != 0):
            # Fallback ONLY when the controller actually produced fills or PnL
            # somewhere in the window — otherwise "+0.000%" is a lie: quotes
            # were placed and cancelled with zero fills.
            first_ts = min(e["ts"] for e in evs)
            last_ts = max(e["ts"] for e in evs)
            active_days = max((last_ts - first_ts) / 86400.0, 1.0)
            reb_win = win_vol * rebate_rate
            yield_24h = (win_pnl + reb_win) / cap_disp * 100.0 / active_days
        else:
            yield_24h = None

        rows.append({
            "Controller": cid,
            "Type": ctype,
            "Pair": pair or "?",
            "Status": (f"🔴 stalled {ss['idle_hours']:.0f}h" if is_stalled
                       else "🟢 active" if is_active else "stopped"),
            "Capital": _fmt(cap_disp) if cap_disp > 0 else "—",
            "24h Yield": (f"{yield_24h:+.3f}%" if yield_24h is not None else "—"),
            "Runs": len(evs),
            "Fill %": (f"{fill_pct:.1f}%" if fill_pct is not None else "—"),
            "Window Vol": _fmt(win_vol),
            "Window PnL+Reb": _fmt(win_pnl + win_reb),
            "Live Vol": _fmt(live_vol) if is_active else "—",
            "Live PnL": _fmt(live_pnl) if is_active else "—",
            "Unreal.": _fmt(unreal) if is_active else "—",
            "Win rate": (f"{wins / len(evs) * 100:.0f}%" if evs else "—"),
            "Last close": (datetime.fromtimestamp(last_close, tz=timezone.utc).strftime("%b %d %H:%M") if last_close else "—"),
            # Sort by 24h yield desc — controllers earning the most on their
            # capital rise to the top. Rows with no yield (no capital or no
            # recent activity) sort to the bottom.
            "_sort": yield_24h if yield_24h is not None else float("-inf"),
            # Hidden raw numerics kept alongside display strings so the
            # column-heatmap post-pass below has values to normalize on.
            "_r_Capital": float(cap_disp) if cap_disp > 0 else None,
            "_r_24h Yield": yield_24h,
            "_r_Runs": float(len(evs)),
            "_r_Fill %": fill_pct,
            "_r_Window Vol": float(win_vol),
            "_r_Window PnL+Reb": float(win_pnl + win_reb),
            "_r_Live Vol": float(live_vol) if is_active else None,
            "_r_Live PnL": float(live_pnl) if is_active else None,
            "_r_Unreal.": float(unreal) if is_active else None,
        })
    rows.sort(key=lambda r: r["_sort"], reverse=True)

    # ── Per-column heatmap coloring ──
    # For each numeric column, normalize values in [0,1] and wrap the display
    # cell in an inline <span> with a background color from the same cyan →
    # yellow → magenta ramp used by the parameter heatmap. Diverging columns
    # (PnL, Unreal.) center on 0: reds for losses, greens for gains.
    def _interp(t: float, stops: list[tuple[float, tuple[int,int,int]]]) -> str:
        for i in range(len(stops)-1):
            (a, ca), (b, cb) = stops[i], stops[i+1]
            if a <= t <= b:
                r = (t-a)/(b-a) if b>a else 0
                rr = int(ca[0] + r*(cb[0]-ca[0]))
                gg = int(ca[1] + r*(cb[1]-ca[1]))
                bb = int(ca[2] + r*(cb[2]-ca[2]))
                return f"rgb({rr},{gg},{bb})"
        return "rgb(120,120,120)"
    SEQ_STOPS = [(0.0,(79,172,254)),(0.5,(255,209,102)),(1.0,(239,71,111))]  # cyan→yellow→magenta
    DIV_STOPS = [(0.0,(220,80,80)),(0.5,(120,120,120)),(1.0,(80,200,120))]   # red→gray→green
    SEQ_COLS = {"Capital", "Runs", "Fill %", "Window Vol", "Live Vol"}
    DIV_COLS = {"24h Yield", "Window PnL+Reb", "Live PnL", "Unreal."}

    # Report table now escapes cell text (upstream security tightening), so we
    # wrap in Html() to signal this string is pre-built HTML the routine owns
    # and must be rendered as-is (no escape).
    from condor.reports import Html as _RawHtml

    def _wrap(display: str, color: str):
        return _RawHtml(
            f'<span style="background:{color};padding:2px 6px;border-radius:3px;'
            f'color:#0d1117;font-weight:600;">{display}</span>'
        )

    for col in SEQ_COLS | DIV_COLS:
        vals = [r.get(f"_r_{col}") for r in rows if r.get(f"_r_{col}") is not None]
        if not vals:
            continue
        vmin, vmax = min(vals), max(vals)
        for r in rows:
            v = r.get(f"_r_{col}")
            if v is None:
                continue
            if col in DIV_COLS:
                m = max(abs(vmin), abs(vmax)) or 1.0
                t = 0.5 + (v / m) * 0.5
                t = max(0.0, min(1.0, t))
                color = _interp(t, DIV_STOPS)
            else:
                t = (v - vmin) / (vmax - vmin) if vmax > vmin else 0.5
                color = _interp(t, SEQ_STOPS)
            r[col] = _wrap(str(r[col]), color)

    for r in rows:
        for k in list(r.keys()):
            if k.startswith("_r_") or k == "_sort":
                del r[k]
    return rows, perf_by_ctrl


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> RoutineResult:
    chat_id = context._chat_id if hasattr(context, "_chat_id") else None
    client = await get_client(chat_id, context=context)
    if not client:
        return RoutineResult(text="No server available. Configure servers in /config.")

    # 1. Active bots → live perf + configs + bot names + container health/heartbeat.
    live, configs, active_bots, bot_health, cid_bot = await gather_active(client)
    active_ids = set(live)

    # Build type/pair lookups from configs; smuggle active bot names for db-path construction.
    type_lookup: dict[str, str] = {}
    pair_lookup: dict[str, str] = {}
    for cid, cfg in configs.items():
        type_lookup[cid] = cfg.get("controller_name") or infer_type(cid, None)
        pair_lookup[cid] = (cfg.get("trading_pair") or infer_pair(cid, None)).upper()
    type_lookup["__active_bots__"] = "|".join(active_bots)

    # 2. Executor events (archived + active instances) within window.
    events, drops, skipped = await gather_executor_events(client, config, type_lookup, pair_lookup)
    type_lookup.pop("__active_bots__", None)

    # Apply the user's controller/type/exclude filters to EVERY downstream
    # source so the report is truly filtered — not just the events. Without
    # this, `active_ids` (from orchestration) and `snap_series` (from VDA
    # state) still leaked non-matching controllers into the table + charts.
    _incl = _parse_csv(config.controllers)
    _excl = _parse_csv(config.exclude_controllers)
    _types = _parse_csv(config.controller_types)
    _want_pairs_dbg = {p.upper() for p in _parse_csv(config.trading_pair)}
    logger.info(
        "controller_performance filters: pairs=%r types=%r include=%r exclude=%r "
        "(raw trading_pair=%r raw types=%r)",
        _want_pairs_dbg, _types, _incl, _excl,
        config.trading_pair, config.controller_types,
    )

    _want_pairs = {p.upper() for p in _parse_csv(config.trading_pair)}

    def _keep_cid(cid: str) -> bool:
        cl = cid.lower()
        if _incl and not any(t in cl for t in _incl):
            return False
        if _excl and any(t in cl for t in _excl):
            return False
        if _types:
            ct = (type_lookup.get(cid) or infer_type(cid, None) or "").lower()
            if ct not in _types:
                return False
        if _want_pairs:
            cp = (pair_lookup.get(cid) or infer_pair(cid, None) or "").upper()
            if cp and cp not in _want_pairs:
                return False
        return True

    if _incl or _excl or _types or _want_pairs:
        active_ids = {c for c in active_ids if _keep_cid(c)}
        live = {c: v for c, v in live.items() if c in active_ids}
        configs = {c: v for c, v in configs.items() if _keep_cid(c)}

    if not events and not active_ids:
        note = ", ".join(f"{k}={v}" for k, v in drops.items()) or "no databases"
        return RoutineResult(text=f"No controller activity in the last {config.lookback_days}d ({note}).")

    # 2b. Convert ALL volume & PnL into the display currency (USDT) via a BTC cross-rate,
    # so every number downstream (table, KPIs, charts, alerts) is already in USDT.
    quotes = {_quote_of(p) for p in pair_lookup.values()} | {_quote_of(e["pair"]) for e in events}
    rates: dict[str, float] = {}
    if config.quote_display.strip().upper() not in ("", "NATIVE"):
        rates = await get_usdt_rates(client, config.candle_connector, quotes)
    for e in events:
        r = rates.get(_quote_of(e["pair"]), 1.0)
        e["volume"] *= r
        e["pnl"] *= r
        e["fees"] = e.get("fees", 0.0) * r
        e["capital"] = e.get("capital", 0.0) * r
    for cid, lv in live.items():
        r = rates.get(_quote_of(pair_lookup.get(cid, "") or infer_pair(cid, None)), 1.0)
        for k in ("volume_traded", "global_pnl_quote", "realized", "unrealized", "capital_quote"):
            if k in lv:
                lv[k] = float(lv[k]) * r
    # Config capital (`total_amount_quote`) is in the bot's NATIVE quote (e.g. BRL).
    # Convert it too so the Capital column — and the 24h-Yield that divides by it —
    # are in USDT like PnL/Volume, instead of mixing currencies (which understated
    # yield by the FX rate). Feeds both the summary table and the parameter heatmaps.
    for cid, cfg in configs.items():
        if not isinstance(cfg, dict) or cfg.get("total_amount_quote") is None:
            continue
        r = rates.get(_quote_of(pair_lookup.get(cid, "") or infer_pair(cid, None)), 1.0)
        if r != 1.0:
            try:
                cfg["total_amount_quote"] = float(cfg["total_amount_quote"]) * r
            except (TypeError, ValueError):
                pass

    # Per-controller time-series from Volume Drop Alert's snapshot store (real curves
    # for every controller incl. live PMMs). Also drives stall detection below.
    cutoff = time.time() - config.lookback_days * 86400
    snap_series = load_snapshot_series(config.snapshot_state_file, cutoff) if config.snapshot_state_file else {}
    if _incl or _excl or _types or _want_pairs:
        snap_series = {c: v for c, v in snap_series.items() if _keep_cid(c)}
    for cid, info in snap_series.items():  # convert snapshot curves to USDT too
        r = rates.get(_quote_of(pair_lookup.get(cid, "") or infer_pair(cid, None)), 1.0)
        if r != 1.0:
            info["snaps"] = [(ts, v * r, p * r) for (ts, v, p) in info["snaps"]]
    stall_status, store_latest = compute_stall_status(
        snap_series, config.stall_hours, bot_health, cid_bot, now=time.time()
    )
    stalled_ids = {cid for cid, s in stall_status.items() if s["stalled"] and cid in active_ids}

    # 2c. Primary-source cross-check: for controllers stale beyond verify_stale_days,
    # confirm via the bot's live trade table whether it actually traded since the freeze.
    verify: dict[str, dict] = {}
    if config.verify_stale_days > 0 and stalled_ids:
        stale_cut = config.verify_stale_days * 86400
        bot_check: dict[str, dict] = {}
        for cid in stalled_ids:
            if stall_status[cid]["idle_hours"] * 3600 < stale_cut:
                continue
            bot = cid_bot.get(cid) or (snap_series.get(cid) or {}).get("bot")
            if not bot:
                continue
            r = rates.get(_quote_of(pair_lookup.get(cid, "") or infer_pair(cid, None)), 1.0)
            since = stall_status[cid]["last_move_ts"]
            if bot not in bot_check or since > bot_check[bot]["since"]:
                bot_check[bot] = {"since": since, "rate": r}

        async def _vc(bot, meta):
            return bot, await verify_bot_activity(client, bot, meta["since"], meta["rate"])

        for bot, res in await asyncio.gather(*[_vc(b, m) for b, m in bot_check.items()]):
            verify[bot] = res

    summary_rows, perf_by_ctrl = build_summary(
        events, live, type_lookup, pair_lookup, config.rebate_rate, active_ids, stall_status,
        configs=configs, snap_series=snap_series,
    )

    # Candle pairs: if the user pinned one, we render only that; otherwise we
    # render one Price+PnL chart PER pair that has traded controllers, sorted
    # by 14d volume so the biggest market comes first.
    if _want_pairs:
        candle_pairs = sorted(_want_pairs)
    else:
        vol_by_pair: dict[str, float] = defaultdict(float)
        for e in events:
            if e["pair"]:
                vol_by_pair[e["pair"]] += e["volume"]
        # Also include pairs from live active controllers even if they had 0
        # closed events (e.g. PMMs that just quote).
        for cid in active_ids:
            p = pair_lookup.get(cid) or infer_pair(cid, None)
            if p and p not in vol_by_pair:
                vol_by_pair[p] = 0.0
        candle_pairs = sorted(vol_by_pair, key=lambda p: -vol_by_pair[p]) or ["BTC-BRL"]

    async def _load_candles(p: str) -> list:
        try:
            cd = await client.market_data.get_candles_last_days(
                config.candle_connector, p, max(1, config.lookback_days), config.candle_interval
            )
            return _extract_rows(cd) or (cd if isinstance(cd, list) else [])
        except Exception as e:
            logger.warning("get_candles_last_days(%s) failed: %s", p, e)
            return []

    candles_by_pair: dict[str, list] = {}
    for p in candle_pairs:
        candles_by_pair[p] = await _load_candles(p)
    # Backwards-compat aliases for the rest of the function (fallbacks etc.).
    candle_pair = candle_pairs[0]
    candles = candles_by_pair.get(candle_pair, [])

    # Per-controller time-series: prefer Volume Drop Alert's snapshot store (real
    # curves for every controller incl. live PMMs); fall back to executor history.
    #
    # Cumulative-volume chart specifically needs monotonic math — VDA snapshots
    # RESET on every redeploy (their loader picks the most recent deployment
    # only, dropping prior history), which produced the sharp fake "drop" at
    # Jun 30 in the report. Events-derived ts is truly monotonic because it
    # sums closed-executor `filled_amount_quote` across the whole window.
    ts_events = build_timeseries(events, config.bucket_hours)
    if snap_series:
        ts_all = build_ts_from_snapshots(snap_series, config.bucket_hours)
        # VDA only started snapshotting recently, so controllers that stopped
        # BEFORE VDA existed have no snap entries and would silently disappear
        # from every chart driven by ts_all (candles+PnL, volume-share). Merge
        # in event-derived curves for any cid that has closed executors but no
        # snapshot record.
        snap_cids = set(ts_all["by_ctrl"])
        extra_cids = set(ts_events["by_ctrl"]) - snap_cids
        if extra_cids:
            # Extend the master bucket list to the union of both sources so
            # both series line up on the same X axis. Every controller's
            # cum_vol / cum_pnl dict must contain a value for EVERY bucket in
            # ts_all["buckets"], otherwise chart_candles_pnl / _cumulative
            # crash with KeyError. We fill missing buckets by holding the
            # last known value forward (cumulative values don't decrease when
            # nothing new happens).
            all_buckets = sorted(set(ts_all["buckets"]) | set(ts_events["buckets"]))
            def _fill(d: dict, key: str) -> dict:
                out = {}
                last = 0.0
                for b in all_buckets:
                    if b in d:
                        last = d[b]
                    out[b] = last
                return out
            for cid, info in ts_all["by_ctrl"].items():
                info["cum_vol"] = _fill(info.get("cum_vol", {}), "cum_vol")
                info["cum_pnl"] = _fill(info.get("cum_pnl", {}), "cum_pnl")
                info["bucket_vol"] = {b: info.get("bucket_vol", {}).get(b, 0.0) for b in all_buckets}
            for cid in extra_cids:
                ev_info = ts_events["by_ctrl"][cid]
                ts_all["by_ctrl"][cid] = {
                    "cum_vol": _fill(ev_info.get("cum_vol", {}), "cum_vol"),
                    "cum_pnl": _fill(ev_info.get("cum_pnl", {}), "cum_pnl"),
                    "bucket_vol": {b: ev_info.get("bucket_vol", {}).get(b, 0.0) for b in all_buckets},
                }
            ts_all["buckets"] = all_buckets
        ts_source = f"live snapshots ({len(snap_series)}) + event history ({len(extra_cids)})"
    else:
        ts_all = ts_events
        ts_source = "closed executors"
    candle_cids = {c for c in ts_all["by_ctrl"] if (pair_lookup.get(c) or infer_pair(c, None)) == candle_pair}

    n_types = len({type_lookup.get(c) or "?" for c in (set(events and [e["controller_id"] for e in events]) | active_ids)})

    # ----- Web report -----
    fig_candle = None
    try:
        from condor.reports import ReportBuilder

        builder = ReportBuilder(f"Controller Performance ({config.lookback_days}d)")
        builder.source("routine", "controller_performance").tags(["controllers", "performance", "pnl", "volume"])
        builder.manual_order()

        total_vol = sum(e["volume"] for e in events)
        total_pnl = sum(e["pnl"] for e in events)
        total_reb = total_vol * config.rebate_rate
        n_active = len(active_ids)
        n_trading = n_active - len(stalled_ids)
        cur = config.quote_display.strip().upper() or "USDT"
        builder.kpi("Controllers", str(len(summary_rows)))
        builder.kpi("Trading now", f"{n_trading}/{n_active}",
                    trend="down" if stalled_ids else "neutral")
        if stalled_ids:
            builder.kpi("🔴 Stalled", str(len(stalled_ids)), trend="down")
        builder.kpi(f"Volume ({cur})", _fmt(total_vol))
        builder.kpi(f"Realized PnL + Rebates ({cur})", _fmt(total_pnl + total_reb),
                    trend="up" if total_pnl + total_reb >= 0 else "down")

        # Prominent alert: active controllers whose bot container is up but whose
        # strategy loop has gone silent (HUNG), cross-checked against the trade table.
        if stalled_ids:
            det = []
            for cid in sorted(stalled_ids, key=lambda c: -(stall_status[c].get("log_silent_hours") or stall_status[c]["idle_hours"])):
                s = stall_status[cid]
                bot = cid_bot.get(cid, "") or (snap_series.get(cid) or {}).get("bot", "")
                silent = s.get("log_silent_hours")
                if s.get("hung") and silent is not None:
                    reason = (f"container **running** but **no log activity for {silent:.0f}h** "
                              f"(strategy loop frozen)")
                else:
                    last = datetime.fromtimestamp(s["last_move_ts"], tz=timezone.utc).strftime("%b %d %H:%M")
                    reason = f"cumulative volume idle **{s['idle_hours']:.0f}h** (last moved {last})"
                line = f"- **`{cid}`** — {reason}" + (f"  _(bot `{bot}`)_" if bot else "")
                err = (bot_health.get(bot) or {}).get("last_error")
                if s.get("hung") and err:
                    line += f"\n    last error: `{err}`"
                v = verify.get(bot)
                if v is not None:
                    if v["traded"] is False:
                        line += "  → ✅ **primary source confirms halt** (0 trades since)"
                    elif v["traded"]:
                        line += (f"  → ⚠ trade table shows **{v['n_trades']} fills / "
                                 f"{_fmt(v['volume_usdt'])} {cur}** since — recording lag, not halted")
                det.append(line)
            builder.markdown(
                f"## 🔴 {len(stalled_ids)} active controller(s) STALLED / HUNG\n"
                f"_These are flagged on the bot's **container log heartbeat** (authoritative): the "
                f"container is 'running' (so orchestration & VDA report them alive) but the strategy "
                f"loop has emitted no logs for ≥{config.stall_hours:.0f}h. Cross-checked against the "
                f"live trade table — the primary source — to confirm:_\n\n"
                + "\n".join(det)
            )
        # Data-freshness warning: the whole snapshot store may be stale (VDA not running).
        if store_latest:
            store_age_h = (time.time() - store_latest) / 3600.0
            if store_age_h >= config.stall_hours:
                builder.markdown(
                    f"_⚠ Snapshot data is **{store_age_h:.0f}h old** (newest snapshot "
                    f"{datetime.fromtimestamp(store_latest, tz=timezone.utc).strftime('%b %d %H:%M')} UTC) — "
                    f"Volume Drop Alert may not be running. Stall flags are measured against this "
                    f"as-of time, so they reflect bot activity up to then, not the last few hours._"
                )

        rate_note = ("all values in **" + cur + "**"
                     + (f" (BRL→{cur} via live BTC cross-rate)" if any(q not in ("USDT", "USDC", "USD") for q in quotes) else ""))
        builder.markdown(
            f"## Controller Performance — last {config.lookback_days} days\n"
            f"_Every controller that ran in the window (**{n_active} active**, "
            f"**{len(summary_rows) - n_active} stopped**) across {n_types} type(s); {rate_note}. "
            f"Volume/Realized PnL/Fees are summed from **closed executors** in the window; "
            f"`Unreal. (live)` is current open-position PnL for active controllers (from bot "
            f"orchestration). PnL includes a {config.rebate_rate * 100:.3f}% maker rebate._"
        )
        builder.markdown("### Per-Controller Summary\n_Sorted by realized PnL + rebates. Active controllers show live cumulative volume/PnL; stopped ones show window-realized totals._")
        builder.table(summary_rows, columns=SUMMARY_COLUMNS)

        # Per-controller comparison bars — ALWAYS covers every controller (active +
        # stopped), unlike the executor time-series which needs closed executors.
        fig_bars = chart_controller_bars(perf_by_ctrl, type_lookup, active_ids)
        if fig_bars is not None:
            builder.markdown(
                "### All Controllers — PnL & Volume\n"
                "_Every controller that ran in the window. 🟢 = currently active "
                "(live cumulative from bot orchestration); others are window-realized. "
                "Colored by controller type._"
            )
            builder.plotly(fig_bars)

        # One Price & PnL chart per pair so controllers on different markets
        # (e.g. BTC-BRL vs USDT-BRL) each get their own candles + curves.
        fig_candle = None
        for p_ in candle_pairs:
            p_candles = candles_by_pair.get(p_) or []
            p_cids = {c for c in ts_all["by_ctrl"]
                      if (pair_lookup.get(c) or infer_pair(c, None)) == p_}
            if not p_candles or not p_cids:
                continue
            try:
                fig = chart_candles_pnl(p_candles, ts_all, p_cids, p_, config.rebate_rate)
            except Exception as e:
                logger.warning("chart_candles_pnl(%s) failed: %r", p_, e, exc_info=True)
                fig = None
            if fig is None:
                continue
            builder.markdown(
                f"### Price & PnL — {p_}\n"
                f"_Candles (top) with each controller's cumulative PnL + rebates (bottom) on "
                f"a shared timeline, from {ts_source}. Rising PnL while price chops = the "
                f"market maker earning the spread._"
            )
            builder.plotly(fig)
            if fig_candle is None:
                fig_candle = fig  # kept for Telegram photo fallback below

        for p_ in candle_pairs:
            p_candles = candles_by_pair.get(p_) or []
            try:
                fig_orders = chart_candles_orders(p_candles, events, p_, config.rebate_rate)
            except Exception as e:
                logger.warning("chart_candles_orders(%s) failed: %r", p_, e, exc_info=True)
                fig_orders = None
            if fig_orders is not None:
                builder.markdown(
                    f"### Candle + Orders by Controller — {p_}\n"
                    "_Each point is a closed executor plotted at its fill price. "
                    "All controllers start hidden — toggle each on via the legend. "
                    "▲ = buy side, ▼ = sell side._"
                )
                builder.plotly(fig_orders)

        # Use events-derived ts (monotonic) for the volume-share and cumulative
        # charts, not snapshot-derived (which resets to 0 on every redeploy).
        try:
            fig_share = chart_volume_share(ts_events, config.bucket_hours)
        except Exception as e:
            logger.warning("chart_volume_share failed: %r", e, exc_info=True); fig_share = None
        if fig_share is not None:
            builder.markdown("### Volume Share Over Time\n_Each controller's share of traded volume per bucket; line = total volume._")
            builder.plotly(fig_share)
        try:
            fig_cumvol = chart_cumulative_volume(ts_events)
        except Exception as e:
            logger.warning("chart_cumulative_volume failed: %r", e, exc_info=True); fig_cumvol = None
        if fig_cumvol is not None:
            builder.markdown("### Cumulative Volume per Controller\n_Total throughput each controller has produced, stacked._")
            builder.plotly(fig_cumvol)

        # Per-type parameter heatmaps.
        configs_by_type: dict[str, dict[str, dict]] = defaultdict(dict)
        for cid, cfg in configs.items():
            # Only controllers that appear in this report (active or had events).
            if cid in active_ids or cid in {e["controller_id"] for e in events}:
                configs_by_type[cfg.get("controller_name") or infer_type(cid, None)][cid] = cfg
        if configs_by_type:
            builder.markdown(
                "## Parameters by Controller Type\n"
                "_One heatmap per controller type. Columns are parameters that **differ** "
                "between that type's controllers (your tuning levers), plus capital, PnL+rebate, "
                "and volume. Cell color = value normalized within the column (green = highest)._"
            )
            for ctype in sorted(configs_by_type):
                fig_p = chart_params_for_type(ctype, configs_by_type[ctype], perf_by_ctrl, config.rebate_rate)
                if fig_p is not None:
                    builder.plotly(fig_p)

        if skipped:
            builder.markdown(
                "_Time-series skipped for large bots (shown in bars/table via live data): "
                + ", ".join(f"`{n.split('.')[0][:32]}` ({c:,} executors)" for n, c in skipped[:8])
                + "._"
            )
        if drops:
            builder.markdown("_Executor rows excluded: " + ", ".join(f"{v} {k.replace('_', ' ')}" for k, v in sorted(drops.items())) + "._")
        await builder.save()
    except Exception as e:
        logger.warning(f"Report generation failed: {e}", exc_info=True)

    # ----- Telegram summary -----
    lines = [
        f"*Controller Performance* — last {config.lookback_days}d",
        f"{len(summary_rows)} controllers ({len(active_ids)} active), {len(events)} closed runs",
    ]
    if stalled_ids:
        def _silent(c):
            return stall_status[c].get("log_silent_hours") or stall_status[c]["idle_hours"]
        lines.append(f"🔴 *{len(stalled_ids)} STALLED*: "
                     + ", ".join(f"`{c}` ({_silent(c):.0f}h{' hung' if stall_status[c].get('hung') else ''})"
                                 for c in sorted(stalled_ids, key=lambda c: -_silent(c))[:4]))
        confirmed = [b for b, v in verify.items() if v.get("traded") is False]
        if confirmed:
            lines.append(f"  ↳ primary source confirms halt: {', '.join('`'+b+'`' for b in confirmed[:3])}")
    lines.append("")
    for r in summary_rows[:7]:
        win = r["Window PnL+Reb"]
        live_pnl = r["Live PnL"]
        vol = r["Live Vol"] if r["Live Vol"] != "—" else r["Window Vol"]
        lines.append(f"`{r['Controller'][:26]}` [{r['Type']}] {r['Status'].replace('🟢 ','')} "
                     f"PnL {live_pnl if live_pnl != '—' else win} Vol {vol}")
    if len(summary_rows) > 7:
        lines.append(f"…and {len(summary_rows) - 7} more")
    lines.append("\nSee web report for candle+PnL, volume share, and per-type parameter charts.")
    text = "\n".join(lines)

    chart_bytes = None
    try:
        if fig_candle is not None:
            import io
            buf = io.BytesIO()
            fig_candle.write_image(buf, format="png", scale=2)
            chart_bytes = buf.getvalue()
            if chat_id and context.bot:
                buf.seek(0)
                await context.bot.send_photo(chat_id=chat_id, photo=buf,
                                             caption=f"Price & PnL — {candle_pair}")
    except Exception as e:
        logger.debug(f"Chart export failed: {e}")

    return RoutineResult(text=text, table_data=summary_rows, table_columns=SUMMARY_COLUMNS, chart_image=chart_bytes)
