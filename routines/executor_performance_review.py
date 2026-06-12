"""Review historical executor performance: daily volume, per-group comparison, param correlations, and ready-to-run recommendations."""

CATEGORY = "Bot Analysis"

import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Literal

import numpy as np
from pydantic import BaseModel, Field
from telegram.ext import ContextTypes

from condor.fetchers.executors import (
    fetch_all_executors,
    get_executor_fees,
    get_executor_pnl,
    get_executor_type,
    get_executor_volume,
)
from config_manager import get_client
from routines.base import RoutineResult

logger = logging.getLogger(__name__)


TYPE_DISPLAY = {
    "grid": "Grids",
    "order": "PMM-style bots (order_executor)",
    "position": "Directional bots (position_executor)",
    "dca": "DCA bots",
    "lp": "LP positions",
}

USD_STABLES = {"USDT", "USDC", "USD", "BUSD", "DAI", "TUSD", "FDUSD", "USDP"}
DEFAULT_MAX_CHART_SERIES = 10  # default top-N hue series in stacked bars; overridable per-run

# Live bot controllers — Hummingbot does not always set the executor-level
# 'type' for live controllers, so map controller_name → executor type for the
# row we synthesize.
LIVE_CONTROLLER_TYPE_HINTS = (
    ("grid", "grid"),
    ("pmm", "order"),
    ("market_making", "order"),
    ("dman", "position"),
    ("directional", "position"),
    ("dca", "dca"),
    ("lp", "lp"),
)


class Config(BaseModel):
    """Daily volume, per-group comparison, Spearman correlations, and promote/retire recommendations for each executor type. Exclude noisy controllers/pairs before running."""

    lookback_days: int = Field(default=60, description="Days of history to analyze")
    focus_types: str = Field(
        default="grid,order,position,dca,lp",
        description="Comma-separated executor types to deep-analyze (grid, order, position, dca, lp). One section per type.",
    )
    exclude_groups: str = Field(
        default="",
        description=(
            "Comma-separated controller IDs, grids, or 'PAIR SIDE' tokens to DROP before analysis "
            "(case-insensitive substring match against controller_id, 'PAIR SIDE', or executor_id). "
            "Use this to exclude a noisy controller or grid that distorts the rest of the data. "
            "Examples: 'pmm_btc_main', 'BTC-USDT SELL', 'grid_doge'."
        ),
    )
    exclude_pairs: str = Field(
        default="",
        description=(
            "Comma-separated trading pairs to exclude entirely (e.g. 'BTC-BRL,DOGE-USDT'). "
            "Useful when one pair dominates volume and hides signal in the others."
        ),
    )
    group_by: Literal["auto", "controller", "pair_side", "executor"] = Field(
        default="auto",
        description=(
            "How to group rows in per-type tables and chart hues. "
            "'auto' = controller if multiple exist, else pair+side, else executor. "
            "'controller' = force controller/grid name (good when many bots share one strategy class). "
            "'pair_side' = force pair + side (good when one controller wraps many pairs). "
            "'executor' = one row per executor (most granular, noisy)."
        ),
    )
    min_runs_for_corr: int = Field(
        default=6,
        description="Minimum executors of a type required to run correlation analysis",
    )
    top_n_promote: int = Field(default=3, description="How many groups to promote per type")
    top_n_retire: int = Field(default=3, description="How many groups to retire per type")
    max_chart_series: int = Field(
        default=10,
        description=(
            "Maximum distinct series in the volume stacked bar charts before the rest collapse "
            "into 'other (N)'. Bump this (e.g. 25) when you want every controller/pair visible "
            "in the chart instead of hidden in an aggregated bucket."
        ),
    )
    include_active_bots: bool = Field(
        default=True,
        description=(
            "Include live/running bot controllers (fetched via bot_orchestration.get_active_bots_status) "
            "as synthetic rows alongside closed executors. When false, the report only shows "
            "historical (closed) executors."
        ),
    )


# ---------------------------------------------------------------------------
# USD conversion
# ---------------------------------------------------------------------------

def _quote_of(pair: str) -> str:
    if "-" in pair:
        return pair.split("-")[-1].upper()
    return pair.upper()


async def build_usd_rates(client, rows: list[dict]) -> dict[str, float]:
    """Return {quote_currency: multiplier_to_usd}. amount_usd = amount_quote * multiplier."""
    rates: dict[str, float] = {}
    quotes: dict[str, str] = {}  # quote -> a connector that traded it
    for r in rows:
        q = _quote_of(r["pair"])
        if not q:
            continue
        if q in USD_STABLES:
            rates[q] = 1.0
            continue
        if q not in quotes and r["connector"]:
            quotes[q] = r["connector"]

    for q, conn in quotes.items():
        # USDT-{q} spot tells us how many q = 1 USDT
        try:
            resp = await client.market_data.get_prices(conn, [f"USDT-{q}"])
            price = None
            if isinstance(resp, dict):
                price = resp.get(f"USDT-{q}") or resp.get("prices", {}).get(f"USDT-{q}")
                if price is None and "data" in resp:
                    inner = resp.get("data") or {}
                    if isinstance(inner, dict):
                        price = inner.get(f"USDT-{q}")
            if price:
                rates[q] = 1.0 / float(price)
                continue
        except Exception as e:
            logger.debug(f"USDT-{q} price fetch failed: {e}")
        # Fallback: try inverse pair {q}-USDT
        try:
            resp = await client.market_data.get_prices(conn, [f"{q}-USDT"])
            price = None
            if isinstance(resp, dict):
                price = resp.get(f"{q}-USDT") or resp.get("prices", {}).get(f"{q}-USDT")
            if price:
                rates[q] = float(price)
                continue
        except Exception:
            pass
        logger.warning(f"No USD rate found for {q}; leaving as 1.0 (volume will be in {q})")
        rates[q] = 1.0
    return rates


def to_usd(amount: float, pair: str, rates: dict[str, float]) -> float:
    return amount * rates.get(_quote_of(pair), 1.0)


# ---------------------------------------------------------------------------
# Row extraction
# ---------------------------------------------------------------------------

def _row(ex: dict) -> dict[str, Any]:
    cfg = ex.get("config") if isinstance(ex.get("config"), dict) else {}
    ex_type = get_executor_type(ex)
    ts_open = float(cfg.get("timestamp") or ex.get("timestamp") or 0)
    ts_close = float(ex.get("close_timestamp") or 0)
    if ts_close <= 0 and str(ex.get("status", "")).upper() == "RUNNING":
        ts_close = time.time()
    runtime_h = max(0.0, (ts_close - ts_open) / 3600.0) if ts_open else 0.0

    controller_id = (
        cfg.get("controller_id")
        or ex.get("controller_id")
        or f"ad-hoc:{ex_type}"
    )

    return {
        "id": str(ex.get("id") or ex.get("executor_id") or ""),
        "type": ex_type,
        "connector": cfg.get("connector_name") or ex.get("connector_name") or "",
        "pair": cfg.get("trading_pair") or ex.get("trading_pair") or "",
        "side": str(cfg.get("side") or ex.get("side") or ""),
        "status": str(ex.get("status") or "").upper(),
        "close_type": str(ex.get("close_type") or ""),
        "pnl_quote": get_executor_pnl(ex),
        "volume_quote": get_executor_volume(ex),
        "fees_quote": get_executor_fees(ex),
        "amount_quote": float(cfg.get("total_amount_quote") or cfg.get("amount") or 0),
        # USD-converted fields populated by apply_usd_rates()
        "pnl": 0.0,
        "volume": 0.0,
        "fees": 0.0,
        "amount": 0.0,
        "ts_open": ts_open,
        "ts_close": ts_close,
        "runtime_h": runtime_h,
        "controller_id": str(controller_id),
        "config": cfg,
    }


def apply_usd_rates(rows: list[dict], rates: dict[str, float]) -> None:
    for r in rows:
        rate = rates.get(_quote_of(r["pair"]), 1.0)
        r["pnl"] = r["pnl_quote"] * rate
        r["volume"] = r["volume_quote"] * rate
        r["fees"] = r["fees_quote"] * rate
        r["amount"] = r["amount_quote"] * rate


# ---------------------------------------------------------------------------
# Parameter flattening per executor type
# ---------------------------------------------------------------------------

NUMERIC_PARAMS_BY_TYPE: dict[str, list[str]] = {
    "grid": [
        "total_amount_quote",
        "min_spread_between_orders",
        "min_order_amount_quote",
        "order_frequency",
        "max_orders_per_batch",
        "max_open_orders",
        "activation_bounds",
        "leverage",
        "limit_price",
        "safe_extra_spread",
    ],
    "position": [
        "amount",
        "leverage",
        "entry_price",
        "stop_loss",
        "take_profit",
        "time_limit",
        "activation_bounds",
        "trailing_stop_activation_price",
        "trailing_stop_trailing_delta",
        "open_order_type",
    ],
    "dca": [
        "leverage",
        "time_limit",
        "stop_loss",
        "take_profit",
        "activation_bounds",
    ],
    "order": ["amount", "price", "leverage", "execution_strategy_id"],
    "lp": ["total_amount_quote"],
}

TRIPLE_BARRIER_NUMERIC = ["stop_loss", "take_profit", "time_limit", "trailing_stop"]

EXECUTION_STRATEGY_MAP = {
    "MARKET": 1.0, "LIMIT": 2.0, "LIMIT_MAKER": 3.0, "LIMIT_CHASER": 4.0,
}

# Controller-level numeric params that get merged into the group breakdown
# when we successfully join an executor's controller_id to a controller config.
MARKET_MAKING_CONTROLLER_PARAMS = [
    "executor_refresh_time",
    "cooldown_time",
    "leverage",
    "total_amount_quote",
    "stop_loss",
    "take_profit",
    "time_limit",
    "trailing_stop_activation_price",
    "trailing_stop_trailing_delta",
]
GRID_CONTROLLER_PARAMS = [
    "total_amount_quote",
    "min_spread_between_orders",
    "min_order_amount_quote",
    "order_frequency",
    "max_orders_per_batch",
    "max_open_orders",
    "activation_bounds",
    "leverage",
]


def _coerce_float(v: Any) -> float | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v)
        except ValueError:
            return None
    return None


def flatten_params(cfg: dict, ex_type: str) -> dict[str, float]:
    out: dict[str, float] = {}
    base = NUMERIC_PARAMS_BY_TYPE.get(ex_type, [])
    for k in base:
        v = cfg.get(k)
        coerced = _coerce_float(v)
        if coerced is not None:
            out[k] = coerced

    # Execution strategy enum → numeric, so it can correlate
    if ex_type == "order":
        es = cfg.get("execution_strategy")
        if isinstance(es, str) and es.upper() in EXECUTION_STRATEGY_MAP:
            out["execution_strategy_id"] = EXECUTION_STRATEGY_MAP[es.upper()]
        elif isinstance(es, (int, float)) and not isinstance(es, bool):
            out["execution_strategy_id"] = float(es)
        chaser = cfg.get("chaser_config")
        if isinstance(chaser, dict):
            for k in ("distance", "refresh_threshold"):
                cv = _coerce_float(chaser.get(k))
                if cv is not None:
                    out[f"chaser_{k}"] = cv

    tb = cfg.get("triple_barrier_config") or {}
    if isinstance(tb, dict):
        for k in TRIPLE_BARRIER_NUMERIC:
            v = tb.get(k)
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                out[f"tb_{k}"] = float(v)
            elif isinstance(v, dict):
                inner = v.get("trailing_delta") or v.get("activation_price_delta")
                if isinstance(inner, (int, float)):
                    out[f"tb_{k}"] = float(inner)

    # Some configs put trailing_stop at top-level as a dict
    ts = cfg.get("trailing_stop")
    if isinstance(ts, dict):
        ap = _coerce_float(ts.get("activation_price") or ts.get("activation_price_delta"))
        td = _coerce_float(ts.get("trailing_delta"))
        if ap is not None:
            out.setdefault("trailing_stop_activation_price", ap)
        if td is not None:
            out.setdefault("trailing_stop_trailing_delta", td)

    if ex_type == "grid":
        sp = cfg.get("start_price")
        ep = cfg.get("end_price")
        if isinstance(sp, (int, float)) and isinstance(ep, (int, float)) and sp:
            out["grid_range_pct"] = abs(float(ep) - float(sp)) / float(sp) * 100.0

    if ex_type == "dca":
        amounts = cfg.get("amounts_quote")
        prices = cfg.get("prices")
        if isinstance(amounts, list) and amounts:
            out["dca_levels"] = float(len(amounts))
            try:
                out["dca_total_quote"] = float(sum(float(x) for x in amounts))
            except (TypeError, ValueError):
                pass
        if isinstance(prices, list) and len(prices) >= 2:
            try:
                pmin, pmax = min(float(x) for x in prices), max(float(x) for x in prices)
                if pmin:
                    out["dca_price_range_pct"] = (pmax - pmin) / pmin * 100.0
                    out["dca_price_avg"] = float(sum(float(x) for x in prices) / len(prices))
            except (TypeError, ValueError):
                pass

    return out


# ---------------------------------------------------------------------------
# Controller config join (best-effort)
# ---------------------------------------------------------------------------

def _infer_type_from_controller_name(name: str) -> str:
    """Map a controller_name (e.g. 'pmm_mister', 'grid_simple') to executor type."""
    if not name:
        return "order"
    n = name.lower()
    for hint, ex_type in LIVE_CONTROLLER_TYPE_HINTS:
        if hint in n:
            return ex_type
    return "order"


async def fetch_active_bot_rows(client, controller_configs: dict[str, dict]) -> list[dict]:
    """Fetch live bot controllers via bot_orchestration and convert them to row dicts.

    Each running controller becomes a single synthetic row using cumulative
    realized+unrealized PnL and volume_traded. Type is inferred from the
    controller_name in its saved config (pmm_* → order, *grid* → grid, etc).
    Returns [] on any failure — historical-only is always a valid fallback.
    """
    try:
        result = await client.bot_orchestration.get_active_bots_status()
    except Exception as e:
        logger.debug(f"get_active_bots_status failed: {e}")
        return []

    if not isinstance(result, dict):
        return []
    bots_data = result.get("data") or {}
    if isinstance(bots_data, list):
        bots_iter = bots_data
    elif isinstance(bots_data, dict):
        bots_iter = [{"bot_name": bn, **(b if isinstance(b, dict) else {})}
                     for bn, b in bots_data.items()]
    else:
        return []

    now = time.time()
    rows: list[dict] = []
    for bot in bots_iter:
        if not isinstance(bot, dict):
            continue
        bot_name = bot.get("bot_name", "")
        perf = bot.get("performance") or {}
        if not isinstance(perf, dict):
            continue
        for ctrl_id, ctrl_info in perf.items():
            if not isinstance(ctrl_info, dict):
                continue
            ctrl_perf = ctrl_info.get("performance") or {}
            if not isinstance(ctrl_perf, dict):
                ctrl_perf = {}

            ctrl_cfg = controller_configs.get(ctrl_id) or {}
            ctrl_name = ctrl_cfg.get("controller_name") or ""
            ex_type = _infer_type_from_controller_name(ctrl_name)

            positions = ctrl_perf.get("positions_summary") or []
            pair = ctrl_cfg.get("trading_pair") or ""
            side = ""
            connector = ctrl_cfg.get("connector_name") or ""
            if isinstance(positions, list) and positions:
                first = positions[0] if isinstance(positions[0], dict) else {}
                pair = pair or first.get("trading_pair") or ""
                connector = connector or first.get("connector_name") or ""
                raw_side = str(first.get("side") or "")
                if "BUY" in raw_side.upper():
                    side = "BUY"
                elif "SELL" in raw_side.upper():
                    side = "SELL"

            realized = float(ctrl_perf.get("realized_pnl_quote") or 0)
            unrealized = float(ctrl_perf.get("unrealized_pnl_quote") or 0)
            pnl_quote = float(ctrl_perf.get("global_pnl_quote") or (realized + unrealized))
            volume_quote = float(ctrl_perf.get("volume_traded") or 0)
            fees_quote = 0.0
            if isinstance(positions, list):
                for p in positions:
                    if isinstance(p, dict):
                        fees_quote += float(p.get("cum_fees_quote") or 0)

            amount_quote = float(ctrl_cfg.get("total_amount_quote") or 0)
            ts_open = now  # Conservative — actual deploy ts isn't always present
            runtime_h = 0.0

            rows.append({
                "id": f"live:{ctrl_id}",
                "type": ex_type,
                "connector": connector,
                "pair": pair,
                "side": side,
                "status": "RUNNING",
                "close_type": "",
                "pnl_quote": pnl_quote,
                "volume_quote": volume_quote,
                "fees_quote": fees_quote,
                "amount_quote": amount_quote,
                "pnl": 0.0,
                "volume": 0.0,
                "fees": 0.0,
                "amount": 0.0,
                "ts_open": ts_open,
                "ts_close": 0,
                "runtime_h": runtime_h,
                "controller_id": ctrl_id,
                "config": dict(ctrl_cfg),  # so flatten_params can pull from it
                "_live": True,
                "_bot_name": bot_name,
            })
    return rows


async def fetch_controller_configs(client) -> dict[str, dict]:
    """Return {controller_id: full_config_dict} merged from the global library AND
    any currently-running bots (bot-local configs are not in the global list)."""
    out: dict[str, dict] = {}

    try:
        items = await client.controllers.list_controller_configs()
        if isinstance(items, list):
            for it in items:
                if not isinstance(it, dict):
                    continue
                cfg = it.get("config") if isinstance(it.get("config"), dict) else it
                cid = cfg.get("id") or cfg.get("config_name") or it.get("id") or it.get("name")
                if cid:
                    out[str(cid)] = cfg
    except Exception as e:
        logger.debug(f"controllers.list_controller_configs failed: {e}")

    # Augment with bot-local configs (e.g. 'btcbrl-v4' only exists inside the bot).
    try:
        bots_status = await client.bot_orchestration.get_active_bots_status()
        bots_data = (bots_status or {}).get("data") if isinstance(bots_status, dict) else None
        bot_names: list[str] = []
        if isinstance(bots_data, dict):
            bot_names = list(bots_data.keys())
        elif isinstance(bots_data, list):
            bot_names = [b.get("bot_name", "") for b in bots_data if isinstance(b, dict)]
        for bn in [n for n in bot_names if n]:
            try:
                bot_cfgs = await client.controllers.get_bot_controller_configs(bn)
            except Exception:
                continue
            if not isinstance(bot_cfgs, list):
                continue
            for cfg in bot_cfgs:
                if not isinstance(cfg, dict):
                    continue
                cid = cfg.get("id") or cfg.get("controller_id") or cfg.get("config_name")
                if cid:
                    out[str(cid)] = cfg
    except Exception as e:
        logger.debug(f"bot controller config augmentation failed: {e}")

    return out


def flatten_controller_params(ctrl_cfg: dict) -> dict[str, float]:
    """Pull numeric scalars from a controller config + derive aggregates from list params."""
    out: dict[str, float] = {}
    ctype = (ctrl_cfg.get("controller_type") or "").lower()
    cname = (ctrl_cfg.get("controller_name") or "").lower()
    is_grid = "grid" in cname or ctype == "generic"
    keys = GRID_CONTROLLER_PARAMS if is_grid else MARKET_MAKING_CONTROLLER_PARAMS

    for k in keys:
        cv = _coerce_float(ctrl_cfg.get(k))
        if cv is not None:
            out[f"ctrl_{k}"] = cv

    # PMM spreads / amount distributions are lists — derive mean and count
    for list_key in ("buy_spreads", "sell_spreads", "buy_amounts_pct", "sell_amounts_pct"):
        v = ctrl_cfg.get(list_key)
        if isinstance(v, list) and v:
            try:
                nums = [float(x) for x in v]
                out[f"ctrl_{list_key}_n"] = float(len(nums))
                out[f"ctrl_{list_key}_mean"] = float(sum(nums) / len(nums))
            except (TypeError, ValueError):
                pass
    return out


# ---------------------------------------------------------------------------
# Spearman correlation (no scipy dependency)
# ---------------------------------------------------------------------------

def _rank(arr: np.ndarray) -> np.ndarray:
    order = arr.argsort()
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, len(arr) + 1)
    _, inv, counts = np.unique(arr, return_inverse=True, return_counts=True)
    sums = np.zeros(len(counts))
    np.add.at(sums, inv, ranks)
    means = sums / counts
    return means[inv]


def spearman(x: np.ndarray, y: np.ndarray) -> float:
    if len(x) < 3 or np.all(x == x[0]) or np.all(y == y[0]):
        return 0.0
    rx, ry = _rank(x), _rank(y)
    if np.std(rx) == 0 or np.std(ry) == 0:
        return 0.0
    return float(np.corrcoef(rx, ry)[0, 1])


# ---------------------------------------------------------------------------
# Day labels
# ---------------------------------------------------------------------------

def _bucket_key(ts: float) -> str:
    """Return a sortable YYYY-MM-DD label for the day bucket."""
    if ts <= 0:
        return "unknown"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _bucket_label(key: str) -> str:
    if key == "unknown":
        return key
    try:
        d = datetime.strptime(key, "%Y-%m-%d")
        return d.strftime("%b %d")
    except ValueError:
        return key


# ---------------------------------------------------------------------------
# Row exclusion
# ---------------------------------------------------------------------------

def _parse_csv(s: str) -> list[str]:
    return [t.strip() for t in s.split(",") if t.strip()]


def filter_rows(
    rows: list[dict], exclude_groups: str, exclude_pairs: str
) -> tuple[list[dict], dict[str, int]]:
    """Drop rows matching any exclusion token. Returns (kept, drop_stats)."""
    grp_tokens = [t.lower() for t in _parse_csv(exclude_groups)]
    pair_tokens = {t.upper() for t in _parse_csv(exclude_pairs)}
    if not grp_tokens and not pair_tokens:
        return rows, {}
    stats: dict[str, int] = defaultdict(int)
    kept: list[dict] = []
    for r in rows:
        if r["pair"].upper() in pair_tokens:
            stats[r["pair"]] += 1
            continue
        # Build the same haystack values _row_group_key produces so the user can
        # exclude by controller_id, "PAIR SIDE", or executor id.
        haystacks = [
            (r["controller_id"] or "").lower(),
            f"{r['pair']} {_side_label(r['side'])}".lower(),
            (r["id"] or "").lower(),
        ]
        match = next(
            (t for t in grp_tokens if any(t in hs for hs in haystacks if hs)),
            None,
        )
        if match:
            stats[match] += 1
            continue
        kept.append(r)
    return kept, dict(stats)


# ---------------------------------------------------------------------------
# Daily bar chart (overview — all types)
# ---------------------------------------------------------------------------

def _hue_for_row(r: dict, controllers_vary: bool) -> str:
    """Hue label for the overview chart — always at the individual executor
    level so dominant grids/bots stand out instead of being lumped under a
    pair_side or controller bucket."""
    pair = r["pair"] or "?"
    side = _side_label(r["side"]) if r["side"] else ""
    eid = (r["id"] or "")[:6]
    cid = r["controller_id"] or ""
    cid_label = f" · {cid[:12]}" if cid and cid not in ("main", "") else ""
    return f"{r['type']}: {pair} {side}{cid_label} · {eid}".strip()


def _cap_series(hue_volumes: dict[str, list[float]], cap: int) -> dict[str, list[float]]:
    """Keep top-N series by total; collapse remainder into 'other'.

    Pass cap <= 0 to disable capping (show every series). When the count is
    above the cap, the trailing 'other (N)' bucket is appended.
    """
    if cap <= 0 or len(hue_volumes) <= cap:
        return hue_volumes
    sorted_hues = sorted(hue_volumes.keys(), key=lambda h: -sum(hue_volumes[h]))
    top = sorted_hues[:cap]
    n_days = len(next(iter(hue_volumes.values())))
    out = {h: hue_volumes[h] for h in top}
    other = [0.0] * n_days
    for h in sorted_hues[cap:]:
        for i, v in enumerate(hue_volumes[h]):
            other[i] += v
    out[f"other ({len(sorted_hues) - cap})"] = other
    return out


def group_daily_by_hue(
    rows: list[dict], max_series: int = DEFAULT_MAX_CHART_SERIES
) -> tuple[list[str], dict[str, list[float]]]:
    days: set[str] = set()
    hue_day_vol: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    controllers_vary = len({r["controller_id"] for r in rows}) > 1

    for r in rows:
        if r["ts_open"] <= 0:
            continue
        day = _bucket_key(r["ts_open"])
        days.add(day)
        hue = _hue_for_row(r, controllers_vary)
        hue_day_vol[hue][day] += r["volume"]

    day_list = sorted(d for d in days if d != "unknown")
    out = {h: [hue_day_vol[h].get(d, 0.0) for d in day_list] for h in hue_day_vol}
    return day_list, _cap_series(out, max_series)


def _build_daily_bar(
    day_labels: list[str],
    hue_volumes: dict[str, list[float]],
):
    import plotly.graph_objects as go

    fig = go.Figure()
    palette = [
        "#3fb950", "#58a6ff", "#f0883e", "#a371f7", "#f85149",
        "#56d4dd", "#e3b341", "#ff7b72", "#7ee787", "#d2a8ff",
    ]
    display_labels = [_bucket_label(d) for d in day_labels]
    sorted_hues = sorted(hue_volumes.keys(), key=lambda h: -sum(hue_volumes[h]))
    for i, hue in enumerate(sorted_hues):
        fig.add_trace(go.Bar(
            name=hue,
            x=display_labels,
            y=hue_volumes[hue],
            marker_color=palette[i % len(palette)],
            hovertemplate=f"{hue}<br>Day %{{x}}<br>$%{{y:,.0f}}<extra></extra>",
        ))
    fig.update_layout(
        barmode="stack",
        title="Daily Volume by Executor / Controller",
        xaxis_title="Day",
        yaxis_title="Volume (USD)",
        paper_bgcolor="#0d1117",
        plot_bgcolor="#161b22",
        font=dict(color="#c9d1d9", size=11),
        margin=dict(l=60, r=30, t=80, b=60),
        legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02),
    )
    fig.update_xaxes(gridcolor="#21262d")
    fig.update_yaxes(gridcolor="#21262d")
    return fig


# ---------------------------------------------------------------------------
# Per-type daily bar (one type only, hued by individual executor)
# ---------------------------------------------------------------------------

def _build_daily_bar_for_type(
    rows: list[dict],
    focus_type: str,
    max_series: int = DEFAULT_MAX_CHART_SERIES,
):
    import plotly.graph_objects as go

    type_rows = [r for r in rows if r["type"] == focus_type and r["ts_open"] > 0]
    if not type_rows:
        return None

    # Per-type charts always hue at the individual executor/grid level so the
    # user can spot which specific grid is driving volume on any given day.
    # The per-group breakdown TABLE still respects group_by — it's just the
    # chart that's locked to executor granularity.
    def _hue(r):
        pair = r["pair"] or "?"
        side = _side_label(r["side"]) if r["side"] else ""
        cid_label = ""
        if r["controller_id"] and r["controller_id"] not in ("main", ""):
            cid_label = f" · {r['controller_id'][:14]}"
        eid = (r["id"] or "")[:6]
        suffix = f" · {eid}" if eid else ""
        return f"{pair} {side}{cid_label}{suffix}".strip()

    days: set[str] = set()
    hue_day_vol: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for r in type_rows:
        day = _bucket_key(r["ts_open"])
        days.add(day)
        hue_day_vol[_hue(r)][day] += r["volume"]

    day_list = sorted(days)
    display_labels = [_bucket_label(d) for d in day_list]
    raw = {h: [hue_day_vol[h].get(d, 0.0) for d in day_list] for h in hue_day_vol}
    capped = _cap_series(raw, max_series)
    palette = [
        "#58a6ff", "#3fb950", "#f0883e", "#a371f7", "#f85149",
        "#56d4dd", "#e3b341", "#ff7b72", "#7ee787", "#d2a8ff", "#8b949e",
    ]

    fig = go.Figure()
    sorted_hues = sorted(capped.keys(), key=lambda h: -sum(capped[h]))
    for i, hue in enumerate(sorted_hues):
        fig.add_trace(go.Bar(
            name=hue, x=display_labels, y=capped[hue],
            marker_color=palette[i % len(palette)],
            hovertemplate=f"{hue}<br>Day %{{x}}<br>$%{{y:,.0f}}<extra></extra>",
        ))
    fig.update_layout(
        barmode="stack",
        title=f"Daily Volume — {TYPE_DISPLAY.get(focus_type, focus_type)} (by individual executor)",
        xaxis_title="Day",
        yaxis_title="Volume (USD)",
        paper_bgcolor="#0d1117",
        plot_bgcolor="#161b22",
        font=dict(color="#c9d1d9", size=11),
        margin=dict(l=60, r=30, t=80, b=60),
        legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02),
    )
    fig.update_xaxes(gridcolor="#21262d")
    fig.update_yaxes(gridcolor="#21262d")
    return fig


# ---------------------------------------------------------------------------
# Per-group breakdown (group key = controller_id if varied, else trading_pair)
# ---------------------------------------------------------------------------

_GROUP_BY_OVERRIDE = {
    "controller": "controller_id",
    "pair_side": "pair_side",
    "executor": "id",
}


def _pick_group_key(rows: list[dict], focus_type: str, override: str = "auto") -> str:
    """Choose the grouping key for this type.

    With override == "auto" (default), the priority is:
      1. controller_id — if multiple distinct controllers exist.
      2. pair_side — if controller_id is uniform but multiple pairs/sides exist.
      3. id — last-resort per-executor.

    With override == "controller": prefer controller_id, but expand to per-executor
    (`id`) when every row shares the same controller_id — otherwise the table
    would collapse to a single useless row labeled e.g. 'main'.
    """
    type_rows = [r for r in rows if r["type"] == focus_type]
    if override == "controller":
        cids = {r["controller_id"] for r in type_rows}
        return "controller_id" if len(cids) > 1 else "id"
    if override and override != "auto":
        return _GROUP_BY_OVERRIDE.get(override, "controller_id")
    cids = {r["controller_id"] for r in type_rows}
    if len(cids) > 1:
        return "controller_id"
    pair_sides = {(r["pair"], r["side"]) for r in type_rows}
    if len(pair_sides) > 1:
        return "pair_side"
    return "id"


SIDE_LABEL = {"1": "BUY", "2": "SELL", "BUY": "BUY", "SELL": "SELL"}


def _side_label(side: Any) -> str:
    return SIDE_LABEL.get(str(side).upper(), str(side) or "?")


def _row_group_key(r: dict, group_key: str) -> str:
    if group_key == "pair_side":
        pair = r.get("pair") or "?"
        return f"{pair} {_side_label(r.get('side'))}"
    if group_key == "id":
        # Per-executor mode: prefix with pair+side so the row is identifiable
        # without having to scan the trailing columns.
        eid = r.get("id") or "unknown"
        pair = r.get("pair") or ""
        side = _side_label(r.get("side")) if r.get("side") else ""
        if pair and side:
            return f"{pair} {side} · {eid[:8]}"
        if pair:
            return f"{pair} · {eid[:8]}"
        return eid
    return r.get(group_key) or "unknown"


def group_breakdown(
    rows: list[dict],
    focus_type: str,
    group_key: str,
    controller_configs: dict[str, dict] | None = None,
) -> dict[str, dict[str, Any]]:
    by_grp: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        if r["type"] != focus_type:
            continue
        key = _row_group_key(r, group_key)
        by_grp[key].append(r)

    now = time.time()
    out: dict[str, dict[str, Any]] = {}
    for key, ex_list in by_grp.items():
        pnl = sum(e["pnl"] for e in ex_list)
        vol = sum(e["volume"] for e in ex_list)
        fees = sum(e["fees"] for e in ex_list)
        runtime = sum(e["runtime_h"] for e in ex_list)
        amount = sum(e["amount"] for e in ex_list)
        last_close = max((e["ts_close"] for e in ex_list), default=0)
        first_open = min((e["ts_open"] for e in ex_list if e["ts_open"] > 0), default=0)

        canonical_cfg = ex_list[0]["config"]
        flat_params = flatten_params(canonical_cfg, focus_type)

        # Join controller-level params. When grouped by controller_id we look up
        # by key; for pair_side / id groupings we pick the most-frequent
        # controller_id represented in the group's rows so PMM/grid params
        # (refresh time, spreads, etc.) still appear.
        ctrl_cfg: dict | None = None
        if controller_configs:
            if group_key == "controller_id" and key in controller_configs:
                ctrl_cfg = controller_configs[key]
            else:
                cid_counts: dict[str, int] = defaultdict(int)
                for e in ex_list:
                    cid = e.get("controller_id") or ""
                    if cid and cid in controller_configs:
                        cid_counts[cid] += 1
                if cid_counts:
                    best_cid = max(cid_counts, key=lambda c: cid_counts[c])
                    ctrl_cfg = controller_configs[best_cid]
            if ctrl_cfg:
                flat_params.update(flatten_controller_params(ctrl_cfg))

        cutoff = now - 86400
        recent = [
            e for e in ex_list
            if (e["ts_close"] >= cutoff if e["ts_close"] > 0 else e["ts_open"] >= cutoff)
        ]
        recent_pnl = sum(e["pnl"] for e in recent)
        recent_fees = sum(e["fees"] for e in recent)
        tvl = amount / max(1, len(ex_list)) if amount else 0.0
        ratio_24h = ((recent_pnl + recent_fees) / tvl) if tvl > 0 else 0.0
        win_rate = sum(1 for e in ex_list if e["pnl"] > 0) / len(ex_list) if ex_list else 0.0
        avg_pnl = pnl / len(ex_list) if ex_list else 0.0

        out[key] = {
            "group_key": key,
            "executors": len(ex_list),
            "pairs": sorted({e["pair"] for e in ex_list if e["pair"]}),
            "sides": sorted({_side_label(e["side"]) for e in ex_list if e["side"]}),
            "connectors": sorted({e["connector"] for e in ex_list if e["connector"]}),
            "pnl": pnl,
            "volume": vol,
            "fees": fees,
            "runtime_h": runtime,
            "tvl_avg": tvl,
            "ratio_24h_fee_pnl_over_tvl": ratio_24h,
            "win_rate": win_rate,
            "avg_pnl_per_run": avg_pnl,
            "first_open_ts": first_open,
            "last_close_ts": last_close,
            "params": flat_params,
            "canonical_cfg": canonical_cfg,
            "controller_cfg": ctrl_cfg,
        }
    return out


def diff_params_across_groups(breakdown: dict[str, dict[str, Any]]) -> list[str]:
    seen: dict[str, set] = defaultdict(set)
    for v in breakdown.values():
        for k, val in v["params"].items():
            seen[k].add(round(val, 8))
    return sorted(k for k, vs in seen.items() if len(vs) >= 2)


# ---------------------------------------------------------------------------
# Correlations
# ---------------------------------------------------------------------------

def correlation_matrix(rows: list[dict], focus_type: str) -> dict[str, dict[str, float]]:
    target_rows = [r for r in rows if r["type"] == focus_type]
    if len(target_rows) < 3:
        return {}

    flat = [flatten_params(r["config"], focus_type) for r in target_rows]
    all_params = sorted({k for f in flat for k in f.keys()})
    pnl = np.array([r["pnl"] for r in target_rows], dtype=float)
    vol = np.array([r["volume"] for r in target_rows], dtype=float)

    out: dict[str, dict[str, float]] = {}
    for p in all_params:
        col = np.array([f.get(p, np.nan) for f in flat], dtype=float)
        mask = ~np.isnan(col)
        if mask.sum() < 3 or np.all(col[mask] == col[mask][0]):
            continue
        out[p] = {
            "pnl": spearman(col[mask], pnl[mask]),
            "volume": spearman(col[mask], vol[mask]),
            "n": int(mask.sum()),
        }
    return out


def _build_corr_chart(correlations: dict[str, dict[str, float]], focus_type: str):
    """Heatmap sorted by max(|ρ(PnL)|, |ρ(Volume)|) descending."""
    import plotly.graph_objects as go

    if not correlations:
        return None

    params = sorted(
        correlations.keys(),
        key=lambda p: max(abs(correlations[p]["pnl"]), abs(correlations[p]["volume"])),
        reverse=True,
    )
    pnl_vals = [correlations[p]["pnl"] for p in params]
    vol_vals = [correlations[p]["volume"] for p in params]
    z = [[pnl_vals[i], vol_vals[i]] for i in range(len(params))]
    text = [[f"{pnl_vals[i]:+.2f}", f"{vol_vals[i]:+.2f}"] for i in range(len(params))]

    # Plotly heatmaps render the FIRST y entry at the bottom by default.
    # Reverse the lists so the strongest |ρ| parameter ends up at the TOP.
    y_params = params[::-1]
    z = z[::-1]
    text = text[::-1]

    fig = go.Figure(go.Heatmap(
        z=z,
        x=["ρ vs PnL", "ρ vs Volume"],
        y=y_params,
        text=text,
        texttemplate="%{text}",
        textfont=dict(size=12, color="#0d1117"),
        colorscale=[
            [0.0, "#f85149"],
            [0.5, "#161b22"],
            [1.0, "#3fb950"],
        ],
        zmid=0, zmin=-1, zmax=1,
        colorbar=dict(title="ρ", tickfont=dict(color="#c9d1d9")),
        hovertemplate="%{y} → %{x}<br>ρ = %{z:.2f}<extra></extra>",
    ))
    fig.update_layout(
        title=f"Spearman ρ Heatmap — {TYPE_DISPLAY.get(focus_type, focus_type)} parameters",
        paper_bgcolor="#0d1117",
        plot_bgcolor="#161b22",
        font=dict(color="#c9d1d9", size=11),
        margin=dict(l=200, r=80, t=80, b=40),
        yaxis=dict(categoryorder="array", categoryarray=y_params),
    )
    return fig


# ---------------------------------------------------------------------------
# Recommendations + variations
# ---------------------------------------------------------------------------

def make_recommendations(
    breakdown: dict[str, dict[str, Any]],
    correlations: dict[str, dict[str, float]],
    top_promote: int,
    top_retire: int,
) -> dict[str, Any]:
    ranked = sorted(
        breakdown.values(),
        key=lambda v: v["pnl"] + v["fees"] * 0.5,
        reverse=True,
    )
    promote = ranked[:top_promote]
    retire = ranked[-top_retire:] if len(ranked) > top_retire else []

    influence = sorted(
        correlations.items(),
        key=lambda kv: max(abs(kv[1]["pnl"]), abs(kv[1]["volume"])),
        reverse=True,
    )
    top_params = [p for p, _ in influence[:3]]

    variations: list[dict[str, Any]] = []
    if promote and top_params:
        winner = promote[0]
        base_params = dict(winner["params"])
        for label, factor in (("aggressive", 1.25), ("conservative", 0.80)):
            v_params = dict(base_params)
            for p in top_params:
                if p not in v_params:
                    continue
                corr = correlations.get(p, {}).get("pnl", 0)
                direction = 1 if corr >= 0 else -1
                bump = factor if label == "aggressive" else (1 / factor)
                if direction < 0:
                    bump = (1 / factor) if label == "aggressive" else factor
                v_params[p] = round(v_params[p] * bump, 8)
            variations.append({
                "label": label,
                "based_on": winner["group_key"],
                "params": v_params,
                "tuned_keys": top_params,
            })

    return {
        "promote": promote,
        "retire": retire,
        "top_params": top_params,
        "variations": variations,
    }


def build_condor_prompt(focus_type: str, rec: dict[str, Any]) -> str:
    if not rec["variations"]:
        return f"(No actionable {focus_type} variations — not enough data)"

    lines = [
        f"Deploy {len(rec['variations'])} {focus_type} executors as variations of "
        f"best-performing group `{rec['variations'][0]['based_on']}`.",
        "",
        f"Influential parameters (Spearman): {', '.join(rec['top_params']) or '—'}",
        "",
    ]
    for v in rec["variations"]:
        params_str = ", ".join(f"{k}={v['params'][k]:g}" for k in sorted(v["params"]))
        lines.append(
            f"- **{v['label']}** → `manage_executors action=create executor_type={focus_type}` "
            f"with {params_str}"
        )
    lines.extend([
        "",
        "Use the canonical config of the source group for non-numeric fields "
        "(connector, trading_pair, side, order types). Confirm with me before placing orders.",
    ])
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def fmt_usd(v: float) -> str:
    sign = "-" if v < 0 else ""
    v = abs(v)
    if v >= 1_000_000:
        return f"{sign}${v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"{sign}${v / 1_000:.1f}K"
    return f"{sign}${v:.2f}"


# ---------------------------------------------------------------------------
# Per-type section builder
# ---------------------------------------------------------------------------

def _short_param(k: str) -> str:
    """Compact column name: trim ctrl_ / tb_ / chaser_ prefixes for table readability."""
    if k.startswith("ctrl_"):
        return k[5:]
    return k


def _fmt_param(val: float | None) -> str:
    if val is None:
        return "—"
    if abs(val) >= 1_000 or (abs(val) > 0 and abs(val) < 0.001):
        return f"{val:.3g}"
    return f"{val:g}"


def _emit_type_section(
    builder,
    rows: list[dict],
    focus_type: str,
    config: Config,
    controller_configs: dict[str, dict] | None = None,
) -> tuple[str, dict[str, Any]]:
    """Append a full analysis section for one executor type. Returns (telegram_snippet, recommendation)."""
    type_label = TYPE_DISPLAY.get(focus_type, focus_type)
    type_rows = [r for r in rows if r["type"] == focus_type]
    if not type_rows:
        builder.markdown(f"## {type_label}\n_No executors of type `{focus_type}` in this window._")
        return f"\n*{type_label}*: no data", {"promote": [], "retire": [], "variations": [], "top_params": []}

    builder.markdown(
        f"# {type_label}\n"
        f"_Deep-dive analysis of all `{focus_type}_executor` activity. "
        f"{len(type_rows)} executor instances across "
        f"{len({r['pair'] for r in type_rows if r['pair']})} trading pair(s)._"
    )

    # Volume chart for this type (always daily, hued by individual executor)
    daily_chart = _build_daily_bar_for_type(rows, focus_type, config.max_chart_series)
    if daily_chart is not None:
        builder.markdown(
            f"### Daily Volume — {type_label}\n"
            f"_Stacked bars show **USD volume per day**, hued by **individual executor / grid**. "
            f"This isolates how much throughput this strategy class is producing day-by-day._"
        )
        builder.plotly(daily_chart)

    # Per-group comparison
    group_key = _pick_group_key(rows, focus_type, config.group_by)
    breakdown = group_breakdown(rows, focus_type, group_key, controller_configs)
    diff_keys = diff_params_across_groups(breakdown)

    if group_key == "controller_id":
        group_label, granularity_blurb = "Controller", "controller"
    elif group_key == "pair_side":
        group_label, granularity_blurb = "Pair / Side", "pair + side"
    else:
        group_label, granularity_blurb = "Executor ID", "individual executor"

    joined_ctrl_count = sum(1 for v in breakdown.values() if v.get("controller_cfg"))
    join_blurb = (
        f" Controller-level params (prefixed `ctrl_…`) joined from saved configs for "
        f"{joined_ctrl_count}/{len(breakdown)} groups."
        if joined_ctrl_count
        else ""
    )

    builder.markdown(
        f"### Per-{group_label} Comparison — {type_label}\n"
        f"_Each row is one **{granularity_blurb}**, sorted by PnL. "
        f"Right-hand columns ({', '.join(f'`{_short_param(k)}`' for k in diff_keys) or '—'}) "
        f"show parameter values that **differ between rows** — your tuning levers."
        f"{join_blurb} All monetary values in **USD** (non-USD quotes converted at current spot rate)._"
    )
    if breakdown:
        base_cols = [
            group_label, "Pair(s)", "Side", "Runs",
            "PnL (USD)", "Volume (USD)", "Fees (USD)",
            "Runtime (h)", "TVL (USD)", "24h (fee+pnl)/TVL", "Win rate",
        ]
        table_rows = []
        for c in sorted(breakdown.values(), key=lambda v: v["pnl"], reverse=True):
            pair_str = c["pairs"][0] if len(c["pairs"]) == 1 else (
                ", ".join(c["pairs"][:2]) + (f" +{len(c['pairs']) - 2}" if len(c["pairs"]) > 2 else "")
            )
            side_str = c["sides"][0] if len(c["sides"]) == 1 else ",".join(c["sides"][:2])
            row_data = {
                group_label: c["group_key"][:40],
                "Pair(s)": pair_str,
                "Side": side_str,
                "Runs": c["executors"],
                "PnL (USD)": fmt_usd(c["pnl"]),
                "Volume (USD)": fmt_usd(c["volume"]),
                "Fees (USD)": fmt_usd(c["fees"]),
                "Runtime (h)": f"{c['runtime_h']:.1f}",
                "TVL (USD)": fmt_usd(c["tvl_avg"]),
                "24h (fee+pnl)/TVL": f"{c['ratio_24h_fee_pnl_over_tvl'] * 100:.2f}%",
                "Win rate": f"{c['win_rate'] * 100:.0f}%",
            }
            for k in diff_keys:
                row_data[_short_param(k)] = _fmt_param(c["params"].get(k))
            table_rows.append(row_data)
        diff_cols = [_short_param(k) for k in diff_keys]
        builder.table(table_rows, columns=base_cols + diff_cols)
    else:
        builder.markdown(f"_No grouped data for {focus_type}._")

    # Correlations
    correlations = correlation_matrix(rows, focus_type)
    n_focus = len(type_rows)
    if correlations and n_focus >= config.min_runs_for_corr:
        builder.markdown(
            f"### Spearman Correlations — {type_label}\n"
            f"_How each parameter co-moves with PnL and Volume across **{n_focus} {focus_type} executors**. "
            f"ρ near **+1** = increasing the parameter tends to increase the outcome; "
            f"near **−1** = decreasing it does; near **0** = no monotonic relationship. "
            f"Rows are sorted by **max(|ρ(PnL)|, |ρ(Volume)|)** descending — strongest levers first._"
        )
        builder.plotly(_build_corr_chart(correlations, focus_type))
    else:
        builder.markdown(
            f"### Spearman Correlations — {type_label}\n"
            f"_Not enough varied data: {n_focus} {focus_type} executors "
            f"(need ≥ {config.min_runs_for_corr} with parameter variation)._"
        )

    # Recommendations — rich tables at controller/grid level
    rec = make_recommendations(breakdown, correlations, config.top_n_promote, config.top_n_retire)
    builder.markdown(
        f"### Recommendations — {type_label}\n"
        f"_Each row aggregates a **{granularity_blurb}** over the {config.lookback_days}-day window. "
        f"Groups ranked by `PnL + 0.5 × fees`. "
        f"Promote = scale up. Retire = stop or rebuild. "
        f"Param columns reflect the canonical config of the group (controller config when available)._"
    )

    # Build a shared rich-column layout for promote+retire so the user sees full context.
    # Includes the top influential params so they can see exactly what made the winner win.
    top_params_for_cols = rec.get("top_params") or []
    rich_base_cols = [
        group_label, "Pair(s)", "Runs",
        "PnL (USD)", "Volume (USD)", "Fees (USD)",
        "Runtime (h)", "Avg TVL (USD)", "24h yield", "Win rate",
        "Avg PnL/run",
    ]
    rich_param_cols = [_short_param(k) for k in top_params_for_cols]

    def _rich_row(c: dict) -> dict:
        pair_str = c["pairs"][0] if len(c["pairs"]) == 1 else (
            ", ".join(c["pairs"][:2]) + (f" +{len(c['pairs']) - 2}" if len(c["pairs"]) > 2 else "")
        )
        row = {
            group_label: c["group_key"][:40],
            "Pair(s)": pair_str,
            "Runs": c["executors"],
            "PnL (USD)": fmt_usd(c["pnl"]),
            "Volume (USD)": fmt_usd(c["volume"]),
            "Fees (USD)": fmt_usd(c["fees"]),
            "Runtime (h)": f"{c['runtime_h']:.1f}",
            "Avg TVL (USD)": fmt_usd(c["tvl_avg"]),
            "24h yield": f"{c['ratio_24h_fee_pnl_over_tvl'] * 100:.2f}%",
            "Win rate": f"{c['win_rate'] * 100:.0f}%",
            "Avg PnL/run": fmt_usd(c["avg_pnl_per_run"]),
        }
        for k, short_k in zip(top_params_for_cols, rich_param_cols):
            row[short_k] = _fmt_param(c["params"].get(k))
        return row

    if rec["promote"]:
        builder.markdown(
            f"#### Promote — top {len(rec['promote'])} {granularity_blurb}s worth scaling up\n"
            f"_Best risk-adjusted earners over the lookback. Consider adding capital, running additional "
            f"instances, or copying these configs to other pairs. Param columns at the right show the "
            f"top-{len(top_params_for_cols)} influential parameters (Spearman) — these are why they won._"
        )
        builder.table(
            [_rich_row(c) for c in rec["promote"]],
            columns=rich_base_cols + rich_param_cols,
        )
    if rec["retire"]:
        builder.markdown(
            f"#### Retire — bottom {len(rec['retire'])} {granularity_blurb}s worth stopping\n"
            f"_Capital is being consumed without matching returns. Stop these or rebuild from scratch — "
            f"compare their right-hand param values vs. the Promote table to see where they diverge._"
        )
        builder.table(
            [_rich_row(c) for c in rec["retire"]],
            columns=rich_base_cols + rich_param_cols,
        )
    if rec["variations"]:
        winner = rec["promote"][0] if rec["promote"] else None
        winner_key = winner["group_key"] if winner else "?"
        builder.markdown(
            f"#### Suggested Variations of the Winner (`{winner_key[:40]}`)\n"
            f"_Two new parameter sets derived from the top {granularity_blurb}. **Aggressive** bumps the "
            f"top-{len(top_params_for_cols)} influential params **+25%** in the direction that historically "
            f"improved PnL; **Conservative** pulls them **−20%**. All other params stay identical to the "
            f"winner. Use the side-by-side table below to compare values, then deploy via the Condor prompt._"
        )
        # Side-by-side variation table: param | winner | aggressive | conservative
        winner_params = (winner or {}).get("params", {}) if winner else {}
        agg_params = next((v["params"] for v in rec["variations"] if v["label"] == "aggressive"), {})
        con_params = next((v["params"] for v in rec["variations"] if v["label"] == "conservative"), {})
        all_keys = sorted(set(winner_params) | set(agg_params) | set(con_params))
        tuned_set = set(rec.get("top_params") or [])
        variation_rows = []
        for k in all_keys:
            row = {
                "Parameter": _short_param(k) + ("  *(tuned)*" if k in tuned_set else ""),
                "Winner": _fmt_param(winner_params.get(k)),
                "Aggressive (+25%)": _fmt_param(agg_params.get(k)),
                "Conservative (−20%)": _fmt_param(con_params.get(k)),
            }
            variation_rows.append(row)
        if variation_rows:
            builder.table(
                variation_rows,
                columns=["Parameter", "Winner", "Aggressive (+25%)", "Conservative (−20%)"],
            )

    # Condor prompt
    prompt = build_condor_prompt(focus_type, rec)
    builder.markdown(
        f"#### Condor Prompt — {type_label}\n"
        f"_Ready-to-paste deploy instructions. Sends to a Condor session, which will then ask you to "
        f"confirm before placing orders._"
    )
    builder.markdown(f"```\n{prompt}\n```")

    # Telegram summary snippet
    snip_lines = [f"\n*{type_label}* ({n_focus} runs)"]
    for c in rec["promote"][:2]:
        snip_lines.append(
            f"  ↑ `{c['group_key'][:24]}` PnL {fmt_usd(c['pnl'])} Vol {fmt_usd(c['volume'])}"
        )
    if rec["top_params"]:
        snip_lines.append(f"  • levers: {', '.join(_short_param(p) for p in rec['top_params'])}")
    return "\n".join(snip_lines), rec


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> RoutineResult:
    chat_id = context._chat_id if hasattr(context, "_chat_id") else None
    client = await get_client(chat_id, context=context)
    if not client:
        return RoutineResult(text="No server available. Configure servers in /config.")

    cutoff = time.time() - config.lookback_days * 86400

    try:
        raw = await fetch_all_executors(client, max_items=1000)
    except Exception as e:
        logger.exception("fetch_all_executors failed")
        return RoutineResult(text=f"Failed to fetch executors: {e}")

    rows = [_row(ex) for ex in raw]
    rows = [r for r in rows if r["ts_open"] >= cutoff or r["status"] == "RUNNING"]

    # Best-effort: fetch controller configs (global + bot-local) so we can join
    # PMM/grid controller-level params and resolve live controllers' types.
    controller_configs = await fetch_controller_configs(client)
    if controller_configs:
        logger.info(f"Loaded {len(controller_configs)} controller configs for join")

    # Inject synthetic rows for live/running bot controllers — the search-DB
    # historical fetch above misses everything that hasn't closed yet.
    live_rows: list[dict] = []
    if config.include_active_bots:
        try:
            live_rows = await fetch_active_bot_rows(client, controller_configs)
            if live_rows:
                logger.info(f"Including {len(live_rows)} live bot controller(s) as synthetic rows")
                rows.extend(live_rows)
        except Exception as e:
            logger.warning(f"Active bot fetch failed: {e}")

    if not rows:
        return RoutineResult(text=f"No executors found in last {config.lookback_days} days.")

    # USD conversion — populates r["pnl"], r["volume"], r["fees"], r["amount"]
    # from their *_quote counterparts. Without this every monetary value is 0.
    try:
        rates = await build_usd_rates(client, rows)
        apply_usd_rates(rows, rates)
    except Exception as e:
        logger.warning(f"USD conversion failed, falling back to identity rates: {e}")
        apply_usd_rates(rows, {})

    focus_types_list = [t.strip() for t in config.focus_types.split(",") if t.strip()]

    # ----- Build HTML report -----
    telegram_snippets: list[str] = []
    try:
        from condor.reports import ReportBuilder

        builder = ReportBuilder(f"Executor Performance Review ({config.lookback_days}d)")
        builder.source("routine", "executor_performance_review").tags(
            ["executors", "performance"] + focus_types_list
        )
        # Keep markdown/plotly/table cells in the order we append them, so each
        # table's title + subtitle stays next to it (otherwise ReportBuilder
        # default sort pushes all markdown to the bottom).
        builder.manual_order()

        builder.markdown(
            f"## Overview\n"
            f"- Lookback: **{config.lookback_days}** days\n"
            f"- Executors analyzed: **{len(rows)}**\n"
            f"- Type breakdown: " + ", ".join(
                f"**{TYPE_DISPLAY.get(t, t)}**: {sum(1 for r in rows if r['type'] == t)}"
                for t in sorted({r['type'] for r in rows})
            ) + "\n"
            f"- Deep-dive sections below: " + ", ".join(
                f"`{t}`" for t in focus_types_list
            )
        )

        # Overview chart (all types combined) — daily, hued by individual executor
        day_labels, hue_vols = group_daily_by_hue(rows, config.max_chart_series)
        if day_labels:
            builder.markdown(
                "## Daily Volume — All Activity\n"
                "_Stacked bars show **USD volume per day**, hued by **executor type + pair (+ controller / id)**. "
                "This is the master view: every dollar transacted across every strategy class. "
                "Use the per-type sections below for tuning-grade detail._"
            )
            builder.plotly(_build_daily_bar(day_labels, hue_vols))

        # Per-type sections
        for focus_type in focus_types_list:
            snip, _rec = _emit_type_section(
                builder, rows, focus_type, config, controller_configs
            )
            telegram_snippets.append(snip)

        builder.save()
    except Exception as e:
        logger.warning(f"Report generation failed: {e}", exc_info=True)

    # ----- Telegram text summary -----
    text = (
        f"*Executor Performance Review* — last {config.lookback_days}d\n"
        f"Total executors: {len(rows)}\n"
        + "".join(telegram_snippets)
        + "\n\nSee web report for charts, parameter tables, and Condor prompts."
    )

    # Try to ship the overview chart to Telegram too
    chart_bytes = None
    try:
        if day_labels:
            import io
            fig = _build_daily_bar(day_labels, hue_vols)
            buf = io.BytesIO()
            fig.write_image(buf, format="png", scale=2)
            buf.seek(0)
            chart_bytes = buf.getvalue()
            if chat_id and context.bot:
                buf.seek(0)
                await context.bot.send_photo(
                    chat_id=chat_id, photo=buf, caption="Daily volume — all activity"
                )
    except Exception as e:
        logger.debug(f"Chart export failed: {e}")

    return RoutineResult(text=text, chart_image=chart_bytes)
