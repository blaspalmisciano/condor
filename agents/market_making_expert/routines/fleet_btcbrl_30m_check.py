"""30-minute post-deployment check for pmm-fleet-btcbrl.

Sleeps delay_minutes, then fetches bot_orchestration status for the fleet bot,
formats per-controller PnL/volume in BRL and USDT, and sends a Telegram notification.
"""

import asyncio
import logging
from datetime import datetime, timezone

from pydantic import BaseModel, Field
from telegram.ext import ContextTypes

from config_manager import get_client

logger = logging.getLogger(__name__)

CATEGORY = "Bot Analysis"

_CLOSE_LABELS = {
    "TAKE_PROFIT": "TP",
    "STOP_LOSS": "SL",
    "TRAILING_STOP": "Trail",
    "EARLY_STOP": "Early",
    "POSITION_HOLD": "Hold",
    "HOLD": "Hold",
    "EXPIRED": "Exp",
    "FAILED": "Fail",
}


def _close_label(raw: str) -> str:
    code = raw.split(".")[-1] if "." in raw else raw
    return _CLOSE_LABELS.get(code.upper(), code)


class Config(BaseModel):
    """30-min post-deployment check for a BTC-BRL fleet bot."""

    bot_name: str = Field(
        default="pmm-fleet-btcbrl-20260825-054703",
        description="Exact bot instance name to check",
    )
    delay_minutes: int = Field(
        default=30, description="Minutes to wait before checking"
    )
    brl_usd_rate: float = Field(
        default=5.17, description="BRL per USD conversion rate"
    )


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> str:
    chat_id = context._chat_id

    await context.bot.send_message(
        chat_id=chat_id,
        text=f"⏱ *30-min check scheduled* for `{config.bot_name}` — will report at T+{config.delay_minutes}min.",
        parse_mode="Markdown",
    )

    # Sleep in 60s chunks so the process stays alive
    remaining = config.delay_minutes * 60
    while remaining > 0:
        await asyncio.sleep(min(60, remaining))
        remaining -= 60

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    client = await get_client(chat_id, context=context)
    if not client:
        msg = "❌ 30-min check failed: no server client available."
        await context.bot.send_message(chat_id=chat_id, text=msg)
        return msg

    # Fetch active bots status
    bots_data = {}
    fetch_error = None
    try:
        resp = await client.bot_orchestration.get_active_bots_status()
        raw = resp if isinstance(resp, dict) else {}
        bots_data = raw.get("data", raw) if isinstance(raw, dict) else {}
    except Exception as e:
        fetch_error = str(e)

    if fetch_error:
        msg = f"❌ 30-min check failed fetching bot status: {fetch_error}"
        await context.bot.send_message(chat_id=chat_id, text=msg)
        return msg

    # Find the fleet bot (match by prefix in case instance suffix differs)
    fleet_data = None
    actual_bot_name = None
    bot_prefix = config.bot_name.split("-20260825")[0]  # e.g. pmm-fleet-btcbrl
    for bname, bdata in bots_data.items():
        if config.bot_name in bname or bot_prefix in bname:
            fleet_data = bdata
            actual_bot_name = bname
            break

    if not fleet_data:
        found_bots = list(bots_data.keys())
        msg = (
            f"⚠️ Bot `{config.bot_name}` not found in active bots at T+{config.delay_minutes}min.\n"
            f"Active bots: {found_bots or 'none'}"
        )
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
        return msg

    # Parse controller performance
    perf_dict = fleet_data.get("performance", {}) or {}
    ctrl_lines = []
    total_realized = 0.0
    total_volume = 0.0
    error_count = 0

    # Error logs
    errs = [
        e for e in (fleet_data.get("error_logs") or [])
        if isinstance(e, dict) and str(e.get("level_name", "")).upper() in {"ERROR", "CRITICAL", "FATAL"}
    ]
    error_count = len(errs)

    for ctrl_name, ctrl_data in sorted(perf_dict.items()):
        if not isinstance(ctrl_data, dict):
            continue
        inner = ctrl_data.get("performance", ctrl_data)
        if not isinstance(inner, dict):
            continue

        realized = float(inner.get("realized_pnl_quote", 0) or 0)
        unrealized = float(inner.get("unrealized_pnl_quote", 0) or 0)
        volume = float(inner.get("volume_traded", 0) or 0)
        total_realized += realized
        total_volume += volume

        # Close type breakdown
        close_counts = inner.get("close_type_counts", {}) or {}
        close_parts = []
        for raw_ct, cnt in close_counts.items():
            label = _close_label(str(raw_ct))
            close_parts.append(f"{label}:{cnt}")
        close_str = " ".join(close_parts) or "—"

        realized_usd = realized / config.brl_usd_rate
        vol_usd = volume / config.brl_usd_rate

        status_icon = "✅" if realized >= 0 else "🔴"
        ctrl_lines.append(
            f"{status_icon} `{ctrl_name}`\n"
            f"   PnL: {realized:+.2f} BRL ({realized_usd:+.2f} USD) | Vol: {volume:,.0f} BRL | {close_str}"
        )

    total_realized_usd = total_realized / config.brl_usd_rate
    total_volume_usd = total_volume / config.brl_usd_rate

    pnl_icon = "✅" if total_realized >= 0 else "🔴"
    err_icon = "⚠️" if error_count > 0 else "✅"

    header = (
        f"📊 *pmm-fleet-btcbrl — T+{config.delay_minutes}min check* — {now}\n"
        f"Bot: `{actual_bot_name}`\n"
        f"Controllers: {len(ctrl_lines)}/12\n\n"
        f"{pnl_icon} *Total realized PnL:* {total_realized:+.2f} BRL ({total_realized_usd:+.2f} USD)\n"
        f"📈 *Total volume:* {total_volume:,.0f} BRL ({total_volume_usd:,.0f} USD)\n"
        f"{err_icon} *Errors:* {error_count}\n"
    )

    ctrl_block = "\n".join(ctrl_lines) if ctrl_lines else "No controller data yet."

    full_msg = header + "\n*Per-controller:*\n" + ctrl_block

    await context.bot.send_message(chat_id=chat_id, text=full_msg, parse_mode="Markdown")

    return f"30-min check complete. {len(ctrl_lines)} controllers. Total PnL: {total_realized:+.2f} BRL."
