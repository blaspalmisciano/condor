"""Callback handler for volume_drop_alert routine alerts.

Pattern: volalert:<action>:<controller_id>
Actions: stop, rebal, rconf, rcancel, ack
"""

import asyncio
import logging
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config_manager import get_client
from utils.auth import restricted

logger = logging.getLogger(__name__)


def _parse(callback_data: str) -> tuple[str, str]:
    parts = callback_data.split(":", 2)
    if len(parts) != 3 or parts[0] != "volalert":
        return "", ""
    return parts[1], parts[2]


async def _stop_controller_executors(client, controller_id: str) -> dict:
    page = await client.executors.search_executors(
        controller_ids=[controller_id], status="RUNNING", limit=100
    )
    records = (
        page.get("data") or page.get("executors") or []
        if isinstance(page, dict)
        else list(page or [])
    )
    stopped, failed = [], []
    for ex in records:
        eid = ex.get("executor_id")
        if not eid:
            continue
        try:
            await client.executors.stop_executor(eid, keep_position=False)
            stopped.append(eid)
        except Exception as e:
            failed.append((eid, str(e)))
    return {"stopped": stopped, "failed": failed, "total": len(records)}


async def _build_rebalance_spec(client, controller_id: str, meta: dict) -> dict:
    """Compute a grid_executor config that offsets the controller's net inventory."""
    page = await client.executors.search_executors(
        controller_ids=[controller_id], status="RUNNING", limit=100
    )
    records = (
        page.get("data") or page.get("executors") or []
        if isinstance(page, dict)
        else list(page or [])
    )
    if not records:
        return {"error": "no running executors for this controller"}

    # Aggregate net inventory across running executors.
    # Per-executor: realized_buy_size_quote - realized_sell_size_quote ≈ net base bought (in quote terms)
    net_quote = 0.0
    pairs: dict[str, int] = {}
    connectors: dict[str, int] = {}
    accounts: dict[str, int] = {}
    for ex in records:
        ci = ex.get("custom_info") or {}
        try:
            buy = float(ci.get("realized_buy_size_quote") or 0.0)
            sell = float(ci.get("realized_sell_size_quote") or 0.0)
        except (TypeError, ValueError):
            buy = sell = 0.0
        net_quote += buy - sell
        if ex.get("trading_pair"):
            pairs[ex["trading_pair"]] = pairs.get(ex["trading_pair"], 0) + 1
        if ex.get("connector_name"):
            connectors[ex["connector_name"]] = connectors.get(ex["connector_name"], 0) + 1
        if ex.get("account_name"):
            accounts[ex["account_name"]] = accounts.get(ex["account_name"], 0) + 1

    if abs(net_quote) < 1.0:
        return {"error": f"net inventory ~0 (= {net_quote:.2f}), nothing to rebalance"}

    pair = max(pairs, key=pairs.get) if pairs else None
    connector = max(connectors, key=connectors.get) if connectors else None
    account = max(accounts, key=accounts.get) if accounts else None
    if not pair or not connector:
        return {"error": "could not infer trading pair / connector"}

    try:
        prices = await client.market_data.get_prices(connector, [pair])
    except Exception as e:
        return {"error": f"price fetch failed: {e}"}

    mid = None
    if isinstance(prices, dict):
        if pair in prices:
            mid = prices[pair]
        else:
            for v in prices.values():
                if isinstance(v, (int, float)):
                    mid = v
                    break
                if isinstance(v, dict):
                    mid = v.get("mid_price") or v.get("price")
                    if mid:
                        break
    if not mid:
        return {"error": f"could not determine mid price for {pair}"}
    mid = float(mid)

    if net_quote > 0:
        side_int = 2  # SELL — we're long, sell to reduce
        side_str = "SELL"
        start_price = mid * 1.0001
        end_price = mid * 1.005
        limit_price = mid * 1.006
    else:
        side_int = 1  # BUY — we're short, buy to cover
        side_str = "BUY"
        start_price = mid * 0.9999
        end_price = mid * 0.995
        limit_price = mid * 0.994

    total_amount_quote = round(abs(net_quote), 2)

    config = {
        "type": "grid_executor",
        "connector_name": connector,
        "trading_pair": pair,
        "side": side_int,
        "start_price": round(start_price, 4),
        "end_price": round(end_price, 4),
        "limit_price": round(limit_price, 4),
        "total_amount_quote": total_amount_quote,
        "min_spread_between_orders": 1e-05,
        "min_order_amount_quote": 10.0,
        "max_open_orders": 5,
        "max_orders_per_batch": 2,
        "order_frequency": 1,
        "leverage": 1,
        "activation_bounds": 0.05,
        "triple_barrier_config": {
            "take_profit": 5e-05,
            "open_order_type": "LIMIT_MAKER",
            "take_profit_order_type": "LIMIT_MAKER",
        },
    }
    return {
        "config": config,
        "account": account,
        "side_str": side_str,
        "mid": mid,
        "net_quote": net_quote,
        "pair": pair,
        "connector": connector,
    }


async def _collect_bot_running_executors(client, controller_ids: list[str]) -> list[dict]:
    """Fetch all RUNNING executors for a list of controller_ids, deduped by executor_id."""
    seen: dict[str, dict] = {}
    for cid in controller_ids:
        try:
            page = await client.executors.search_executors(
                controller_ids=[cid], status="RUNNING", limit=100
            )
        except Exception as e:
            logger.warning(f"search_executors failed for {cid}: {e}")
            continue
        records = (
            page.get("data") or page.get("executors") or []
            if isinstance(page, dict)
            else list(page or [])
        )
        for ex in records:
            eid = ex.get("executor_id")
            if eid and eid not in seen:
                seen[eid] = ex
    return list(seen.values())


async def _build_rebalance_spec_bot(client, bot_name: str, meta: dict) -> dict:
    """Compute a grid_executor config offsetting NET inventory across all bot's controllers."""
    controller_ids = list(meta.get("controller_ids") or [])
    if not controller_ids:
        return {"error": f"no controllers tracked for bot {bot_name}"}

    records = await _collect_bot_running_executors(client, controller_ids)
    if not records:
        return {"error": f"no running executors for bot {bot_name}"}

    net_quote = 0.0
    pairs: dict[str, int] = {}
    connectors: dict[str, int] = {}
    accounts: dict[str, int] = {}
    per_controller: dict[str, int] = {}
    for ex in records:
        ci = ex.get("custom_info") or {}
        try:
            buy = float(ci.get("realized_buy_size_quote") or 0.0)
            sell = float(ci.get("realized_sell_size_quote") or 0.0)
        except (TypeError, ValueError):
            buy = sell = 0.0
        net_quote += buy - sell
        if ex.get("trading_pair"):
            pairs[ex["trading_pair"]] = pairs.get(ex["trading_pair"], 0) + 1
        if ex.get("connector_name"):
            connectors[ex["connector_name"]] = connectors.get(ex["connector_name"], 0) + 1
        if ex.get("account_name"):
            accounts[ex["account_name"]] = accounts.get(ex["account_name"], 0) + 1
        cid = ex.get("controller_id")
        if cid:
            per_controller[cid] = per_controller.get(cid, 0) + 1

    if abs(net_quote) < 1.0:
        return {"error": f"net inventory ~0 (= {net_quote:.2f}), nothing to rebalance"}

    pair = max(pairs, key=pairs.get) if pairs else None
    connector = max(connectors, key=connectors.get) if connectors else None
    account = max(accounts, key=accounts.get) if accounts else None
    primary_controller = (
        max(per_controller, key=per_controller.get) if per_controller else controller_ids[0]
    )
    if not pair or not connector:
        return {"error": "could not infer trading pair / connector across bot"}

    try:
        prices = await client.market_data.get_prices(connector, [pair])
    except Exception as e:
        return {"error": f"price fetch failed: {e}"}

    mid = None
    if isinstance(prices, dict):
        # response may nest under "prices"
        inner = prices.get("prices") if isinstance(prices.get("prices"), dict) else prices
        if pair in inner:
            v = inner[pair]
            if isinstance(v, (int, float)):
                mid = v
            elif isinstance(v, dict):
                mid = v.get("mid_price") or v.get("price")
        if mid is None:
            for v in inner.values():
                if isinstance(v, (int, float)):
                    mid = v
                    break
                if isinstance(v, dict):
                    mid = v.get("mid_price") or v.get("price")
                    if mid:
                        break
    if not mid:
        return {"error": f"could not determine mid price for {pair}"}
    mid = float(mid)

    if net_quote > 0:
        side_int = 2  # SELL
        side_str = "SELL"
        start_price = mid * 1.0001
        end_price = mid * 1.005
        limit_price = mid * 1.006
    else:
        side_int = 1  # BUY
        side_str = "BUY"
        start_price = mid * 0.9999
        end_price = mid * 0.995
        limit_price = mid * 0.994

    total_amount_quote = round(abs(net_quote), 2)

    config = {
        "type": "grid_executor",
        "connector_name": connector,
        "trading_pair": pair,
        "side": side_int,
        "start_price": round(start_price, 4),
        "end_price": round(end_price, 4),
        "limit_price": round(limit_price, 4),
        "total_amount_quote": total_amount_quote,
        "min_spread_between_orders": 1e-05,
        "min_order_amount_quote": 10.0,
        "max_open_orders": 5,
        "max_orders_per_batch": 2,
        "order_frequency": 1,
        "leverage": 1,
        "activation_bounds": 0.05,
        "triple_barrier_config": {
            "take_profit": 5e-05,
            "open_order_type": "LIMIT_MAKER",
            "take_profit_order_type": "LIMIT_MAKER",
        },
    }
    return {
        "config": config,
        "account": account,
        "side_str": side_str,
        "mid": mid,
        "net_quote": net_quote,
        "pair": pair,
        "connector": connector,
        "primary_controller": primary_controller,
        "controller_ids": controller_ids,
        "running_executor_ids": [ex.get("executor_id") for ex in records if ex.get("executor_id")],
    }


@restricted
async def volume_alert_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    await query.answer()
    action, cid = _parse(query.data or "")
    if not action or not cid:
        await query.edit_message_text("⚠️ malformed callback")
        return

    chat_id = query.message.chat_id if query.message else None
    client = await get_client(chat_id, context=context)
    if not client:
        await query.edit_message_text("⚠️ no API server configured")
        return

    if action == "ack":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await context.bot.send_message(chat_id, f"✅ Acknowledged `{cid}`", parse_mode="Markdown")
        return

    if action == "ackbot":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await context.bot.send_message(chat_id, f"✅ Acknowledged bot `{cid}`", parse_mode="Markdown")
        return

    if action == "stopbot":
        bot_name = cid  # for stopbot, the second segment is the bot name
        try:
            res = await client.bot_orchestration.stop_bot(bot_name)
        except Exception as e:
            await query.edit_message_text(
                f"⚠️ stop_bot failed for `{bot_name}`: {e}",
                parse_mode="Markdown",
            )
            return
        status = res.get("status") if isinstance(res, dict) else "?"
        await query.edit_message_text(
            f"⛔ Stop requested on bot `{bot_name}` (api: {status})",
            parse_mode="Markdown",
        )
        return

    if action == "stop":
        try:
            result = await _stop_controller_executors(client, cid)
        except Exception as e:
            await query.edit_message_text(f"⚠️ stop failed for `{cid}`: {e}", parse_mode="Markdown")
            return
        msg = (
            f"⛔ Stop requested on `{cid}`\n"
            f"Stopped: {len(result['stopped'])}/{result['total']} executors"
        )
        if result["failed"]:
            msg += f"\nFailed: {len(result['failed'])} (first error: {result['failed'][0][1]})"
        await query.edit_message_text(msg, parse_mode="Markdown")
        return

    if action == "rebal":
        meta = (context.bot_data.get("volalert_meta") or {}).get(cid, {})
        try:
            spec = await _build_rebalance_spec(client, cid, meta)
        except Exception as e:
            await query.edit_message_text(
                f"⚠️ rebalance spec failed for `{cid}`: {e}",
                parse_mode="Markdown",
            )
            return
        if "error" in spec:
            await query.edit_message_text(
                f"⚖️ Cannot auto-rebalance `{cid}`: {spec['error']}",
                parse_mode="Markdown",
            )
            return

        cfg = spec["config"]
        context.bot_data.setdefault("volalert_pending_rebal", {})[cid] = spec

        preview = (
            f"⚖️ Proposed rebalance for `{cid}`\n"
            f"Pair: `{spec['pair']}` @ `{spec['connector']}` (acct: `{spec['account']}`)\n"
            f"Net inventory: {spec['net_quote']:+.2f} (quote)\n"
            f"Mid price: {spec['mid']:.4f}\n"
            f"Grid: *{spec['side_str']}* {cfg['total_amount_quote']:.2f} quote\n"
            f"Range: {cfg['start_price']:.4f} → {cfg['end_price']:.4f} (limit {cfg['limit_price']:.4f})"
        )
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ Confirm grid", callback_data=f"volalert:rconf:{cid}"
                    ),
                    InlineKeyboardButton(
                        "❌ Cancel", callback_data=f"volalert:rcancel:{cid}"
                    ),
                ]
            ]
        )
        await query.edit_message_text(preview, parse_mode="Markdown", reply_markup=kb)
        return

    if action == "rconf":
        pending = (context.bot_data.get("volalert_pending_rebal") or {}).pop(cid, None)
        if not pending:
            await query.edit_message_text(
                f"⚠️ no pending rebalance for `{cid}` (timed out?)",
                parse_mode="Markdown",
            )
            return
        try:
            res = await client.executors.create_executor(
                executor_config=pending["config"],
                account_name=pending.get("account"),
                controller_id=cid,
            )
        except Exception as e:
            await query.edit_message_text(
                f"⚠️ grid creation failed for `{cid}`: {e}",
                parse_mode="Markdown",
            )
            return
        new_id = res.get("executor_id") if isinstance(res, dict) else None
        await query.edit_message_text(
            f"✅ Rebalance grid created on `{cid}`\n"
            f"Executor: `{new_id or '?'}`",
            parse_mode="Markdown",
        )
        return

    if action == "rcancel":
        (context.bot_data.get("volalert_pending_rebal") or {}).pop(cid, None)
        await query.edit_message_text(f"❌ Rebalance cancelled for `{cid}`", parse_mode="Markdown")
        return

    if action == "rebalbot":
        bot_name = cid  # second segment is the bot name
        meta = (context.bot_data.get("volalert_bot_meta") or {}).get(bot_name)
        if not meta:
            await query.edit_message_text(
                f"⚠️ no tracked metadata for bot `{bot_name}` (stale alert? run the routine again)",
                parse_mode="Markdown",
            )
            return
        try:
            spec = await _build_rebalance_spec_bot(client, bot_name, meta)
        except Exception as e:
            await query.edit_message_text(
                f"⚠️ rebalance spec failed for bot `{bot_name}`: {e}",
                parse_mode="Markdown",
            )
            return
        if "error" in spec:
            await query.edit_message_text(
                f"⚖️ Cannot auto-rebalance bot `{bot_name}`: {spec['error']}",
                parse_mode="Markdown",
            )
            return

        cfg = spec["config"]
        context.bot_data.setdefault("volalert_pending_rebal_bot", {})[bot_name] = spec

        preview = (
            f"⚖️ Proposed rebalance for bot `{bot_name}`\n"
            f"Pair: `{spec['pair']}` @ `{spec['connector']}` (acct: `{spec['account']}`)\n"
            f"Controllers tracked: {len(spec['controller_ids'])}, "
            f"running execs: {len(spec['running_executor_ids'])}\n"
            f"Net inventory: {spec['net_quote']:+.2f} (quote)\n"
            f"Mid price: {spec['mid']:.4f}\n"
            f"Grid: *{spec['side_str']}* {cfg['total_amount_quote']:.2f} quote\n"
            f"Range: {cfg['start_price']:.4f} → {cfg['end_price']:.4f} (limit {cfg['limit_price']:.4f})\n"
            f"⚠️ Confirm will STOP all running execs (keep_position=true) then create the grid."
        )
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ Confirm stop+grid", callback_data=f"volalert:rconfbot:{bot_name}"
                    ),
                    InlineKeyboardButton(
                        "❌ Cancel", callback_data=f"volalert:rcancelbot:{bot_name}"
                    ),
                ]
            ]
        )
        await query.edit_message_text(preview, parse_mode="Markdown", reply_markup=kb)
        return

    if action == "rconfbot":
        bot_name = cid
        pending = (context.bot_data.get("volalert_pending_rebal_bot") or {}).pop(bot_name, None)
        if not pending:
            await query.edit_message_text(
                f"⚠️ no pending rebalance for bot `{bot_name}` (timed out?)",
                parse_mode="Markdown",
            )
            return

        # Step 1: stop all running execs with keep_position=True so inventory survives the swap.
        stopped, failed = [], []
        for eid in pending.get("running_executor_ids") or []:
            try:
                await client.executors.stop_executor(eid, keep_position=True)
                stopped.append(eid)
            except Exception as e:
                failed.append((eid, str(e)))

        # Step 2: brief settle so cancels propagate before we open new orders.
        await asyncio.sleep(3)

        # Step 3: create the rebalance grid on the primary controller.
        try:
            res = await client.executors.create_executor(
                executor_config=pending["config"],
                account_name=pending.get("account"),
                controller_id=pending.get("primary_controller"),
            )
        except Exception as e:
            await query.edit_message_text(
                f"⚠️ grid creation failed for bot `{bot_name}` "
                f"(stopped {len(stopped)} execs first): {e}",
                parse_mode="Markdown",
            )
            return

        new_id = res.get("executor_id") if isinstance(res, dict) else None
        msg = (
            f"✅ Rebalance executed for bot `{bot_name}`\n"
            f"Stopped: {len(stopped)} execs (keep_position=true)\n"
            f"New grid: `{new_id or '?'}` on controller `{pending.get('primary_controller')}`"
        )
        if failed:
            msg += f"\n⚠️ {len(failed)} stop failures (first: {failed[0][1]})"
        await query.edit_message_text(msg, parse_mode="Markdown")
        return

    if action == "rcancelbot":
        bot_name = cid
        (context.bot_data.get("volalert_pending_rebal_bot") or {}).pop(bot_name, None)
        await query.edit_message_text(
            f"❌ Rebalance cancelled for bot `{bot_name}`", parse_mode="Markdown"
        )
        return

    await query.edit_message_text(f"⚠️ unknown action: {action}")
