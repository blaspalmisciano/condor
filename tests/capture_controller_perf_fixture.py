"""Capture a frozen bundle of the brigado API responses that controller_performance
needs, so the routine can be iterated on fully OFFLINE.

Run this ONCE while you still have internet:

    python3 tests/capture_controller_perf_fixture.py

It writes tests/fixtures/controller_perf_fixture.json. All calls are READ-ONLY
(get_active_bots_status, list/summary/executors, candles) — nothing touches the bots.

Then use tests/offline_controller_perf.py (no network) to iterate on the charts.
"""

import asyncio
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config_manager import ConfigManager  # noqa: E402
from routines import controller_performance as cp  # noqa: E402

FIXTURE = ROOT / "tests" / "fixtures" / "controller_perf_fixture.json"

# Match the routine's defaults so the captured data covers what run() would fetch.
CFG = cp.Config()


async def main():
    cm = ConfigManager.instance()
    client = await cm.get_client("brigado")
    print("connected to brigado")

    # 1. Live perf + configs + active bot names + container health/heartbeat.
    live, configs, active_bots, bot_health, cid_bot = await cp.gather_active(client)
    print(f"  live controllers={len(live)} configs={len(configs)} active_bots={active_bots}")

    type_lookup, pair_lookup = {}, {}
    for cid, cfg in configs.items():
        type_lookup[cid] = cfg.get("controller_name") or cp.infer_type(cid, None)
        pair_lookup[cid] = (cfg.get("trading_pair") or cp.infer_pair(cid, None)).upper()
    type_lookup["__active_bots__"] = "|".join(active_bots)

    # 2. Closed-executor events within window.
    events, drops, skipped = await cp.gather_executor_events(client, CFG, type_lookup, pair_lookup)
    type_lookup.pop("__active_bots__", None)
    print(f"  events={len(events)} drops={drops} skipped={len(skipped)}")

    # 3. Candles for the highest-volume pair (or BTC-BRL default).
    from collections import defaultdict
    vol_by_pair = defaultdict(float)
    for e in events:
        if e["pair"]:
            vol_by_pair[e["pair"]] += e["volume"]
    candle_pair = max(vol_by_pair, key=vol_by_pair.get) if vol_by_pair else "BTC-BRL"
    candles = []
    try:
        cd = await client.market_data.get_candles_last_days(
            CFG.candle_connector, candle_pair, max(1, CFG.lookback_days), CFG.candle_interval
        )
        candles = cp._extract_rows(cd) or (cd if isinstance(cd, list) else [])
    except Exception as e:
        print(f"  candle fetch failed: {e}")
    print(f"  candle_pair={candle_pair} candles={len(candles)}")

    # 4. FX rates (BRL→USDT etc.) so the offline harness can display in USDT.
    quotes = {cp._quote_of(p) for p in pair_lookup.values()} | {cp._quote_of(e["pair"]) for e in events}
    rates = await cp.get_usdt_rates(client, CFG.candle_connector, quotes)
    print(f"  rates={rates}")

    # 5. Stall + primary-source verification, so the offline alert reproduces the live one.
    cutoff = time.time() - CFG.lookback_days * 86400
    snap = cp.load_snapshot_series(CFG.snapshot_state_file, cutoff) if CFG.snapshot_state_file else {}
    for cid, info in snap.items():
        r = rates.get(cp._quote_of(pair_lookup.get(cid, "") or cp.infer_pair(cid, None)), 1.0)
        if r != 1.0:
            info["snaps"] = [(ts, v * r, p * r) for (ts, v, p) in info["snaps"]]
    capture_ts = time.time()
    stall_status, _ = cp.compute_stall_status(snap, CFG.stall_hours, bot_health, cid_bot, now=capture_ts)
    active_ids = set(live)
    stalled = {c for c, s in stall_status.items() if s["stalled"] and c in active_ids}
    verify = {}
    for cid in stalled:
        if stall_status[cid]["idle_hours"] * 3600 < CFG.verify_stale_days * 86400:
            continue
        bot = (snap.get(cid) or {}).get("bot")
        if not bot or bot in verify:
            continue
        r = rates.get(cp._quote_of(pair_lookup.get(cid, "") or cp.infer_pair(cid, None)), 1.0)
        verify[bot] = await cp.verify_bot_activity(client, bot, stall_status[cid]["last_move_ts"], r)
        print(f"  verify {bot}: {verify[bot]}")

    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_text(json.dumps({
        "live": live,
        "configs": configs,
        "active_bots": active_bots,
        "events": events,
        "drops": drops,
        "skipped": skipped,
        "candle_pair": candle_pair,
        "candles": candles,
        "rates": rates,
        "verify": verify,
        "bot_health": bot_health,
        "cid_bot": cid_bot,
        "capture_ts": capture_ts,
    }, default=str))
    print(f"\nwrote {FIXTURE}  ({FIXTURE.stat().st_size/1024:.0f} KB)")

    try:
        await client.close()
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(main())
