"""Reliability tests for PMM Autopilot — offline, mocked, no network/live calls.

Covers the fixes for the failures that made a scheduled lap hang silently and a
deploy 500 AFTER the fleet was already stopped:
- deploy-safe config-id minting (no dots/×/spaces; ^[a-z0-9_-]+$, <=80),
- a deploy PRE-FLIGHT that refuses an unsafe id BEFORE any bot is stopped,
- the per-lap heartbeat file (a stall can no longer look like a quiet healthy lap),
- the round-robin controller cap that keeps a lap inside its window and still
  covers the whole fleet across laps.

Reuses the fakes from the Phase 0 suite.
"""
import asyncio
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import routines.pmm_autopilot as ap
from tests.test_pmm_autopilot_phase0 import FakeLive, big_wallet, cand, fleet


# ── 1. deploy-safe id minting ────────────────────────────────────────────────
def test_cand_id_is_always_deploy_safe():
    """Adversarial labels (dots, ×, spaces, unicode) must mint an id that passes _id_ok."""
    base = "btcbrl-alloc-500__effectivization_2_0"
    for label in ["TP×1.25 Spread×0.75", "band 0.3-0.55", "refresh=12.0s",
                  "  weird//name..dots  ", "λ-effektiv", "", "a" * 200]:
        nid = ap._cand_id(base, label, {"tp_mult": 1.25, "spread_mult": 0.75})
        assert re.fullmatch(r"[a-z0-9_-]+", nid), f"unsafe id from {label!r}: {nid!r}"
        assert len(nid) <= 80, f"id too long from {label!r}: {len(nid)}"
        assert ap._id_ok(nid)


def test_id_ok_semantics():
    assert ap._id_ok("btcbrl-alloc-500__tp_1_25")      # hyphens + underscores ok
    assert not ap._id_ok("btcbrl__tpx1.25")            # a dot is fatal (server appends .yml)
    assert not ap._id_ok("has space")
    assert not ap._id_ok("tp×1.25")
    assert not ap._id_ok("")                           # empty rejected
    assert not ap._id_ok("x" * 81)                     # over length rejected


# ── 2. deploy pre-flight refuses an unsafe id before touching live ───────────
def test_deploy_preflight_refuses_dotted_id_before_any_stop():
    """A dotted config id must abort the reshape with ZERO stops/deploys — the 500
    used to land AFTER the old bots were already archived."""
    bot = "botA"
    live = FakeLive(bot_configs={bot: [{"id": "p1", "controller_name": "pmm_mister"},
                                       {"id": "p2", "controller_name": "pmm_mister"}]},
                    wallet=big_wallet())
    bot_ctrls = {bot: [{"cid": "p1", "type": "pmm_mister"}, {"cid": "p2", "type": "pmm_mister"}]}
    fl = fleet({"p1": {"bot": bot}, "p2": {"bot": bot}}, bot_ctrls)
    # one clean candidate, one with a fatal dotted id
    selected = [cand("p1__tp_1_25", "p1", bot), cand("btcbrl__tpx1.25", "p2", bot)]
    res = asyncio.run(ap._execute_reshape(live, selected, fl, "T"))
    assert res["ok"] is False
    assert res.get("aborted") == "unsafe config id"
    assert live.bot_orchestration.stopped == []          # nothing stopped
    assert live.bot_orchestration.deployed == []          # nothing deployed


# ── 3. heartbeat file ────────────────────────────────────────────────────────
def test_write_heartbeat_records_status(tmp_path, monkeypatch):
    hb = tmp_path / "hb.json"
    monkeypatch.setattr(ap, "HEARTBEAT_STORE", str(hb))
    ap._write_heartbeat(7, 12, 3, "ok")
    data = ap._load(str(hb))
    assert data["lap"] == 7
    assert data["n_controllers"] == 12
    assert data["n_proposals"] == 3
    assert data["status"] == "ok"
    assert data.get("finished_at")                       # a timestamp was stamped


# ── 4. round-robin controller cap ────────────────────────────────────────────
def test_round_robin_cap_covers_fleet_across_laps(tmp_path, monkeypatch):
    """With 12 live controllers and cap=6, each lap sweeps 6 and the offset rotates
    so lap 1 covers the first 6 and lap 2 the other 6 — the whole fleet over 2 laps,
    each lap bounded (the uncapped 12-way serial sweep is what hung the schedule)."""
    for store in ("RR_STORE", "POOL_STORE", "FLEET_STORE", "RESHAPE_STORE"):
        monkeypatch.setattr(ap, store, str(tmp_path / f"{store}.json"))

    ctrls = [f"p{i:02d}" for i in range(12)]
    live_perf = {c: {"volume_traded": float(1000 - i)} for i, c in enumerate(ctrls)}  # keeps p00..p11 order
    cfg_by = {c: {"controller_name": "pmm_mister", "trading_pair": "BTC-BRL",
                  "total_amount_quote": 500} for c in ctrls}
    cid_bot = {c: f"bot-{c}" for c in ctrls}

    async def fake_gather(_live):
        return live_perf, cfg_by, list(cid_bot.values()), {}, cid_bot

    swept = []

    async def fake_sweep(local, cid, base_cfg, w0, w1, scfg, cfg, log):
        swept.append(cid)
        return None                                       # no candidates → lap ends after RR write

    monkeypatch.setattr(ap, "gather_active", fake_gather)
    monkeypatch.setattr(ap, "_sweep_controller", fake_sweep)

    cfg = ap.Config(reshape_max_controllers=6, only_pair="")

    swept.clear()
    asyncio.run(ap._reshape_lap(cfg, live=object(), local=object(), log=lambda m: None))
    lap1 = list(swept)
    swept.clear()
    asyncio.run(ap._reshape_lap(cfg, live=object(), local=object(), log=lambda m: None))
    lap2 = list(swept)

    assert len(lap1) == 6 and len(lap2) == 6              # each lap bounded to the cap
    assert set(lap1).isdisjoint(lap2)                     # rotation, not repetition
    assert set(lap1) | set(lap2) == set(ctrls)            # whole fleet covered over 2 laps
