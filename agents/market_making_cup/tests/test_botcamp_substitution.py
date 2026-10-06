"""Integration test for the botcamp agent's substitution path — the heart of the agent.

Proves, with NO exchange and NO money, that the full chain fires correctly:
  stall detection (check_triggers) → pick the best eligible bench config (select_substitute)
  → apply it (apply_substitution) with the fail-closed position check and the pmm_mister invariant.

Run: uv run pytest tests/test_botcamp_substitution.py -q
"""
import asyncio
import time

from routines.botcamp_mm_agent import (
    Config, check_triggers, apply_substitution, base_config,
)
from routines.botcamp_bench import make_bench_entry, bench_upsert, select_substitute


# --------------------------------------------------------------------------- #
# a minimal fake Hummingbot API client — records writes, no network           #
# --------------------------------------------------------------------------- #
class _Controllers:
    def __init__(self, live_type="pmm_mister"):
        self.live_type = live_type
        self.saved = []   # (cid, payload)
        self.applied = []  # (bot, cid, payload)

    async def get_bot_controller_configs(self, bot):
        # the live target the invariant guard re-reads before writing
        return [{"id": "ctrl-1", "controller_name": self.live_type, "trading_pair": "ATH-USDT"}]

    async def create_or_update_controller_config(self, cid, payload):
        self.saved.append((cid, payload))

    async def update_bot_controller_config(self, bot, cid, payload):
        self.applied.append((bot, cid, payload))


class _Orch:
    def __init__(self, volume_traded, position):
        self._v, self._p = volume_traded, position

    async def get_active_bots_status(self):
        return {"data": {"bot-A": {"performance": {"ctrl-1": {
            "volume_traded": self._v, "positions": self._p}}}}}


class FakeLive:
    def __init__(self, live_type="pmm_mister", volume_traded=0.0, position=0.0):
        self.controllers = _Controllers(live_type)
        self.bot_orchestration = _Orch(volume_traded, position)


def _cfg(**kw):
    return Config(trading_pair="ATH-USDT", connector_name="gate_io", dry_run=False,
                  sub_no_trade_h=2.0, max_subs_per_hour=3, **kw)


def _populated_bench(cfg):
    """A bench with one clearly-eligible winner (positive in all 7 windows)."""
    bench = []
    winner = dict(base_config(cfg)); winner["take_profit"] = 0.0015  # distinct from base
    bench = bench_upsert(bench, make_bench_entry(winner, [5.0] * 7))     # +7/7 → eligible
    loser = dict(base_config(cfg)); loser["take_profit"] = 0.0009
    bench = bench_upsert(bench, make_bench_entry(loser, [-1.0] * 7))     # 0/7 → not eligible
    return bench


# --------------------------------------------------------------------------- #
# 1) stall detection: no new volume for >= sub_no_trade_h fires the trigger    #
# --------------------------------------------------------------------------- #
def test_check_triggers_fires_after_no_trade_window():
    cfg = _cfg()
    state = {"vol_hist": {}}
    t0 = time.time()
    fleet = {"ctrl-1": {"unrealized": 0.0, "volume": 100.0, "bot": "bot-A"}}
    # first sighting — clock starts, no trigger
    assert check_triggers(fleet, state, cfg, t0) == []
    # 2h later, volume unchanged → trigger
    out = check_triggers(fleet, state, cfg, t0 + 2 * 3600 + 1)
    assert len(out) == 1 and out[0][0] == "ctrl-1"


def test_check_triggers_resets_when_volume_advances():
    cfg = _cfg()
    state = {"vol_hist": {}}
    t0 = time.time()
    check_triggers({"ctrl-1": {"unrealized": 0.0, "volume": 100.0, "bot": "bot-A"}}, state, cfg, t0)
    # volume advanced within the window → clock resets, no trigger at 2h
    out = check_triggers({"ctrl-1": {"unrealized": 0.0, "volume": 140.0, "bot": "bot-A"}},
                         state, cfg, t0 + 2 * 3600 + 1)
    assert out == []


# --------------------------------------------------------------------------- #
# 2) selection: picks the eligible winner, never the loser                     #
# --------------------------------------------------------------------------- #
def test_select_substitute_picks_eligible_winner():
    cfg = _cfg()
    pick = select_substitute(_populated_bench(cfg), cfg.min_positive_windows, exclude_sigs=set())
    assert pick is not None
    assert pick["config"]["take_profit"] == 0.0015  # the +7/7 winner


# --------------------------------------------------------------------------- #
# 3) application: flat controller → the bench config is written to the slot    #
# --------------------------------------------------------------------------- #
def test_apply_substitution_writes_bench_config_when_flat():
    cfg = _cfg()
    live = FakeLive(live_type="pmm_mister", volume_traded=0.0, position=0.0)  # flat → swap allowed
    pick = select_substitute(_populated_bench(cfg), cfg.min_positive_windows, exclude_sigs=set())
    fleet = {"ctrl-1": {"unrealized": 0.0, "volume": 0.0, "bot": "bot-A"}}
    ok = asyncio.run(apply_substitution(live, cfg, "ctrl-1", "bot-A", pick["config"], fleet, print))
    assert ok is True
    assert live.controllers.saved, "expected the new config to be written"
    cid, payload = live.controllers.saved[-1]
    assert cid == "ctrl-1" and payload["id"] == "ctrl-1"          # slot id preserved
    assert payload["take_profit"] == 0.0015                        # the bench winner's params
    assert payload["controller_name"] == "pmm_mister"


# --------------------------------------------------------------------------- #
# 4) fail-closed: a controller HOLDING inventory is NOT swapped (no orphan)    #
# --------------------------------------------------------------------------- #
def test_apply_substitution_refused_when_holding_inventory():
    cfg = _cfg()
    live = FakeLive(live_type="pmm_mister", volume_traded=500.0, position=12.5)  # holds base
    pick = select_substitute(_populated_bench(cfg), cfg.min_positive_windows, exclude_sigs=set())
    fleet = {"ctrl-1": {"unrealized": -5.0, "volume": 500.0, "bot": "bot-A"}}
    ok = asyncio.run(apply_substitution(live, cfg, "ctrl-1", "bot-A", pick["config"], fleet, print))
    assert ok is False
    assert not live.controllers.saved, "must NOT write/orphan a controller that holds inventory"


# --------------------------------------------------------------------------- #
# 5) HARD INVARIANT: never touch a NON-pmm_mister controller                   #
# --------------------------------------------------------------------------- #
def test_invariant_refuses_non_pmm_mister_target():
    cfg = _cfg()
    live = FakeLive(live_type="rebate_mill", volume_traded=0.0, position=0.0)  # live target NOT ours
    pick = select_substitute(_populated_bench(cfg), cfg.min_positive_windows, exclude_sigs=set())
    fleet = {"ctrl-1": {"unrealized": 0.0, "volume": 0.0, "bot": "bot-A"}}
    ok = asyncio.run(apply_substitution(live, cfg, "ctrl-1", "bot-A", pick["config"], fleet, print))
    assert ok is False
    assert not live.controllers.saved, "must refuse to write a non-pmm_mister controller"


def test_invariant_refuses_non_pmm_mister_replacement():
    cfg = _cfg()
    live = FakeLive(live_type="pmm_mister", volume_traded=0.0, position=0.0)
    bad = dict(base_config(cfg)); bad["controller_name"] = "something_else"
    fleet = {"ctrl-1": {"unrealized": 0.0, "volume": 0.0, "bot": "bot-A"}}
    ok = asyncio.run(apply_substitution(live, cfg, "ctrl-1", "bot-A", bad, fleet, print))
    assert ok is False
    assert not live.controllers.saved
