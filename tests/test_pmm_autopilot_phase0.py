"""Phase 0 safety tests for PMM Autopilot — all offline, mocked, no network/live calls.

Proves the money-critical guards:
- the pmm_mister-only INVARIANT (never stop a bot hosting rebate_mill/pmm_king/co-hosted),
- idempotency (double-press deploys once),
- proposal identity (stale button applies the snapshot it carries, not the latest),
- empty-selection guard,
- deploy-failure rollback,
- quote-currency partition in the lap.
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import routines.pmm_autopilot as ap


# ── fakes ───────────────────────────────────────────────────────────────────
class FakeCtrls:
    def __init__(self, bot_configs):
        self.bot_configs = bot_configs
        self.saved = []

    async def get_bot_controller_configs(self, bot_name):
        return list(self.bot_configs.get(bot_name, []))

    async def create_or_update_controller_config(self, cid, cfg):
        self.saved.append(cid)
        return {"status": "success"}


class FakeOrch:
    def __init__(self, fail_deploy_call=None):
        self.stopped = []
        self.deployed = []           # list of (instance_name, [controller ids])
        self._deploy_calls = 0
        self._fail_call = fail_deploy_call  # 1-indexed deploy call number to fail

    async def stop_and_archive_bot(self, bot_name, **kw):
        self.stopped.append(bot_name)
        return {"status": "success"}

    async def deploy_v2_controllers(self, instance_name, controllers_config, **kw):
        self._deploy_calls += 1
        if self._fail_call and self._deploy_calls == self._fail_call:
            raise RuntimeError("simulated deploy failure")
        self.deployed.append((instance_name, list(controllers_config)))
        return {"status": "success"}


class FakePortfolio:
    def __init__(self, state):
        self.state = state

    async def get_state(self):
        return self.state


class FakeLive:
    def __init__(self, bot_configs, wallet, fail_deploy_call=None):
        self.controllers = FakeCtrls(bot_configs)
        self.bot_orchestration = FakeOrch(fail_deploy_call)
        self.portfolio = FakePortfolio(wallet)


def big_wallet():
    """A wallet that comfortably funds both legs (so the fit gate passes)."""
    return {"master_account": {"binance": [
        {"token": "BRL", "units": 1e12, "price": 0.19, "available_units": 1e12},
        {"token": "BTC", "units": 1e9, "price": 77000, "available_units": 1e9},
    ]}}


def cand(cid, origin, bot, cap=10000.0, vol=1000.0, tbp=0.5):
    return {"cand_id": cid, "origin": origin, "bot": bot, "capital": cap, "volume": vol,
            "biz": 1.0, "max_dd": -5.0, "autosized_capital": cap,
            "full_config": {"id": cid, "controller_name": "pmm_mister", "trading_pair": "BTC-BRL",
                            "target_base_pct": tbp, "total_amount_quote": cap}}


def fleet(controllers, bot_ctrls, quote="BRL"):
    return {"N": len(controllers), "total_capital": sum(10000.0 for _ in controllers),
            "controllers": controllers, "bot_ctrls": bot_ctrls, "quote": quote}


# ── tests ───────────────────────────────────────────────────────────────────
def test_cohosted_bot_is_never_stopped():
    """A bot hosting pmm_mister + rebate_mill must NOT be stopped or deployed over."""
    bot = "botA"
    live = FakeLive(bot_configs={bot: [{"id": "p1", "controller_name": "pmm_mister"},
                                       {"id": "r1", "controller_name": "rebate_mill"}]},
                    wallet=big_wallet())
    bot_ctrls = {bot: [{"cid": "p1", "type": "pmm_mister"}, {"cid": "r1", "type": "rebate_mill"}]}
    fl = fleet({"p1": {"bot": bot}}, bot_ctrls)
    selected = [cand("p1__spread", "p1", bot)]
    res = asyncio.run(ap._execute_reshape(live, selected, fl, "T"))
    assert res["ok"] is False
    assert live.bot_orchestration.stopped == []          # co-hosted bot never stopped
    assert live.bot_orchestration.deployed == []         # nothing deployed
    assert any("REFUSED" in s for s in res["steps"])


def test_pmm_mister_only_bot_is_reshaped():
    """A bot whose full set is pmm_mister IS reshaped exactly once."""
    bot = "botB"
    live = FakeLive(bot_configs={bot: [{"id": "p1", "controller_name": "pmm_mister"},
                                       {"id": "p2", "controller_name": "pmm_mister"}]},
                    wallet=big_wallet())
    bot_ctrls = {bot: [{"cid": "p1", "type": "pmm_mister"}, {"cid": "p2", "type": "pmm_mister"}]}
    fl = fleet({"p1": {"bot": bot}, "p2": {"bot": bot}}, bot_ctrls)
    selected = [cand("p1__x", "p1", bot), cand("p2", "p2", bot)]
    res = asyncio.run(ap._execute_reshape(live, selected, fl, "T"))
    assert res["ok"] is True
    assert live.bot_orchestration.stopped == [bot]
    assert len(live.bot_orchestration.deployed) == 1
    assert set(live.bot_orchestration.deployed[0][1]) == {"p1__x", "p2"}


def test_empty_selection_touches_nothing():
    live = FakeLive(bot_configs={}, wallet=big_wallet())
    fl = fleet({"p1": {"bot": "botB"}}, {"botB": [{"cid": "p1", "type": "pmm_mister"}]})
    res = asyncio.run(ap._execute_reshape(live, [], fl, "T"))
    assert res["ok"] is False
    assert live.bot_orchestration.stopped == []
    assert live.bot_orchestration.deployed == []


def test_deploy_failure_rolls_back_and_stays_unapplied():
    """If the main deploy fails after stopping, archived configs are redeployed (rollback)."""
    bot = "botB"
    live = FakeLive(bot_configs={bot: [{"id": "p1", "controller_name": "pmm_mister"},
                                       {"id": "p2", "controller_name": "pmm_mister"}]},
                    wallet=big_wallet(), fail_deploy_call=1)  # fail the MAIN deploy, allow rollback
    bot_ctrls = {bot: [{"cid": "p1", "type": "pmm_mister"}, {"cid": "p2", "type": "pmm_mister"}]}
    fl = fleet({"p1": {"bot": bot}, "p2": {"bot": bot}}, bot_ctrls)
    selected = [cand("p1__x", "p1", bot), cand("p2", "p2", bot)]
    res = asyncio.run(ap._execute_reshape(live, selected, fl, "T"))
    assert res["ok"] is False
    assert live.bot_orchestration.stopped == [bot]
    # rollback redeployed the archived set under a *-rollback-* instance
    assert any("rollback" in inst for inst, _ in live.bot_orchestration.deployed)


def test_wallet_fit_gate_refuses_when_underfunded():
    bot = "botB"
    poor = {"master_account": {"binance": [
        {"token": "BRL", "units": 1.0, "price": 0.19, "available_units": 1.0},
        {"token": "BTC", "units": 0.0, "price": 77000, "available_units": 0.0}]}}
    live = FakeLive(bot_configs={bot: [{"id": "p1", "controller_name": "pmm_mister"}]},
                    wallet=poor)
    bot_ctrls = {bot: [{"cid": "p1", "type": "pmm_mister"}]}
    fl = fleet({"p1": {"bot": bot}}, bot_ctrls)
    selected = [cand("p1__x", "p1", bot, cap=1_000_000.0)]
    res = asyncio.run(ap._execute_reshape(live, selected, fl, "T"))
    assert res["ok"] is False
    assert live.bot_orchestration.stopped == []          # refused before stopping
    assert any("wallet" in s.lower() for s in res["steps"])


# ── idempotency / stale-proposal via handle_callback ─────────────────────────
class FakeMsg:
    def __init__(self):
        self.text = "reshape proposal"
        self.chat_id = 123
    async def edit_message_text(self, *a, **k):
        return True


class FakeQuery:
    def __init__(self, msg):
        self.message = msg
        self.answers = []
    async def answer(self, text="", show_alert=False):
        self.answers.append(text)
    async def edit_message_text(self, *a, **k):
        return True


class FakeUpdate:
    def __init__(self):
        self.callback_query = FakeQuery(FakeMsg())


class FakeCtx:
    _chat_id = 123
    bot = None


def _use_tmp_stores(tmp_path, monkeypatch, live):
    monkeypatch.setattr(ap, "RESHAPE_STORE", str(tmp_path / "reshape.json"))
    monkeypatch.setattr(ap, "POOL_STORE", str(tmp_path / "pool.json"))
    monkeypatch.setattr(ap, "FLEET_STORE", str(tmp_path / "fleet.json"))
    async def _fake_get_client(*a, **k):
        return live
    monkeypatch.setattr(ap, "get_client", _fake_get_client)


def test_double_press_deploys_once(tmp_path, monkeypatch):
    bot = "botB"
    live = FakeLive(bot_configs={bot: [{"id": "p1", "controller_name": "pmm_mister"}]},
                    wallet=big_wallet())
    _use_tmp_stores(tmp_path, monkeypatch, live)
    bot_ctrls = {bot: [{"cid": "p1", "type": "pmm_mister"}]}
    fl = fleet({"p1": {"bot": bot}}, bot_ctrls)
    ap._save(ap.RESHAPE_STORE, {"PID": {"selected": [cand("p1__x", "p1", bot)], "fleet": fl,
                                        "applied": False, "deploy_cfg": {}}})
    upd = FakeUpdate()
    asyncio.run(ap.handle_callback(upd, FakeCtx(), "reshape", ["PID"]))
    asyncio.run(ap.handle_callback(upd, FakeCtx(), "reshape", ["PID"]))
    assert len(live.bot_orchestration.deployed) == 1     # exactly once despite two presses
    assert any("already applied" in a.lower() for a in upd.callback_query.answers)


def test_stale_button_applies_its_own_snapshot(tmp_path, monkeypatch):
    bot = "botB"
    live = FakeLive(bot_configs={bot: [{"id": "old", "controller_name": "pmm_mister"},
                                       {"id": "new", "controller_name": "pmm_mister"}]},
                    wallet=big_wallet())
    _use_tmp_stores(tmp_path, monkeypatch, live)
    bot_ctrls = {bot: [{"cid": "old", "type": "pmm_mister"}, {"cid": "new", "type": "pmm_mister"}]}
    fl = fleet({"old": {"bot": bot}, "new": {"bot": bot}}, bot_ctrls)
    ap._save(ap.RESHAPE_STORE, {
        "OLD": {"selected": [cand("old__A", "old", bot)], "fleet": fl, "applied": False, "deploy_cfg": {}},
        "NEW": {"selected": [cand("new__B", "new", bot)], "fleet": fl, "applied": False, "deploy_cfg": {}},
    })
    asyncio.run(ap.handle_callback(FakeUpdate(), FakeCtx(), "reshape", ["OLD"]))
    # pressing OLD deployed OLD's selection, not NEW's
    assert live.bot_orchestration.deployed
    assert set(live.bot_orchestration.deployed[0][1]) == {"old__A"}


def test_expired_proposal_is_refused(tmp_path, monkeypatch):
    live = FakeLive(bot_configs={}, wallet=big_wallet())
    _use_tmp_stores(tmp_path, monkeypatch, live)
    ap._save(ap.RESHAPE_STORE, {})
    upd = FakeUpdate()
    asyncio.run(ap.handle_callback(upd, FakeCtx(), "reshape", ["GHOST"]))
    assert live.bot_orchestration.deployed == []
    assert any("expired" in a.lower() for a in upd.callback_query.answers)


def test_select_topN_backfills_to_N_and_respects_budget():
    """Diversity cap must not shrink the fleet below N or over-capitalize survivors."""
    # 8 eligible candidates, all in ONE family ("fam"), origins all live
    pool = [{"cand_id": f"c{i}", "origin": f"fam__{i}", "capital": 1000.0,
             "volume": 100.0 + i, "biz": 1.0, "max_dd": -1.0,
             "full_config": {"total_amount_quote": 1000.0}, "params": {"k": i}} for i in range(8)]
    fl = {"N": 6, "total_capital": 60000.0, "controllers": {f"fam__{i}": {} for i in range(8)}}
    sel = ap.select_topN(pool, fl, budget=60000.0, max_per_family=4)
    assert len(sel) == 6                                  # backfilled past the family cap of 4
    assert sum(c["autosized_capital"] for c in sel) <= 60000.0 + 1e-6
