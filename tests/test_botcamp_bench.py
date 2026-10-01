"""Unit tests for the botcamp competition agent core (bench + fleet-manager rules)."""
from routines.botcamp_bench import (
    trial_pnl, robust_score, n_positive, k_windows, param_sig,
    make_bench_entry, bench_upsert, bench_eligible, select_substitute, needs_substitution,
)


def test_trial_pnl_is_realized_plus_unrealized():
    assert trial_pnl({"realized": 10, "unrealized": -3}) == 7


def test_robust_score_is_median_so_a_fluke_cannot_inflate():
    # one +100 window must NOT lift a config whose other windows are negative
    assert robust_score([100, -5, -4, -6, -3]) == -4
    assert n_positive([1, -1, 2, 0, 3]) == 3


def test_k_windows_are_disjoint_daily_and_newest_first():
    w = k_windows(now=1_000_000, n=7, window_h=24)
    assert len(w) == 7
    assert all(w[i][0] == w[i + 1][1] for i in range(6))  # disjoint, contiguous
    assert w[0][1] < 1_000_000                            # newest ends before now
    assert w[0][1] - w[0][0] == 24 * 3600                 # 24h span


def test_param_sig_dedups_identical_and_splits_on_change():
    c1 = {"buy_spreads": [0.0005], "take_profit": 0.0001, "executor_refresh_time": 300}
    assert param_sig(c1) == param_sig(dict(c1))
    assert param_sig(c1) != param_sig(dict(c1, take_profit=0.0002))


def test_bench_upsert_sorts_desc_and_dedups_freshest_wins():
    c1 = {"take_profit": 0.0001}
    c3 = {"take_profit": 0.0002}
    b = []
    b = bench_upsert(b, make_bench_entry(c1, [5, 6, 7, 4, 5, 6, 5], ts=1))    # median 5
    b = bench_upsert(b, make_bench_entry(c3, [9, 10, 8, 9, 11, 9, 10], ts=1))  # median 9
    assert [e["median_pnl"] for e in b] == sorted([e["median_pnl"] for e in b], reverse=True)
    assert len(b) == 2
    b = bench_upsert(b, make_bench_entry(c1, [20] * 7, ts=2))  # re-score c1 much higher
    assert len(b) == 2 and b[0]["median_pnl"] == 20            # dedup, freshest wins, re-sorted


def test_eligibility_gate_and_substitute_excludes_failed():
    c_good = {"take_profit": 0.0001}
    c_weak = {"take_profit": 0.0009}
    b = []
    b = bench_upsert(b, make_bench_entry(c_good, [20] * 7, ts=1))            # 7 positive
    b = bench_upsert(b, make_bench_entry(c_weak, [1, 1, -1, -1, -1, -1, -1], ts=1))  # 2 positive
    elig = bench_eligible(b, min_positive=5)
    assert all(e["n_positive"] >= 5 for e in elig)
    top = b[0]["sig"]
    sub = select_substitute(b, min_positive=5, exclude_sigs={top})
    assert sub is None or sub["sig"] != top  # never re-deploy the config that just died
    # with only one eligible and it excluded -> nothing to sub in
    assert select_substitute(b, min_positive=5, exclude_sigs={top}) is None


def test_substitution_triggers():
    s, r = needs_substitution(unrealized_pnl=-25, last_trade_ts=1_000_000, now=1_000_100)
    assert s and "unrealized" in r
    s, r = needs_substitution(unrealized_pnl=0, last_trade_ts=1_000_000, now=1_000_000 + 5 * 3600)
    assert s and "no trade" in r
    s, _ = needs_substitution(unrealized_pnl=-5, last_trade_ts=1_000_000, now=1_000_000 + 3600)
    assert not s
