"""The whole simulated period on a small world, all faults on: the platform's claims, end to end."""
import duckdb
import pytest

from acm.replay import run_replay

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def replay(tmp_path_factory):
    root = tmp_path_factory.mktemp("replay") / "lake"
    rep = run_replay(root, seed=7, customers=6, step_hours=24)
    return rep, root


def alerts(rep, check):
    return [a for a in rep.alerts if a["check"] == check]


def test_revenue_matches_the_truth_to_the_cent(replay):
    rep, _ = replay
    for month in ["2026-09", "2026-10"]:
        ev = rep.evaluation[month]
        assert ev["max_error_without_inferred_stop"] <= 0.01, month
        assert ev["max_error_with_inferred_stop"] <= 3.67, month          # one heartbeat of the largest VM


def test_naive_pipeline_is_wrong_on_the_same_data(replay):
    rep, _ = replay
    for month, v in rep.evaluation["naive"].items():
        assert abs(v["naive"] - v["truth"]) > 100, month


def test_september_closes_on_time_and_late_data_restates_it_with_a_reason(replay):
    rep, _ = replay
    closed = [t for t in rep.timeline if t["closed"]]
    assert [(t["at"], t["closed"]) for t in closed] == [("2026-10-03T12:00:00Z", ["2026-09"])]
    restated = [t for t in rep.timeline if t["restated_usd"]]
    assert restated[0]["at"] == "2026-10-04T12:00:00Z"
    (a,) = alerts(rep, "restatement")
    assert "events ingested after close" in a["message"]
    sep = rep.evaluation["2026-09"]
    assert sep["closed_total"] + sep["adjustments"] == pytest.approx(sep["ours"], abs=0.01)


def test_the_outage_is_detected_while_it_happens(replay):
    rep, _ = replay
    eu_north = [a for a in alerts(rep, "freshness") if a["key"].endswith("eu-north1") and not a["after_simulation_end"]]
    assert eu_north and all("2026-09-30" <= a["opened_at"] <= "2026-10-01T12" for a in eu_north)
    assert all(a["resolved_at"] and a["resolved_at"] < "2026-10-03" for a in eu_north)


def test_the_unit_bug_pages_and_clears_after_the_correction(replay):
    rep, _ = replay
    bug = [a for a in alerts(rep, "token_plausibility")]
    assert bug and all(a["key"].startswith("us-central1/") for a in bug)
    assert all("2026-10-06" <= a["opened_at"] < "2026-10-08" for a in bug)
    assert all(a["resolved_at"] and a["resolved_at"] <= "2026-10-09T01:00:00Z" for a in bug)


def test_reconciliation_explains_every_invoice(replay):
    _, root = replay
    con = duckdb.connect(str(root / "warehouse.duckdb"), read_only=True)
    worst, overlap, rounding = con.execute("""SELECT max(abs(unexplained)), sum(overlapping_commitment), sum(hourly_rounding)
                                              FROM recon_waterfall WHERE month = '2026-09'""").fetchone()
    assert worst <= 1.0
    assert overlap > 0 or rounding > 0


def test_no_error_alert_is_left_open_except_what_is_still_true(replay):
    rep, _ = replay
    still_open = {a["check"] for a in rep.alerts if not a["resolved_at"] and not a["after_simulation_end"]
                  and a["severity"] == "error"}
    assert still_open == set()
