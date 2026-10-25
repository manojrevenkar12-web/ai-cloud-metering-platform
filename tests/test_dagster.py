"""The Dagster definitions load and materialise the same pipeline on a small world."""
import dagster
import pytest

from acm import dagster_defs as D
from acm.replay import prepare


def test_definitions_load():
    assert D.defs.resolve_job_def("meter").name == "meter"
    assert {s.name for s in D.defs.schedules} == {"every_three_hours", "period_close"}
    assert D._close_cron() == "5 12 3 * *"
    assert set(D.CHECK_ASSET) >= {"freshness", "reconciliation", "mart_contracts"}


@pytest.mark.slow
def test_materialize_at_close_and_after(tmp_path):
    root = tmp_path / "lake"
    prepare(root, customers=3)
    results = {}
    for now in ["2026-10-03T12:00:00Z", "2026-10-04T12:00:00Z"]:
        r = dagster.materialize(D.ASSETS + [D.data_quality], resources={"warehouse": D.Warehouse(root=str(root), now=now)})
        assert r.success
        meta = {e.asset_key.to_user_string(): {k: v.value for k, v in e.materialization.metadata.items()}
                for e in r.get_asset_materialization_events()}
        checks = {e.check_name: e.passed for e in r.get_asset_check_evaluations()}
        results[now] = (meta, checks)
    at_close, after = results["2026-10-03T12:00:00Z"], results["2026-10-04T12:00:00Z"]
    assert at_close[0]["mart_finance_revenue"]["closed_now"] == "2026-09"
    assert at_close[1]["reconciliation"] and at_close[1]["mart_contracts"]
    assert after[0]["mart_finance_revenue"]["restated_usd"] > 0 and not after[1]["restatement"]
