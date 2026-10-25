"""Period close, restatement and credits."""
import pytest

from acm import periods, pipeline
from acm.pricing import ts
from tests.conftest import run_at


def mart(lake):
    con = lake.connect()
    try:
        cols = [d[0] for d in con.execute("DESCRIBE mart_finance_revenue").fetchall()]
        return {(r[0], r[1]): dict(zip(cols, r)) for r in con.execute("SELECT * FROM mart_finance_revenue").fetchall()}
    finally:
        con.close()


def sept_usage(landing, customer="c002"):
    landing.vm("vm1", customer, "2026-09-10T00:00:00Z", "2026-09-10T02:00:00Z")   # $47.20


def test_close_time_comes_from_config():
    assert pipeline.close_at("2026-09") == ts("2026-10-03T12:00:00Z")
    assert pipeline.close_at("2026-12") == ts("2027-01-03T12:00:00Z")


def test_month_closes_at_the_configured_time_once(landing):
    sept_usage(landing)
    lake = landing.write()
    assert run_at(lake, "2026-10-03T11:00:00Z").closed == []
    assert run_at(lake, "2026-10-03T12:00:00Z").closed == ["2026-09"]
    assert run_at(lake, "2026-10-03T18:00:00Z").closed == []
    m = mart(lake)[("c002", "2026-09")]
    assert m["status"] == "closed" and float(m["closed_total"]) == pytest.approx(47.20)


def test_late_usage_restates_a_closed_month_without_changing_what_was_reported(landing):
    sept_usage(landing)
    landing.storage("c002", "2026-09-30T12:00:00Z", 730_000 / 14.7, )                 # arrives on time
    lake = landing.write()
    run_at(lake, "2026-10-03T12:00:00Z")
    closed = mart(lake)[("c002", "2026-09")]
    landing.vm("vm2", "c002", "2026-09-30T20:00:00Z", "2026-09-30T21:00:00Z", arrives="2026-10-04T09:00:00Z")
    landing.write()
    res = run_at(lake, "2026-10-04T12:00:00Z")
    assert res.restated and res.restated[0]["reason"].startswith("events ingested after close: 3 vm_lifecycle")
    after = mart(lake)[("c002", "2026-09")]
    assert after["closed_total"] == closed["closed_total"]
    assert float(after["adjustments"]) == pytest.approx(8 * 2.95)
    assert float(after["total"]) == pytest.approx(float(closed["total"]) + 8 * 2.95)
    con = lake.connect()
    assert periods.revenue_as_of(con, "2026-09", ts("2026-10-04T00:00:00Z")) == pytest.approx(float(closed["total"]))
    assert periods.revenue_as_of(con, "2026-09", ts("2026-10-04T12:00:00Z")) == pytest.approx(float(after["total"]))
    kinds = con.execute("SELECT entry_type, count(*) FROM revenue_ledger GROUP BY 1 ORDER BY 1").fetchall()
    assert kinds == [("adjustment", 1), ("close", 5)]


def test_restatement_is_not_repeated(landing):
    sept_usage(landing)
    lake = landing.write()
    run_at(lake, "2026-10-03T12:00:00Z")
    landing.vm("vm2", "c002", "2026-09-30T20:00:00Z", "2026-09-30T21:00:00Z", arrives="2026-10-04T09:00:00Z")
    landing.write()
    assert run_at(lake, "2026-10-04T12:00:00Z").restated
    assert not run_at(lake, "2026-10-05T12:00:00Z").restated


def test_credits_pay_on_demand_oldest_month_first_never_commitments(landing):
    sept_usage(landing)                                                                    # Sep: 47.20
    landing.vm("vm3", "c002", "2026-10-02T00:00:00Z", "2026-10-02T01:00:00Z")             # Oct: 8 x 2.65 = 21.20
    lake = landing.write(credits=[{"id": "g1", "customer": "c002", "usd": 60}],
                         commitments=[{"id": "k1", "customer": "c002", "gpus": 1, "rate": 1.0, "region": "us-central1",
                                       "starts": "2026-09-01T00:00:00Z", "ends": "2026-11-01T00:00:00Z"}])
    run_at(lake, "2026-10-02T06:00:00Z", months={"2026-09", "2026-10"})
    m = mart(lake)
    assert float(m[("c002", "2026-09")]["credits_applied"]) == pytest.approx(47.20)
    assert float(m[("c002", "2026-10")]["credits_applied"]) == pytest.approx(12.80)
    assert float(m[("c002", "2026-09")]["total"]) == pytest.approx(720 * 1.0)             # the commitment is still owed


def test_finance_mart_meets_its_contract(landing):
    from acm.contracts import check_mart
    sept_usage(landing)
    lake = landing.write()
    run_at(lake, "2026-10-03T12:00:00Z")
    con = lake.connect()
    assert check_mart(con, "finance_revenue") == []
    assert check_mart(con, "product_capacity") == []
