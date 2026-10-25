"""Metering and rating on cases small enough to work out by hand (docs/BILLING_RULES.md)."""
import pytest

from tests.conftest import run_at


def charges(lake, month="2026-09"):
    con = lake.connect()
    try:
        return {c: dict(zip(["gpu_on_demand", "commitment", "inference", "storage"], [round(x, 2) for x in v]))
                for c, *v in con.execute("""SELECT customer_id, gpu_on_demand, commitment, inference, storage
                                            FROM gold_charges WHERE month = ?""", [month]).fetchall()}
    finally:
        con.close()


def sessions(lake):
    con = lake.connect()
    try:
        return {v: (src, until) for v, src, until in
                con.execute("SELECT vm_id, end_source, billed_until FROM silver_vm_sessions").fetchall()}
    finally:
        con.close()


NOW = "2026-09-12T00:00:00Z"


def test_per_second_billing_two_hours_of_eight_h100(landing):
    landing.vm("vm1", "c002", "2026-09-10T00:00:00Z", "2026-09-10T02:00:00Z")
    landing.beats("vm1", "2026-09-10T00:00:00Z", 24)
    lake = landing.write()
    run_at(lake, NOW)
    assert charges(lake)["c002"]["gpu_on_demand"] == pytest.approx(2 * 8 * 2.95)


def test_duplicates_and_replayed_spools_count_once(landing):
    landing.vm("vm1", "c002", "2026-09-10T00:00:00Z", "2026-09-10T02:00:00Z")
    landing.beats("vm1", "2026-09-10T00:00:00Z", 24)
    landing.beats("vm1", "2026-09-10T00:00:00Z", 24, arrives="2026-09-10T05:00:00Z", event_prefix="replay")
    lake = landing.write()
    run_at(lake, NOW)
    con = lake.connect()
    assert con.execute("SELECT count(*), max(copies) FROM silver_heartbeats").fetchone() == (24, 2)
    assert charges(lake)["c002"]["gpu_on_demand"] == pytest.approx(47.20)


def test_skewed_host_clock_moves_nothing(landing):
    landing.vm("vm1", "c002", "2026-09-10T00:00:00Z", "2026-09-10T02:00:00Z")
    landing.beats("vm1", "2026-09-10T00:00:00Z", 24, skew_s=240)
    lake = landing.write()
    res = run_at(lake, NOW)
    assert charges(lake)["c002"]["gpu_on_demand"] == pytest.approx(47.20)
    assert "clock_skew:hosts" in res.alerts_opened


def test_lost_stop_event_bills_to_one_heartbeat_after_the_last(landing):
    landing.vm("vm1", "c002", "2026-09-10T00:00:00Z")                  # stop event never sent
    landing.beats("vm1", "2026-09-10T00:00:00Z", 24)                   # last beat 01:55
    lake = landing.write()
    run_at(lake, NOW)
    src, until = sessions(lake)["vm1"]
    assert src == "inferred" and until == 1789005600                    # 02:00
    assert charges(lake)["c002"]["gpu_on_demand"] == pytest.approx(47.20)


def test_running_vm_is_metered_up_to_now_and_not_inferred_while_beating(landing):
    landing.vm("vm1", "c002", "2026-09-11T22:00:00Z")
    landing.beats("vm1", "2026-09-11T22:00:00Z", 24)
    lake = landing.write()
    run_at(lake, NOW)
    assert sessions(lake)["vm1"][0] == "running"
    assert charges(lake)["c002"]["gpu_on_demand"] == pytest.approx(2 * 8 * 2.95)


def test_sixty_second_minimum(landing):
    landing.vm("vm1", "c002", "2026-09-10T00:00:00Z", "2026-09-10T00:00:20Z")
    lake = landing.write()
    run_at(lake, NOW)
    assert charges(lake)["c002"]["gpu_on_demand"] == pytest.approx(8 * 2.95 / 60, abs=0.005)


def test_committed_gpus_absorb_usage_and_overage_is_on_demand(landing):
    landing.vm("vm1", "c001", "2026-09-10T00:00:00Z", "2026-09-10T01:00:00Z", gpus=8)
    landing.vm("vm2", "c001", "2026-09-10T00:00:00Z", "2026-09-10T01:00:00Z", gpus=4)
    lake = landing.write(commitments=[{"id": "k1", "customer": "c001", "gpus": 8, "rate": 2.0,
                                       "starts": "2026-09-01T00:00:00Z", "ends": "2026-10-01T00:00:00Z"}])
    run_at(lake, NOW)
    c = charges(lake)["c001"]
    assert c["commitment"] == pytest.approx(8 * 11 * 24 * 2.0)          # accrues to now while the month is open
    assert c["gpu_on_demand"] == pytest.approx(4 * 2.95)
    run_at(lake, "2026-10-01T02:00:00Z", months={"2026-09"})
    assert charges(lake)["c001"]["commitment"] == pytest.approx(8 * 720 * 2.0)


def test_commitment_in_another_region_does_not_absorb(landing):
    landing.vm("vm1", "c001", "2026-09-10T00:00:00Z", "2026-09-10T01:00:00Z", region="eu-west1")
    lake = landing.write(commitments=[{"id": "k1", "customer": "c001", "gpus": 8, "rate": 2.0,
                                       "starts": "2026-09-01T00:00:00Z", "ends": "2026-10-01T00:00:00Z"}])
    run_at(lake, NOW)
    assert charges(lake)["c001"]["gpu_on_demand"] == pytest.approx(8 * 3.10)


def test_renewal_replaces_the_commitment_it_renews(landing):
    landing.storage("c001", "2026-09-10T00:00:00Z", 1)
    lake = landing.write(commitments=[
        {"id": "k1", "customer": "c001", "gpus": 8, "rate": 2.0, "starts": "2026-08-01T00:00:00Z", "ends": "2026-10-01T00:00:00Z"},
        {"id": "k2", "customer": "c001", "gpus": 16, "rate": 1.9, "starts": "2026-09-16T00:00:00Z",
         "ends": "2027-03-16T00:00:00Z", "replaces": "k1"}])
    run_at(lake, "2026-10-02T00:00:00Z", months={"2026-09"})
    assert charges(lake)["c001"]["commitment"] == pytest.approx(8 * 15 * 24 * 2.0 + 16 * 15 * 24 * 1.9)


def test_price_change_at_month_boundary(landing):
    landing.vm("vm1", "c002", "2026-09-30T23:00:00Z", "2026-10-01T01:00:00Z")
    lake = landing.write()
    run_at(lake, "2026-10-01T03:00:00Z", months={"2026-09", "2026-10"})
    assert charges(lake, "2026-09")["c002"]["gpu_on_demand"] == pytest.approx(8 * 2.95)
    assert charges(lake, "2026-10")["c002"]["gpu_on_demand"] == pytest.approx(8 * 2.65)


def test_highest_revision_wins_and_retries_count_once(landing):
    w = "2026-09-10T00:00:00Z"
    landing.inference("c002", w, 2_000, 500, event_id="win1", revision=1)                 # thousands: the unit bug
    landing.inference("c002", w, 2_000_000, 500_000, event_id="win1", revision=2, arrives="2026-09-11T00:00:00Z")
    landing.inference("c002", w, 2_000_000, 500_000, event_id="win1", revision=2, arrives="2026-09-11T01:00:00Z")
    lake = landing.write()
    run_at(lake, NOW)
    assert charges(lake)["c002"]["inference"] == pytest.approx(2 * 0.13 + 0.5 * 0.40)


def test_storage_is_gb_hours(landing):
    for h in range(24):
        landing.storage("c002", f"2026-09-10T{h:02d}:00:00Z", 1000)
    lake = landing.write()
    run_at(lake, NOW)
    assert charges(lake)["c002"]["storage"] == pytest.approx(1000 * 0.0147 / 730 * 24, abs=0.005)


def test_unpriced_usage_is_flagged_not_dropped(landing):
    landing.vm("vm1", "c002", "2026-09-10T00:00:00Z", "2026-09-10T01:00:00Z", gpu="b200")   # before B200 launch
    lake = landing.write()
    res = run_at(lake, NOW)
    con = lake.connect()
    assert con.execute("SELECT unpriced_seconds FROM gold_charges WHERE customer_id = 'c002'").fetchone()[0] == 8 * 3600
    assert "unpriced_usage:gpu" in res.alerts_opened


def test_usage_from_a_customer_the_crm_does_not_know_pages(landing):
    landing.vm("vm1", "c099", "2026-09-10T00:00:00Z", "2026-09-10T01:00:00Z")
    lake = landing.write()
    assert "unknown_customers:all" in run_at(lake, NOW).alerts_opened
