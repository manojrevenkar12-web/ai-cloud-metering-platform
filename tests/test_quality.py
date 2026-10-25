"""Checks and alerting: one alert per problem, open until it is fixed."""
from acm import quality
from acm.pricing import ts
from tests.conftest import run_at


def alerts(lake):
    con = lake.connect()
    try:
        return con.execute("SELECT check_name, key, opened_at, resolved_at FROM alerts ORDER BY opened_at").fetchall()
    finally:
        con.close()


def test_unit_bug_opens_one_alert_that_resolves_when_the_correction_arrives(landing):
    w = "2026-09-10T00:00:00Z"
    landing.inference("c002", w, 900, 300, requests=300, event_id="w1", region="us-central1")   # 3 tokens/request
    lake = landing.write()
    assert "token_plausibility:us-central1/llama-70b/2026-09-10" in run_at(lake, "2026-09-10T02:00:00Z").alerts_opened
    again = run_at(lake, "2026-09-10T03:00:00Z").alerts_opened
    assert not [a for a in again if a.startswith("token_plausibility")]              # still open, no second page
    landing.inference("c002", w, 900_000, 300_000, requests=300, event_id="w1", revision=2, region="us-central1",
                      arrives="2026-09-10T03:30:00Z")
    landing.write()
    run_at(lake, "2026-09-10T05:00:00Z")
    (check, key, opened, resolved), = [a for a in alerts(lake) if a[0] == "token_plausibility"]
    assert opened == ts("2026-09-10T02:00:00Z") and resolved == ts("2026-09-10T05:00:00Z")


def test_silent_region_opens_a_freshness_alert(landing):
    for h in range(6):
        landing.storage("c002", f"2026-09-10T{h:02d}:00:00Z", 10, region="eu-west1")
        landing.storage("c002", f"2026-09-10T{h:02d}:00:00Z", 10, region="eu-north1")
    for h in range(6, 12):
        landing.storage("c002", f"2026-09-10T{h:02d}:00:00Z", 10, region="eu-north1")      # eu-west1 goes quiet
    landing.vm("vm1", "c002", "2026-09-10T00:00:00Z", "2026-09-10T00:10:00Z", region="eu-west1")
    landing.vm("vm2", "c002", "2026-09-10T00:00:00Z", "2026-09-10T00:10:00Z", region="eu-north1")
    lake = landing.write()
    opened = run_at(lake, "2026-09-10T12:00:00Z").alerts_opened
    assert "freshness:storage_snapshot/eu-west1" in opened
    assert "freshness:storage_snapshot/eu-north1" not in opened


def test_thresholds_live_in_config():
    cfg = quality.load_config()
    assert cfg["reconciliation"]["max_unexplained_usd"] == 1.0
    assert {"freshness", "duplicates", "quarantine", "token_plausibility", "mart_contracts"} <= set(cfg)


def test_info_checks_never_page(landing):
    landing.vm("vm1", "c002", "2026-09-10T00:00:00Z", "2026-09-10T00:10:00Z")
    lake = landing.write()
    run_at(lake, "2026-09-10T02:00:00Z")
    con = lake.connect()
    severities = {s for (s,) in con.execute("SELECT DISTINCT severity FROM alerts").fetchall()}
    assert "info" not in severities
