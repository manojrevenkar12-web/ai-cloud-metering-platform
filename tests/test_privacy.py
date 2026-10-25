"""GDPR erasure: every copy goes, revenue does not move, later arrivals are pseudonymised."""
import duckdb

from acm import privacy
from acm.pricing import ts
from tests.conftest import run_at


def an_email(lake):
    con = duckdb.connect(str(lake.warehouse), read_only=True)
    try:
        return con.execute("SELECT api_key_owner FROM silver_inference WHERE api_key_owner NOT LIKE 'erased:%' LIMIT 1").fetchone()[0]
    finally:
        con.close()


def test_erasure_removes_every_copy_and_keeps_revenue(small_lake_copy):
    lake = small_lake_copy
    email = an_email(lake)
    now = ts("2026-10-04T13:00:00Z")
    con = lake.connect()
    before = privacy.scan(lake, con, email, now)
    assert before["landing"] and before["bronze"] and before["warehouse"]
    rep = privacy.erase(lake, con, email, now, "DSR-1")
    assert rep.verified and rep.remaining == 0
    assert rep.revenue_before == rep.revenue_after
    assert privacy.scan(lake, con, email, now) == {"landing": 0, "bronze": 0, "quarantine": 0, "warehouse": 0}
    log = con.execute("SELECT subject_hash, verified FROM erasure_log").fetchall()
    assert log == [(rep.subject_hash, True)] and email not in rep.subject_hash


def test_data_arriving_after_erasure_is_pseudonymised_on_ingest(small_lake_copy):
    lake = small_lake_copy
    email = an_email(lake)
    con = lake.connect()
    privacy.erase(lake, con, email, ts("2026-10-04T13:00:00Z"), "DSR-2")
    con.close()
    run_at(lake, "2026-10-06T12:00:00Z")                      # two more days of files, the subject still in them
    con = lake.connect()
    assert privacy.scan(lake, con, email, ts("2026-10-06T12:00:00Z")) == {"landing": 0, "bronze": 0, "quarantine": 0,
                                                                          "warehouse": 0}
    assert con.execute("SELECT count(*) FROM silver_inference WHERE api_key_owner LIKE 'erased:%'").fetchone()[0] > 0


def test_the_audit_never_holds_the_email(small_lake_copy):
    lake = small_lake_copy
    email = an_email(lake)
    con = lake.connect()
    privacy.erase(lake, con, email, ts("2026-10-04T13:00:00Z"), "DSR-3")
    rows = con.execute("SELECT * FROM erasure_log").fetchall() + con.execute("SELECT * FROM erasure_suppression").fetchall()
    assert not any(email in str(v) for row in rows for v in row)


def test_ingested_landing_files_are_not_reported_as_tampered_after_erasure(small_lake_copy):
    from acm.ingest import ingest_events
    lake = small_lake_copy
    email = an_email(lake)
    con = lake.connect()
    privacy.erase(lake, con, email, ts("2026-10-04T13:00:00Z"), "DSR-4")
    assert ingest_events(lake, con, ts("2026-10-04T13:00:00Z")).rewritten == []


def test_erasing_someone_we_do_not_hold_is_a_verified_no_op(small_lake_copy):
    lake = small_lake_copy
    con = lake.connect()
    rep = privacy.erase(lake, con, "nobody@nowhere.example", ts("2026-10-04T13:00:00Z"), "DSR-5")
    assert rep.verified and rep.landing_files == rep.bronze_files == rep.warehouse_rows == 0
