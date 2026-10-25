import gzip
import json

from acm.ingest import compact, ingest_events
from acm.pricing import ts


def _bronze_rows(lake, con, dataset):
    return con.execute(f"SELECT count(*) FROM read_parquet('{lake.bronze / dataset}/*/*.parquet')").fetchone()[0]


def test_records_failing_the_contract_are_quarantined_with_the_reason(landing):
    landing.vm("vm1", "c001", "2026-09-10T00:00:00Z", "2026-09-10T01:00:00Z")
    landing.add("vm_lifecycle", "2026-09-10T00:00:00Z", {"event_id": "bad", "vm_id": "vm1"})
    lake = landing.write()
    con = lake.connect()
    res = ingest_events(lake, con, ts("2026-09-11T00:00:00Z"))
    assert res.rows == 3 and res.quarantined == 1
    q = [json.loads(line) for line in next(lake.quarantine.rglob("*.jsonl")).read_text().splitlines()]
    assert q[0]["reason"].startswith("unexpected fields") or q[0]["reason"].startswith("missing")
    assert _bronze_rows(lake, con, "vm_lifecycle") == 3


def test_a_file_is_ingested_once_and_only_when_complete(landing):
    landing.vm("vm1", "c001", "2026-09-10T10:20:00Z")
    lake = landing.write()
    con = lake.connect()
    assert ingest_events(lake, con, ts("2026-09-10T10:59:00Z")).files == 0   # the hour is still being written
    assert ingest_events(lake, con, ts("2026-09-10T11:00:00Z")).files == 1
    assert ingest_events(lake, con, ts("2026-09-10T12:00:00Z")).files == 0


def test_a_rewritten_landing_file_is_reported(landing):
    landing.vm("vm1", "c001", "2026-09-10T10:20:00Z")
    lake = landing.write()
    con = lake.connect()
    ingest_events(lake, con, ts("2026-09-10T12:00:00Z"))
    p = next((lake.landing / "vm_lifecycle").rglob("*.gz"))
    p.write_bytes(gzip.compress(gzip.decompress(p.read_bytes()) + b"\n"))
    assert ingest_events(lake, con, ts("2026-09-10T13:00:00Z")).rewritten == [str(p.relative_to(lake.root))]


def test_bronze_is_partitioned_by_event_date_not_arrival(landing):
    landing.beats("vm1", "2026-09-30T23:50:00Z", 1, arrives="2026-10-02T08:00:00Z")
    lake = landing.write()
    con = lake.connect()
    res = ingest_events(lake, con, ts("2026-10-03T00:00:00Z"))
    assert res.months == {"2026-09"}
    assert (lake.bronze / "gpu_heartbeat" / "event_date=2026-09-30").exists()


def test_compaction_merges_settled_days_and_keeps_every_row(landing):
    landing.beats("vm1", "2026-09-10T00:00:00Z", 36)                      # three landing hours
    lake = landing.write()
    con = lake.connect()
    ingest_events(lake, con, ts("2026-09-10T04:00:00Z"))
    part = lake.bronze / "gpu_heartbeat" / "event_date=2026-09-10"
    assert len(list(part.glob("*.parquet"))) == 3
    assert compact(lake, ts("2026-09-11T00:00:00Z")) == 0                 # not settled yet
    assert compact(lake, ts("2026-09-13T00:00:00Z")) == 2
    assert len(list(part.glob("*.parquet"))) == 1 and _bronze_rows(lake, con, "gpu_heartbeat") == 36
