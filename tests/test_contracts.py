import duckdb
import pytest

from acm import contracts
from acm.contracts import diff_mart, diff_producer, load_producer, parse_ts, upcast, validate


def lifecycle(**over):
    rec = {"event_id": "e1", "schema_version": 1, "vm_id": "vm1", "customer_id": "c001", "project_id": "p",
           "event_type": "started", "event_time": "2026-09-01T00:00:00Z", "region": "eu-north1",
           "gpu_type": "h100", "gpu_count": 8, "host_id": "h1"}
    rec.update(over)
    return rec


def test_valid_record_gets_epoch_timestamp():
    rec, reason = validate(load_producer("vm_lifecycle"), lifecycle())
    assert reason is None and rec["event_time_ts"] == 1788220800


@pytest.mark.parametrize("over, reason", [
    ({"gpu_type": "a100"}, "not in"),
    ({"customer_id": "acme"}, "does not match"),
    ({"gpu_count": 9}, "above"),
    ({"gpu_count": "8"}, "not an integer"),
    ({"event_time": "2026-09-01T00:00:00+02:00"}, "UTC"),
    ({"schema_version": 3}, "unknown schema_version"),
    ({"surprise": 1}, "unexpected fields"),
])
def test_contract_violations_are_named(over, reason):
    rec, why = validate(load_producer("vm_lifecycle"), lifecycle(**over))
    assert rec is None and reason in why


def test_missing_required_field():
    r = lifecycle()
    del r["host_id"]
    assert validate(load_producer("vm_lifecycle"), r) == (None, "missing host_id")


def test_schema_v2_is_upcast_to_canonical_names():
    c = load_producer("vm_lifecycle")
    r = lifecycle(schema_version=2)
    r["accelerator"], r["accelerator_count"] = r.pop("gpu_type"), r.pop("gpu_count")
    assert upcast(c, r)["gpu_type"] == "h100"
    rec, reason = validate(c, r)
    assert reason is None and rec["gpu_count"] == 8


def test_parse_ts_rejects_naive_times():
    with pytest.raises(ValueError):
        parse_ts("2026-09-01T00:00:00")


def test_breaking_producer_changes_are_found():
    old = load_producer("gpu_heartbeat").raw
    new = {**old, "fields": {k: v for k, v in old["fields"].items() if k != "seq"}, "dedup_key": ["event_id"]}
    problems = diff_producer(old, new)
    assert "field seq removed" in problems and any("dedup_key" in p for p in problems)


def test_declared_rename_is_not_breaking():
    old = load_producer("vm_lifecycle").raw
    assert diff_producer({**old, "versions": {1: {}}}, old) == []


def test_mart_diff():
    old = contracts.load_mart("finance_revenue")
    cols = dict(old["columns"])
    cols["total"] = {"type": "double", "nullable": False}
    cols.pop("adjustments")
    problems = diff_mart(old, {**old, "columns": cols})
    assert "column adjustments removed" in problems and any("total changed type" in p for p in problems)
    assert diff_mart(old, {**old, "columns": {**old["columns"], "new_col": {"type": "string"}}}) == []


def test_built_mart_is_checked_against_its_contract():
    con = duckdb.connect()
    con.execute("""CREATE TABLE mart_product_capacity AS SELECT * FROM (VALUES
        (DATE '2026-09-01', 'eu-north1', 'h100', 1.0, 1.0, 0.0, 1.0, false),
        (DATE '2026-09-01', 'eu-north1', 'h100', 2.0, NULL, 0.0, 1.0, false))
        t(day, region, gpu_type, allocated_gpu_hours, busy_gpu_hours, committed_gpu_hours, heartbeat_coverage, low_coverage)""")
    problems = contracts.check_mart(con, "product_capacity")
    assert any("nulls" in p for p in problems) and any("duplicate" in p for p in problems)


def test_every_contract_is_valid():
    from acm.cli import main
    assert main(["contracts", "--check"]) == 0
