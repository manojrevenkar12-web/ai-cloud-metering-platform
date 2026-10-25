"""Lineage is derived from the SQL, and it proves two things the billing rules promise."""
import ast
from pathlib import Path

import duckdb
import pytest

import acm
from acm import lineage


@pytest.fixture(scope="module")
def nodes(small_lake):
    con = duckdb.connect(str(small_lake.warehouse), read_only=True)
    try:
        return lineage.build(con)
    finally:
        con.close()


def test_every_column_of_every_model_is_resolved(nodes):
    assert {t: n.unresolved for t, n in nodes.items() if n.unresolved} == {}
    assert all(n.columns for n in nodes.values())


def test_revenue_comes_from_prices_contracts_and_usage(nodes):
    src = lineage.trace(nodes, "mart_finance_revenue", "total")
    assert {"ref_prices.usd", "ref_commitments.usd_per_gpu_hour", "ref_credit_grants.usd",
            "bronze_inference_usage.input_tokens", "bronze_storage_snapshot.bytes_stored"} <= src


def test_host_clocks_cannot_move_money(nodes):
    """Heartbeat time comes from the schedule; a host's clock only measures its skew."""
    hit = lineage.impact(nodes, "bronze_gpu_heartbeat", "host_time_ts")
    assert hit == {"silver_heartbeats.host_skew_s", "silver_vm_sessions.host_skew_s"}


def test_personal_data_never_reaches_a_mart(nodes):
    for table, col in [("bronze_inference_usage", "api_key_owner"), ("ref_customers", "contact_email")]:
        assert not [c for c in lineage.impact(nodes, table, col) if c.startswith("mart_")]


def test_openlineage_events_name_inputs_and_column_facets(nodes):
    events = lineage.openlineage_events(nodes)
    gold = next(e for e in events if e["outputs"][0]["name"] == "gold_charges")
    assert {"namespace": "acm", "name": "silver_inference"} in gold["inputs"]
    fields = gold["outputs"][0]["facets"]["columnLineage"]["fields"]
    assert {"namespace": "acm", "name": "silver_inference", "field": "input_tokens"} in fields["inference"]["inputFields"]


def test_python_steps_declare_what_their_sql_reads():
    """The tables built in Python declare their inputs by hand; check the code agrees."""
    text = (Path(acm.__file__).parent / "periods.py").read_text()
    for table, step in lineage.PYTHON_STEPS.items():
        for i in step["inputs"]:
            assert i in text, f"{table} declares {i} but periods.py never reads it"
    ast.parse(text)
