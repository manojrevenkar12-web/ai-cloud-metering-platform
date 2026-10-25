"""Bronze -> silver -> gold, in DuckDB.

Silver tables are rebuilt from bronze on every run: at this volume (hundreds of thousands of
events a month) that takes about a second and removes a class of incremental-update bugs. Gold is
recomputed per affected month (the months touched by newly ingested events, plus the open month),
so a late September event recomputes September and nothing else (docs/adr/0002).
"""
from __future__ import annotations

import time
from pathlib import Path

import duckdb

from acm.contracts import producers
from acm.ingest import Lake
from acm.pricing import iso, price_rows, ts

MODELS = Path(__file__).parent / "models"
SILVER = ["01_silver_vm_events", "02_silver_vms", "03_silver_heartbeats", "04_silver_vm_sessions",
          "05_silver_inference", "06_silver_storage", "07_silver_commitment_terms"]
GOLD = ["10_gold_gpu_minutes", "11_gold_charges"]
MARTS = ["20_mart_product_capacity"]
GRACE_S = 900  # a VM silent for 15 min past its next heartbeat is treated as stopped
DUCK = {"string": "VARCHAR", "int": "BIGINT", "float": "DOUBLE", "timestamp": "VARCHAR"}


def sql(name: str) -> str:
    return (MODELS / f"{name}.sql").read_text()


def month_bounds(month: str) -> tuple[int, int]:
    start = ts(f"{month}-01T00:00:00Z")
    y, m = int(month[:4]), int(month[5:7])
    nxt = f"{y + (m == 12)}-{1 if m == 12 else m + 1:02d}"
    return start, ts(f"{nxt}-01T00:00:00Z")


def bind_bronze(lake: Lake, con: duckdb.DuckDBPyConnection, visible_until: int | None = None) -> None:
    """bronze_<dataset> views over the Parquet partitions (empty tables before the first file).

    `visible_until` limits them to what had arrived by then, so the same models can answer "what
    could we (or the billing system) have known at that moment?".
    """
    for name, c in producers().items():
        files = list((lake.bronze / name).glob("*/*.parquet"))
        if files:
            cut = f" WHERE _visible_at <= {int(visible_until)}" if visible_until is not None else ""
            con.execute(f"""CREATE OR REPLACE VIEW bronze_{name} AS
                SELECT * FROM read_parquet('{lake.bronze / name}/*/*.parquet', hive_partitioning = true){cut}""")
        else:
            cols = ", ".join(f"NULL::{DUCK[s['type']]} AS {n}" for n, s in c.fields.items())
            con.execute(f"""CREATE OR REPLACE VIEW bronze_{name} AS SELECT {cols}, NULL::BIGINT AS {c.event_time}_ts,
                            NULL::VARCHAR AS _file, NULL::BIGINT AS _visible_at, NULL::BIGINT AS _ingested_at,
                            NULL::VARCHAR AS event_date WHERE false""")


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE OR REPLACE TABLE ref_prices (sku VARCHAR, valid_from BIGINT, valid_to BIGINT, usd DOUBLE)")
    con.executemany("INSERT INTO ref_prices VALUES (?, ?, ?, ?)",
                    [(p.sku, p.valid_from, p.valid_to, p.usd) for p in price_rows()])
    for t, ddl in {
        "ref_commitments": "commitment_id VARCHAR, customer_id VARCHAR, gpu_type VARCHAR, region VARCHAR, gpus INTEGER, "
                           "usd_per_gpu_hour DOUBLE, starts BIGINT, ends BIGINT, replaces VARCHAR",
        "ref_credit_grants": "grant_id VARCHAR, customer_id VARCHAR, usd DOUBLE, granted_at BIGINT, expires_at BIGINT",
        "ref_customers": "customer_id VARCHAR, name VARCHAR, contact_email VARCHAR, country VARCHAR, segment VARCHAR",
        "gold_gpu_minutes": "month VARCHAR, customer_id VARCHAR, gpu_type VARCHAR, region VARCHAR, minute BIGINT, "
                            "gpu_seconds DOUBLE, committed_seconds DOUBLE, overage_seconds DOUBLE, usd_per_gpu_hour DOUBLE",
        "gold_charges": "month VARCHAR, customer_id VARCHAR, gpu_on_demand DOUBLE, commitment DOUBLE, inference DOUBLE, "
                        "storage DOUBLE, gpu_seconds DOUBLE, unpriced_seconds DOUBLE",
    }.items():
        con.execute(f"CREATE TABLE IF NOT EXISTS {t} ({ddl})")


def run(lake: Lake, con: duckdb.DuckDBPyConnection, now: int, months: set[str], horizon: int | None = None,
        visible_until: int | None = None, silver_only: bool = False, gold_only: bool = False) -> dict:
    """Rebuilds silver, recomputes gold for `months`, refreshes marts. Returns timings in seconds.

    `horizon` caps metering for the open month (default: now). Evaluations set it to the end of the
    simulated activity so the open month is compared with the truth over the same span.
    """
    timings = {}
    if not gold_only:
        ensure_tables(con)
        bind_bronze(lake, con, visible_until)
    params = {"now": now, "grace": GRACE_S}
    for name in ([] if gold_only else SILVER):
        t0 = time.perf_counter()
        statement = sql(name)
        con.execute(statement, {k: v for k, v in params.items() if f"${k}" in statement})
        timings[name] = time.perf_counter() - t0
    if silver_only:
        return timings
    for month in sorted(months):
        a, b = month_bounds(month)
        for name in GOLD:
            t0 = time.perf_counter()
            body = "\n".join(line for line in sql(name).splitlines() if not line.lstrip().startswith("--"))
            for statement in [s for s in body.split(";") if s.strip()]:
                p = {"month": month, "month_start": a, "month_end": min(b, horizon or now)}
                con.execute(statement, {k: v for k, v in p.items() if f"${k}" in statement})
            timings[f"{name}[{month}]"] = time.perf_counter() - t0
        con.execute("CREATE TABLE IF NOT EXISTS gold_runs (month VARCHAR PRIMARY KEY, month_end BIGINT, run_at BIGINT)")
        con.execute("INSERT OR REPLACE INTO gold_runs VALUES (?, ?, ?)", [month, min(b, horizon or now), now])
    for name in MARTS:
        t0 = time.perf_counter()
        con.execute(sql(name))
        timings[name] = time.perf_counter() - t0
    return timings


def open_months(now: int) -> set[str]:
    return {iso(now)[:7]}
