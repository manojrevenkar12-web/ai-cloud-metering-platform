"""Dagster definitions: the same pipeline steps as `acm run`, as assets with asset checks.

    bronze_events -> silver_tables -> gold_charges, mart_product_capacity
                                   -> mart_finance_revenue -> recon_waterfall

Every data-quality check in config/checks.yaml is an asset check on the asset it guards, so the
Dagster UI shows a failing freshness check on bronze_events and a failing reconciliation on
recon_waterfall. A sensor starts a run when new landing files appear, a schedule runs it every
three hours regardless, and a second schedule runs it at the period close (config/periods.yaml).

    dagster dev -m acm.dagster_defs        # UI on :3000
    ACM_LAKE=/data/lake ACM_NOW=2026-10-04T12:00:00Z dagster asset materialize -m acm.dagster_defs --select '*'

The warehouse is one DuckDB file, so the job runs in process: one writer at a time.
"""

import os
import time
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    ConfigurableResource,
    Definitions,
    MaterializeResult,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    asset,
    define_asset_job,
    in_process_executor,
    multi_asset,
    multi_asset_check,
    sensor,
)

from acm import pipeline, quality, transform
from acm.ingest import Lake, visible_at
from acm.pricing import iso, ts

# Which asset each check guards.
CHECK_ASSET = {
    "freshness": "bronze_events", "duplicates": "bronze_events", "quarantine": "bronze_events",
    "token_plausibility": "silver_tables", "heartbeat_coverage": "silver_tables", "clock_skew": "silver_tables",
    "inferred_session_end": "silver_tables", "unpriced_usage": "gold_charges", "unknown_customers": "gold_charges",
    "restatement": "mart_finance_revenue", "reconciliation": "recon_waterfall", "mart_contracts": "mart_finance_revenue",
}


class Warehouse(ConfigurableResource):
    """Where the lake lives and what time it is. `now` (ISO, UTC) pins the clock for replays and
    backfills; empty means the wall clock."""

    root: str = os.environ.get("ACM_LAKE", "data/lake")
    now: str = os.environ.get("ACM_NOW", "")

    def lake(self) -> Lake:
        return Lake(Path(self.root))

    def clock(self) -> int:
        return ts(self.now) if self.now else int(time.time())


@asset(group_name="ingest", description="Landing files validated against their contracts into bronze Parquet; "
       "reference exports (CRM, contracts, invoices) loaded.")
def bronze_events(context: AssetExecutionContext, warehouse: Warehouse) -> MaterializeResult:
    lake, now = warehouse.lake(), warehouse.clock()
    con = lake.connect()
    try:
        res = pipeline.RunResult(iso(now))
        months = pipeline.ingest(lake, con, now, res)
    finally:
        con.close()
    context.log.info(f"{res.files} files, {res.rows} rows, {res.quarantined} quarantined; recompute {months}")
    return MaterializeResult(metadata={"files": res.files, "rows": res.rows, "quarantined": res.quarantined,
                                       "months_to_recompute": ", ".join(months), "as_of": iso(now)})


@asset(group_name="transform", deps=[bronze_events],
       description="Deduplicated events, VMs, heartbeats on the schedule clock, billable sessions, inference "
       "(highest revision), storage, commitment terms (renewals replace).")
def silver_tables(warehouse: Warehouse) -> MaterializeResult:
    lake, now = warehouse.lake(), warehouse.clock()
    con = lake.connect()
    try:
        timings = transform.run(lake, con, now, set(), silver_only=True)
        sessions = con.execute("SELECT end_source, count(*) FROM silver_vm_sessions GROUP BY 1").fetchall()
    finally:
        con.close()
    return MaterializeResult(metadata={"seconds": round(sum(timings.values()), 2),
                                       **{f"sessions_{k}": v for k, v in sessions}})


@multi_asset(group_name="transform", specs=[
    AssetSpec("gold_charges", deps=[silver_tables], description="Charges per customer and month: on-demand "
              "overage per minute, commitments, inference, storage."),
    AssetSpec("mart_product_capacity", deps=[silver_tables], description="contracts/marts/product_capacity.yaml"),
])
def gold(warehouse: Warehouse):
    lake, now = warehouse.lake(), warehouse.clock()
    con = lake.connect()
    try:
        months = pipeline.get_state(con, "pending_months", [])
        transform.run(lake, con, now, set(months), gold_only=True)
        rows = con.execute("SELECT count(*) FROM gold_charges").fetchone()[0]
        cap = con.execute("SELECT count(*) FROM mart_product_capacity").fetchone()[0]
    finally:
        con.close()
    yield MaterializeResult(asset_key="gold_charges", metadata={"months": ", ".join(months), "rows": rows})
    yield MaterializeResult(asset_key="mart_product_capacity", metadata={"rows": cap})


@asset(group_name="finance", deps=[AssetKey("gold_charges")],
       description="contracts/marts/finance_revenue.yaml: credits applied, months closed on schedule, "
       "closed months restated by adjustment entries with a reason.")
def mart_finance_revenue(warehouse: Warehouse) -> MaterializeResult:
    lake, now = warehouse.lake(), warehouse.clock()
    con = lake.connect()
    try:
        res = pipeline.RunResult(iso(now))
        pipeline.revenue(con, now, res)
        by_month = dict(con.execute("SELECT month, sum(total) FROM mart_finance_revenue GROUP BY 1").fetchall())
    finally:
        con.close()
    return MaterializeResult(metadata={"closed_now": ", ".join(res.closed) or "none",
                                       "restated_usd": round(sum(r["delta"] for r in res.restated), 2),
                                       **{f"revenue_{m}": float(v) for m, v in sorted(by_month.items())}})


@asset(group_name="finance", deps=[mart_finance_revenue],
       description="The billing system's invoices against ours, explained step by step (acm.recon).")
def recon_waterfall(warehouse: Warehouse) -> MaterializeResult:
    lake = warehouse.lake()
    con = lake.connect()
    try:
        res = pipeline.RunResult("")
        pipeline.reconcile(lake, con, res)
    finally:
        con.close()
    meta = {}
    for month, rows in res.recon.items():
        meta[f"{month}_invoiced"] = round(sum(r["invoiced"] for r in rows), 2)
        meta[f"{month}_ours"] = round(sum(r["ours"] for r in rows), 2)
        meta[f"{month}_max_unexplained"] = max((abs(r["unexplained"]) for r in rows), default=0.0)
    return MaterializeResult(metadata=meta or {"invoices": "none yet"})


@multi_asset_check(
    specs=[AssetCheckSpec(name, asset=CHECK_ASSET[name], description=f"config/checks.yaml: {name}")
           for name in CHECK_ASSET],
    can_subset=False,
)
def data_quality(warehouse: Warehouse):
    """Runs config/checks.yaml once, records results and alerts, reports one result per check."""
    lake, now = warehouse.lake(), warehouse.clock()
    con = lake.connect()
    try:
        restated = pipeline.get_state(con, "last_restated", [])
        recon_rows: dict[str, list[dict]] = {}
        if con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'recon_waterfall'").fetchone()[0]:
            cols = [d[0] for d in con.execute("DESCRIBE recon_waterfall").fetchall()]
            for row in con.execute("SELECT * FROM recon_waterfall").fetchall():
                r = dict(zip(cols, row))
                recon_rows.setdefault(r["month"], []).append(r)
        results = quality.run_checks(con, now, restated=restated, recon=recon_rows or None)
        quality.record(con, now, results)
    finally:
        con.close()
    for name in CHECK_ASSET:
        mine = [r for r in results if r.check == name]
        failed = [r for r in mine if not r.ok]
        severity = AssetCheckSeverity.ERROR if any(r.severity == "error" for r in failed) else AssetCheckSeverity.WARN
        yield AssetCheckResult(
            check_name=name, asset_key=CHECK_ASSET[name], passed=not failed, severity=severity,
            metadata={"evaluated": len(mine), "failed": len(failed),
                      "messages": "\n".join(r.message for r in failed[:10]) or "ok"})


ASSETS = [bronze_events, silver_tables, gold, mart_finance_revenue, recon_waterfall]

meter = define_asset_job("meter", selection="*", executor_def=in_process_executor,
                         description="One pipeline run: ingest, transform, revenue, reconcile, checks.")


def _close_cron() -> str:
    c = pipeline.period_config()["close"]
    return f"5 {c['hour_utc']} {c['day_of_month']} * *"


every_three_hours = ScheduleDefinition(name="every_three_hours", job=meter, cron_schedule="0 */3 * * *",
                                       execution_timezone="UTC")
period_close = ScheduleDefinition(name="period_close", job=meter, cron_schedule=_close_cron(), execution_timezone="UTC",
                                  description="Runs right after the close time so the previous month is frozen on time.")


@sensor(job=meter, minimum_interval_seconds=300, description="New landing files that are complete and not yet ingested.")
def landing_files(context: SensorEvaluationContext, warehouse: Warehouse):
    lake, now = warehouse.lake(), warehouse.clock()
    seen = set()
    if lake.warehouse.exists():
        import duckdb
        con = duckdb.connect(str(lake.warehouse), read_only=True)
        try:
            if con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'ingest_log'").fetchone()[0]:
                seen = {p for (p,) in con.execute("SELECT path FROM ingest_log").fetchall()}
        finally:
            con.close()
    new = [p for p in lake.landing.glob("*/*/*.jsonl.gz")
           if visible_at(p) <= now and str(p.relative_to(lake.root)) not in seen] if lake.landing.exists() else []
    if not new:
        return SkipReason("no new landing files")
    newest = max(visible_at(p) for p in new)
    return RunRequest(run_key=f"landing-{newest}-{len(new)}", tags={"files": str(len(new))})


defs = Definitions(
    assets=ASSETS,
    asset_checks=[data_quality],
    jobs=[meter],
    schedules=[every_three_hours, period_close],
    sensors=[landing_files],
    resources={"warehouse": Warehouse()},
)

__all__ = ["defs"]
