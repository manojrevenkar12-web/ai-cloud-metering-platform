"""Data-quality checks and alerting (config/checks.yaml).

Every run records one row per check and key in `check_results`. An `error` that fails opens an
alert (one per check and key) that stays open until the check passes again, so on-call sees one
page per problem, not one per run; the runbook (docs/RUNBOOK.md) says what to do for each check.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import yaml

from acm.pricing import CONFIG


@dataclass
class Result:
    check: str
    key: str
    ok: bool
    severity: str
    value: float
    message: str


def load_config(path: Path | None = None) -> dict:
    with open(path or CONFIG / "checks.yaml") as f:
        return yaml.safe_load(f)


def ensure(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""CREATE TABLE IF NOT EXISTS check_results (
        run_at BIGINT, check_name VARCHAR, key VARCHAR, ok BOOLEAN, severity VARCHAR, value DOUBLE, message VARCHAR)""")
    con.execute("""CREATE TABLE IF NOT EXISTS alerts (
        check_name VARCHAR, key VARCHAR, severity VARCHAR, opened_at BIGINT, resolved_at BIGINT, message VARCHAR)""")


def _region_of(dataset: str) -> str:
    return {"gpu_heartbeat": "silver_heartbeats", "inference_usage": "silver_inference",
            "storage_snapshot": "silver_storage"}[dataset]


def run_checks(con: duckdb.DuckDBPyConnection, now: int, cfg: dict | None = None,
               restated: list[dict] | None = None, recon: dict[str, list[dict]] | None = None) -> list[Result]:
    cfg = cfg or load_config()
    out: list[Result] = []
    day = now - 86400

    c = cfg["freshness"]
    time_col = {"gpu_heartbeat": "beat_ts", "inference_usage": "window_ts + 600", "storage_snapshot": "snapshot_ts"}
    regions = [r for (r,) in con.execute("SELECT DISTINCT region FROM silver_vms ORDER BY 1").fetchall()]
    for ds in c["datasets"]:
        table = _region_of(ds)
        latest = dict(con.execute(f"SELECT region, max({time_col[ds]}) FROM {table} WHERE visible_at <= ? GROUP BY region",
                                  [now]).fetchall())
        for region in sorted(set(regions) | set(latest)):
            if region not in latest:
                continue
            lag_h = (now - latest[region]) / 3600
            out.append(Result("freshness", f"{ds}/{region}", lag_h <= c["max_lag_hours"], c["severity"], lag_h,
                              f"no {ds} from {region} for {lag_h:.1f} h"))

    c = cfg["duplicates"]
    for ds, key in [("vm_lifecycle", "event_id"), ("gpu_heartbeat", "vm_id, seq"), ("inference_usage", "event_id"),
                    ("storage_snapshot", "event_id")]:
        total, distinct = con.execute(f"""SELECT count(*), count(DISTINCT ({key})) FROM bronze_{ds}
                                          WHERE _visible_at > ? AND _visible_at <= ?""", [day, now]).fetchone()
        share = (total - distinct) / total if total else 0.0
        # A share of a handful of events is noise: judge only a day with enough events.
        out.append(Result("duplicates", ds, share <= c["max_share"] or total < c["min_events"], c["severity"], share,
                          f"{share:.1%} duplicate copies in {ds} over the last 24 h"))

    c = cfg["quarantine"]
    good = con.execute("SELECT coalesce(sum(rows), 0), coalesce(sum(quarantined), 0) FROM ingest_log WHERE visible_at > ?",
                       [day]).fetchone()
    share = good[1] / (good[0] + good[1]) if (good[0] + good[1]) else 0.0
    out.append(Result("quarantine", "all", share <= c["max_share"], c["severity"], share,
                      f"{share:.2%} of records failed their contract in the last 24 h"))

    c = cfg["token_plausibility"]
    for region, model, d, tpr in con.execute("""
            SELECT region, model, CAST(to_timestamp(window_ts) AS DATE) AS d, median(input_tokens / requests)
            FROM silver_inference WHERE window_ts > ? - 3 * 86400 AND model <> 'embed-small' GROUP BY ALL""", [now]).fetchall():
        out.append(Result("token_plausibility", f"{region}/{model}/{d}", tpr >= c["min_input_tokens_per_request"],
                          c["severity"], tpr, f"median {tpr:.1f} input tokens per request on {region} {model} {d}"))

    c = cfg["heartbeat_coverage"]
    low = con.execute("""SELECT vm_id, beats * 300.0 / nullif(least(billed_until, ?) - started_at, 0) AS cov
                         FROM silver_vm_sessions WHERE least(billed_until, ?) - started_at > 3600
                         AND beats * 300.0 / nullif(least(billed_until, ?) - started_at, 0) < ?""",
                      [now, now, now, c["min_coverage"]]).fetchall()
    out.append(Result("heartbeat_coverage", "sessions", not low, c["severity"], len(low),
                      f"{len(low)} sessions with under {c['min_coverage']:.0%} heartbeat coverage"))

    c = cfg["clock_skew"]
    skewed = con.execute("SELECT host_id, median(host_skew_s) FROM silver_heartbeats GROUP BY host_id HAVING abs(median(host_skew_s)) > ?",
                         [c["max_abs_skew_s"]]).fetchall()
    out.append(Result("clock_skew", "hosts", not skewed, c["severity"], len(skewed),
                      f"{len(skewed)} hosts with clocks off by more than {c['max_abs_skew_s']} s (corrected from the schedule)"))

    c = cfg["inferred_session_end"]
    n = con.execute("""SELECT count(*) FROM silver_vm_sessions WHERE end_source = 'inferred'
                       AND billed_until > ? - ? * 3600""", [now, c["window_hours"]]).fetchone()[0]
    out.append(Result("inferred_session_end", "sessions", n <= c["max_sessions"], c["severity"], n,
                      f"{n} sessions in the last {c['window_hours']} h ended without a stop event "
                      "(billed to one heartbeat after the last)"))

    c = cfg["unpriced_usage"]
    unpriced = con.execute("SELECT coalesce(sum(unpriced_seconds), 0) FROM gold_charges").fetchone()[0]
    out.append(Result("unpriced_usage", "gpu", unpriced == 0, c["severity"], unpriced,
                      f"{unpriced / 3600:.1f} GPU-hours with no list price"))

    c = cfg["unknown_customers"]
    unknown = con.execute("""SELECT count(DISTINCT customer_id) FROM (
            SELECT customer_id FROM silver_vms UNION SELECT customer_id FROM silver_inference
            UNION SELECT customer_id FROM silver_storage) WHERE customer_id NOT IN (SELECT customer_id FROM ref_customers)""").fetchone()[0]
    out.append(Result("unknown_customers", "all", unknown == 0, c["severity"], unknown,
                      f"{unknown} customer ids in usage that the CRM does not know"))

    c = cfg["mart_contracts"]
    from acm.contracts import check_mart
    for mart in c["marts"]:
        problems = check_mart(con, mart)
        out.append(Result("mart_contracts", mart, not problems, c["severity"], len(problems),
                          "; ".join(problems[:3]) if problems else f"mart_{mart} matches its contract"))

    if restated is not None:
        c = cfg["restatement"]
        delta = sum(abs(r["delta"]) for r in restated)
        months = sorted({r["month"] for r in restated})
        reason = restated[0]["reason"] if restated else ""
        out.append(Result("restatement", ",".join(months) or "none", not restated, c["severity"], delta,
                          f"closed month restated by ${delta:,.2f} gross ({reason})" if restated else "no restatement"))

    if recon:
        c = cfg["reconciliation"]
        for month, rows in sorted(recon.items()):
            worst = max((abs(r["unexplained"]) for r in rows), default=0.0)
            out.append(Result("reconciliation", month, worst <= c["max_unexplained_usd"], c["severity"], worst,
                              f"largest unexplained invoice difference for {month}: ${worst:,.2f}"))
    return out


def record(con: duckdb.DuckDBPyConnection, now: int, results: list[Result]) -> list[tuple[str, str, str]]:
    """Stores results and opens/resolves alerts. Returns alerts opened in this run."""
    ensure(con)
    con.executemany("INSERT INTO check_results VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [(now, r.check, r.key, r.ok, r.severity, float(r.value), r.message) for r in results])
    opened = []
    open_now = {(c, k) for c, k in con.execute("SELECT check_name, key FROM alerts WHERE resolved_at IS NULL").fetchall()}
    for r in results:
        if r.severity == "info":
            continue
        key = (r.check, r.key)
        if not r.ok and key not in open_now:
            con.execute("INSERT INTO alerts VALUES (?, ?, ?, ?, NULL, ?)", [r.check, r.key, r.severity, now, r.message])
            opened.append((r.check, r.key, r.message))
        elif r.ok and key in open_now:
            con.execute("UPDATE alerts SET resolved_at = ? WHERE check_name = ? AND key = ? AND resolved_at IS NULL",
                        [now, r.check, r.key])
    return opened
