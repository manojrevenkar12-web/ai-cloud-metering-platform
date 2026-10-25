"""Reconciliation: the billing system's invoices against the platform's own numbers.

The difference for each customer is explained as a waterfall over charges before prepaid credits
(a waterfall on net totals would miss an overcharge that a customer's credits silently paid). It
starts from what the platform says now, then:
  1. late_usage             what the billing system could not have seen: our models re-run on
                            data visible at the invoice cutoff
  2. missing_stop_event     sessions with no stop event billed to month end, as billing does
  3. hourly_rounding        us-central1 sessions rounded up to whole hours
  4. overlapping_commitment renewals charged on top of the commitment they replace
  5. duplicate_events       inference retries counted twice
  6. unexplained            whatever is left: the reconciliation check fails above $1
and reports, beside it, how differently the two systems consumed each customer's credits.

Steps 2 to 5 are hypotheses about the billing system's behaviour, each tested by re-running our own
models with that one behaviour switched on; a hypothesis that explains nothing moves nothing. A
cause stays "unexplained" until someone writes the step that accounts for it.
"""
from __future__ import annotations

import duckdb

from acm import periods, transform
from acm.ingest import Lake

STEPS = ["late_usage", "missing_stop_event", "hourly_rounding", "overlapping_commitment", "duplicate_events"]
REF_TABLES = ["ref_prices", "ref_commitments", "ref_credit_grants", "ref_customers"]


def _scratch(lake: Lake, con: duckdb.DuckDBPyConnection, cutoff: int) -> duckdb.DuckDBPyConnection:
    s = duckdb.connect()
    s.execute("SET TimeZone = 'UTC'")
    for t in REF_TABLES:
        cols = con.execute(f"DESCRIBE {t}").fetchall()
        s.execute(f"CREATE TABLE {t} (" + ", ".join(f'"{c[0]}" {c[1]}' for c in cols) + ")")
        rows = con.execute(f"SELECT * FROM {t}").fetchall()
        if rows:
            s.executemany(f"INSERT INTO {t} VALUES ({', '.join('?' * len(cols))})", rows)
    for t, ddl in [("gold_gpu_minutes", "month VARCHAR, customer_id VARCHAR, gpu_type VARCHAR, region VARCHAR, minute BIGINT, "
                                         "gpu_seconds DOUBLE, committed_seconds DOUBLE, overage_seconds DOUBLE, usd_per_gpu_hour DOUBLE"),
                   ("gold_charges", "month VARCHAR, customer_id VARCHAR, gpu_on_demand DOUBLE, commitment DOUBLE, inference DOUBLE, "
                                    "storage DOUBLE, gpu_seconds DOUBLE, unpriced_seconds DOUBLE")]:
        s.execute(f"CREATE TABLE {t} ({ddl})")
    transform.bind_bronze(lake, s, visible_until=cutoff)
    return s


def _totals(s: duckdb.DuckDBPyConnection, month: str, cutoff: int) -> dict[str, float]:
    transform.run(None, s, cutoff, {month}, gold_only=True)
    periods.apply_credits(s)
    periods.current(s)
    return {c: float(t) for c, t in s.execute("""SELECT customer_id, gpu_on_demand + commitment + inference + storage
                                                 FROM revenue_current WHERE month = ?""", [month]).fetchall()}


def reconcile(lake: Lake, con: duckdb.DuckDBPyConnection, month: str) -> list[dict]:
    if not con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'ref_invoices'").fetchone()[0]:
        return []
    invoices = {c: (float(g), int(i), float(cr)) for c, g, i, cr in con.execute(
        """SELECT customer_id, round(gpu_on_demand, 2) + round(commitment, 2) + round(inference, 2) + round(storage, 2),
                  issued_at, credits_applied FROM ref_invoices WHERE period = ?""", [month]).fetchall()}
    if not invoices:
        return []
    cutoff = next(iter(invoices.values()))[1]
    ours, credits = {}, {}
    for c, g, cr in con.execute("""SELECT customer_id, gpu_on_demand + commitment + inference + storage, credits_applied
                                   FROM mart_finance_revenue WHERE month = ?""", [month]).fetchall():
        ours[c], credits[c] = float(g), float(cr)
    a, b = transform.month_bounds(month)
    levels = _cutoff_levels(lake, con, month, cutoff, b)
    levels["current"] = ours
    return _waterfall(con, month, invoices, ours, levels, credits)


def _fingerprint(con: duckdb.DuckDBPyConnection, month: str, cutoff: int) -> str:
    """What the cutoff-side levels depend on: reference data and the events visible at the cutoff
    (which never change after it). Unchanged fingerprint: reuse the stored levels."""
    parts = ["gross", month, str(cutoff)]
    for t in REF_TABLES:
        parts.append(con.execute(f"SELECT coalesce(md5(string_agg(CAST(t AS VARCHAR), '|' ORDER BY CAST(t AS VARCHAR))), '') "
                                 f"FROM {t} t").fetchone()[0])
    parts.append(str(con.execute("SELECT coalesce(sum(rows), 0) FROM ingest_log WHERE visible_at <= ?", [cutoff]).fetchone()[0]))
    return "/".join(parts)


def _cutoff_levels(lake: Lake, con: duckdb.DuckDBPyConnection, month: str, cutoff: int, b: int) -> dict[str, dict[str, float]]:
    con.execute("""CREATE TABLE IF NOT EXISTS recon_levels (month VARCHAR, fingerprint VARCHAR, step VARCHAR,
                   customer_id VARCHAR, total DOUBLE)""")
    fp = _fingerprint(con, month, cutoff)
    cached = con.execute("SELECT step, customer_id, total FROM recon_levels WHERE month = ? AND fingerprint = ?",
                         [month, fp]).fetchall()
    if cached:
        levels: dict[str, dict[str, float]] = {step: {} for step in STEPS}
        for step, cid, total in cached:
            if cid is not None:
                levels[step][cid] = total
        return levels
    levels = _compute_levels(lake, con, month, cutoff, b)
    con.execute("DELETE FROM recon_levels WHERE month = ?", [month])
    rows = [(month, fp, step, cid, v) for step, vals in levels.items() for cid, v in vals.items()]
    rows += [(month, fp, step, None, None) for step in STEPS]      # marks the fingerprint as computed
    con.executemany("INSERT INTO recon_levels VALUES (?, ?, ?, ?, ?)", rows)
    return levels


def _compute_levels(lake: Lake, con: duckdb.DuckDBPyConnection, month: str, cutoff: int, b: int) -> dict[str, dict[str, float]]:
    s = _scratch(lake, con, cutoff)
    # The billing system decided at the cutoff with what it had: our models on the same data.
    transform.run(lake, s, cutoff, set(), visible_until=cutoff, silver_only=True)
    levels = {"late_usage": _totals(s, month, cutoff)}
    # Its behaviours, one at a time, cumulatively.
    s.execute("UPDATE silver_vm_sessions SET billed_until = ? WHERE end_source <> 'stop_event'", [b])
    levels["missing_stop_event"] = _totals(s, month, cutoff)
    s.execute("""UPDATE silver_vm_sessions
                 SET billed_until = started_at + CAST(ceil((billed_until - started_at) / 3600.0) * 3600 AS BIGINT)
                 WHERE region = 'us-central1'""")
    levels["hourly_rounding"] = _totals(s, month, cutoff)
    s.execute("""CREATE OR REPLACE TABLE silver_commitment_terms AS
                 SELECT commitment_id, customer_id, gpu_type, region, gpus, usd_per_gpu_hour, starts, ends, false AS replaced
                 FROM ref_commitments""")
    levels["overlapping_commitment"] = _totals(s, month, cutoff)
    s.execute("""CREATE OR REPLACE TABLE silver_inference AS
                 SELECT event_id, revision, customer_id, model, region, window_start_ts AS window_ts, requests, input_tokens,
                        output_tokens, api_key_owner, _visible_at AS visible_at, 1 AS copies FROM bronze_inference_usage""")
    levels["duplicate_events"] = _totals(s, month, cutoff)
    s.close()
    return levels


def _waterfall(con: duckdb.DuckDBPyConnection, month: str, invoices: dict, ours: dict, levels: dict,
               credits: dict) -> list[dict]:
    out = []
    for cid in sorted(set(invoices) | set(ours)):
        prev = ours.get(cid, 0.0)
        row = {"customer_id": cid, "month": month, "ours": round(prev, 2), "invoiced": round(invoices.get(cid, (0.0, 0))[0], 2)}
        for step in STEPS:
            level = levels[step].get(cid, 0.0)
            row[step] = round(level - prev, 2)
            prev = level
        row["unexplained"] = round(row["invoiced"] - prev, 2)
        # Credits follow the charges: an overcharge a customer's prepaid balance paid shows here.
        row["credits_difference"] = round(invoices.get(cid, (0.0, 0, 0.0))[2] - credits.get(cid, 0.0), 2)
        out.append(row)
    cols = ["customer_id", "month", "ours", "invoiced", *STEPS, "unexplained", "credits_difference"]
    con.execute("CREATE OR REPLACE TABLE recon_waterfall_new (customer_id VARCHAR, month VARCHAR, " +
                ", ".join(f"{c} DOUBLE" for c in cols[2:]) + ")")
    if con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'recon_waterfall'").fetchone()[0]:
        have = [d[0] for d in con.execute("DESCRIBE recon_waterfall").fetchall()]
        if have == cols:
            con.execute("INSERT INTO recon_waterfall_new SELECT * FROM recon_waterfall")
        con.execute("DROP TABLE recon_waterfall")
    con.execute("ALTER TABLE recon_waterfall_new RENAME TO recon_waterfall")
    con.execute("DELETE FROM recon_waterfall WHERE month = ?", [month])
    con.executemany(f"INSERT INTO recon_waterfall VALUES ({', '.join('?' * len(cols))})", [[r[c] for c in cols] for r in out])
    return out
