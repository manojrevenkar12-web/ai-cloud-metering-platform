"""Credits, the revenue ledger, period close and restatements.

Finance reports a month once it is closed. After that, its numbers must not move under anyone's
feet: late usage, a corrected producer batch or a contract-register fix do not edit the closed
figures, they add adjustment entries that say what changed and why. The ledger is append-only,
so "what did we report for September on 5 October?" has an answer (`revenue_as_of`).
"""
from __future__ import annotations

from collections import defaultdict

import duckdb

from acm.pricing import iso
from acm.transform import month_bounds

COMPONENTS = ["gpu_on_demand", "commitment", "inference", "storage", "credits_applied"]


def ensure(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""CREATE TABLE IF NOT EXISTS revenue_ledger (
        entry_id BIGINT, customer_id VARCHAR, month VARCHAR, component VARCHAR, amount DECIMAL(18,2),
        entry_type VARCHAR, recorded_at BIGINT, reason VARCHAR)""")
    con.execute("CREATE TABLE IF NOT EXISTS period_status (month VARCHAR PRIMARY KEY, closed_at BIGINT)")
    con.execute("CREATE SEQUENCE IF NOT EXISTS ledger_seq")


def apply_credits(con: duckdb.DuckDBPyConnection) -> None:
    """Prepaid credits pay for on-demand usage (not commitments), oldest month first."""
    rows = con.execute("""SELECT customer_id, month, gpu_on_demand + inference + storage FROM gold_charges
                          ORDER BY month""").fetchall()
    eligible: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for cid, month, amount in rows:
        eligible[cid].append((month, amount))
    grants = con.execute("SELECT customer_id, usd FROM ref_credit_grants ORDER BY granted_at").fetchall()
    applied: dict[tuple[str, str], float] = defaultdict(float)
    for cid, usd in grants:
        remaining = usd
        for month, amount in eligible.get(cid, []):
            use = min(remaining, max(0.0, amount - applied[(cid, month)]))
            applied[(cid, month)] += use
            remaining -= use
    con.execute("CREATE OR REPLACE TABLE gold_credits (customer_id VARCHAR, month VARCHAR, credits_applied DOUBLE)")
    if applied:
        con.executemany("INSERT INTO gold_credits VALUES (?, ?, ?)", [(c, m, v) for (c, m), v in applied.items()])


def current(con: duckdb.DuckDBPyConnection) -> None:
    """revenue_current: what the warehouse computes right now, per customer, month and component."""
    con.execute("""CREATE OR REPLACE TABLE revenue_current AS
        SELECT g.customer_id, g.month, round(g.gpu_on_demand, 2) AS gpu_on_demand, round(g.commitment, 2) AS commitment,
               round(g.inference, 2) AS inference, round(g.storage, 2) AS storage,
               least(round(coalesce(c.credits_applied, 0), 2),
                     round(g.gpu_on_demand, 2) + round(g.inference, 2) + round(g.storage, 2)) AS credits_applied
        FROM gold_charges g LEFT JOIN gold_credits c USING (customer_id, month)""")


def _late_reason(con: duckdb.DuckDBPyConnection, month: str, closed_at: int) -> str:
    a, b = month_bounds(month)
    parts = []
    for dataset, col in [("vm_lifecycle", "event_time_ts"), ("gpu_heartbeat", "host_time_ts"),
                         ("inference_usage", "window_start_ts"), ("storage_snapshot", "snapshot_time_ts")]:
        n = con.execute(f"""SELECT count(*) FROM bronze_{dataset} WHERE _ingested_at > ? AND {col} >= ? AND {col} < ?""",
                        [closed_at, a, b]).fetchone()[0]
        if n:
            parts.append(f"{n} {dataset}")
    if parts:
        return "events ingested after close: " + ", ".join(parts)
    return "reference data changed after close (contracts, credits or prices)"


def close(con: duckdb.DuckDBPyConnection, month: str, now: int) -> int:
    ensure(con)
    if con.execute("SELECT 1 FROM period_status WHERE month = ?", [month]).fetchone():
        return 0
    rows = con.execute("SELECT * FROM revenue_current WHERE month = ?", [month]).fetchall()
    entries = []
    for cid, m, *amounts in rows:
        for comp, amt in zip(COMPONENTS, amounts):
            entries.append((cid, m, comp, amt, "close", now, f"period {m} closed {iso(now)}"))
    con.executemany("""INSERT INTO revenue_ledger SELECT nextval('ledger_seq'), ?, ?, ?, ?, ?, ?, ?""", entries)
    con.execute("INSERT INTO period_status VALUES (?, ?)", [month, now])
    return len(entries)


def restate(con: duckdb.DuckDBPyConnection, now: int) -> list[dict]:
    """Adds an adjustment for every closed (customer, month, component) whose computed amount no
    longer equals what the ledger says. Returns the adjustments made."""
    ensure(con)
    made = []
    for month, closed_at in con.execute("SELECT month, closed_at FROM period_status").fetchall():
        diffs = con.execute(f"""
            WITH cur AS (
              UNPIVOT (SELECT * FROM revenue_current WHERE month = ?) ON {", ".join(COMPONENTS)}
              INTO NAME component VALUE amount
            ), led AS (
              SELECT customer_id, component, sum(amount) AS amount FROM revenue_ledger WHERE month = ? GROUP BY ALL
            )
            SELECT coalesce(cur.customer_id, led.customer_id), coalesce(cur.component, led.component),
                   coalesce(cur.amount, 0) - coalesce(led.amount, 0) AS delta
            FROM cur FULL JOIN led USING (customer_id, component)
            WHERE abs(coalesce(cur.amount, 0) - coalesce(led.amount, 0)) >= 0.005""", [month, month]).fetchall()
        if not diffs:
            continue
        reason = _late_reason(con, month, closed_at)
        rows = [(cid, month, comp, float(delta), "adjustment", now, reason) for cid, comp, delta in diffs]
        con.executemany("INSERT INTO revenue_ledger SELECT nextval('ledger_seq'), ?, ?, ?, ?, ?, ?, ?", rows)
        made += [{"customer_id": c, "month": month, "component": comp, "delta": round(float(d), 2), "reason": reason}
                 for c, comp, d in diffs]
    return made


def finance_mart(con: duckdb.DuckDBPyConnection) -> None:
    """mart_finance_revenue (contracts/marts/finance_revenue.yaml)."""
    ensure(con)
    con.execute("""CREATE OR REPLACE TABLE mart_finance_revenue AS
        WITH led AS (
          SELECT customer_id, month,
                 sum(amount) FILTER (WHERE component = 'gpu_on_demand') AS gpu_on_demand,
                 sum(amount) FILTER (WHERE component = 'commitment') AS commitment,
                 sum(amount) FILTER (WHERE component = 'inference') AS inference,
                 sum(amount) FILTER (WHERE component = 'storage') AS storage,
                 sum(amount) FILTER (WHERE component = 'credits_applied') AS credits_applied,
                 sum(CASE WHEN component = 'credits_applied' THEN -amount ELSE amount END)
                   FILTER (WHERE entry_type = 'close') AS closed_total,
                 coalesce(sum(CASE WHEN component = 'credits_applied' THEN -amount ELSE amount END)
                   FILTER (WHERE entry_type = 'adjustment'), 0) AS adjustments
          FROM revenue_ledger GROUP BY ALL
        ), closed AS (
          SELECT l.customer_id, l.month, 'closed' AS status,
                 coalesce(l.gpu_on_demand, 0) AS gpu_on_demand, coalesce(l.commitment, 0) AS commitment,
                 coalesce(l.inference, 0) AS inference, coalesce(l.storage, 0) AS storage,
                 coalesce(l.credits_applied, 0) AS credits_applied, l.closed_total, l.adjustments
          FROM led l
        ), open_ AS (
          SELECT r.customer_id, r.month, 'open' AS status, CAST(r.gpu_on_demand AS DECIMAL(18,2)),
                 CAST(r.commitment AS DECIMAL(18,2)), CAST(r.inference AS DECIMAL(18,2)), CAST(r.storage AS DECIMAL(18,2)),
                 CAST(r.credits_applied AS DECIMAL(18,2)), CAST(NULL AS DECIMAL(18,2)), CAST(0 AS DECIMAL(18,2))
          FROM revenue_current r WHERE r.month NOT IN (SELECT month FROM period_status)
        )
        SELECT *, CAST(gpu_on_demand + commitment + inference + storage - credits_applied AS DECIMAL(18,2)) AS total
        FROM (SELECT * FROM closed UNION ALL SELECT * FROM open_)""")


def revenue_as_of(con: duckdb.DuckDBPyConnection, month: str, as_of: int) -> float:
    """What the ledger said about a closed month at a past instant."""
    v = con.execute("""SELECT sum(CASE WHEN component = 'credits_applied' THEN -amount ELSE amount END)
                       FROM revenue_ledger WHERE month = ? AND recorded_at <= ?""", [month, as_of]).fetchone()[0]
    return float(v or 0)
