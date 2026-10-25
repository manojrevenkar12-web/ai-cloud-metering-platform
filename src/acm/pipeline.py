"""One pipeline run, as steps. The replay, the CLI and the Dagster assets all call these, so the
simulated clock and production run the same code.

    ingest -> reference -> transform -> revenue (credits, close, restate, finance mart)
           -> reconcile -> quality
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field

import duckdb
import yaml

from acm import periods, quality, recon, transform
from acm.ingest import Lake, compact, ingest_events, load_reference
from acm.pricing import CONFIG, iso, ts


def period_config() -> dict:
    with open(CONFIG / "periods.yaml") as f:
        return yaml.safe_load(f)


def close_at(month: str, cfg: dict | None = None) -> int:
    cfg = cfg or period_config()
    _, nxt = transform.month_bounds(month)
    day = iso(nxt)[:8] + f"{cfg['close']['day_of_month']:02d}"
    return ts(f"{day}T{cfg['close']['hour_utc']:02d}:00:00Z")


def months_due(con: duckdb.DuckDBPyConnection, now: int, cfg: dict | None = None) -> list[str]:
    cfg = cfg or period_config()
    known = {m for (m,) in con.execute("SELECT DISTINCT month FROM gold_charges").fetchall()}
    return sorted(m for m in known if m >= cfg["first_month"] and close_at(m, cfg) <= now)


@dataclass
class RunResult:
    at: str
    files: int = 0
    rows: int = 0
    quarantined: int = 0
    months: list[str] = field(default_factory=list)
    closed: list[str] = field(default_factory=list)
    restated: list[dict] = field(default_factory=list)
    recon: dict[str, list[dict]] = field(default_factory=dict)
    alerts_opened: list[str] = field(default_factory=list)
    seconds: float = 0.0

    def summary(self) -> dict:
        return {"at": self.at, "files": self.files, "rows": self.rows, "quarantined": self.quarantined,
                "months": self.months, "closed": self.closed,
                "restated_usd": round(sum(r["delta"] for r in self.restated), 2),
                "alerts_opened": self.alerts_opened, "seconds": self.seconds}


def get_state(con: duckdb.DuckDBPyConnection, key: str, default=None):
    con.execute("CREATE TABLE IF NOT EXISTS pipeline_state (key VARCHAR PRIMARY KEY, value VARCHAR)")
    row = con.execute("SELECT value FROM pipeline_state WHERE key = ?", [key]).fetchone()
    return json.loads(row[0]) if row else default


def set_state(con: duckdb.DuckDBPyConnection, key: str, value) -> None:
    con.execute("CREATE TABLE IF NOT EXISTS pipeline_state (key VARCHAR PRIMARY KEY, value VARCHAR)")
    con.execute("INSERT OR REPLACE INTO pipeline_state VALUES (?, ?)", [key, json.dumps(value)])


def ingest(lake: Lake, con: duckdb.DuckDBPyConnection, now: int, res: RunResult) -> list[str]:
    """Landing -> bronze and reference tables. Returns (and stores) the months to recompute."""
    transform.ensure_tables(con)
    state = get_state(con, "reference", {})
    ing = ingest_events(lake, con, now)
    ref = load_reference(lake, con, now)
    res.files, res.rows, res.quarantined = ing.files, ing.rows, ing.quarantined
    compact(lake, now)
    months = set(ing.months) | transform.open_months(now)
    if ref.get("contracts") != state.get("contracts") or ref.get("crm") != state.get("crm"):
        # Contracts or customers changed: every month may price differently.
        months |= {m for (m,) in con.execute("SELECT DISTINCT month FROM gold_charges").fetchall()}
    set_state(con, "reference", {**state, **ref})
    first = period_config()["first_month"]
    months = sorted(m for m in months if m >= first)
    set_state(con, "pending_months", months)
    return months


def revenue(con: duckdb.DuckDBPyConnection, now: int, res: RunResult) -> None:
    periods.apply_credits(con)
    periods.current(con)
    for month in months_due(con, now):
        if periods.close(con, month, now):
            res.closed.append(month)
    res.restated = periods.restate(con, now)
    periods.finance_mart(con)
    set_state(con, "last_restated", res.restated)


def reconcile(lake: Lake, con: duckdb.DuckDBPyConnection, res: RunResult) -> None:
    if not con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'ref_invoices'").fetchone()[0]:
        return
    for (month,) in con.execute("SELECT DISTINCT period FROM ref_invoices ORDER BY 1").fetchall():
        res.recon[month] = recon.reconcile(lake, con, month)


def check(con: duckdb.DuckDBPyConnection, now: int, res: RunResult) -> list[quality.Result]:
    results = quality.run_checks(con, now, restated=res.restated, recon=res.recon or None)
    res.alerts_opened = [f"{c}:{k}" for c, k, _ in quality.record(con, now, results)]
    return results


def run_once(lake: Lake, con: duckdb.DuckDBPyConnection, now: int) -> RunResult:
    t0 = time.perf_counter()
    res = RunResult(iso(now))
    months = ingest(lake, con, now, res)
    transform.run(lake, con, now, set(months))
    res.months = months
    revenue(con, now, res)
    reconcile(lake, con, res)
    check(con, now, res)
    res.seconds = round(time.perf_counter() - t0, 2)
    return res
