"""Runs the platform on a simulated clock, the way it runs in production: files land, a run every
few hours ingests what is new and recomputes the affected months, September is closed on 3 Oct at
noon, late data restates it, the billing system's invoices arrive, checks raise and resolve alerts.

`run_replay` returns the timeline and, at the end, compares every customer's month with the
simulator's ground truth. The naive pipeline (acm.naive) is run on the same landing zone at the
close and at the end, for comparison.
"""
from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path

from acm import naive, periods, pipeline, transform
from acm.ingest import Lake
from acm.pricing import iso, ts


@dataclass
class Replay:
    timeline: list[dict] = field(default_factory=list)
    alerts: list[dict] = field(default_factory=list)
    evaluation: dict = field(default_factory=dict)
    world: dict = field(default_factory=dict)
    seconds: float = 0.0


def prepare(root: Path, seed: int = 7, customers: int = 36, faults: set[str] | None = None) -> tuple[Lake, object]:
    """Simulates a world and lays its landing zone down (including the billing system's invoices)."""
    from acm.sim import billing, delivery, world

    if root.exists():
        shutil.rmtree(root)
    w = world.build(seed, customers)
    lake = Lake(root)
    delivery.write_landing(w, lake.landing, faults)
    events = delivery.emit(w, faults)
    billing.write_invoices(billing.invoices(w, events), lake.landing)
    return lake, w


def tick(lake: Lake, con, now: int) -> dict:
    return pipeline.run_once(lake, con, now).summary()


def evaluate(lake: Lake, con, world, horizon: int, now: int) -> dict:
    """Recomputes every month to the end of the simulated activity and compares with the truth."""
    from acm.sim import truth

    months = {m for (m,) in con.execute("SELECT DISTINCT month FROM gold_charges").fetchall()}
    transform.run(lake, con, now, months, horizon=horizon)
    periods.apply_credits(con)
    periods.current(con)
    periods.restate(con, now)
    periods.finance_mart(con)
    tr = truth.compute(world)
    ours = {(c, m): float(t) for c, m, t in con.execute("SELECT customer_id, month, total FROM mart_finance_revenue").fetchall()}
    # A lost stop event cannot be recovered exactly: such sessions are billed to one heartbeat after
    # the last one (docs/BILLING_RULES.md), the truth knows the real stop. Report those separately.
    inferred = set()
    for cid, a, b in con.execute("SELECT customer_id, started_at, billed_until FROM silver_vm_sessions WHERE end_source = 'inferred'").fetchall():
        inferred |= {(cid, m) for m in {iso(a)[:7], iso(max(a, b - 1))[:7]}}
    out = {}
    for month in sorted({m for _, m in tr}):
        keys = [k for k in tr if k[1] == month]
        errors = {k: abs(ours.get(k, 0.0) - tr[k].rounded()["total"]) for k in keys}
        exact = [e for k, e in errors.items() if k not in inferred]
        out[month] = {"customers": len(keys), "truth": round(sum(tr[k].rounded()["total"] for k in keys), 2),
                      "ours": round(sum(ours.get(k, 0.0) for k in keys), 2),
                      "max_customer_error": round(max(errors.values()), 2),
                      "customers_with_inferred_stop": sum(k in inferred for k in keys),
                      "max_error_without_inferred_stop": round(max(exact, default=0.0), 2),
                      "max_error_with_inferred_stop": round(max([e for k, e in errors.items() if k in inferred], default=0.0), 2)}
    closed = con.execute("SELECT month, closed_at FROM period_status").fetchall()
    for month, at in closed:
        out[month]["closed_total"] = round(periods.revenue_as_of(con, month, at), 2)
        out[month]["adjustments"] = round(float(con.execute(
            """SELECT coalesce(sum(CASE WHEN component = 'credits_applied' THEN -amount ELSE amount END), 0)
               FROM revenue_ledger WHERE month = ? AND entry_type = 'adjustment'""", [month]).fetchone()[0]), 2)
    return out


def run_replay(root: Path, seed: int = 7, customers: int = 36, faults: set[str] | None = None,
               step_hours: int = 12, with_naive: bool = True, mutate=None) -> Replay:
    """`mutate(lake, world)` may change the landing zone before the clock starts (drills use it
    for faults the simulator does not model, such as an undeclared schema change)."""
    from acm.sim import world as W

    started = time.perf_counter()
    lake, world = prepare(root, seed, customers, faults)
    if mutate:
        mutate(lake, world)
    con = lake.connect()
    rep = Replay()
    stops = list(range(W.START + 3600, W.DELIVERY_END + 1, step_hours * 3600))
    close_sep = pipeline.close_at("2026-09")
    stops = sorted(set(stops) | {close_sep, ts("2026-10-04T12:00:00Z"), W.DELIVERY_END})
    naive_at_close = {}
    for now in stops:
        rep.timeline.append(tick(lake, con, now))
        if with_naive and now == close_sep:
            naive_at_close = naive.revenue(lake, now)
    rep.alerts = [dict(zip(["check", "key", "severity", "opened_at", "resolved_at", "message"], r)) for r in
                  con.execute("SELECT check_name, key, severity, opened_at, resolved_at, message FROM alerts ORDER BY opened_at").fetchall()]
    for a in rep.alerts:
        # The simulated producers stop at W.END; alerts after that are the platform noticing.
        a["after_simulation_end"] = a["opened_at"] > W.END
        a["opened_at"], a["resolved_at"] = iso(a["opened_at"]), a["resolved_at"] and iso(a["resolved_at"])
    rep.evaluation = evaluate(lake, con, world, W.END, W.DELIVERY_END)
    rows, bad, files = con.execute("SELECT sum(rows), sum(quarantined), count(*) FROM ingest_log").fetchone()
    rep.world = {"seed": seed, "customers": len(world.customers), "vms": len(world.vms),
                 "commitments": len(world.commitments), "events_ingested": int(rows), "events_quarantined": int(bad),
                 "landing_files": int(files), "pipeline_runs": len(rep.timeline), "step_hours": step_hours,
                 "faults": sorted(faults) if faults is not None else "all",
                 "bronze_files_after_compaction": sum(1 for _ in lake.bronze.rglob("*.parquet"))}
    if with_naive:
        from acm.sim import truth
        tr = truth.compute(world)
        final = naive.revenue(lake, W.DELIVERY_END)
        rep.evaluation["naive"] = {}
        for month in ["2026-09", "2026-10"]:
            keys = {k for k in tr if k[1] == month}
            rep.evaluation["naive"][month] = {
                "truth": round(sum(tr[k].rounded()["total"] for k in keys), 2),
                "naive": round(sum(v for k, v in final.items() if k[1] == month), 2),
                "naive_at_close": round(sum(v for k, v in naive_at_close.items() if k[1] == month), 2),
            }
    rep.seconds = round(time.perf_counter() - started, 1)
    con.close()
    return rep


def save(rep: Replay, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"world": rep.world, "evaluation": rep.evaluation, "alerts": rep.alerts,
                                "timeline": rep.timeline, "seconds": rep.seconds}, indent=1))
