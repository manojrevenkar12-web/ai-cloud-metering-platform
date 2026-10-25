"""acm: the command line.

    acm simulate --root data/lake               lay down a simulated landing zone (all faults)
    acm run --root data/lake [--now ISO]        one pipeline run (what the scheduler calls)
    acm replay                                  the whole simulated period; writes reports/
    acm drill [--only outage unit_bug]          each fault alone; writes reports/DRILLS.md
    acm erase --root data/lake --email x@y      GDPR erasure with verification
    acm contracts --check [--against REF]       validate contracts; fail on breaking changes vs a git ref
    acm lineage --root data/lake                column lineage (OpenLineage JSON + docs/LINEAGE.md)
    acm spark-rate --root data/replay           the rating step on Spark, compared with DuckDB
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path

import yaml

from acm.pricing import ROOT, ts

REPORTS = ROOT / "reports"


def _now(value: str | None) -> int:
    return ts(value) if value else int(time.time())


def cmd_simulate(a) -> int:
    from acm.replay import prepare
    faults = set(a.faults) if a.faults is not None else None
    lake, w = prepare(Path(a.root), a.seed, a.customers, faults)
    print(f"{len(w.customers)} customers, {len(w.vms)} VMs; landing zone at {lake.landing}")
    return 0


def cmd_run(a) -> int:
    from acm import pipeline
    from acm.ingest import Lake
    lake = Lake(Path(a.root))
    con = lake.connect()
    try:
        res = pipeline.run_once(lake, con, _now(a.now))
    finally:
        con.close()
    print(json.dumps(res.summary(), indent=1))
    return 0


def cmd_replay(a) -> int:
    from acm import lineage, reports
    from acm.replay import run_replay, save
    root = Path(a.root)
    rep = run_replay(root, a.seed, a.customers, step_hours=a.step_hours)
    out = Path(a.out)
    save(rep, out / "replay.json")
    written = reports.write_all(out / "replay.json", root / "warehouse.duckdb", out)
    import duckdb
    con = duckdb.connect(str(root / "warehouse.duckdb"), read_only=True)
    try:
        nodes = lineage.build(con)
    finally:
        con.close()
    lineage.write(nodes, out / "lineage.json", event_time="2026-10-18T00:00:00Z")
    (ROOT / "docs" / "LINEAGE.md").write_text(lineage.markdown(nodes))
    print(json.dumps(rep.evaluation, indent=1))
    print("wrote", ", ".join(str(p.relative_to(ROOT)) if p.is_relative_to(ROOT) else str(p) for p in written))
    sep = rep.evaluation["2026-09"]
    return 0 if sep["max_error_without_inferred_stop"] <= 0.01 else 1


def cmd_drill(a) -> int:
    from acm import drills
    results = drills.run(Path(a.root), a.customers, a.step_hours, a.only)
    out = Path(a.out)
    drills.save(results, out / "drills.json")
    (out / "DRILLS.md").write_text(drills.markdown(results, a.customers, a.step_hours))
    for r in results:
        print(f"{r.name:18} detected={r.detected!s:5} correct={r.correct!s:5} error=${r.max_error_usd:.2f} "
              f"alerts={','.join(r.opened) or '-'}")
    failed = [r.name for r in results if not r.detected or (not r.correct and r.name != "undeclared_rename")]
    if failed:
        print("FAILED:", ", ".join(failed))
    return 1 if failed else 0


def cmd_erase(a) -> int:
    from acm import privacy
    from acm.ingest import Lake
    lake = Lake(Path(a.root))
    con = lake.connect()
    try:
        rep = privacy.erase(lake, con, a.email, _now(a.now), a.request_id)
    finally:
        con.close()
    print(json.dumps({**asdict(rep), "verified": rep.verified}, indent=1))
    return 0 if rep.verified else 1


def _git_show(ref: str, path: str) -> str | None:
    r = subprocess.run(["git", "show", f"{ref}:{path}"], cwd=ROOT, capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


def cmd_contracts(a) -> int:
    from acm import contracts
    problems = []
    for p in sorted((ROOT / "contracts" / "producers").glob("*.yaml")):
        c = contracts.load_producer(p.stem)
        for f in c.dedup_key + [c.event_time] + c.pii:
            if f not in c.fields:
                problems.append(f"{p.name}: {f} is not a field")
        for v, spec in c.versions.items():
            for old, new in ((spec or {}).get("renames") or {}).items():
                if new not in c.fields:
                    problems.append(f"{p.name}: version {v} renames {old} to unknown field {new}")
    for p in sorted((ROOT / "contracts" / "marts").glob("*.yaml")):
        m = contracts.load_mart(p.stem)
        for g in m["grain"]:
            if g not in m["columns"]:
                problems.append(f"{p.name}: grain column {g} is not a column")
    breaking = []
    if a.against:
        for kind, differ in [("producers", contracts.diff_producer), ("marts", contracts.diff_mart)]:
            for p in sorted((ROOT / "contracts" / kind).glob("*.yaml")):
                old = _git_show(a.against, f"contracts/{kind}/{p.name}")
                if old is None:
                    continue
                for problem in differ(yaml.safe_load(old), yaml.safe_load(p.read_text())):
                    breaking.append(f"{kind}/{p.name}: {problem}")
    for x in problems:
        print("invalid:", x)
    for x in breaking:
        print("breaking:", x)
    if not problems and not breaking:
        print("contracts ok" + (f" (no breaking changes against {a.against})" if a.against else ""))
    return 1 if problems or breaking else 0


def cmd_lineage(a) -> int:
    import duckdb

    from acm import lineage
    con = duckdb.connect(str(Path(a.root) / "warehouse.duckdb"), read_only=True)
    try:
        nodes = lineage.build(con)
    finally:
        con.close()
    if a.trace:
        table, col = a.trace.split(".", 1)
        print("\n".join(sorted(lineage.trace(nodes, table, col))))
        return 0
    if a.impact:
        table, col = a.impact.split(".", 1)
        print("\n".join(sorted(lineage.impact(nodes, table, col))))
        return 0
    lineage.write(nodes, Path(a.out) / "lineage.json")
    (ROOT / "docs" / "LINEAGE.md").write_text(lineage.markdown(nodes))
    unresolved = sum(len(n.unresolved) for n in nodes.values())
    print(f"{len(nodes)} tables, {sum(len(n.columns) for n in nodes.values())} columns, {unresolved} unresolved")
    return 0


def cmd_spark_rate(a) -> int:
    import duckdb

    from acm import spark_rating
    root = Path(a.root)
    con = duckdb.connect(str(root / "warehouse.duckdb"), read_only=True)
    try:
        silver = spark_rating.export_silver(con, root / "export" / "silver")
        spark = spark_rating.session(cores=a.cores)
        spark.sparkContext.setLogLevel("ERROR")
        months = a.months or [m for (m,) in con.execute("SELECT month FROM gold_runs ORDER BY 1").fetchall()]
        t0 = time.perf_counter()
        result = spark_rating.compare(con, spark, silver, months)
        result["spark_seconds"] = round(time.perf_counter() - t0, 1)
        spark.stop()
    finally:
        con.close()
    out = Path(a.out) / "spark_rating.json"
    out.write_text(json.dumps(result, indent=1))
    print(json.dumps(result, indent=1))
    return 0 if result["max_difference_usd"] <= 0.01 else 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="acm", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("simulate")
    s.add_argument("--root", default="data/lake")
    s.add_argument("--seed", type=int, default=7)
    s.add_argument("--customers", type=int, default=36)
    s.add_argument("--faults", nargs="*", default=None, help="subset of faults (default: all)")
    s.set_defaults(fn=cmd_simulate)

    s = sub.add_parser("run")
    s.add_argument("--root", default="data/lake")
    s.add_argument("--now", default=None, help="ISO time in UTC (default: the wall clock)")
    s.set_defaults(fn=cmd_run)

    s = sub.add_parser("replay")
    s.add_argument("--root", default="data/replay")
    s.add_argument("--seed", type=int, default=7)
    s.add_argument("--customers", type=int, default=36)
    s.add_argument("--step-hours", type=int, default=12)
    s.add_argument("--out", default=str(REPORTS))
    s.set_defaults(fn=cmd_replay)

    s = sub.add_parser("drill")
    s.add_argument("--root", default="data/drills")
    s.add_argument("--customers", type=int, default=8)
    s.add_argument("--step-hours", type=int, default=24)
    s.add_argument("--only", nargs="*", default=None)
    s.add_argument("--out", default=str(REPORTS))
    s.set_defaults(fn=cmd_drill)

    s = sub.add_parser("erase")
    s.add_argument("--root", default="data/lake")
    s.add_argument("--email", required=True)
    s.add_argument("--request-id", required=True)
    s.add_argument("--now", default=None)
    s.set_defaults(fn=cmd_erase)

    s = sub.add_parser("contracts")
    s.add_argument("--check", action="store_true", help="validate (always on)")
    s.add_argument("--against", default=None, help="git ref to diff against, e.g. origin/main")
    s.set_defaults(fn=cmd_contracts)

    s = sub.add_parser("lineage")
    s.add_argument("--root", default="data/replay")
    s.add_argument("--out", default=str(REPORTS))
    s.add_argument("--trace", default=None, help="table.column: where does it come from")
    s.add_argument("--impact", default=None, help="table.column: what changes if it changes")
    s.set_defaults(fn=cmd_lineage)

    s = sub.add_parser("spark-rate", help="rate on Spark and compare with DuckDB to the cent")
    s.add_argument("--root", default="data/replay")
    s.add_argument("--months", nargs="*", default=None)
    s.add_argument("--cores", type=int, default=2)
    s.add_argument("--out", default=str(REPORTS))
    s.set_defaults(fn=cmd_spark_rate)

    a = p.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
