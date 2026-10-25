"""Fault drills: each production fault on its own, on a fresh simulated world, through the whole
replay (ingest, close, restatement, reconciliation, checks). For each drill:

  detected  the check that should notice the fault opened an alert (or, for faults the platform
            absorbs silently by design, no error alert opened)
  correct   every customer-month matches the ground truth to the cent (for lost stop events:
            within one heartbeat period of GPU time, which is the billing rule)
  naive     what the naive pipeline (acm.naive) reports for the same data, against the truth

Alerts opened after the simulated producers stop (W.END) are the platform noticing that; they are
listed but not scored.
"""
from __future__ import annotations

import gzip
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from acm.pricing import ts
from acm.replay import run_replay


def _undeclared_rename(lake, world) -> None:
    """eu-west1's storage service renames bytes_stored -> size_bytes on 12 Oct without telling
    anyone or bumping schema_version."""
    since = ts("2026-10-12T00:00:00Z")
    for p in sorted((lake.landing / "storage_snapshot").glob("*/*.jsonl.gz")):
        if ts(f"{p.parent.name}T00:00:00Z") < since - 86400:
            continue
        lines, changed = [], False
        for line in gzip.decompress(p.read_bytes()).decode().splitlines():
            r = json.loads(line)
            if r.get("region") == "eu-west1" and ts(r["snapshot_time"]) >= since:
                r["size_bytes"] = r.pop("bytes_stored")
                changed = True
            lines.append(json.dumps(r))
        if changed:
            p.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode(), mtime=0))


@dataclass
class Drill:
    name: str
    faults: list[str]
    expect: list[str]                       # checks that must open an error/warn alert
    description: str
    tolerance_usd: float = 0.01
    mutate: object = None
    corrects: bool = True                   # False: detection only (data is held back in quarantine)


DRILLS = [
    Drill("clean", [], [], "no faults: nothing may page"),
    Drill("duplicates", ["retries", "replays"], ["duplicates"],
          "producers retry (same event_id) and node agents replay their spool (new event_id, same seq)"),
    Drill("late", ["late"], [], "0.5% of events arrive 1 to 48 hours late (absorbed: recomputed, restated if closed)"),
    Drill("outage", ["outage"], ["freshness", "restatement"],
          "eu-north1 collector down 30 Sep 18:00 to 1 Oct 06:00, delivered 4 Oct, after September closed"),
    # One heartbeat (300 s) of the largest VM, 8 x B200 at $5.50/h, is $3.67: the most a lost stop
    # event can move a bill under the billing rule.
    Drill("lost_stops", ["lost_stops"], ["inferred_session_end"],
          "2% of stop events are never sent", tolerance_usd=3.67),
    Drill("clock_skew", ["skew"], ["clock_skew"], "15% of hosts have clocks off by 30 s to 5 min"),
    Drill("schema_v2", ["schema_v2"], [], "eu-west1 ships lifecycle schema v2 (declared in the contract): upcast, no alert"),
    Drill("unit_bug", ["unit_bug"], ["token_plausibility"],
          "us-central1 gateway reports tokens in thousands for 36 h, then re-sends corrected revisions"),
    Drill("undeclared_rename", [], ["quarantine", "freshness"],
          "eu-west1 storage renames a field without a contract change: records are quarantined, not guessed",
          mutate=_undeclared_rename, corrects=False),
]


@dataclass
class DrillResult:
    name: str
    description: str
    faults: list[str]
    expected: list[str]
    opened: list[str]
    detected: bool
    false_alarms: list[str]
    max_error_usd: float
    correct: bool
    naive_error_usd: dict = field(default_factory=dict)
    seconds: float = 0.0


def run(root: Path, customers: int = 12, step_hours: int = 24, only: list[str] | None = None, seed: int = 7) -> list[DrillResult]:
    out = []
    for d in DRILLS:
        if only and d.name not in only:
            continue
        rep = run_replay(root / d.name, seed=seed, customers=customers, faults=set(d.faults),
                         step_hours=step_hours, mutate=d.mutate)
        scored = [a for a in rep.alerts if not a["after_simulation_end"]]
        opened = sorted({a["check"] for a in scored})
        ev = rep.evaluation
        months = [m for m in ev if m != "naive"]
        err = max(ev[m]["max_customer_error"] for m in months)
        out.append(DrillResult(
            d.name, d.description, d.faults, d.expect, opened,
            detected=all(c in opened for c in d.expect),
            false_alarms=sorted(set(opened) - set(d.expect) - {"restatement"} if d.name != "clean" else set(opened)),
            max_error_usd=err, correct=(err <= d.tolerance_usd) if d.corrects else False,
            naive_error_usd={m: round(v["naive"] - v["truth"], 2) for m, v in ev.get("naive", {}).items()},
            seconds=rep.seconds))
    return out


def markdown(results: list[DrillResult], customers: int, step_hours: int) -> str:
    lines = ["# Fault drills", "",
             f"Generated by `acm drill` ({customers} customers, a pipeline run every {step_hours} h, seed 7). "
             "Each fault runs alone on a fresh world; see `src/acm/drills.py` for what is scored.", "",
             "| drill | fault | expected alert | opened | detected | max customer-month error | correct | naive Sep / Oct vs truth |",
             "|---|---|---|---|---|---|---|---|"]
    for r in results:
        naive = " / ".join(f"{v:+,.2f}" for v in r.naive_error_usd.values())
        lines.append(f"| {r.name} | {r.description} | {', '.join(r.expected) or 'none'} | {', '.join(r.opened) or 'none'} | "
                     f"{'yes' if r.detected else 'NO'} | ${r.max_error_usd:,.2f} | "
                     f"{'yes' if r.correct else ('held back' if r.name == 'undeclared_rename' else 'NO')} | {naive} |")
    lines += ["", "Other alerts opened (not expected for that drill):", ""]
    lines += [f"- {r.name}: {', '.join(r.false_alarms)}" for r in results if r.false_alarms] or ["- none"]
    return "\n".join(lines) + "\n"


def save(results: list[DrillResult], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([asdict(r) for r in results], indent=1))
