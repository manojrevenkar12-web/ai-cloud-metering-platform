"""The billing system: a separate team's service that issues September invoices on 3 Oct 12:00 from
the events it had received by then. It is the thing the data platform reconciles against, and it
has the kinds of defects such systems have. Each is a reconciliation cause the pipeline must find:

  late_usage             events that arrived after the cutoff are not billed (the outage window)
  missing_stop_event     a VM with no stop event is billed until month end
  hourly_rounding        us-central1 sessions are rounded up to whole hours (a legacy region rule)
  overlapping_commitment a renewal that replaces a commitment is charged on top of it on the overlap
  duplicate_events       inference retries (same event_id) are not de-duplicated
"""
from __future__ import annotations

import csv
import math
from collections import defaultdict
from pathlib import Path

from acm.pricing import lookup, price_rows, ts
from acm.sim.delivery import Delivered
from acm.sim.truth import gpu_charges

CUTOFF = ts("2026-10-03T12:00:00Z")
SEP = (ts("2026-09-01T00:00:00Z"), ts("2026-10-01T00:00:00Z"))


def _t(s: str) -> int:
    return ts(s)


def invoices(world, delivered: list[Delivered]) -> dict[str, dict]:
    prices = price_rows()
    seen = [d for d in delivered if d.delivered_at < CUTOFF]
    out: dict[str, dict] = defaultdict(lambda: defaultdict(float))

    # Commitments: every row in the register, overlaps included.
    for k in world.commitments:
        lo, hi = max(k.starts, SEP[0]), min(k.ends, SEP[1])
        if hi > lo:
            out[k.customer_id]["commitment"] += k.gpus * (hi - lo) / 3600 * k.usd_per_gpu_hour

    # GPU sessions from lifecycle events (de-duplicated by event_id, which this team did do).
    sessions: dict[str, dict] = {}
    seen_ids = set()
    for d in seen:
        if d.producer != "vm_lifecycle" or d.record["event_id"] in seen_ids:
            continue
        seen_ids.add(d.record["event_id"])
        r = d.record
        s = sessions.setdefault(r["vm_id"], {"customer": r["customer_id"], "region": r["region"],
                                             "gpu": r.get("gpu_type"), "count": r.get("gpu_count")})
        if r["event_type"] == "started":
            s["start"] = _t(r["event_time"])
        elif r["event_type"] == "stopped":
            s["stop"] = _t(r["event_time"])
    billed = []
    for sess in sessions.values():
        if "start" not in sess or sess["gpu"] is None:
            continue
        a = sess["start"]
        b = sess.get("stop", SEP[1])  # no stop event: billed to month end
        dur = max(b - a, 60)
        if sess["region"] == "us-central1":
            dur = math.ceil(dur / 3600) * 3600
        lo, hi = max(a, SEP[0]), min(a + dur, SEP[1])
        if hi > lo:
            billed.append((sess["customer"], sess["gpu"], sess["region"], sess["count"], lo, hi))
    # Every commitment row is applied for its full term, renewals on top of what they replace.
    spans = [(k.customer_id, k.gpu_type, k.region, k.gpus, k.starts, k.ends) for k in world.commitments]
    for (cid, month), (on_demand, _) in gpu_charges(billed, spans, prices).items():
        if month == "2026-09":
            out[cid]["gpu_on_demand"] += on_demand

    for d in seen:  # no de-duplication here
        r = d.record
        if d.producer == "inference_usage" and SEP[0] <= _t(r["window_start"]) < SEP[1]:
            pin = lookup(prices, f"inference:{r['model']}:input", _t(r["window_start"]))
            pout = lookup(prices, f"inference:{r['model']}:output", _t(r["window_start"]))
            out[r["customer_id"]]["inference"] += r["input_tokens"] / 1e6 * pin.usd + r["output_tokens"] / 1e6 * pout.usd
    storage_ids = set()
    for d in seen:
        r = d.record
        if d.producer == "storage_snapshot" and r["event_id"] not in storage_ids and SEP[0] <= _t(r["snapshot_time"]) < SEP[1]:
            storage_ids.add(r["event_id"])
            p = lookup(prices, "storage:object", _t(r["snapshot_time"]))
            out[r["customer_id"]]["storage"] += r["bytes_stored"] / 1e9 * p.usd / 730

    result = {}
    grants = defaultdict(float)
    for g in world.grants:
        grants[g.customer_id] += g.usd
    for c in world.customers:
        row = out.get(c.customer_id, {})
        charges = {k: row.get(k, 0.0) for k in ("gpu_on_demand", "commitment", "inference", "storage")}
        eligible = charges["gpu_on_demand"] + charges["inference"] + charges["storage"]
        credits = min(grants.get(c.customer_id, 0.0), eligible)
        total = sum(charges.values()) - credits
        if total == 0 and not any(charges.values()):
            continue
        result[c.customer_id] = {**{k: round(v, 2) for k, v in charges.items()},
                                 "credits_applied": round(credits, 2), "total": round(total, 2)}
    return result


def write_invoices(rows: dict[str, dict], landing: Path) -> Path:
    path = landing / "billing" / "invoices_2026-09.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["invoice_id", "customer_id", "period", "issued_at", "gpu_on_demand", "commitment",
                    "inference", "storage", "credits_applied", "total"])
        for i, (cid, r) in enumerate(sorted(rows.items())):
            w.writerow([f"INV-2026-09-{i + 1:04d}", cid, "2026-09", "2026-10-03T12:00:00Z", r["gpu_on_demand"],
                        r["commitment"], r["inference"], r["storage"], r["credits_applied"], r["total"]])
    return path
