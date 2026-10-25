"""Turns what happened into what the data team receives: event files from four producers, a daily
CRM export and the contract register, each with the faults real collectors have.

Faults (each can be switched off for a drill, see FAULTS):
  retries          1% of events delivered twice with the same event_id
  replays          after an agent restart, the last heartbeats are re-sent with new event_ids
  late             0.5% of events arrive 1 to 48 hours late
  outage           the eu-north1 collector is down from 30 Sep 18:00 to 1 Oct 06:00; its events
                   arrive on 4 Oct 09:00, after September has been closed
  lost_stops       2% of VM stop events are never sent (heartbeats just end)
  skew             15% of hosts have clocks off by 30 s to 5 min (heartbeats only; the control
                   plane's lifecycle events are on true time)
  schema_v2        eu-west1's control plane switches to lifecycle schema v2 on 10 Oct
                   (gpu_type -> accelerator, gpu_count -> accelerator_count)
  unit_bug         us-central1's inference gateway reports tokens in thousands from 6 Oct 00:00
                   to 7 Oct 12:00, then re-sends the same events corrected (revision 2) on 8 Oct
"""
from __future__ import annotations

import csv
import gzip
import json
import random
import shutil
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from acm.pricing import iso, ts
from acm.sim.world import DELIVERY_END, END, HEARTBEAT_S, World

FAULTS = ("retries", "replays", "late", "outage", "lost_stops", "skew", "schema_v2", "unit_bug")
OUTAGE = (ts("2026-09-30T18:00:00Z"), ts("2026-10-01T06:00:00Z"))
OUTAGE_DELIVERED = ts("2026-10-04T09:00:00Z")
SCHEMA_V2_FROM = ts("2026-10-10T00:00:00Z")
UNIT_BUG = (ts("2026-10-06T00:00:00Z"), ts("2026-10-07T12:00:00Z"))
UNIT_FIX_DELIVERED = ts("2026-10-08T10:00:00Z")
RENEWAL_BOOKED = ts("2026-09-10T00:00:00Z")


@dataclass
class Delivered:
    producer: str
    delivered_at: int
    record: dict


class Emitter:
    def __init__(self, world: World, faults: set[str], seed: int):
        self.world = world
        self.faults = faults
        self.rng = random.Random(seed * 31 + 1)
        self.out: list[Delivered] = []
        self.n = 0
        self.lost_stop_vms: set[str] = set()

    def eid(self, prefix: str) -> str:
        self.n += 1
        return f"{prefix}-{self.n:08d}"

    def deliver(self, producer: str, true_time: int, region: str, record: dict, base_delay: tuple[int, int]):
        delay = self.rng.randint(*base_delay)
        at = true_time + delay
        if "late" in self.faults and self.rng.random() < 0.005:
            at += self.rng.randint(3600, 48 * 3600)
        if "outage" in self.faults and region == "eu-north1" and OUTAGE[0] <= true_time < OUTAGE[1]:
            at = OUTAGE_DELIVERED + self.rng.randint(0, 1800)
        at = min(at, DELIVERY_END - 1)
        self.out.append(Delivered(producer, at, record))
        if "retries" in self.faults and self.rng.random() < 0.01:
            self.out.append(Delivered(producer, min(at + self.rng.randint(1, 900), DELIVERY_END - 1), dict(record)))

    def lifecycle(self):
        for vm in self.world.vms:
            events = [("created", vm.created), ("started", vm.started)]
            if vm.stopped < END:
                if "lost_stops" in self.faults and self.rng.random() < 0.02:
                    self.lost_stop_vms.add(vm.vm_id)
                else:
                    events.append(("stopped", vm.stopped))
                events.append(("deleted", vm.stopped + 30))
            for kind, t in events:
                if t >= END:
                    continue
                rec = {"event_id": self.eid("lc"), "schema_version": 1, "vm_id": vm.vm_id,
                       "customer_id": vm.customer_id, "project_id": vm.project_id, "event_type": kind,
                       "event_time": iso(t), "region": vm.region, "gpu_type": vm.gpu_type,
                       "gpu_count": vm.gpu_count, "host_id": vm.host_id}
                if "schema_v2" in self.faults and vm.region == "eu-west1" and t >= SCHEMA_V2_FROM:
                    rec["schema_version"] = 2
                    rec["accelerator"] = rec.pop("gpu_type")
                    rec["accelerator_count"] = rec.pop("gpu_count")
                self.deliver("vm_lifecycle", t, vm.region, rec, (1, 20))

    def heartbeats(self):
        for vm in self.world.vms:
            skew = vm.host_skew_s if "skew" in self.faults else 0
            end = min(vm.stopped, END)
            seq, t = 0, vm.started
            while t < end:
                in_gap = any(a <= t < b for a, b in vm.restart_gaps)
                if not in_gap:
                    rec = {"event_id": self.eid("hb"), "vm_id": vm.vm_id, "host_id": vm.host_id, "seq": seq,
                           "host_time": iso(t + skew), "gpu_util_pct": round(self.rng.uniform(5, 99), 1),
                           "gpu_mem_used_gb": round(self.rng.uniform(4, 75), 1)}
                    self.deliver("gpu_heartbeat", t, vm.region, rec, (5, 120))
                    for a, b in vm.restart_gaps:
                        if "replays" in self.faults and b <= t < b + HEARTBEAT_S:
                            # The restarted agent re-sends the last two heartbeats it spooled before the
                            # gap, under new event_ids: only (vm_id, seq) tells them apart.
                            last = (a - vm.started - 1) // HEARTBEAT_S
                            for s in (last - 1, last):
                                if s >= 0:
                                    old_t = vm.started + s * HEARTBEAT_S
                                    dup = dict(rec, event_id=self.eid("hb"), seq=s, host_time=iso(old_t + skew))
                                    self.deliver("gpu_heartbeat", t, vm.region, dup, (5, 120))
                seq += 1
                t += HEARTBEAT_S

    def inference(self):
        for w in self.world.inference:
            rec = {"event_id": self.eid("inf"), "revision": 1, "customer_id": w.customer_id, "model": w.model,
                   "region": w.region, "window_start": iso(w.window_start), "window_seconds": 600,
                   "requests": w.requests, "input_tokens": w.input_tokens, "output_tokens": w.output_tokens,
                   "api_key_owner": w.api_key_owner}
            bug = "unit_bug" in self.faults and w.region == "us-central1" and UNIT_BUG[0] <= w.window_start < UNIT_BUG[1]
            if bug:
                wrong = dict(rec, input_tokens=w.input_tokens // 1000, output_tokens=w.output_tokens // 1000)
                self.deliver("inference_usage", w.window_start + 600, w.region, wrong, (30, 300))
                fixed = dict(rec, revision=2)
                self.out.append(Delivered("inference_usage", UNIT_FIX_DELIVERED + self.rng.randint(0, 600), fixed))
            else:
                self.deliver("inference_usage", w.window_start + 600, w.region, rec, (30, 300))

    def storage(self):
        for s in self.world.storage:
            rec = {"event_id": self.eid("st"), "bucket_id": s.bucket_id, "customer_id": s.customer_id,
                   "region": s.region, "snapshot_time": iso(s.snapshot_time), "bytes_stored": s.bytes_stored}
            self.deliver("storage_snapshot", s.snapshot_time, s.region, rec, (10, 200))


def emit(world: World, faults: set[str] | None = None) -> list[Delivered]:
    faults = set(FAULTS) if faults is None else set(faults)
    e = Emitter(world, faults, world.seed)
    e.lifecycle()
    e.heartbeats()
    e.inference()
    e.storage()
    return e.out


def write_landing(world: World, out_dir: Path, faults: set[str] | None = None) -> dict:
    """Writes the landing zone exactly as collectors would leave it on object storage:

    landing/<producer>/<YYYY-MM-DD>/<HH>.jsonl.gz   one file per producer and delivery hour, visible
                                                    at the end of that hour
    landing/crm/<YYYY-MM-DD>.csv                    daily customer export (contains PII)
    landing/contracts/<YYYY-MM-DD>/commitments.csv  daily contract register export
    landing/contracts/<YYYY-MM-DD>/credit_grants.csv
    """
    if out_dir.exists():
        shutil.rmtree(out_dir)
    events = emit(world, faults)
    files: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for d in events:
        files[(d.producer, d.delivered_at // 3600 * 3600)].append(d.record)
    for (producer, hour), records in files.items():
        day = iso(hour)[:10]
        path = out_dir / producer / day / f"{iso(hour)[11:13]}.jsonl.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "at") as f:
            for r in records:
                f.write(json.dumps(r, separators=(",", ":")) + "\n")
    day = world_start = ts("2026-09-01T00:00:00Z")
    while day < DELIVERY_END:
        d = iso(day)[:10]
        crm = out_dir / "crm" / f"{d}.csv"
        crm.parent.mkdir(parents=True, exist_ok=True)
        with open(crm, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["customer_id", "name", "contact_email", "country", "segment"])
            for c in world.customers:
                w.writerow([c.customer_id, c.name, c.contact_email, c.country, c.segment])
        reg = out_dir / "contracts" / d
        reg.mkdir(parents=True, exist_ok=True)
        with open(reg / "commitments.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["commitment_id", "customer_id", "gpu_type", "region", "gpus", "usd_per_gpu_hour",
                        "starts", "ends", "replaces"])
            for k in world.commitments:
                if k.replaces and day < RENEWAL_BOOKED:
                    continue  # the renewal is booked on 10 Sep
                w.writerow([k.commitment_id, k.customer_id, k.gpu_type, k.region, k.gpus, k.usd_per_gpu_hour,
                            iso(k.starts), iso(k.ends), k.replaces or ""])
        with open(reg / "credit_grants.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["grant_id", "customer_id", "usd", "granted_at", "expires_at"])
            for g in world.grants:
                w.writerow([g.grant_id, g.customer_id, g.usd, iso(g.granted_at), iso(g.expires_at)])
        day += 86400
    by_producer: dict[str, int] = defaultdict(int)
    for d in events:
        by_producer[d.producer] += 1
    return {"events": len(events), "by_producer": dict(by_producer), "files": len(files), "start": world_start}
