"""Shared fixtures.

`LandingBuilder` writes landing files by hand for the small, exact tests: a few VMs and windows
whose right answer can be worked out on paper. `small_lake` is a simulated world (4 customers,
all faults) after two pipeline runs, for tests that need the whole platform.
"""
from __future__ import annotations

import csv
import gzip
import itertools
import json
import shutil
from pathlib import Path

import pytest

from acm import pipeline, transform
from acm.ingest import Lake
from acm.pricing import iso, ts

_ids = itertools.count(1)


class LandingBuilder:
    def __init__(self, root: Path):
        self.lake = Lake(root)
        self.files: dict[tuple[str, str], list[dict]] = {}

    def add(self, producer: str, arrives: str, record: dict) -> dict:
        """Puts `record` into the landing file for the hour `arrives` (ISO)."""
        hour = iso(ts(arrives) // 3600 * 3600)
        self.files.setdefault((producer, hour), []).append(record)
        return record

    # ------------------------------------------------------------------ records
    def vm(self, vm_id: str, customer: str, started: str, stopped: str | None = None, region: str = "eu-north1",
           gpu: str = "h100", gpus: int = 8, arrives: str | None = None, schema_version: int = 1) -> None:
        events = [("created", started), ("started", started)] + ([("stopped", stopped)] if stopped else [])
        for kind, at in events:
            rec = {"event_id": f"e{next(_ids)}", "schema_version": schema_version, "vm_id": vm_id,
                   "customer_id": customer, "project_id": "p1", "event_type": kind, "event_time": at,
                   "region": region, "host_id": f"h-{vm_id}"}
            if schema_version == 2:
                rec.update(accelerator=gpu, accelerator_count=gpus)
            else:
                rec.update(gpu_type=gpu, gpu_count=gpus)
            self.add("vm_lifecycle", arrives or at, rec)

    def beats(self, vm_id: str, started: str, n: int, skew_s: int = 0, arrives: str | None = None,
              event_prefix: str = "b") -> None:
        for seq in range(n):
            t = ts(started) + seq * 300
            self.add("gpu_heartbeat", arrives or iso(t), {
                "event_id": f"{event_prefix}{next(_ids)}", "vm_id": vm_id, "host_id": f"h-{vm_id}", "seq": seq,
                "host_time": iso(t + skew_s), "gpu_util_pct": 50.0, "gpu_mem_used_gb": 40.0})

    def inference(self, customer: str, window: str, input_tokens: int, output_tokens: int, requests: int = 100,
                  model: str = "llama-70b", revision: int = 1, event_id: str | None = None, arrives: str | None = None,
                  owner: str = "ops@acme.example", region: str = "eu-north1") -> dict:
        return self.add("inference_usage", arrives or iso(ts(window) + 600), {
            "event_id": event_id or f"i{next(_ids)}", "revision": revision, "customer_id": customer, "model": model,
            "region": region, "window_start": window, "window_seconds": 600, "requests": requests,
            "input_tokens": input_tokens, "output_tokens": output_tokens, "api_key_owner": owner})

    def storage(self, customer: str, at: str, gb: float, region: str = "eu-north1") -> None:
        self.add("storage_snapshot", at, {"event_id": f"s{next(_ids)}", "bucket_id": f"bk-{customer}",
                                          "customer_id": customer, "region": region, "snapshot_time": at,
                                          "bytes_stored": int(gb * 1e9)})

    # ------------------------------------------------------------------ write
    def write(self, customers=("c001", "c002"), commitments: list[dict] | None = None,
              credits: list[dict] | None = None, day: str = "2026-09-01") -> Lake:
        for (producer, hour), recs in self.files.items():
            p = self.lake.landing / producer / hour[:10] / f"{hour[11:13]}.jsonl.gz"
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(gzip.compress("".join(json.dumps(r) + "\n" for r in recs).encode(), mtime=0))
        crm = self.lake.landing / "crm" / f"{day}.csv"
        crm.parent.mkdir(parents=True, exist_ok=True)
        with open(crm, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["customer_id", "name", "contact_email", "country", "segment"])
            for c in customers:
                w.writerow([c, f"{c} GmbH", f"ops@{c}.example", "DE", "startup"])
        reg = self.lake.landing / "contracts" / day
        reg.mkdir(parents=True, exist_ok=True)
        with open(reg / "commitments.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["commitment_id", "customer_id", "gpu_type", "region", "gpus", "usd_per_gpu_hour", "starts", "ends", "replaces"])
            for k in commitments or []:
                w.writerow([k["id"], k["customer"], k.get("gpu", "h100"), k.get("region", "eu-north1"), k["gpus"],
                            k["rate"], k["starts"], k["ends"], k.get("replaces", "")])
        with open(reg / "credit_grants.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["grant_id", "customer_id", "usd", "granted_at", "expires_at"])
            for g in credits or []:
                w.writerow([g["id"], g["customer"], g["usd"], g.get("granted", "2026-08-01T00:00:00Z"), "2027-08-01T00:00:00Z"])
        self.files.clear()
        return self.lake


@pytest.fixture
def landing(tmp_path) -> LandingBuilder:
    return LandingBuilder(tmp_path / "lake")


def run_at(lake: Lake, now: str, months: set[str] | None = None) -> pipeline.RunResult:
    con = lake.connect()
    try:
        if months is None:
            return pipeline.run_once(lake, con, ts(now))
        transform.ensure_tables(con)
        res = pipeline.RunResult(now)
        pipeline.ingest(lake, con, ts(now), res)
        transform.run(lake, con, ts(now), months)
        pipeline.revenue(con, ts(now), res)
        return res
    finally:
        con.close()


@pytest.fixture(scope="session")
def small_lake(tmp_path_factory) -> Lake:
    """4 simulated customers, all faults, pipeline run at September's close and the day after."""
    from acm.replay import prepare
    root = tmp_path_factory.mktemp("small") / "lake"
    lake, _ = prepare(root, seed=7, customers=4)
    for now in ["2026-09-20T00:00:00Z", "2026-10-03T12:00:00Z", "2026-10-04T12:00:00Z"]:
        run_at(lake, now)
    return lake


@pytest.fixture
def small_lake_copy(small_lake, tmp_path) -> Lake:
    dst = tmp_path / "copy"
    shutil.copytree(small_lake.root, dst)
    return Lake(dst)
