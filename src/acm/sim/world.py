"""A simulated AI cloud: customers, contracts and what they really ran.

This is the answer key. The pipeline never imports anything under acm.sim (tests/test_isolation.py);
it only sees the event files the simulator delivers, the CRM export and the contract register,
exactly as a data team would.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field

from acm.pricing import ts

START = ts("2026-09-01T00:00:00Z")
END = ts("2026-10-16T00:00:00Z")          # activity horizon
DELIVERY_END = ts("2026-10-18T00:00:00Z")  # late events keep arriving until here
HEARTBEAT_S = 300
B200_LAUNCH = ts("2026-09-20T00:00:00Z")

REGIONS = ["eu-north1", "eu-west1", "us-central1"]
MODELS = ["llama-70b", "qwen-235b", "embed-small"]
FIRST = ["Aino", "Lukas", "Mara", "Jonas", "Elif", "Pieter", "Sofia", "Tomas", "Yara", "Nils", "Ines", "Karl"]
COMPANY = ["Helix", "Northwind", "Quanta", "Basalt", "Lumen", "Orbit", "Tessel", "Vireo", "Kelp", "Arbor",
           "Sable", "Mistral Bay", "Ferro", "Gale", "Nimbus", "Pylon", "Rook", "Strata"]


@dataclass
class Customer:
    customer_id: str
    name: str
    contact_email: str
    country: str
    segment: str


@dataclass
class Commitment:
    commitment_id: str
    customer_id: str
    gpu_type: str
    region: str
    gpus: int
    usd_per_gpu_hour: float
    starts: int
    ends: int
    replaces: str | None = None  # a renewal supersedes the commitment it replaces from its own start


@dataclass
class CreditGrant:
    grant_id: str
    customer_id: str
    usd: float
    granted_at: int
    expires_at: int


@dataclass
class Vm:
    vm_id: str
    customer_id: str
    project_id: str
    region: str
    gpu_type: str
    gpu_count: int
    host_id: str
    created: int
    started: int
    stopped: int          # true stop time (may be after END: still running at the horizon)
    host_skew_s: int = 0  # host clock minus true time
    restart_gaps: list[tuple[int, int]] = field(default_factory=list)  # heartbeat gaps (agent restarts)


@dataclass
class InferenceWindow:
    customer_id: str
    model: str
    region: str
    window_start: int
    requests: int
    input_tokens: int
    output_tokens: int
    api_key_owner: str


@dataclass
class StorageSnapshot:
    bucket_id: str
    customer_id: str
    region: str
    snapshot_time: int
    bytes_stored: int


@dataclass
class World:
    seed: int
    customers: list[Customer]
    commitments: list[Commitment]
    grants: list[CreditGrant]
    vms: list[Vm]
    inference: list[InferenceWindow]
    storage: list[StorageSnapshot]


def build(seed: int = 7, n_customers: int = 36) -> World:
    rng = random.Random(seed)
    customers: list[Customer] = []
    for i in range(n_customers):
        cid = f"c{i + 1:03d}"
        company = f"{COMPANY[i % len(COMPANY)]}{'' if i < len(COMPANY) else ' ' + str(i // len(COMPANY) + 1)}"
        first = FIRST[rng.randrange(len(FIRST))]
        slug = company.lower().replace(" ", "")
        customers.append(Customer(cid, company + " GmbH", f"{first.lower()}@{slug}.example",
                                  rng.choice(["DE", "NL", "FI", "FR", "GB", "US", "IL"]),
                                  rng.choice(["startup", "enterprise", "research"])))

    commitments: list[Commitment] = []
    grants: list[CreditGrant] = []
    for i, c in enumerate(customers):
        if i % 4 == 0:  # reserved capacity
            gpu = "h100" if i % 8 == 0 else "h200"
            region = "eu-north1" if i % 3 else "us-central1"
            gpus = rng.choice([8, 16, 32])
            list_price = {"h100": 2.95, "h200": 3.50}[gpu]
            rate = round(list_price * 0.72, 4)
            first_end = ts("2026-09-15T00:00:00Z") if i % 12 == 0 else ts("2027-02-01T00:00:00Z")
            first = Commitment(f"k{len(commitments) + 1:03d}", c.customer_id, gpu, region, gpus, rate,
                               ts("2026-08-01T00:00:00Z"), first_end)
            commitments.append(first)
            if i % 12 == 0:
                # Renewal with more GPUs, booked to start a day before the old term ends. The register
                # says it replaces the old one, so the overlap day is charged once (at the new terms).
                commitments.append(Commitment(f"k{len(commitments) + 1:03d}", c.customer_id, gpu, region, gpus * 2,
                                              round(rate * 0.97, 4), ts("2026-09-14T00:00:00Z"),
                                              ts("2027-03-01T00:00:00Z"), replaces=first.commitment_id))
        elif i % 3 == 0:  # prepaid credits
            grants.append(CreditGrant(f"g{len(grants) + 1:03d}", c.customer_id, float(rng.choice([2000, 5000, 20000])),
                                      ts("2026-08-15T00:00:00Z"), ts("2027-08-15T00:00:00Z")))

    vms: list[Vm] = []
    host_n = 0
    for _i, c in enumerate(customers):
        reserved = [k for k in commitments if k.customer_id == c.customer_id]
        n_vms = rng.randint(3, 9) + (4 if reserved else 0)
        for j in range(n_vms):
            if reserved and j < 4:
                k = reserved[0]
                gpu, region, count = k.gpu_type, k.region, 8
            else:
                gpu = rng.choices(["h100", "h200", "b200"], weights=[5, 3, 2])[0]
                region = rng.choice(REGIONS if gpu != "b200" else ["eu-north1", "us-central1"])
                count = rng.choice([1, 1, 2, 8])
            earliest = B200_LAUNCH if gpu == "b200" else START
            created = rng.randint(earliest, END - 3600)
            started = created + rng.randint(20, 240)
            r = rng.random()
            if r < 0.08:
                life = rng.randint(5, 55)                     # under the 60 s minimum
            elif r < 0.35:
                life = rng.randint(600, 6 * 3600)
            else:
                life = int(rng.expovariate(1 / (3.5 * 86400))) + 3600
            host_n += 1
            vm = Vm(f"vm-{c.customer_id}-{j:02d}", c.customer_id, f"p-{c.customer_id}-{j % 2}", region, gpu, count,
                    f"h{host_n:04d}", created, started, started + life)
            if rng.random() < 0.15:
                vm.host_skew_s = rng.choice([-1, 1]) * rng.randint(30, 300)
            if life > 86400 and rng.random() < 0.4:  # the node agent restarts: heartbeats stop for a while
                gap_start = started + rng.randint(3600, life - 7200)
                vm.restart_gaps.append((gap_start, gap_start + rng.randint(600, 2400)))
            vms.append(vm)

    inference: list[InferenceWindow] = []
    users = [c for i, c in enumerate(customers) if i % 2 == 1 or i % 5 == 0]
    for c in users:
        model_mix = rng.sample(MODELS, k=rng.randint(1, 2))
        region = rng.choice(REGIONS)
        base = rng.choice([5, 20, 80])
        owner = c.contact_email
        t = START
        while t < END:
            hour = (t // 3600) % 24
            daytime = 1.0 if 7 <= hour <= 19 else 0.35
            for m in model_mix:
                if rng.random() < 0.7:
                    req = max(1, int(rng.gauss(base * daytime, base * 0.3)))
                    tin = req * rng.randint(400, 2400)
                    tout = 0 if m == "embed-small" else req * rng.randint(80, 600)
                    inference.append(InferenceWindow(c.customer_id, m, region, t, req, tin, tout, owner))
            t += 600

    storage: list[StorageSnapshot] = []
    for c in customers:
        for b in range(rng.randint(1, 3)):
            size = rng.randint(50, 5000) * 10**9
            growth = rng.uniform(0.0, 0.004)  # per hour
            region = rng.choice(REGIONS)
            t = START
            while t < END:
                storage.append(StorageSnapshot(f"bkt-{c.customer_id}-{b}", c.customer_id, region, t, int(size)))
                size *= 1 + growth
                t += 3600
    return World(seed, customers, commitments, grants, vms, inference, storage)
