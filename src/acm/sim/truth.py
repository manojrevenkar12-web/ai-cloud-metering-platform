"""What every customer truly owes, computed from what truly happened (docs/BILLING_RULES.md).

Written independently of the pipeline's SQL, in plain Python loops, so the two can check each
other: the pipeline only ever sees delayed, duplicated, skewed and partly lost events.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from acm.pricing import Price, lookup, price_rows, ts
from acm.sim.world import END, World

MONTHS = {"2026-09": (ts("2026-09-01T00:00:00Z"), ts("2026-10-01T00:00:00Z")),
          "2026-10": (ts("2026-10-01T00:00:00Z"), END)}


def month_of(t: int) -> str | None:
    for m, (a, b) in MONTHS.items():
        if a <= t < b:
            return m
    return None


@dataclass
class Bill:
    gpu_on_demand: float = 0.0
    commitment: float = 0.0
    inference: float = 0.0
    storage: float = 0.0
    credits_applied: float = 0.0
    gpu_seconds: float = 0.0

    @property
    def charges(self) -> float:
        return self.gpu_on_demand + self.commitment + self.inference + self.storage

    @property
    def total(self) -> float:
        return self.charges - self.credits_applied

    def rounded(self) -> dict:
        """Invoice lines are rounded to the cent and the total is the sum of the lines; credits
        never exceed the rounded lines they pay for (docs/BILLING_RULES.md)."""
        lines = {k: round(getattr(self, k), 2) for k in ("gpu_on_demand", "commitment", "inference", "storage")}
        credits = min(round(self.credits_applied, 2),
                      round(lines["gpu_on_demand"] + lines["inference"] + lines["storage"], 2))
        total = round(sum(lines.values()) - credits, 2)
        return {**lines, "credits_applied": credits, "total": total}


def billed_interval(vm) -> tuple[int, int]:
    """Per-second billing from started to stopped, 60 s minimum; running VMs count up to END."""
    end = max(vm.stopped, vm.started + 60)
    return vm.started, min(end, END)


def effective_commitments(world: World):
    """A renewal replaces the commitment it names from the renewal's start."""
    out = []
    for k in world.commitments:
        end = k.ends
        for r in world.commitments:
            if r.replaces == k.commitment_id:
                end = min(end, r.starts)
        out.append((k, k.starts, end))
    return out


def gpu_charges(sessions, spans, prices) -> dict[tuple[str, str], tuple[float, float]]:
    """On-demand GPU charges per (customer, month).

    sessions: (customer, gpu, region, gpu_count, start, end) billed intervals
    spans:    (customer, gpu, region, gpus, start, end) commitment terms in force
    Usage is bucketed per minute so concurrent GPUs can be set against committed GPUs; only usage
    above the committed count is charged, at the list price in force in that minute.
    """
    usage: dict[tuple[str, str, str, int], float] = defaultdict(float)
    for cid, gpu, region, count, a, b in sessions:
        t = a
        while t < b:
            minute = t // 60
            nxt = min(b, (minute + 1) * 60)
            usage[(cid, gpu, region, minute)] += (nxt - t) * count
            t = nxt
    committed: dict[tuple[str, str, str], list[tuple[int, int, int]]] = defaultdict(list)
    for cid, gpu, region, n, a, b in spans:
        committed[(cid, gpu, region)].append((a, b, n))
    out: dict[tuple[str, str], list[float]] = defaultdict(lambda: [0.0, 0.0])
    for (cid, gpu, region, minute), gpu_s in usage.items():
        t = minute * 60
        m = month_of(t)
        if m is None:
            continue
        cover = sum(n for a, b, n in committed.get((cid, gpu, region), []) if a <= t < b) * 60
        over = max(0.0, gpu_s - cover)
        out[(cid, m)][1] += gpu_s
        if over > 0:
            p = lookup(prices, f"gpu:{gpu}:{region}", t)
            if p is None:
                raise ValueError(f"no price for {gpu} in {region} at {t}")
            out[(cid, m)][0] += over / 3600 * p.usd
    return {k: (v[0], v[1]) for k, v in out.items()}


def compute(world: World, prices: list[Price] | None = None) -> dict[tuple[str, str], Bill]:
    prices = prices or price_rows()
    bills: dict[tuple[str, str], Bill] = defaultdict(Bill)
    commitments = effective_commitments(world)

    # Commitments are charged in full for their term, used or not.
    for k, a, b in commitments:
        for m, (ma, mb) in MONTHS.items():
            lo, hi = max(a, ma), min(b, mb)
            if hi > lo:
                bills[(k.customer_id, m)].commitment += k.gpus * (hi - lo) / 3600 * k.usd_per_gpu_hour

    sessions = []
    for vm in world.vms:
        a, b = billed_interval(vm)
        sessions.append((vm.customer_id, vm.gpu_type, vm.region, vm.gpu_count, a, b))
    spans = [(k.customer_id, k.gpu_type, k.region, k.gpus, a, b) for k, a, b in commitments]
    for key, (on_demand, gpu_s) in gpu_charges(sessions, spans, prices).items():
        bills[key].gpu_on_demand += on_demand
        bills[key].gpu_seconds += gpu_s

    for w in world.inference:
        m = month_of(w.window_start)
        if m is None:
            continue
        pin = lookup(prices, f"inference:{w.model}:input", w.window_start)
        pout = lookup(prices, f"inference:{w.model}:output", w.window_start)
        bills[(w.customer_id, m)].inference += w.input_tokens / 1e6 * pin.usd + w.output_tokens / 1e6 * pout.usd

    for s in world.storage:
        m = month_of(s.snapshot_time)
        if m is None:
            continue
        p = lookup(prices, "storage:object", s.snapshot_time)
        bills[(s.customer_id, m)].storage += s.bytes_stored / 1e9 * p.usd / 730

    # Prepaid credits pay for on-demand usage (not commitments), oldest month first.
    for g in sorted(world.grants, key=lambda g: g.granted_at):
        remaining = g.usd
        for m in MONTHS:
            bill = bills.get((g.customer_id, m))
            if bill is None or remaining <= 0:
                continue
            eligible = bill.gpu_on_demand + bill.inference + bill.storage - bill.credits_applied
            use = min(remaining, max(0.0, eligible))
            bill.credits_applied += use
            remaining -= use
    return dict(bills)
