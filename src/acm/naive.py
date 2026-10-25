"""The pipeline most teams write first, run on the same landing zone for comparison.

It is not a straw man; every shortcut here is common and looks fine on clean data:
  - usage belongs to the month the file arrived in (processing time), not the month it happened
  - no de-duplication: every row in every file counts
  - GPU usage = heartbeats x 300 s x GPUs, joined to the VM's type from its lifecycle events
    (rows whose type it cannot read price at nothing)
  - the highest revision of a corrected event is not chosen: both copies count
  - every commitment row is charged for its full term
  - a month is recomputed from scratch on every run, so a closed month silently changes
"""
from __future__ import annotations

import gzip
import json

import duckdb
import pyarrow as pa

from acm.ingest import Lake, visible_at
from acm.pricing import price_rows


def revenue(lake: Lake, now: int) -> dict[tuple[str, str], float]:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE TABLE ref_prices (sku VARCHAR, valid_from BIGINT, valid_to BIGINT, usd DOUBLE)")
    con.executemany("INSERT INTO ref_prices VALUES (?, ?, ?, ?)", [(p.sku, p.valid_from, p.valid_to, p.usd) for p in price_rows()])
    land = lake.landing
    tables: dict[str, pa.Table] = {}

    keep = {"vm_lifecycle": ["vm_id", "customer_id", "region", "gpu_type", "gpu_count"],
            "gpu_heartbeat": ["vm_id", "host_time"],
            "inference_usage": ["customer_id", "model", "window_start", "input_tokens", "output_tokens"],
            "storage_snapshot": ["customer_id", "snapshot_time", "bytes_stored"]}

    def table(name: str, producer: str) -> bool:
        """Every row of every file visible at `now`, as it arrived (fields it does not know are null)."""
        cols: dict[str, list] = {k: [] for k in keep[producer] + ["arrival_month"]}
        for p in sorted((land / producer).glob("*/*.jsonl.gz")):
            if visible_at(p) > now:
                continue
            month = p.parent.name[:7]
            with gzip.open(p, "rt") as f:
                for line in f:
                    try:
                        r = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    for k in keep[producer]:
                        cols[k].append(r.get(k))
                    cols["arrival_month"].append(month)
        if not cols["arrival_month"]:
            return False
        tables[name] = pa.table(cols)
        con.register(f"_{name}", tables[name])
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM _{name}")
        con.unregister(f"_{name}")
        return True

    out: dict[tuple[str, str], float] = {}

    def add(rows):
        for cid, month, usd in rows:
            if usd is not None:
                out[(cid, month)] = out.get((cid, month), 0.0) + float(usd)

    if table("lc", "vm_lifecycle") and table("hb", "gpu_heartbeat"):
        add(con.execute("""
            WITH vm AS (SELECT vm_id, any_value(customer_id) AS customer_id, any_value(region) AS region,
                               max(gpu_type) AS gpu_type, max(gpu_count) AS gpu_count FROM lc GROUP BY vm_id)
            SELECT vm.customer_id, hb.arrival_month, sum(300 * vm.gpu_count / 3600.0 * p.usd)
            FROM hb JOIN vm USING (vm_id)
            LEFT JOIN ref_prices p ON p.sku = 'gpu:' || vm.gpu_type || ':' || vm.region
              AND epoch(CAST(hb.host_time AS TIMESTAMPTZ)) >= p.valid_from AND epoch(CAST(hb.host_time AS TIMESTAMPTZ)) < p.valid_to
            GROUP BY ALL""").fetchall())
    if table("inf", "inference_usage"):
        add(con.execute("""
            SELECT i.customer_id, i.arrival_month, sum(i.input_tokens / 1e6 * pi.usd + i.output_tokens / 1e6 * po.usd)
            FROM inf i
            JOIN ref_prices pi ON pi.sku = 'inference:' || i.model || ':input'
              AND epoch(CAST(i.window_start AS TIMESTAMPTZ)) >= pi.valid_from AND epoch(CAST(i.window_start AS TIMESTAMPTZ)) < pi.valid_to
            JOIN ref_prices po ON po.sku = 'inference:' || i.model || ':output'
              AND epoch(CAST(i.window_start AS TIMESTAMPTZ)) >= po.valid_from AND epoch(CAST(i.window_start AS TIMESTAMPTZ)) < po.valid_to
            GROUP BY ALL""").fetchall())
    if table("st", "storage_snapshot"):
        add(con.execute("""
            SELECT s.customer_id, s.arrival_month, sum(s.bytes_stored / 1e9 * p.usd / 730)
            FROM st s JOIN ref_prices p ON p.sku = 'storage:object'
              AND epoch(CAST(s.snapshot_time AS TIMESTAMPTZ)) >= p.valid_from AND epoch(CAST(s.snapshot_time AS TIMESTAMPTZ)) < p.valid_to
            GROUP BY ALL""").fetchall())
    reg = sorted(d for d in (land / "contracts").iterdir()) if (land / "contracts").exists() else []
    from acm.pricing import ts
    reg = [d for d in reg if ts(f"{d.name}T00:00:00Z") + 3600 <= now]
    if reg:
        from acm.transform import month_bounds
        con.execute("CREATE TABLE k AS SELECT * FROM read_csv(?, header = true)", [str(reg[-1] / "commitments.csv")])
        for month in sorted({m for _, m in out}):
            a, b = month_bounds(month)
            b = min(b, now)
            add(con.execute("""SELECT customer_id, ?, sum(CAST(gpus AS DOUBLE) * greatest(0, least(epoch(CAST(ends AS TIMESTAMPTZ)), ?) -
                                   greatest(epoch(CAST(starts AS TIMESTAMPTZ)), ?)) / 3600.0 * CAST(usd_per_gpu_hour AS DOUBLE))
                               FROM k GROUP BY customer_id""", [month, b, a]).fetchall())
        con.execute("CREATE TABLE g AS SELECT * FROM read_csv(?, header = true)", [str(reg[-1] / "credit_grants.csv")])
        for cid, usd in con.execute("SELECT customer_id, sum(CAST(usd AS DOUBLE)) FROM g GROUP BY ALL").fetchall():
            remaining = float(usd)
            for (c, m) in sorted(k for k in out if k[0] == cid):
                use = min(remaining, max(0.0, out[(c, m)]))
                out[(c, m)] -= use
                remaining -= use
    con.close()
    return out
