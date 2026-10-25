"""Landing zone -> bronze.

Each landing file is ingested once (tracked by path and SHA-256 in `ingest_log`), validated record
by record against its producer's contract, and written as Parquet partitioned by the record's own
event date. What fails the contract goes to quarantine with the reason. Ingestion never decides
which duplicate wins or what a late record means: bronze keeps everything that is valid, with
the file it came from and when it became visible, so every later decision can be explained.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from acm.contracts import Contract, producers, validate
from acm.pricing import iso, ts

TYPE = {"string": pa.string(), "int": pa.int64(), "float": pa.float64(), "timestamp": pa.string()}


@dataclass
class Lake:
    root: Path

    @property
    def landing(self) -> Path:
        return self.root / "landing"

    @property
    def bronze(self) -> Path:
        return self.root / "bronze"

    @property
    def quarantine(self) -> Path:
        return self.root / "quarantine"

    @property
    def warehouse(self) -> Path:
        return self.root / "warehouse.duckdb"

    def connect(self) -> duckdb.DuckDBPyConnection:
        self.root.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(self.warehouse))
        con.execute("SET TimeZone = 'UTC'")
        return con


@dataclass
class IngestResult:
    files: int = 0
    rows: int = 0
    quarantined: int = 0
    reasons: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    months: set[str] = field(default_factory=set)        # event months touched (what to recompute)
    by_dataset: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    rewritten: list[str] = field(default_factory=list)    # landing files changed after ingestion


def _schema(c: Contract) -> pa.Schema:
    cols = [pa.field(n, TYPE[s["type"]]) for n, s in c.fields.items()]
    cols += [pa.field(f"{c.event_time}_ts", pa.int64()), pa.field("_file", pa.string()),
             pa.field("_visible_at", pa.int64()), pa.field("_ingested_at", pa.int64())]
    return pa.schema(cols)


def ensure_log(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""CREATE TABLE IF NOT EXISTS ingest_log (
        path VARCHAR PRIMARY KEY, sha256 VARCHAR, dataset VARCHAR, visible_at BIGINT, ingested_at BIGINT,
        rows BIGINT, quarantined BIGINT)""")


def visible_at(path: Path) -> int:
    """landing/<producer>/<YYYY-MM-DD>/<HH>.jsonl.gz is complete at the end of that hour."""
    return ts(f"{path.parent.name}T{path.name[:2]}:00:00Z") + 3600


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _suppression(con: duckdb.DuckDBPyConnection):
    """A function that pseudonymises values of erased subjects (acm.privacy), or None."""
    from acm import privacy

    salt, hashes = privacy.suppressed(con)
    if not hashes:
        return None

    def apply(value):
        if isinstance(value, str):
            h = hashlib.sha256((salt + value.strip().lower()).encode()).hexdigest()
            if h in hashes:
                return privacy.pseudonym(h)
        return value
    return apply


def ingest_events(lake: Lake, con: duckdb.DuckDBPyConnection, now: int) -> IngestResult:
    ensure_log(con)
    done = {p: s for p, s in con.execute("SELECT path, sha256 FROM ingest_log").fetchall()}
    suppress = _suppression(con)
    result = IngestResult()
    for name, contract in producers().items():
        base = lake.landing / name
        if not base.exists():
            continue
        schema = _schema(contract)
        et = f"{contract.event_time}_ts"
        for path in sorted(base.glob("*/*.jsonl.gz")):
            rel = str(path.relative_to(lake.root))
            vis = visible_at(path)
            if vis > now:
                continue
            sha = _sha(path)
            if rel in done:
                if done[rel] != sha:
                    result.rewritten.append(rel)  # landing files are immutable: someone rewrote one
                continue
            parts: dict[str, list[dict]] = defaultdict(list)
            bad: list[dict] = []
            lines, erased = [], 0
            with gzip.open(path, "rt") as f:
                for line in f:
                    try:
                        raw = json.loads(line)
                    except json.JSONDecodeError:
                        bad.append({"line": line.strip()[:500], "reason": "not JSON"})
                        lines.append(line)
                        continue
                    if suppress and contract.pii:
                        for k in contract.pii:
                            if k in raw and suppress(raw[k]) != raw[k]:
                                raw[k] = suppress(raw[k])
                                erased += 1
                        lines.append(json.dumps(raw) + "\n")
                    rec, reason = validate(contract, raw)
                    if rec is None:
                        bad.append({"record": raw, "reason": reason})
                        result.reasons[f"{name}: {reason.split('=')[0]}"] += 1
                        continue
                    rec.update(_file=rel, _visible_at=vis, _ingested_at=now)
                    parts[iso(rec[et])[:10]].append(rec)
            for day, rows in parts.items():
                out = lake.bronze / name / f"event_date={day}"
                out.mkdir(parents=True, exist_ok=True)
                table = pa.Table.from_pylist(rows, schema=schema)
                pq.write_table(table, out / f"{path.parent.name}T{path.name[:2]}.parquet", compression="zstd")
                result.months.add(day[:7])
            if bad:
                q = lake.quarantine / name
                q.mkdir(parents=True, exist_ok=True)
                with open(q / f"{path.parent.name}T{path.name[:2]}.jsonl", "w") as f:
                    for b in bad:
                        f.write(json.dumps(b) + "\n")
            if erased:
                # An erased subject's data arrived again: the landing copy is pseudonymised too.
                path.write_bytes(gzip.compress("".join(lines).encode(), mtime=0))
                sha = _sha(path)
            n_good = sum(len(r) for r in parts.values())
            con.execute("INSERT INTO ingest_log VALUES (?, ?, ?, ?, ?, ?, ?)",
                        [rel, sha, name, vis, now, n_good, len(bad)])
            result.files += 1
            result.rows += n_good
            result.quarantined += len(bad)
            result.by_dataset[name] += n_good
    return result


def compact(lake: Lake, now: int, settle_hours: int = 36) -> int:
    """Merges each event-date partition's small files into one once the day has settled.

    Ingestion writes one Parquet file per landing file and event date, which is right for
    lineage but leaves thousands of small files. Rows keep `_file` and `_visible_at`, so nothing is
    lost by merging; a late record that arrives afterwards lands in a new small file and is merged
    on the next run. Returns the number of files removed.
    """
    removed = 0
    for dataset in sorted(p for p in lake.bronze.iterdir() if p.is_dir()) if lake.bronze.exists() else []:
        for part in sorted(dataset.glob("event_date=*")):
            day_end = ts(f"{part.name.split('=')[1]}T00:00:00Z") + 86400
            files = sorted(part.glob("*.parquet"))
            if len(files) < 2 or day_end + settle_hours * 3600 > now:
                continue
            table = pa.concat_tables([pq.read_table(f) for f in files])
            n = sum(1 for f in files if f.name.startswith("compacted-"))
            out = part / f"compacted-{n + len(files):05d}.parquet"
            tmp = out.with_suffix(".tmp")
            pq.write_table(table, tmp, compression="zstd")
            for f in files:
                f.unlink()
            tmp.rename(out)
            removed += len(files) - 1
    return removed


def _latest(folder: Path, now: int, suffix: str) -> Path | None:
    """Daily exports are produced at 00:00 and visible an hour later."""
    best = None
    for p in sorted(folder.glob(f"*{suffix}")):
        day = p.name[:10] if suffix == ".csv" else p.parent.name
        if ts(f"{day}T00:00:00Z") + 3600 <= now:
            best = p
    return best


def load_reference(lake: Lake, con: duckdb.DuckDBPyConnection, now: int) -> dict:
    """Replaces the reference tables with the latest exports visible at `now`."""
    loaded = {}
    crm = _latest(lake.landing / "crm", now, ".csv")
    if crm:
        suppress = _suppression(con)
        if suppress:
            # Every export that has arrived, not only the latest: an erased contact must not linger.
            for p in sorted((lake.landing / "crm").glob("*.csv")):
                if ts(f"{p.name[:10]}T00:00:00Z") + 3600 > now:
                    continue
                with open(p) as f:
                    rows = list(csv.reader(f))
                new = [rows[0]] + [[suppress(v) if rows[0][i] == "contact_email" else v for i, v in enumerate(r)]
                                   for r in rows[1:]]
                if new != rows:
                    with open(p, "w", newline="") as f:
                        csv.writer(f, lineterminator="\n").writerows(new)
        con.execute("CREATE OR REPLACE TABLE ref_customers AS SELECT * FROM read_csv(?, header=true, all_varchar=true)",
                    [str(crm)])
        loaded["crm"] = crm.name
    reg = lake.landing / "contracts"
    if reg.exists():
        days = [d for d in sorted(reg.iterdir()) if ts(f"{d.name}T00:00:00Z") + 3600 <= now]
        if days:
            d = days[-1]
            con.execute("""CREATE OR REPLACE TABLE ref_commitments AS
                SELECT commitment_id, customer_id, gpu_type, region, CAST(gpus AS INTEGER) AS gpus,
                       CAST(usd_per_gpu_hour AS DOUBLE) AS usd_per_gpu_hour,
                       epoch(CAST(starts AS TIMESTAMPTZ))::BIGINT AS starts,
                       epoch(CAST(ends AS TIMESTAMPTZ))::BIGINT AS ends, NULLIF(replaces, '') AS replaces
                FROM read_csv(?, header=true, all_varchar=true)""", [str(d / "commitments.csv")])
            con.execute("""CREATE OR REPLACE TABLE ref_credit_grants AS
                SELECT grant_id, customer_id, CAST(usd AS DOUBLE) AS usd,
                       epoch(CAST(granted_at AS TIMESTAMPTZ))::BIGINT AS granted_at,
                       epoch(CAST(expires_at AS TIMESTAMPTZ))::BIGINT AS expires_at
                FROM read_csv(?, header=true, all_varchar=true)""", [str(d / "credit_grants.csv")])
            loaded["contracts"] = d.name
    inv = lake.landing / "billing" / "invoices_2026-09.csv"
    if inv.exists():
        with open(inv) as f:
            issued = ts(next(csv.DictReader(f))["issued_at"])
        if issued <= now:
            con.execute("""CREATE OR REPLACE TABLE ref_invoices AS
                SELECT invoice_id, customer_id, period, epoch(CAST(issued_at AS TIMESTAMPTZ))::BIGINT AS issued_at,
                       CAST(gpu_on_demand AS DOUBLE) AS gpu_on_demand, CAST(commitment AS DOUBLE) AS commitment,
                       CAST(inference AS DOUBLE) AS inference, CAST(storage AS DOUBLE) AS storage,
                       CAST(credits_applied AS DOUBLE) AS credits_applied, CAST(total AS DOUBLE) AS total
                FROM read_csv(?, header=true, all_varchar=true)""", [str(inv)])
            loaded["invoices"] = inv.name
    return loaded
