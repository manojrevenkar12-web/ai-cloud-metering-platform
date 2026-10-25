"""Right to erasure (GDPR Art. 17) for the personal data this platform holds.

Personal data here is small and well-defined: the inference gateway's `api_key_owner` (an email)
and the CRM export's `contact_email`. Usage, money and customer ids are not personal data and are
kept, because invoices must still add up after a person is erased.

`erase` replaces every copy of the subject's email with a stable pseudonym
(`erased:<salted hash>`) everywhere it can live: landing files, bronze Parquet, quarantine and the
warehouse. It then scans all of them to prove nothing is left, checks revenue did not move, and
writes an audit row that holds the salted hash, never the email. The subject's hash goes on a
suppression list, so events that arrive later are pseudonymised as they are ingested.

Rewriting landing files breaks the "landing files are immutable" rule on purpose; the rewritten
files' new checksums are recorded in `ingest_log` so the rewrite is not reported as tampering.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import secrets
import time
from dataclasses import asdict, dataclass

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from acm.contracts import producers
from acm.ingest import Lake, ensure_log, visible_at
from acm.pricing import ts

CRM_PII = ["contact_email"]


@dataclass
class Erasure:
    request_id: str
    subject_hash: str
    requested_at: int
    landing_files: int = 0
    bronze_files: int = 0
    quarantine_files: int = 0
    warehouse_rows: int = 0
    remaining: int = -1
    revenue_before: float = 0.0
    revenue_after: float = 0.0
    seconds: float = 0.0

    @property
    def verified(self) -> bool:
        return self.remaining == 0 and abs(self.revenue_before - self.revenue_after) < 0.005


def ensure(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE TABLE IF NOT EXISTS privacy_salt (salt VARCHAR)")
    if not con.execute("SELECT count(*) FROM privacy_salt").fetchone()[0]:
        con.execute("INSERT INTO privacy_salt VALUES (?)", [secrets.token_hex(16)])
    con.execute("CREATE TABLE IF NOT EXISTS erasure_suppression (subject_hash VARCHAR PRIMARY KEY, since BIGINT)")
    con.execute("""CREATE TABLE IF NOT EXISTS erasure_log (
        request_id VARCHAR, subject_hash VARCHAR, requested_at BIGINT, landing_files BIGINT, bronze_files BIGINT,
        quarantine_files BIGINT, warehouse_rows BIGINT, remaining BIGINT, revenue_before DOUBLE,
        revenue_after DOUBLE, verified BOOLEAN, seconds DOUBLE)""")


def subject_hash(con: duckdb.DuckDBPyConnection, value: str) -> str:
    ensure(con)
    salt = con.execute("SELECT salt FROM privacy_salt").fetchone()[0]
    return hashlib.sha256((salt + value.strip().lower()).encode()).hexdigest()


def pseudonym(h: str) -> str:
    return f"erased:{h[:16]}"


def suppressed(con: duckdb.DuckDBPyConnection) -> tuple[str, set[str]]:
    """(salt, hashes) for ingestion, which pseudonymises suppressed subjects as records arrive."""
    ensure(con)
    salt = con.execute("SELECT salt FROM privacy_salt").fetchone()[0]
    return salt, {h for (h,) in con.execute("SELECT subject_hash FROM erasure_suppression").fetchall()}


def _revenue(con: duckdb.DuckDBPyConnection) -> float:
    if not con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'mart_finance_revenue'").fetchone()[0]:
        return 0.0
    return float(con.execute("SELECT coalesce(sum(total), 0) FROM mart_finance_revenue").fetchone()[0])


def _rewrite_gz(path, needle: str, token: str) -> bool:
    raw = gzip.decompress(path.read_bytes()).decode()
    quoted = f'"{needle}"'
    if quoted not in raw:
        return False
    path.write_bytes(gzip.compress(raw.replace(quoted, f'"{token}"').encode(), mtime=0))
    return True


def _rewrite_csv(path, needle: str, token: str) -> bool:
    text = path.read_text()
    if needle not in text:
        return False
    rows = list(csv.reader(io.StringIO(text)))
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    for row in rows:
        w.writerow([token if cell.strip().lower() == needle else cell for cell in row])
    path.write_text(out.getvalue())
    return True


def _string_columns(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return con.execute("""SELECT c.table_name, c.column_name FROM information_schema.columns c
                          JOIN information_schema.tables t USING (table_schema, table_name)
                          WHERE c.data_type = 'VARCHAR' AND t.table_type = 'BASE TABLE'
                            AND c.table_name NOT IN ('privacy_salt', 'erasure_suppression', 'erasure_log')""").fetchall()


def _arrived(p, now: int | None) -> bool:
    """Landing files that have arrived by `now` (the simulator lays future files down in advance;
    those are handled by suppression when they arrive)."""
    if now is None:
        return True
    if p.suffix == ".gz":
        return visible_at(p) <= now
    day = p.name[:10] if p.parent.name in ("crm",) else p.parent.name
    try:
        return ts(f"{day}T00:00:00Z") + 3600 <= now
    except ValueError:
        return True


def scan(lake: Lake, con: duckdb.DuckDBPyConnection, needle: str, now: int | None = None) -> dict[str, int]:
    """Counts every place `needle` still appears: landing (files arrived by `now`), bronze,
    quarantine and the warehouse."""
    needle = needle.strip().lower()
    found = {"landing": 0, "bronze": 0, "quarantine": 0, "warehouse": 0}
    for p in lake.landing.rglob("*"):
        if not p.is_file() or not _arrived(p, now):
            continue
        if p.suffix == ".gz":
            found["landing"] += gzip.decompress(p.read_bytes()).decode().lower().count(needle)
        elif p.suffix == ".csv":
            found["landing"] += p.read_text().lower().count(needle)
    for p in lake.quarantine.rglob("*.jsonl"):
        found["quarantine"] += p.read_text().lower().count(needle)
    for p in lake.bronze.rglob("*.parquet"):
        t = pq.read_table(p)
        for name in t.column_names:
            if pa.types.is_string(t.schema.field(name).type):
                found["bronze"] += sum(1 for v in t.column(name).to_pylist() if v and needle in v.lower())
    for table, col in _string_columns(con):
        found["warehouse"] += con.execute(f'SELECT count(*) FROM "{table}" WHERE lower("{col}") LIKE ?',
                                          [f"%{needle}%"]).fetchone()[0]
    return found


def erase(lake: Lake, con: duckdb.DuckDBPyConnection, email: str, now: int, request_id: str) -> Erasure:
    t0 = time.perf_counter()
    needle = email.strip().lower()
    h = subject_hash(con, needle)
    token = pseudonym(h)
    rep = Erasure(request_id, h, now, revenue_before=round(_revenue(con), 2))
    ensure_log(con)
    pii_datasets = {name: c.pii for name, c in producers().items() if c.pii}

    # Landing: raw producer files and CRM exports.
    for name in pii_datasets:
        for p in sorted((lake.landing / name).glob("*/*.jsonl.gz")):
            if _arrived(p, now) and _rewrite_gz(p, needle, token):
                rep.landing_files += 1
                sha = hashlib.sha256(p.read_bytes()).hexdigest()
                con.execute("UPDATE ingest_log SET sha256 = ? WHERE path = ?", [sha, str(p.relative_to(lake.root))])
    for p in sorted((lake.landing / "crm").glob("*.csv")):
        if _arrived(p, now):
            rep.landing_files += _rewrite_csv(p, needle, token)

    # Quarantine keeps whole records, so any field may hold it.
    for p in sorted(lake.quarantine.rglob("*.jsonl")):
        text = p.read_text()
        if needle in text.lower():
            p.write_text(text.replace(needle, token))
            rep.quarantine_files += 1

    # Bronze Parquet: only the contract's PII columns can hold it.
    for name, cols in pii_datasets.items():
        for p in sorted((lake.bronze / name).rglob("*.parquet")):
            t = pq.read_table(p)
            changed = False
            for col in cols:
                values = t.column(col).to_pylist()
                if any(v and v.lower() == needle for v in values):
                    new = pa.array([token if v and v.lower() == needle else v for v in values], type=t.schema.field(col).type)
                    t = t.set_column(t.schema.get_field_index(col), col, new)
                    changed = True
            if changed:
                pq.write_table(t, p, compression="zstd")
                rep.bronze_files += 1

    # Warehouse: every string column of every table (silver, reference, marts).
    for table, col in _string_columns(con):
        n = con.execute(f'SELECT count(*) FROM "{table}" WHERE lower("{col}") = ?', [needle]).fetchone()[0]
        if n:
            con.execute(f'UPDATE "{table}" SET "{col}" = ? WHERE lower("{col}") = ?', [token, needle])
            rep.warehouse_rows += n

    con.execute("INSERT OR IGNORE INTO erasure_suppression VALUES (?, ?)", [h, now])
    rep.remaining = sum(scan(lake, con, needle, now).values())
    rep.revenue_after = round(_revenue(con), 2)
    rep.seconds = round(time.perf_counter() - t0, 2)
    d = asdict(rep)
    con.execute("INSERT INTO erasure_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [d["request_id"], d["subject_hash"], d["requested_at"], d["landing_files"], d["bronze_files"],
                 d["quarantine_files"], d["warehouse_rows"], d["remaining"], d["revenue_before"], d["revenue_after"],
                 rep.verified, d["seconds"]])
    return rep
