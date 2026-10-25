"""Data contracts: what producers promise (contracts/producers) and what marts promise their
consumers (contracts/marts). Records are validated on ingestion; a record that breaks its contract
is quarantined with the reason, never silently dropped or coerced.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import yaml

from acm.pricing import ROOT

CONTRACTS = ROOT / "contracts"


@dataclass
class Contract:
    name: str
    owner: str
    event_time: str
    dedup_key: list[str]
    fields: dict[str, dict]
    versions: dict[int, dict]
    pii: list[str] = field(default_factory=list)
    revision: str | None = None
    raw: dict = field(default_factory=dict)

    @property
    def latest_version(self) -> int:
        return max(self.versions)


def load_producer(name: str, root: Path = CONTRACTS) -> Contract:
    with open(root / "producers" / f"{name}.yaml") as f:
        d = yaml.safe_load(f)
    return Contract(d["name"], d["owner"], d["event_time"], list(d["dedup_key"]), d["fields"],
                    {int(k): (v or {}) for k, v in d["versions"].items()}, list(d.get("pii") or []),
                    d.get("revision"), d)


def producers(root: Path = CONTRACTS) -> dict[str, Contract]:
    return {p.stem: load_producer(p.stem, root) for p in sorted((root / "producers").glob("*.yaml"))}


def load_mart(name: str, root: Path = CONTRACTS) -> dict:
    with open(root / "marts" / f"{name}.yaml") as f:
        return yaml.safe_load(f)


def parse_ts(value) -> int:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("timestamp must be an ISO-8601 string in UTC ending in Z")
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC).timestamp())


def upcast(contract: Contract, record: dict) -> dict:
    """Brings a record written under an older or newer schema version to the canonical field names."""
    version = record.get("schema_version", 1)
    if version not in contract.versions:
        raise ValueError(f"unknown schema_version {version}")
    renames = contract.versions[version].get("renames") or {}
    if not renames:
        return record
    out = dict(record)
    for old, new in renames.items():
        if old in out:
            out[new] = out.pop(old)
    return out


_PATTERNS: dict[str, re.Pattern] = {}


def validate(contract: Contract, record: dict) -> tuple[dict | None, str | None]:
    """Returns (canonical record with parsed timestamps as *_ts epoch seconds, None) or (None, reason)."""
    try:
        rec = upcast(contract, record)
    except ValueError as e:
        return None, str(e)
    unknown = set(rec) - set(contract.fields)
    if unknown:
        return None, f"unexpected fields: {', '.join(sorted(unknown))}"
    out = {}
    for name, spec in contract.fields.items():
        if name not in rec or rec[name] is None:
            if spec.get("required"):
                return None, f"missing {name}"
            out[name] = None
            continue
        v = rec[name]
        kind = spec["type"]
        if kind == "string":
            if not isinstance(v, str) or not v:
                return None, f"{name} is not a non-empty string"
            if "pattern" in spec:
                pat = _PATTERNS.setdefault(spec["pattern"], re.compile(spec["pattern"]))
                if not pat.match(v):
                    return None, f"{name} does not match {spec['pattern']}"
        elif kind == "int":
            if isinstance(v, bool) or not isinstance(v, int):
                return None, f"{name} is not an integer"
        elif kind == "float":
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                return None, f"{name} is not a number"
            v = float(v)
        elif kind == "timestamp":
            try:
                out[f"{name}_ts"] = parse_ts(v)
            except ValueError as e:
                return None, f"{name}: {e}"
        if "enum" in spec and v not in spec["enum"]:
            return None, f"{name}={v!r} not in {spec['enum']}"
        if "min" in spec and v < spec["min"]:
            return None, f"{name}={v} below {spec['min']}"
        if "max" in spec and v > spec["max"]:
            return None, f"{name}={v} above {spec['max']}"
        out[name] = v
    return out, None


# ------------------------------------------------------------------------- breaking changes

def diff_producer(old: dict, new: dict) -> list[str]:
    """Breaking changes between two versions of a producer contract (what consumers would feel)."""
    problems = []
    of, nf = old.get("fields", {}), new.get("fields", {})
    renamed = set()
    for v in (new.get("versions") or {}).values():
        renamed |= set(((v or {}).get("renames") or {}).values())
    for name, spec in of.items():
        if name not in nf:
            problems.append(f"field {name} removed")
            continue
        if nf[name]["type"] != spec["type"]:
            problems.append(f"field {name} changed type {spec['type']} -> {nf[name]['type']}")
        if nf[name].get("required") and not spec.get("required") and name not in renamed:
            problems.append(f"field {name} became required")
        if "enum" in spec and "enum" in nf[name] and not set(spec["enum"]) <= set(nf[name]["enum"]):
            problems.append(f"field {name} lost allowed values {sorted(set(spec['enum']) - set(nf[name]['enum']))}")
    if old.get("dedup_key") != new.get("dedup_key"):
        problems.append(f"dedup_key changed {old.get('dedup_key')} -> {new.get('dedup_key')}")
    return problems


def diff_mart(old: dict, new: dict) -> list[str]:
    problems = []
    oc, nc = old.get("columns", {}), new.get("columns", {})
    for name, spec in oc.items():
        if name not in nc:
            problems.append(f"column {name} removed")
        elif nc[name]["type"] != spec["type"]:
            problems.append(f"column {name} changed type {spec['type']} -> {nc[name]['type']}")
        elif not spec.get("nullable") and nc[name].get("nullable"):
            problems.append(f"column {name} became nullable")
    if old.get("grain") != new.get("grain"):
        problems.append(f"grain changed {old.get('grain')} -> {new.get('grain')}")
    return problems


# ------------------------------------------------------------------------- marts as built

MART_TYPES = {"string": ("VARCHAR",), "decimal": ("DECIMAL",), "double": ("DOUBLE", "FLOAT"), "date": ("DATE",),
              "boolean": ("BOOLEAN",), "int": ("BIGINT", "INTEGER")}


def check_mart(con, name: str, table: str | None = None) -> list[str]:
    """Problems with a built mart against its contract: missing or extra columns, wrong types,
    nulls where the contract says none, duplicate grain, values outside an enum."""
    spec = load_mart(name)
    table = table or f"mart_{name}"
    cols = {c: t for c, t in con.execute(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?", [table]).fetchall()}
    if not cols:
        return [f"{table} does not exist"]
    problems = []
    for col, s in spec["columns"].items():
        if col not in cols:
            problems.append(f"{table}.{col} missing")
            continue
        if not cols[col].upper().startswith(MART_TYPES[s["type"]]):
            problems.append(f"{table}.{col} is {cols[col]}, contract says {s['type']}")
        if not s.get("nullable"):
            n = con.execute(f'SELECT count(*) FROM {table} WHERE "{col}" IS NULL').fetchone()[0]
            if n:
                problems.append(f"{table}.{col} has {n} nulls")
        if "enum" in s:
            bad = con.execute(f'SELECT DISTINCT "{col}" FROM {table} WHERE "{col}" NOT IN ?', [s["enum"]]).fetchall()
            if bad:
                problems.append(f"{table}.{col} has values outside {s['enum']}: {[b[0] for b in bad]}")
    extra = set(cols) - set(spec["columns"])
    if extra:
        problems.append(f"{table} has columns the contract does not declare: {sorted(extra)}")
    grain = ", ".join(spec["grain"])
    dup = con.execute(f"SELECT count(*) FROM (SELECT {grain} FROM {table} GROUP BY ALL HAVING count(*) > 1)").fetchone()[0]
    if dup:
        problems.append(f"{table} has {dup} duplicate {grain} keys")
    return problems
