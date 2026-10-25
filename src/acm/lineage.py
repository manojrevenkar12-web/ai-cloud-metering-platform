"""Table and column lineage, derived from the SQL itself (sqlglot), not written by hand.

Every model in `acm/models` is parsed. For each table it writes, every output column is traced back
to the bronze, reference or upstream columns it is computed from. The few tables built in Python
(credits, the ledger, the finance mart) declare their inputs in `PYTHON_STEPS`; the test suite
checks those declarations against the code that reads them.

`openlineage_events()` emits one OpenLineage-style RunEvent per model with the `columnLineage`
facet, so the graph can be loaded into Marquez or DataHub; `markdown()` renders it for the docs.
Questions this answers in review: "if gpu_heartbeat changes host_time, which finance columns move?"
(`impact`), "where does mart_finance_revenue.total come from?" (`trace`).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime

import duckdb
import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage as sg_lineage
from sqlglot.optimizer.qualify import qualify

from acm import transform

NAMESPACE = "acm"
PARAM = "__param_"

# Tables built in Python (periods.py): what they read. Kept short on purpose.
PYTHON_STEPS: dict[str, dict[str, list[str]]] = {
    "gold_credits": {"inputs": ["gold_charges", "ref_credit_grants"],
                     "columns": {"credits_applied": ["gold_charges.gpu_on_demand", "gold_charges.inference",
                                                     "gold_charges.storage", "ref_credit_grants.usd"]}},
    "revenue_current": {"inputs": ["gold_charges", "gold_credits"],
                        "columns": {c: [f"gold_charges.{c}"] for c in ["gpu_on_demand", "commitment", "inference", "storage"]}
                        | {"credits_applied": ["gold_credits.credits_applied", "gold_charges.gpu_on_demand",
                                               "gold_charges.inference", "gold_charges.storage"]}},
    "revenue_ledger": {"inputs": ["revenue_current", "period_status"],
                       "columns": {"amount": [f"revenue_current.{c}" for c in
                                              ["gpu_on_demand", "commitment", "inference", "storage", "credits_applied"]]}},
    "mart_finance_revenue": {"inputs": ["revenue_ledger", "revenue_current", "period_status"],
                             "columns": {c: ["revenue_ledger.amount", f"revenue_current.{c}"] for c in
                                         ["gpu_on_demand", "commitment", "inference", "storage", "credits_applied"]}
                             | {"total": ["revenue_ledger.amount"] + [f"revenue_current.{c}" for c in
                                          ["gpu_on_demand", "commitment", "inference", "storage", "credits_applied"]],
                                "closed_total": ["revenue_ledger.amount"], "adjustments": ["revenue_ledger.amount"]}},
}


@dataclass
class Node:
    table: str
    model: str
    inputs: list[str]
    columns: dict[str, list[str]] = field(default_factory=dict)   # output column -> ["table.column", ...]
    unresolved: list[str] = field(default_factory=list)


def _statements(name: str) -> list[str]:
    body = "\n".join(line for line in transform.sql(name).splitlines() if not line.lstrip().startswith("--"))
    return [s.strip().replace("$", PARAM) for s in body.split(";") if s.strip()]


def _schema(con: duckdb.DuckDBPyConnection) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for t, c, ty in con.execute("""SELECT table_name, column_name, data_type FROM information_schema.columns
                                   ORDER BY table_name, ordinal_position""").fetchall():
        out.setdefault(t, {})[c] = ty
    return out


def _target(tree: exp.Expression) -> str | None:
    if isinstance(tree, (exp.Create, exp.Insert)):
        t = tree.this
        if isinstance(t, exp.Schema):
            t = t.this
        return t.name
    return None


def _leaves(column: str, select: exp.Expression, schema: dict, known: set[str]) -> tuple[list[str], bool]:
    node = sg_lineage(column, select, schema=schema, dialect="duckdb")
    out, ok = set(), True
    for n in node.walk():
        if n.downstream:
            continue
        if PARAM in n.name:
            continue                      # a run parameter ($now, $month), not a column
        col = n.name.split(".")[-1]
        table = n.source.name if isinstance(n.source, exp.Table) else n.name.split(".")[0]
        if table in known:
            out.add(f"{table}.{col}")
        elif isinstance(n.expression, exp.Literal) or not isinstance(n.expression, exp.Column) and \
                not list(n.expression.find_all(exp.Column)):
            continue                      # a constant
        else:
            ok = False                     # an alias sqlglot could not resolve
    return sorted(out), ok


def build(con: duckdb.DuckDBPyConnection) -> dict[str, Node]:
    """Lineage of every model, using the warehouse's live schema to expand `*`."""
    schema = _schema(con)
    known = set(schema)
    nodes: dict[str, Node] = {}
    for name in transform.SILVER + transform.GOLD + transform.MARTS:
        for statement in _statements(name):
            tree = sqlglot.parse_one(statement, dialect="duckdb")
            target = _target(tree)
            if tree.expression is not None and target is not None:
                try:
                    tree.set("expression", qualify(tree.expression, schema=schema, dialect="duckdb",
                                                   validate_qualify_columns=False, identify=False))
                except Exception:          # noqa: BLE001 - fall back to the unqualified query
                    pass
            if target is None:
                continue
            ctes = {c.alias for c in tree.find_all(exp.CTE)}
            inputs = sorted({t.name for t in tree.find_all(exp.Table)} - ctes - {target})
            node = Node(target, name, inputs)
            select = tree.expression
            if select is None or target not in schema:
                nodes[target] = node
                continue
            outputs = list(schema[target])
            names = select.named_selects if isinstance(tree, exp.Insert) else outputs
            for out_col, sel_col in zip(outputs, names):
                try:
                    leaves, ok = _leaves(sel_col, select, schema, known)
                except Exception:          # noqa: BLE001 - sqlglot cannot trace every construct
                    leaves, ok = [], False
                node.columns[out_col] = leaves
                if not ok:
                    node.unresolved.append(out_col)
            nodes[target] = node
    for table, step in PYTHON_STEPS.items():
        nodes[table] = Node(table, "periods.py", step["inputs"], dict(step["columns"]))
    return nodes


def trace(nodes: dict[str, Node], table: str, column: str, depth: int = 50) -> set[str]:
    """Every source column (bronze or reference) that `table.column` is computed from."""
    out: set[str] = set()
    stack = [(table, column, 0)]
    seen = set()
    while stack:
        t, c, d = stack.pop()
        if (t, c) in seen or d > depth:
            continue
        seen.add((t, c))
        node = nodes.get(t)
        if node is None or c not in node.columns:
            out.add(f"{t}.{c}")
            continue
        for src in node.columns[c]:
            st, sc = src.split(".", 1)
            stack.append((st, sc, d + 1))
    return out


def impact(nodes: dict[str, Node], table: str, column: str) -> set[str]:
    """Every downstream column that changes if `table.column` changes."""
    hit: set[str] = set()
    frontier = {f"{table}.{column}"}
    while frontier:
        nxt = set()
        for node in nodes.values():
            for out_col, srcs in node.columns.items():
                key = f"{node.table}.{out_col}"
                if key not in hit and frontier & set(srcs):
                    hit.add(key)
                    nxt.add(key)
        frontier = nxt
    return hit


def openlineage_events(nodes: dict[str, Node], run_id: str = "00000000-0000-0000-0000-000000000000",
                       event_time: str | None = None) -> list[dict]:
    now = event_time or datetime.now(UTC).isoformat()
    events = []
    for node in nodes.values():
        fields = {}
        for col, srcs in node.columns.items():
            fields[col] = {"inputFields": [{"namespace": NAMESPACE, "name": s.split(".")[0], "field": s.split(".", 1)[1]}
                                           for s in srcs]}
        events.append({
            "eventType": "COMPLETE", "eventTime": now,
            "run": {"runId": run_id}, "job": {"namespace": NAMESPACE, "name": node.model},
            "inputs": [{"namespace": NAMESPACE, "name": i} for i in node.inputs],
            "outputs": [{"namespace": NAMESPACE, "name": node.table,
                         "facets": {"columnLineage": {"fields": fields}}}],
            "producer": "https://github.com/manojrevenkar12-web/ai-cloud-metering-platform",
        })
    return events


def markdown(nodes: dict[str, Node]) -> str:
    lines = ["# Lineage", "", "Generated by `acm lineage` from the model SQL (sqlglot). Do not edit by hand.", "",
             "```mermaid", "flowchart LR"]
    for node in nodes.values():
        for i in node.inputs:
            lines.append(f"  {i} --> {node.table}")
    lines += ["```", ""]
    for col in ["total", "gpu_on_demand", "inference"]:
        srcs = sorted(trace(nodes, "mart_finance_revenue", col))
        lines += [f"## mart_finance_revenue.{col} comes from", ""] + [f"- `{s}`" for s in srcs] + [""]
    unresolved = [(n.table, c) for n in nodes.values() for c in n.unresolved]
    lines += ["## Columns sqlglot could not fully resolve", ""]
    lines += [f"- `{t}.{c}`" for t, c in unresolved] or ["- none"]
    return "\n".join(lines) + "\n"


def write(nodes: dict[str, Node], path, event_time: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(openlineage_events(nodes, event_time=event_time), indent=1))
