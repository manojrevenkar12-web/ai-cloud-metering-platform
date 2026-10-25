"""The pipeline never sees the simulator's truth. Everything a production deployment would run
(ingest, models, periods, recon, quality, privacy, lineage, Dagster, CLI commands other than the
replay harness) is checked by import graph; the replay and drills are the harness and may."""
import ast
from pathlib import Path

import acm

SRC = Path(acm.__file__).parent
HARNESS = {"replay.py", "drills.py", "naive.py", "cli.py"}   # cli imports the harness lazily, inside commands


def _imports(path: Path) -> set[str]:
    out = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            out |= {a.name for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            out.add(node.module)
            out |= {f"{node.module}.{a.name}" for a in node.names}
    return out


def test_pipeline_modules_never_import_the_simulator():
    for path in sorted(SRC.glob("*.py")):
        if path.name in HARNESS:
            continue
        bad = {m for m in _imports(path) if m.startswith("acm.sim")}
        assert not bad, f"{path.name} imports {bad}"


def test_cli_imports_the_simulator_only_inside_harness_commands():
    tree = ast.parse((SRC / "cli.py").read_text())
    top = {n.module for n in tree.body if isinstance(n, ast.ImportFrom)}
    assert not any(m and m.startswith(("acm.sim", "acm.replay", "acm.drills")) for m in top)


def test_models_read_only_bronze_reference_and_each_other():
    for sql in sorted((SRC / "models").glob("*.sql")):
        text = sql.read_text().lower()
        assert "truth" not in text and "acm.sim" not in text, sql.name


def test_naive_baseline_does_not_import_the_simulator_either():
    assert not {m for m in _imports(SRC / "naive.py") if m.startswith("acm.sim")}
