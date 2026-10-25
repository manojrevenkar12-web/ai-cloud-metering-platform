"""Fault drills on a tiny world (the full set runs with `acm drill`)."""
import pytest

from acm import drills

pytestmark = pytest.mark.slow


def test_clean_world_raises_nothing_and_an_undeclared_rename_is_quarantined(tmp_path):
    results = {r.name: r for r in drills.run(tmp_path, customers=6, step_hours=24, only=["clean", "undeclared_rename"])}
    clean, rename = results["clean"], results["undeclared_rename"]
    assert clean.opened == [] and clean.correct
    assert rename.detected and "quarantine" in rename.opened
