"""The baseline's shortcuts, each shown on a case where it is wrong."""
import pytest

from acm import naive
from acm.pricing import ts


def test_duplicates_and_processing_time_months(landing):
    landing.vm("vm1", "c002", "2026-09-30T22:00:00Z", "2026-09-30T23:00:00Z")
    landing.beats("vm1", "2026-09-30T22:00:00Z", 12, arrives="2026-10-01T00:30:00Z")          # late by an hour or two
    landing.beats("vm1", "2026-09-30T22:00:00Z", 12, arrives="2026-10-01T01:30:00Z", event_prefix="r")   # a replay
    lake = landing.write()
    out = naive.revenue(lake, ts("2026-10-02T00:00:00Z"))
    assert ("c002", "2026-09") not in out                                      # September lost its hour
    assert out[("c002", "2026-10")] == pytest.approx(2 * 8 * 2.95)            # October got it twice
