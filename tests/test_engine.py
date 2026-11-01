from datetime import datetime, timedelta, timezone

import pytest
from shapely.geometry import box

from rasad.engine.flood import exceedance_probability, thresholds_from_history
from rasad.engine.fusion import Candidate, best_match, spatial_match

UTC = timezone.utc


def test_exceedance_bounds_and_monotone():
    q = (100, 200, 300, 400, 500)
    assert exceedance_probability(50, *q) == 1.0
    assert exceedance_probability(600, *q) == 0.0
    assert exceedance_probability(300, *q) == pytest.approx(0.5)
    assert exceedance_probability(250, *q) == pytest.approx(0.625)
    probs = [exceedance_probability(t, *q) for t in range(100, 501, 25)]
    assert all(a >= b for a, b in zip(probs, probs[1:]))


def test_exceedance_missing_and_flat():
    assert exceedance_probability(None, 1, 2, 3, 4, 5) is None
    assert exceedance_probability(3, 1, None, 3, 4, 5) is None
    assert exceedance_probability(3, 3, 3, 3, 3, 3) == 1.0  # threshold at every member
    assert exceedance_probability(250, 100, 300, 200, 400, 500) is not None  # non-monotone summary survives


def test_thresholds_from_history():
    times, vals = [], []
    start = datetime(2000, 1, 1)
    for i in range(365 * 12):
        d = start + timedelta(days=i)
        times.append(d.date().isoformat())
        vals.append(1000.0 + (d.year - 2000) * 10 if d.timetuple().tm_yday == 200 else 100.0)
    th = thresholds_from_history(times, vals)
    assert th is not None
    assert th["years"] >= 10
    assert th["flood_m3s"] == pytest.approx(1050.0, abs=10)
    assert th["warning_m3s"] == 100.0


def test_thresholds_rejects_short_or_dry():
    assert thresholds_from_history(["2000-01-01"], [5.0]) is None
    assert thresholds_from_history([f"20{y:02d}-01-{d:02d}" for y in range(12) for d in range(1, 29)], [0.0] * 336) is None


def test_spatial_match_iou_and_containment():
    a = box(0, 0, 2, 2)
    assert spatial_match(a, box(0, 0, 2, 2)) == pytest.approx(1.0)
    assert spatial_match(a, box(5, 5, 6, 6)) == 0.0
    assert spatial_match(a, box(0.5, 0.5, 1.5, 1.5)) >= 0.4  # contained district inside a state alert


def test_best_match_respects_kind_and_time():
    now = datetime(2026, 11, 1, tzinfo=UTC)
    c = [
        Candidate("rain", "heavy_rain", box(0, 0, 2, 2), None, now, now + timedelta(days=1)),
        Candidate("cyc", "cyclone", box(0, 0, 2, 2), None, now, now + timedelta(days=1)),
        Candidate("old", "heavy_rain", box(0, 0, 2, 2), None, now - timedelta(days=5), now - timedelta(days=4)),
    ]
    assert best_match("heavy_rain", box(0.1, 0.1, 2, 2), None, now, now + timedelta(hours=6), c) == "rain"
    assert best_match("flood", box(0, 0, 2, 2), None, now, None, c) is None
    unlocated = [Candidate("u", "flood", None, "Alpha District", now, None)]
    assert best_match("flood", None, "alpha", now, None, unlocated) == "u"
