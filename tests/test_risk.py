"""Risk engine, shelter and TomTom parsing tests. Inputs are built in code for the test only."""

from datetime import UTC, date, datetime, timedelta

import pytest
from shapely.geometry import LineString, Point, box

from rasad.connectors.shelters import parse_shelters
from rasad.connectors.tomtom import parse_incidents
from rasad.engine.risk import (
    ClosureIn,
    CrossingIn,
    HazardIn,
    circle,
    custom_model,
    exposure,
    zones_at,
)
from rasad.provenance import redact

T0 = datetime(2026, 11, 1, 6, tzinfo=UTC)
IMPACT = {"flood": 0.9, "heavy_rain": 0.4}


def hz(kind="flood", sev=3, cert=0.7, onset=T0, expires=T0 + timedelta(days=1), geom=None):
    return HazardIn("h1", kind, "t", sev, cert, onset, expires, geom or box(0, 0, 1, 1))


def test_hazard_zone_risk_and_time_window():
    z = zones_at(T0, [hz()], [], [], IMPACT, 0.8)
    assert len(z) == 1 and not z[0].hard
    assert z[0].risk == pytest.approx(0.9 * 3 / 4 * 0.7)
    assert zones_at(T0 + timedelta(days=2), [hz()], [], [], IMPACT, 0.8) == []          # expired by then
    assert zones_at(T0 - timedelta(hours=6), [hz()], [], [], IMPACT, 0.8) == []         # not started
    assert zones_at(T0, [hz(kind="thunderstorm")], [], [], IMPACT, 0.8) == []           # no road impact weight
    assert zones_at(T0, [hz(kind="heavy_rain", sev=1, cert=0.4)], [], [], IMPACT, 0.8) == []  # below 0.15


def test_crossing_turns_hard_on_its_day():
    c = CrossingIn(7, "Bridge", 20.0, 85.0, {T0.date(): 0.3, (T0 + timedelta(days=2)).date(): 0.9})
    now = zones_at(T0, [], [c], [], IMPACT, 0.8)
    later = zones_at(T0 + timedelta(days=2), [], [c], [], IMPACT, 0.8)
    assert len(now) == 1 and not now[0].hard and now[0].risk == 0.3
    assert len(later) == 1 and later[0].hard
    assert zones_at(T0 + timedelta(days=1), [], [c], [], IMPACT, 0.8) == []  # no forecast that day


def test_closures_confirmed_vs_suspected():
    road = LineString([(85.0, 20.0), (85.01, 20.0)])
    conf = ClosureIn(1, "tomtom", road, True, "closed", T0 - timedelta(hours=1), None)
    sus = ClosureIn(2, "report", Point(85.0, 20.0), False, "tree", T0, T0 + timedelta(hours=12))
    z = zones_at(T0, [], [], [conf, sus], IMPACT, 0.8)
    assert [x.hard for x in z] == [True, False]
    assert zones_at(T0 + timedelta(hours=13), [], [], [sus], IMPACT, 0.8) == []


def test_custom_model_buckets_and_hard_first():
    zs = zones_at(T0, [hz(sev=4, cert=1.0)], [], [ClosureIn(1, "report", Point(0.5, 0.5), True, "x", T0, None)], IMPACT, 0.8)
    cm = custom_model(zs)
    mults = [p["multiply_by"] for p in cm["priority"]]
    assert mults[0] == "0.0"                          # hard zone first
    assert "0.1" in mults                             # 0.9 risk flood -> strongest soft bucket
    ids = {f["id"] for f in cm["areas"]["features"]}
    assert all(p["if"].removeprefix("in_") in ids for p in cm["priority"])
    assert custom_model([]) is None


def test_exposure_reports_crossed_zones():
    zs = zones_at(T0, [hz(geom=box(0, 0, 1, 1))], [CrossingIn(3, "B", 0.5, 2.0, {T0.date(): 0.95})], [], IMPACT, 0.8)
    worst, hard, crossed = exposure(LineString([(-1, 0.5), (0.5, 0.5)]), zs)
    assert not hard and worst == pytest.approx(0.4725) and crossed[0]["type"] == "hazard"
    worst, hard, crossed = exposure(LineString([(1.9, 0.5), (2.1, 0.5)]), zs)
    assert hard and crossed[0]["type"] == "river_point"


def test_circle_size():
    c = circle(85.0, 20.0, 250)
    minx, miny, maxx, maxy = c.bounds
    assert (maxy - miny) * 111_320 == pytest.approx(500, rel=0.05)


def test_shelter_ranking_and_thinning():
    data = {"elements": [
        {"type": "node", "id": 1, "lat": 20.001, "lon": 85.001, "tags": {"amenity": "school", "name": "School A"}},
        {"type": "way", "id": 2, "center": {"lat": 20.002, "lon": 85.002}, "tags": {"building": "cyclone_shelter"}},
        {"type": "node", "id": 3, "lat": 20.5, "lon": 85.5, "tags": {"amenity": "hospital", "name": "H"}},
        {"type": "node", "id": 4, "lat": 20.9, "lon": 85.9, "tags": {"amenity": "cafe"}},
    ]}
    out = parse_shelters(data, keep=10)
    assert [s.osm_ref for s in out] == ["way/2", "node/3"]  # shelter beats the school in its cell; cafe ignored
    assert out[0].kind == "designated"


def test_tomtom_keeps_only_closing_incidents():
    data = {"incidents": [
        {"geometry": {"type": "LineString", "coordinates": [[85, 20], [85.01, 20]]},
         "properties": {"id": "a", "iconCategory": 8, "from": "X", "to": "Y", "events": [{"description": "Closed"}]}},
        {"geometry": {"type": "Point", "coordinates": [85, 20]},
         "properties": {"id": "b", "iconCategory": 6, "events": [{"description": "Stationary traffic"}]}},
        {"geometry": {"type": "Point", "coordinates": [85, 20]},
         "properties": {"id": "c", "iconCategory": 3, "events": [{"description": "Flooding"}]}},
    ]}
    refs = [i.ref for i in parse_incidents(data)]
    assert refs == ["a", "c"]


def test_redact_api_keys():
    url = "https://api.tomtom.com/x?key=SECRET123&bbox=1,2,3,4&token=abc"
    out = redact(url)
    assert "SECRET123" not in out and "abc" not in out and "bbox=1,2,3,4" in out


def test_date_keys():
    c = CrossingIn(1, None, 0, 0, {date(2026, 11, 1): 0.5})
    assert zones_at(T0, [], [c], [], IMPACT, 0.8)[0].risk == 0.5
