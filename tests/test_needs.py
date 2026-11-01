"""Need maths, catchments and the WorldPop task flow. Responses below are written to the
documented WorldPop task format for the test only."""

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from shapely.geometry import box

from rasad.connectors import worldpop
from rasad.engine.needs import catchments, geom_hash, need_lines, needed_by, round_up, supply_days, urgency
from rasad.provenance import FetchResult

NOW = datetime(2026, 11, 1, 12, tzinfo=UTC)


def test_need_lines_sphere_quantities():
    lines = {ln["item"]: ln for ln in need_lines(240, 3, 5.0)}
    assert lines["drinking_water"]["quantity"] == 10800          # 240 x 15 L x 3
    assert lines["cereal_ration"]["quantity"] == 336             # 240 x 14/30 kg x 3
    assert lines["bathing_soap"]["quantity"] == 6000             # 240 x 250/30 g x 3, no float creep
    assert lines["water_containers"]["quantity"] == 96           # ceil(240/5) households x 2
    assert all("Sphere" in ln["rate"] for ln in lines.values())
    assert need_lines(0, 3, 5.0) == []


def test_round_up_never_down():
    assert round_up(1001) == 1010
    assert round_up(10.2) == 11
    assert round_up(0.41) == 0.5
    assert round_up(6000.0000001) == 6000


def test_supply_days_and_timing():
    assert supply_days(None, NOW, 3, 7) == 3
    assert supply_days(NOW + timedelta(hours=12), NOW, 3, 7) == 3      # 12 h + 48 h recovery = 60 h
    assert supply_days(NOW + timedelta(days=20), NOW, 3, 7) == 7
    assert supply_days(NOW - timedelta(days=5), NOW, 3, 7) == 1
    assert urgency("cut_off", None, NOW) > urgency("closing", NOW + timedelta(hours=6), NOW) > urgency("closing", NOW + timedelta(days=2), NOW) > urgency("open", None, NOW)
    assert needed_by("cut_off", None, NOW) == NOW
    assert needed_by("closing", NOW + timedelta(hours=10), NOW) == NOW + timedelta(hours=7)
    assert needed_by("open", None, NOW) == NOW + timedelta(hours=24)


def test_catchments_partition_the_hazard():
    area = box(0, 0, 3, 1)
    cells = catchments({1: (0.5, 0.5), 2: (1.5, 0.5), 3: (2.5, 0.5)}, area)
    assert set(cells) == {1, 2, 3}
    assert sum(c.area for c in cells.values()) == pytest.approx(area.area)
    assert cells[2].area == pytest.approx(1.0)
    assert catchments({9: (0.5, 0.5)}, area)[9].area == pytest.approx(3.0)
    assert catchments({}, area) == {}
    assert geom_hash(cells[1]) == geom_hash(catchments({1: (0.5, 0.5), 2: (1.5, 0.5), 3: (2.5, 0.5)}, area)[1])


class ScriptedFetcher:
    """Returns pre-written responses in order and records the requests."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def get(self, source, url, params=None, retries=2, method="GET", data=None):
        self.calls.append((url, params))
        status, body = self.responses.pop(0)
        raw = json.dumps(body).encode()
        return FetchResult(len(self.calls), url, 200 <= status < 300, status, raw, "application/json", None if status < 300 else f"HTTP {status}")


@pytest.fixture(autouse=True)
def fast_poll(monkeypatch):
    monkeypatch.setattr(worldpop, "POLL_SECONDS", 0)


def run(c):
    return asyncio.new_event_loop().run_until_complete(c)


def test_worldpop_polls_until_finished():
    f = ScriptedFetcher([
        (200, {"status": "created", "taskid": "t1", "error": False}),
        (200, {"status": "started", "error": False}),
        (200, {"status": "finished", "error": False, "data": {"total_population": 12345.6}}),
    ])
    pop, raw_id, err = run(worldpop.population(f, "https://api", box(0, 0, 0.1, 0.1), 2020))
    assert (pop, raw_id, err) == (12345.6, 3, None)
    assert f.calls[1][0] == "https://api/tasks/t1" and f.calls[2][0] == "https://api/tasks/t1"
    sent = json.loads(f.calls[0][1]["geojson"])
    assert sent["type"] == "FeatureCollection" and f.calls[0][1]["dataset"] == "wpgppop"


def test_worldpop_falls_back_to_bare_geometry():
    f = ScriptedFetcher([
        (200, {"status": "created", "taskid": "t1", "error": False}),
        (200, {"status": "finished", "error": True, "error_message": "Invalid GeoJSON: not valid under any of the given schemas"}),
        (200, {"status": "created", "taskid": "t2", "error": False}),
        (200, {"status": "finished", "error": False, "data": {"total_population": 99}}),
    ])
    pop, _, err = run(worldpop.population(f, "https://api", box(0, 0, 0.1, 0.1), 2020))
    assert pop == 99 and err is None
    assert json.loads(f.calls[2][1]["geojson"])["type"] == "Polygon"


def test_worldpop_error_is_reported_not_guessed():
    f = ScriptedFetcher([(503, {}), (503, {})])
    pop, _, err = run(worldpop.population(f, "https://api", box(0, 0, 0.1, 0.1), 2020))
    assert pop is None and "503" in err
    f = ScriptedFetcher([(200, {"status": "created", "taskid": "t1"}), (200, {"status": "finished", "error": True, "error_message": "server busy"})])
    pop, _, err = run(worldpop.population(f, "https://api", box(0, 0, 0.1, 0.1), 2020))
    assert pop is None and err == "server busy" and len(f.calls) == 2   # no pointless retry for non-format errors
