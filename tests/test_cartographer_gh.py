"""Cartographer against a real GraphHopper and PostGIS.

Run with a GraphHopper loaded on any OSM extract and these variables set:
  RASAD_TEST_DATABASE_URL, RASAD_TEST_GRAPHHOPPER_URL, RASAD_TEST_ROUTE
RASAD_TEST_ROUTE = "depot_lon,depot_lat;shelter_lon,shelter_lat" on that extract's road network.
The test builds a scenario in code (a hazard, a river forecast, field reports) and checks
that access windows open, close and reroute as they should. Skipped when not configured.
"""

import asyncio
import os
from datetime import UTC, date, datetime, timedelta

import pytest

DB = os.environ.get("RASAD_TEST_DATABASE_URL")
GH = os.environ.get("RASAD_TEST_GRAPHHOPPER_URL")
ROUTE = os.environ.get("RASAD_TEST_ROUTE")
pytestmark = pytest.mark.skipif(not (DB and GH and ROUTE), reason="GraphHopper integration not configured")


@pytest.fixture
def env():
    os.environ["RASAD_DATABASE_URL"] = DB
    os.environ["RASAD_GRAPHHOPPER_URL"] = GH
    from rasad import db as dbmod
    from rasad.config import get_settings

    get_settings.cache_clear()
    dbmod._pool = None
    loop = asyncio.new_event_loop()

    async def setup():
        await dbmod.init_schema()
        pool = await dbmod.get_pool()
        await pool.execute(
            "TRUNCATE hazard, hazard_source, river_point, river_point_hazard, river_forecast, event_log, source_status, "
            "raw_fetch, depot, shelter, shelter_hazard, closure, road_report, access_window CASCADE"
        )
        return pool

    pool = loop.run_until_complete(setup())
    yield loop, pool
    loop.run_until_complete(dbmod.close_pool())
    loop.close()


def _pts():
    a, b = ROUTE.split(";")
    return tuple(map(float, a.split(","))), tuple(map(float, b.split(",")))


def test_access_window_lifecycle(env):
    from shapely.geometry import MultiPolygon, box

    from rasad import closures, store
    from rasad.agents.cartographer import Cartographer
    from rasad.connectors.graphhopper import GraphHopper
    from rasad.models import HazardRecord
    from rasad.provenance import Fetcher

    loop, pool = env
    run = loop.run_until_complete
    (dlon, dlat), (slon, slat) = _pts()
    now = datetime.now(UTC)

    # A moderate, possible flood over the whole area: soft risk only.
    area = MultiPolygon([box(min(dlon, slon) - 0.2, min(dlat, slat) - 0.2, max(dlon, slon) + 0.2, max(dlat, slat) + 0.2)])
    hid, _ = run(store.upsert_hazard(pool, HazardRecord(
        source="sachet", source_ref="SCN-1", kind="flood", title="Scenario flood", severity=3, certainty=0.4,
        urgency="Expected", onset=now, expires=now + timedelta(days=5), geom=area, located_by="polygon", sent=now)))
    run(pool.execute("UPDATE hazard SET attrs = attrs || '{\"shelters_searched\": 0}' WHERE id=$1::uuid", hid))
    run(pool.execute("INSERT INTO depot (name, geom) VALUES ('Depot', ST_SetSRID(ST_MakePoint($1,$2),4326))", dlon, dlat))
    sid = run(pool.fetchval(
        "INSERT INTO shelter (name, kind, geom, source) VALUES ('Shelter', 'officer', ST_SetSRID(ST_MakePoint($1,$2),4326), 'officer') RETURNING id",
        slon, slat))

    gh = GraphHopper(GH)
    carto = Cartographer(pool, Fetcher(pool), gh)

    # 1. Open: soft risk 0.27 is under the safe limit.
    res = run(carto.run())
    assert res["ok"] and res["counts"] == {"open": 1}
    w = run(pool.fetchrow("SELECT * FROM access_window WHERE shelter_id=$1", sid))
    assert w["status"] == "open" and w["eta_min"] > 0
    base_eta = w["eta_min"]
    assert any(c["type"] == "hazard" for c in w["crossed"])  # the route is inside the flood zone and says so

    # Take a point on today's route to place the scenario's river crossing and road reports.
    mid = run(pool.fetchrow(
        "SELECT ST_X(p) lon, ST_Y(p) lat FROM (SELECT ST_LineInterpolatePoint(route, 0.5) p FROM access_window WHERE shelter_id=$1) q", sid))

    # 2. Closing: the river under that point is forecast to flood in 2 days (prob 0.9 >= 0.8 hard).
    pid = run(pool.fetchval(
        "INSERT INTO river_point (osm_ref, name, geom) VALUES ('scn/1', 'Scenario bridge', ST_SetSRID(ST_MakePoint($1,$2),4326)) RETURNING id",
        mid["lon"], mid["lat"]))
    run(pool.execute("INSERT INTO river_point_hazard VALUES ($1, $2::uuid)", pid, hid))
    for d in range(0, 8):
        day = date.today() + timedelta(days=d)
        prob = 0.9 if d >= 2 else 0.05
        run(pool.execute("INSERT INTO river_forecast (river_point_id, day, exceed_prob) VALUES ($1,$2,$3)", pid, day, prob))
    res = run(carto.run())
    w = run(pool.fetchrow("SELECT * FROM access_window WHERE shelter_id=$1", sid))
    if w["status"] == "closing":
        assert w["closes_at"] > now
        assert any(b["type"] == "river_point" for b in w["blocked_by"])
    else:  # the network has a detour around the bridge, which is also a correct answer
        assert w["status"] == "open"
        assert w["slices"][-1]["eta_min"] >= base_eta
    run(pool.execute("DELETE FROM river_forecast"))

    # 3. Field reports on the same road: first one is suspected (soft), second reporter confirms (hard).
    r1 = run(closures.add_report(pool, mid["lat"], mid["lon"], "tree across road", "driver-a", "test"))
    assert r1["action"] == "suspected"
    r2 = run(closures.add_report(pool, mid["lat"] + 0.0005, mid["lon"], "confirmed blocked", "driver-b", "test"))
    assert r2["action"] == "confirmed" and r2["closure_id"] == r1["closure_id"]
    res = run(carto.run())
    w = run(pool.fetchrow("SELECT * FROM access_window WHERE shelter_id=$1", sid))
    if w["status"] == "open":
        assert w["eta_min"] > base_eta                                 # rerouted around the closure
        assert not any(c["type"] == "closure" for c in w["crossed"])
    else:
        assert w["status"] == "cut_off"
        assert any(b["type"] == "closure" for b in w["blocked_by"])

    # 4. Officer clears it: back to the original route.
    run(closures.set_closure(pool, r1["closure_id"], clear=True))
    run(carto.run())
    w = run(pool.fetchrow("SELECT * FROM access_window WHERE shelter_id=$1", sid))
    assert w["status"] == "open" and w["eta_min"] == pytest.approx(base_eta, rel=0.05)
    run(gh.aclose())


def test_cut_off_when_every_road_is_closed(env):
    from rasad.agents.cartographer import Cartographer
    from rasad.connectors.graphhopper import GraphHopper
    from rasad.provenance import Fetcher

    loop, pool = env
    run = loop.run_until_complete
    (dlon, dlat), (slon, slat) = _pts()
    run(pool.execute("INSERT INTO depot (name, geom) VALUES ('Depot', ST_SetSRID(ST_MakePoint($1,$2),4326))", dlon, dlat))
    sid = run(pool.fetchval(
        "INSERT INTO shelter (name, kind, geom, source) VALUES ('Shelter', 'officer', ST_SetSRID(ST_MakePoint($1,$2),4326), 'officer') RETURNING id",
        slon, slat))
    gh = GraphHopper(GH)
    carto = Cartographer(pool, Fetcher(pool), gh)
    run(carto.run())
    # The whole neighbourhood around the shelter goes under water (a 400 m confirmed closure).
    # A single 60 m report is not enough here: a parallel street 62 m away still reaches it.
    run(pool.execute(
        """INSERT INTO closure (source, source_ref, geom, confirmed, reason)
           VALUES ('report', 'scn-area', ST_Buffer(ST_SetSRID(ST_MakePoint($1,$2),4326)::geography, 400)::geometry, true, 'locality flooded')""",
        slon, slat))
    run(carto.run())
    w = run(pool.fetchrow("SELECT status, blocked_by FROM access_window WHERE shelter_id=$1", sid))
    assert w["status"] == "cut_off"
    assert any(b["type"] in ("closure", "network") for b in w["blocked_by"])
    ev = run(pool.fetchval("SELECT count(*) FROM event_log WHERE type='access_window.changed' AND payload->>'status'='cut_off'"))
    assert ev == 1
    run(gh.aclose())
