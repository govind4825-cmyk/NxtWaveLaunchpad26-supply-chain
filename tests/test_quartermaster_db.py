"""Quartermaster against a real PostGIS database, with WorldPop replaced by a stub
(the test computes population as 1,000 people per 0.01 square degree so results are checkable).
Skipped unless RASAD_TEST_DATABASE_URL is set."""

import asyncio
import os
from datetime import UTC, datetime, timedelta

import pytest
from shapely.geometry import MultiPolygon, box

DB = os.environ.get("RASAD_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not DB, reason="RASAD_TEST_DATABASE_URL not set")


@pytest.fixture
def env():
    os.environ["RASAD_DATABASE_URL"] = DB
    os.environ["RASAD_EMBED_WORKER"] = "false"
    from rasad import db as dbmod
    from rasad.config import get_settings

    get_settings.cache_clear()
    dbmod._pool = None
    loop = asyncio.new_event_loop()

    async def setup():
        await dbmod.init_schema()
        pool = await dbmod.get_pool()
        await pool.execute(
            "TRUNCATE hazard, hazard_source, event_log, source_status, raw_fetch, depot, shelter, shelter_hazard, "
            "closure, road_report, access_window, catchment, headcount, need CASCADE"
        )
        return pool

    pool = loop.run_until_complete(setup())
    yield loop, pool
    loop.run_until_complete(dbmod.close_pool())
    loop.close()


async def stub_population(geom):
    return geom.area / 0.01 * 1000, None, None


def test_planning_then_headcount_then_access(env):
    from rasad import store
    from rasad.agents.quartermaster import Quartermaster, record_headcount
    from rasad.models import HazardRecord

    loop, pool = env
    run = loop.run_until_complete
    now = datetime.now(UTC)
    hid, _ = run(store.upsert_hazard(pool, HazardRecord(
        source="sachet", source_ref="Q1", kind="flood", title="Flood", severity=3, certainty=0.7, urgency="Expected",
        onset=now, expires=now + timedelta(hours=20), geom=MultiPolygon([box(85.0, 20.0, 85.2, 20.1)]),
        located_by="polygon", sent=now)))
    ids = []
    for lon in (85.05, 85.15):
        sid = run(pool.fetchval("INSERT INTO shelter (name, kind, geom, source) VALUES ('S', 'school', ST_SetSRID(ST_MakePoint($1, 20.05),4326), 'osm') RETURNING id", lon))
        run(pool.execute("INSERT INTO shelter_hazard VALUES ($1, $2::uuid)", sid, hid))
        ids.append(sid)

    qm = Quartermaster(pool, fetcher=None, population_fn=stub_population)
    summary = run(qm.run())
    assert summary["shelters_with_needs"] == 2 and summary["awaiting"] == 0

    # Each shelter serves half the 0.02 sq-degree hazard: 1,000 residents x 25% flood displacement = 250 people.
    n = run(pool.fetchrow("SELECT * FROM need WHERE shelter_id=$1 AND item='drinking_water'", ids[0]))
    assert n["people"] == 250 and n["people_basis"] == "planning"
    assert n["days"] == 3                                   # 20 h to expiry + 48 h recovery
    assert n["quantity"] == 250 * 15 * 3
    assert n["basis"]["catchments"][0]["residents"] == pytest.approx(1000)
    cat = run(pool.fetchrow("SELECT population, pop_year FROM catchment WHERE shelter_id=$1", ids[0]))
    assert cat["population"] == pytest.approx(1000) and cat["pop_year"] == 2020

    # The in-charge reports 410 people: the count replaces the estimate.
    run(record_headcount(pool, ids[0], 410, None, "in-charge", "test", None))
    run(qm.run())
    n = run(pool.fetchrow("SELECT * FROM need WHERE shelter_id=$1 AND item='drinking_water'", ids[0]))
    assert n["people"] == 410 and n["people_basis"] == "headcount"

    # The shelter is cut off: it jumps the priority list and is due now.
    run(pool.execute("INSERT INTO access_window (shelter_id, status) VALUES ($1, 'cut_off')", ids[1]))
    run(qm.run())
    a = run(pool.fetchrow("SELECT priority, needed_by FROM need WHERE shelter_id=$1 AND item='drinking_water'", ids[1]))
    b = run(pool.fetchrow("SELECT priority FROM need WHERE shelter_id=$1 AND item='drinking_water'", ids[0]))
    assert a["priority"] == 250 * 3.0 and b["priority"] == 410 * 1.0
    assert abs((a["needed_by"] - datetime.now(UTC)).total_seconds()) < 60

    # Committed stock (from the Procurer later) survives recomputation.
    run(pool.execute("UPDATE need SET committed=1000 WHERE shelter_id=$1 AND item='drinking_water'", ids[0]))
    run(qm.run())
    assert run(pool.fetchval("SELECT committed FROM need WHERE shelter_id=$1 AND item='drinking_water'", ids[0])) == 1000

    # Moving a shelter changes the catchments, which invalidates the cached population.
    run(pool.execute("UPDATE shelter SET geom = ST_SetSRID(ST_MakePoint(85.18, 20.05),4326) WHERE id=$1", ids[1]))
    run(qm._refresh_catchments(["flood"]))
    assert run(pool.fetchval("SELECT population FROM catchment WHERE shelter_id=$1", ids[1])) is None


def test_no_population_means_no_quantities(env):
    from rasad import store
    from rasad.agents.quartermaster import Quartermaster
    from rasad.models import HazardRecord

    loop, pool = env
    run = loop.run_until_complete
    now = datetime.now(UTC)
    hid, _ = run(store.upsert_hazard(pool, HazardRecord(
        source="sachet", source_ref="Q2", kind="flood", title="Flood", severity=3, certainty=0.7, urgency="Expected",
        onset=now, expires=None, geom=MultiPolygon([box(85.0, 20.0, 85.2, 20.1)]), located_by="polygon", sent=now)))
    sid = run(pool.fetchval("INSERT INTO shelter (name, kind, geom, source) VALUES ('S', 'school', ST_SetSRID(ST_MakePoint(85.1, 20.05),4326), 'osm') RETURNING id"))
    run(pool.execute("INSERT INTO shelter_hazard VALUES ($1, $2::uuid)", sid, hid))

    async def failing(_geom):
        return None, None, "HTTP 503"

    summary = run(Quartermaster(pool, fetcher=None, population_fn=failing).run())
    assert summary == {"shelters_with_needs": 0, "awaiting": 1, "outstanding": {}}
    assert run(pool.fetchval("SELECT count(*) FROM need")) == 0
    assert run(pool.fetchval("SELECT pop_error FROM catchment WHERE shelter_id=$1", sid)) == "HTTP 503"


def test_ledger_endpoint(env):
    from fastapi.testclient import TestClient

    from rasad import db as dbmod

    loop, pool = env
    run = loop.run_until_complete
    sid = run(pool.fetchval("INSERT INTO shelter (name, kind, geom, source) VALUES ('Hall', 'officer', ST_SetSRID(ST_MakePoint(85.1, 20.05),4326), 'officer') RETURNING id"))
    run(pool.execute(
        """INSERT INTO need (shelter_id, item, unit, quantity, committed, people, people_basis, days, rate, needed_by, priority)
           VALUES ($1, 'drinking_water', 'litres', 9000, 9000, 200, 'headcount', 3, 'r', now(), 200),
                  ($1, 'cereal_ration', 'kg', 280, 0, 200, 'headcount', 3, 'r', now(), 200)""", sid))
    run(dbmod.close_pool())
    dbmod._pool = None

    from rasad.api.main import app

    with TestClient(app) as c:
        d = c.get("/api/ledger").json()
        assert d["people"] == 200 and d["people_from_headcounts"] == 200
        items = {i["item"]: i for i in d["items"]}
        assert items["drinking_water"]["status"] == "covered" and items["drinking_water"]["short"] == 0
        assert items["cereal_ration"]["status"] == "short" and items["cereal_ration"]["short"] == 280
        assert c.get("/ledger").status_code == 200
        r = c.post(f"/api/shelters/{sid}/headcount", json={"people": 230, "reporter": "x"})
        assert r.status_code == 200
        assert c.post("/api/shelters/999999/headcount", json={"people": 1, "reporter": "x"}).status_code == 404
        assert c.post(f"/api/shelters/{sid}/headcount", json={"people": -5, "reporter": "x"}).status_code == 422
