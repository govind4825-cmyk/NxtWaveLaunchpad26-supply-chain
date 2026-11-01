"""Dispatcher against a real GraphHopper and PostGIS.

Uses the same variables as test_cartographer_gh.py plus RASAD_TEST_DISPATCH, a list of
road points on the loaded extract: "depot;supplier;shelter1;shelter2;shelter3", each "lon,lat".
The defaults are Andorra la Vella, Escaldes, Encamp, Sant Julia and La Massana, which
fit the Andorra extract used in development. Orders and drivers are test inputs; the
roads, routing and travel times are real. Skipped when not configured.
"""

import asyncio
import os
from datetime import UTC, datetime, timedelta

import pytest

DB = os.environ.get("RASAD_TEST_DATABASE_URL")
GH = os.environ.get("RASAD_TEST_GRAPHHOPPER_URL")
PTS = os.environ.get("RASAD_TEST_DISPATCH", "1.5218,42.5063;1.5340,42.5100;1.5806,42.5354;1.4915,42.4637;1.5150,42.5440")
pytestmark = pytest.mark.skipif(not (DB and GH), reason="GraphHopper integration not configured")


@pytest.fixture
def env():
    os.environ["RASAD_DATABASE_URL"] = DB
    os.environ["RASAD_GRAPHHOPPER_URL"] = GH
    os.environ["RASAD_SOLVER_SECONDS"] = "2"
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
            "raw_fetch, depot, shelter, shelter_hazard, closure, road_report, access_window, need, procurement, "
            "trip_stop, trip, driver, supplier_message, vehicle, vehicle_position CASCADE")
        return pool

    pool = loop.run_until_complete(setup())
    yield loop, pool
    loop.run_until_complete(dbmod.close_pool())
    os.environ.pop("RASAD_SOLVER_SECONDS", None)
    get_settings.cache_clear()
    loop.close()


def _pts():
    return [tuple(map(float, p.split(","))) for p in PTS.split(";")]


async def _shelter(pool, name, lon, lat):
    return await pool.fetchval(
        "INSERT INTO shelter (name, kind, geom, source) VALUES ($1,'officer', ST_SetSRID(ST_MakePoint($2,$3),4326),'officer') RETURNING id",
        name, lon, lat)


async def _order(pool, shelter_id, item, qty, unit, sup, needed_by=None):
    return await pool.fetchval(
        """INSERT INTO procurement (shelter_id, item, quantity, unit, status, transaction_id, provider_name, pickup_lon, pickup_lat, needed_by)
           VALUES ($1,$2,$3,$4,'accepted', gen_random_uuid()::text, 'Escaldes Traders', $5, $6, $7) RETURNING id""",
        shelter_id, item, qty, unit, sup[0], sup[1], needed_by)


def test_plan_deliver_and_reroute(env):
    from rasad import closures, fleet
    from rasad.agents.dispatcher import Dispatcher
    from rasad.connectors.graphhopper import GraphHopper

    loop, pool = env
    run = loop.run_until_complete
    depot, sup, enc, sjl, mas = _pts()
    now = datetime.now(UTC)
    dep_id = run(pool.fetchval("INSERT INTO depot (name, geom) VALUES ('Depot', ST_SetSRID(ST_MakePoint($1,$2),4326)) RETURNING id", *depot))
    s_enc = run(_shelter(pool, "Encamp school", *enc))
    s_sjl = run(_shelter(pool, "Sant Julia hall", *sjl))
    s_mas = run(_shelter(pool, "La Massana hall", *mas))

    # 5,000 L of water is more than one truck carries: it must be split.
    o_water = run(_order(pool, s_enc, "drinking_water", 5000, "litres", sup, now + timedelta(hours=3)))
    o_rice = run(_order(pool, s_sjl, "cereal_ration", 800, "kg", sup, now + timedelta(hours=6)))
    # La Massana's only road in is shut: that order cannot be planned.
    run(pool.execute("""INSERT INTO closure (source, source_ref, geom, confirmed, reason, reporters)
                        VALUES ('officer','t1', ST_SetSRID(ST_MakePoint($1,$2),4326), true, 'bridge washed out', '{}')""", *mas))
    o_cut = run(_order(pool, s_mas, "bathing_soap", 5000, "g", sup))

    run(fleet.register(pool, "Ravi", "9876500001", 3000, "mini truck", None, dep_id))
    run(fleet.register(pool, "Asha", "9876500002", 3000, "mini truck", "asha-phone", dep_id))

    disp = Dispatcher(pool, GraphHopper(GH))
    res = run(disp.run())
    assert res["ok"] and res["trips"] >= 1, res

    trips = run(pool.fetch("SELECT id, driver_id FROM trip WHERE status='planned' ORDER BY id"))
    stops = run(pool.fetch("SELECT * FROM trip_stop ORDER BY trip_id, seq"))
    # Every pickup comes before its drop on the same trip, ETAs only go forward, no truck is overloaded.
    for t in trips:
        ts = [s for s in stops if s["trip_id"] == t["id"]]
        seq_of = {(s["chunk"], s["kind"]): s["seq"] for s in ts}
        for s in ts:
            if s["kind"] == "drop":
                assert seq_of[(s["chunk"], "pickup")] < s["seq"]
        etas = [s["eta"] for s in ts]
        assert etas == sorted(etas)
        load = peak = 0.0
        for s in ts:
            load += s["quantity"] if s["kind"] == "pickup" else -s["quantity"]
            peak = max(peak, load)
        assert peak <= 3000 + 1e-6
    water = [s for s in stops if s["procurement_id"] == o_water and s["kind"] == "drop"]
    assert abs(sum(s["quantity"] for s in water) - 5000) < 1e-6 and len(water) >= 2
    assert any(s["procurement_id"] == o_rice for s in stops)
    assert not any(s["procurement_id"] == o_cut for s in stops)
    note = run(pool.fetchval("SELECT dispatch_note FROM procurement WHERE id=$1", o_cut))
    assert note and "no safe road" in note
    out = run(pool.fetch("SELECT text FROM supplier_message WHERE driver_id IS NOT NULL AND direction='out' AND text LIKE 'RASAD trip%'"))
    assert out and "/d/" in out[0]["text"] and "PICK UP" in out[0]["text"]

    # A second run does not plan the same goods twice.
    res2 = run(disp.run())
    assert res2.get("trips", 0) == 0
    assert run(pool.fetchval("SELECT count(*) FROM trip")) == len(trips)
    assert "no safe road" in run(pool.fetchval("SELECT dispatch_note FROM procurement WHERE id=$1", o_cut))   # real cause kept

    # The driver works the trip from WhatsApp-style commands.
    tid = trips[0]["id"]
    drv = run(pool.fetchrow("SELECT * FROM driver WHERE id=$1", trips[0]["driver_id"]))
    ts = [s for s in stops if s["trip_id"] == tid]
    first_drop = next(s for s in ts if s["kind"] == "drop")
    reply = run(fleet.handle_text(pool, drv, f"DONE {first_drop['seq']}", "whatsapp"))
    assert "Collect it first" in reply
    first_pick = next(s for s in ts if s["kind"] == "pickup" and s["chunk"] == first_drop["chunk"])
    reply = run(fleet.handle_text(pool, drv, f"done {first_pick['seq']}", "whatsapp"))
    assert reply.startswith("Picked up")
    assert run(pool.fetchval("SELECT status FROM procurement WHERE id=$1", first_pick["procurement_id"])) == "in_transit"
    assert run(pool.fetchval("SELECT status FROM trip WHERE id=$1", tid)) == "en_route"
    reply = run(fleet.handle_text(pool, drv, "BLOCKED near the bridge", "whatsapp"))
    assert "need your location" in reply     # no GPS from this driver: the road can't be placed

    # A confirmed closure lands on the road to the first drop: the trip is rerouted or the drop held.
    leg = run(pool.fetchrow(
        "SELECT ST_X(ST_LineInterpolatePoint(leg, 0.5)) AS lon, ST_Y(ST_LineInterpolatePoint(leg, 0.5)) AS lat FROM trip_stop WHERE id=$1",
        first_drop["id"]))
    rep = run(closures.add_report(pool, leg["lat"], leg["lon"], "tree down", "officer-test", "test"))
    run(closures.set_closure(pool, rep["closure_id"], confirmed=True))
    disp._zones_dirty = True
    mon = run(disp.monitor())
    assert mon["zones_checked"]
    after = run(pool.fetchrow("SELECT status, note, leg FROM trip_stop WHERE id=$1", first_drop["id"]))
    if after["status"] == "pending":
        crosses = run(pool.fetchval(
            "SELECT ST_DWithin(leg::geography, ST_SetSRID(ST_MakePoint($2,$3),4326)::geography, 30) FROM trip_stop WHERE id=$1",
            first_drop["id"], leg["lon"], leg["lat"]))
        assert not crosses
    else:
        assert after["status"] == "blocked" and after["note"].startswith("held")   # goods on board wait for a safe road
    assert run(pool.fetchval("SELECT count(*) FROM event_log WHERE type='trip.rerouted'")) >= 1

    # Clear the closure and finish every stop: orders become delivered and the trip closes.
    run(closures.set_closure(pool, rep["closure_id"], clear=True))
    disp._zones_dirty = True
    run(disp.monitor())
    for s in run(pool.fetch("SELECT id, seq, kind FROM trip_stop WHERE trip_id=$1 AND status IN ('pending','blocked') ORDER BY seq", tid)):
        ok, msg = run(fleet.act(pool, drv, s["id"], "done"))
        assert ok, msg
    assert run(pool.fetchval("SELECT status FROM trip WHERE id=$1", tid)) == "done"
    delivered = run(pool.fetch("SELECT DISTINCT procurement_id FROM trip_stop WHERE trip_id=$1", tid))
    for r in delivered:
        p = run(pool.fetchrow("SELECT status, quantity, delivered_at FROM procurement WHERE id=$1", r["procurement_id"]))
        done_q = run(pool.fetchval("SELECT sum(quantity) FROM trip_stop WHERE procurement_id=$1 AND kind='drop' AND status='done'", r["procurement_id"]))
        assert (p["status"] == "delivered") == (abs(done_q - p["quantity"]) < 1e-6)
    run(disp.gh.aclose())


def test_gps_deviation_reroutes_from_where_the_driver_is(env):
    from rasad import fleet
    from rasad.agents.dispatcher import STRIKES_TO_REROUTE, Dispatcher
    from rasad.connectors.graphhopper import GraphHopper

    loop, pool = env
    run = loop.run_until_complete
    depot, sup, enc, sjl, _mas = _pts()
    dep_id = run(pool.fetchval("INSERT INTO depot (name, geom) VALUES ('Depot', ST_SetSRID(ST_MakePoint($1,$2),4326)) RETURNING id", *depot))
    s_enc = run(_shelter(pool, "Encamp school", *enc))
    run(_order(pool, s_enc, "drinking_water", 1000, "litres", sup))
    run(fleet.register(pool, "Asha", "9876500002", 3000, "mini truck", "asha-phone", dep_id))
    run(pool.execute("INSERT INTO vehicle (device_id, name, unique_id, status) VALUES (7, 'Asha phone', 'asha-phone', 'online')"))

    async def fix(lon, lat, minutes_ago=0):
        await pool.execute("INSERT INTO vehicle_position (device_id, fix_time, geom) VALUES (7, $1, ST_SetSRID(ST_MakePoint($2,$3),4326))",
                           datetime.now(UTC) - timedelta(minutes=minutes_ago), lon, lat)

    run(fix(*depot))          # live GPS at the depot: planning starts from the phone's position
    disp = Dispatcher(pool, GraphHopper(GH))
    assert run(disp.run())["trips"] == 1
    tid = run(pool.fetchval("SELECT id FROM trip"))
    assert run(pool.fetchval("SELECT plan->>'start_from' FROM trip WHERE id=$1", tid)) == "gps"

    # The driver heads south to Sant Julia instead: well off the planned road.
    for _ in range(STRIKES_TO_REROUTE):
        run(fix(*sjl))
        run(disp.monitor())
    assert run(pool.fetchval("SELECT status FROM trip WHERE id=$1", tid)) == "en_route"
    ev = run(pool.fetchval("SELECT payload->>'reason' FROM event_log WHERE type='trip.rerouted' ORDER BY id DESC LIMIT 1"))
    assert ev and "off the planned road" in ev
    start = run(pool.fetchrow(
        "SELECT ST_X(ST_StartPoint(leg)) AS lon, ST_Y(ST_StartPoint(leg)) AS lat FROM trip_stop WHERE trip_id=$1 AND status='pending' ORDER BY seq LIMIT 1",
        tid))
    from rasad.engine.risk import haversine_km
    assert haversine_km(start["lon"], start["lat"], *sjl) < 0.3      # new route starts where the driver is
    assert run(pool.fetchval("SELECT deviation_strikes FROM trip WHERE id=$1", tid)) == 0
    msg = run(pool.fetchval("SELECT text FROM supplier_message WHERE driver_id IS NOT NULL ORDER BY id DESC LIMIT 1"))
    assert msg.startswith("Route changed")
    run(disp.gh.aclose())
