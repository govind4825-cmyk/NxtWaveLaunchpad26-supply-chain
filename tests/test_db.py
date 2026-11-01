"""Integration tests against a real PostGIS database.

Set RASAD_TEST_DATABASE_URL to run them; they are skipped otherwise. Records are
built in code to exercise merge/update/cancel paths; nothing here is loaded by the app.
"""

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from shapely.geometry import MultiPolygon, box

TEST_DB = os.environ.get("RASAD_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not TEST_DB, reason="RASAD_TEST_DATABASE_URL not set")

UTC = timezone.utc
NOW = datetime.now(UTC)


def rec(**kw):
    from rasad.models import HazardRecord

    base = dict(
        source="sachet", source_ref="A1", kind="heavy_rain", title="Heavy rain", severity=2, certainty=0.7,
        urgency="Expected", onset=NOW, expires=NOW + timedelta(days=1), geom=MultiPolygon([box(88, 22, 88.5, 22.5)]),
        located_by="polygon", area_desc="Alpha", sender="IMD", sent=NOW, raw_fetch_id=None,
    )
    base.update(kw)
    return HazardRecord(**base)


@pytest.fixture
def db():
    os.environ["RASAD_DATABASE_URL"] = TEST_DB
    from rasad import db as dbmod
    from rasad.config import get_settings

    get_settings.cache_clear()
    dbmod._pool = None

    async def setup():
        await dbmod.init_schema()
        pool = await dbmod.get_pool()
        await pool.execute(
            "TRUNCATE hazard, hazard_source, river_point, river_point_hazard, river_forecast, event_log, source_status, raw_fetch, vehicle, vehicle_position CASCADE"
        )
        return pool

    loop = asyncio.new_event_loop()
    pool = loop.run_until_complete(setup())
    yield loop, pool
    loop.run_until_complete(dbmod.close_pool())
    loop.close()


def test_create_merge_update_cancel(db):
    from rasad import store

    loop, pool = db
    run = loop.run_until_complete

    hid, action = run(store.upsert_hazard(pool, rec()))
    assert action == "created"

    # Same alert seen again with the same sent time: no churn.
    assert run(store.upsert_hazard(pool, rec()))[1] == "unchanged"

    # IMD's own feed reports the same event, more severe, slightly different polygon: merged.
    hid2, action = run(store.upsert_hazard(pool, rec(source="imd_cap", source_ref="IMD-9", severity=3, title="Very heavy rain",
                                                     geom=MultiPolygon([box(88.05, 22.05, 88.5, 22.55)]))))
    assert (hid2, action) == (hid, "merged")
    row = run(pool.fetchrow("SELECT severity, title FROM hazard WHERE id=$1::uuid", hid))
    assert row["severity"] == 3 and row["title"] == "Very heavy rain"
    assert run(pool.fetchval("SELECT count(*) FROM hazard_source WHERE hazard_id=$1::uuid", hid)) == 2

    # A different kind in the same place stays separate.
    _, action = run(store.upsert_hazard(pool, rec(source_ref="T1", kind="thunderstorm")))
    assert action == "created"

    # SACHET issues an Update referencing A1 that downgrades it: replace, not max.
    _, action = run(store.upsert_hazard(pool, rec(source_ref="A2", msg_type="Update", references=["A1"], severity=1,
                                                  sent=NOW + timedelta(hours=1))))
    assert action == "updated"
    assert run(pool.fetchval("SELECT severity FROM hazard WHERE id=$1::uuid", hid)) == 1

    # Cancel referencing the update.
    _, action = run(store.upsert_hazard(pool, rec(source_ref="A3", msg_type="Cancel", references=["A2"],
                                                  sent=NOW + timedelta(hours=2))))
    assert action == "cancelled"
    assert run(pool.fetchval("SELECT status FROM hazard WHERE id=$1::uuid", hid)) == "cancelled"

    # A cancel for something we never saw is ignored, not stored.
    assert run(store.upsert_hazard(pool, rec(source_ref="ZZ", msg_type="Cancel", references=["nope"])))[1] == "ignored"


def test_better_footprint_replaces_district(db):
    from rasad import store

    loop, pool = db
    run = loop.run_until_complete
    hid, _ = run(store.upsert_hazard(pool, rec(source_ref="D1", kind="flood", located_by="district",
                                               geom=MultiPolygon([box(80, 20, 82, 22)]))))
    _, action = run(store.upsert_hazard(pool, rec(source="gdacs", source_ref="FL:1", kind="flood", located_by="gdacs",
                                                  geom=MultiPolygon([box(80.5, 20.5, 81.5, 21.5)]))))
    assert action == "merged"
    assert run(pool.fetchval("SELECT located_by FROM hazard WHERE id=$1::uuid", hid)) == "gdacs"


def test_expire_and_source_status(db):
    from rasad import store

    loop, pool = db
    run = loop.run_until_complete
    hid, _ = run(store.upsert_hazard(pool, rec(source_ref="E1", expires=NOW - timedelta(hours=3), onset=NOW - timedelta(hours=8))))
    assert hid in run(store.expire_old(pool))
    run(store.mark_source(pool, "sachet", False, None, None, "timeout"))
    run(store.mark_source(pool, "sachet", False, None, None, "timeout"))
    assert run(pool.fetchval("SELECT consecutive_failures FROM source_status WHERE source='sachet'")) == 2
    run(store.mark_source(pool, "sachet", True, None, 12, None))
    r = run(pool.fetchrow("SELECT consecutive_failures, items_last_run FROM source_status WHERE source='sachet'"))
    assert (r["consecutive_failures"], r["items_last_run"]) == (0, 12)
