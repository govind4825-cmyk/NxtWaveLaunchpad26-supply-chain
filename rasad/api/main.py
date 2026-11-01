"""Control-room API: hazards as GeoJSON, provenance, vehicles, health, and a live event socket."""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .. import closures as closures_mod
from .. import fleet
from .. import messaging
from ..agents.cartographer import Cartographer, build_slices, load_inputs
from ..agents.dispatcher import Dispatcher
from ..agents.procurer import Procurer
from ..agents.quartermaster import Quartermaster, record_headcount
from ..beckn import routes as beckn_routes
from ..beckn.node import identity
from ..engine.needs import KCAL_PER_PERSON_DAY, SPHERE
from ..supplier_inbox import handle_text
from ..suppliers import ITEMS, describe, new_token, normalise_phone
from ..agents.sentinel import Sentinel
from ..config import get_settings
from ..connectors.boundaries import Boundaries
from ..db import close_pool, get_pool, init_schema
from ..events import STREAM, get_redis, publish, send_command
from ..provenance import Fetcher, raw_body
from ..traccar_link import TraccarLink

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("rasad.api")

WEB_DIR = Path(__file__).resolve().parents[2] / "web"
SOURCES = ["sachet", "imd_cap", "gdacs", "river_watch", "cartographer", "quartermaster", "procurer", "dispatcher", "tomtom", "worldpop"]
REFRESHABLE = ["sachet", "imd_cap", "gdacs", "river_watch", "cartographer", "quartermaster", "procurer", "dispatcher"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    s = get_settings()
    await init_schema()
    pool = await get_pool()
    app.state.pool = pool
    app.state.sentinel = None
    app.state.traccar = TraccarLink(pool)
    fetcher = Fetcher(pool)
    app.state.cartographer = None
    app.state.quartermaster = None
    app.state.procurer = None
    app.state.dispatcher = None
    await identity(pool)
    if s.embed_worker:
        app.state.sentinel = Sentinel(pool, fetcher, Boundaries(s.boundaries_path))
        await app.state.sentinel.start()
        app.state.cartographer = Cartographer(pool, fetcher)
        await app.state.cartographer.start()
        app.state.quartermaster = Quartermaster(pool, fetcher)
        await app.state.quartermaster.start()
        app.state.procurer = Procurer(pool)
        await app.state.procurer.start()
        app.state.dispatcher = Dispatcher(pool)
        await app.state.dispatcher.start()
        await app.state.traccar.start()
    yield
    if app.state.sentinel:
        await app.state.sentinel.stop()
    if app.state.cartographer:
        await app.state.cartographer.stop()
    if app.state.quartermaster:
        await app.state.quartermaster.stop()
    if app.state.procurer:
        await app.state.procurer.stop()
    if app.state.dispatcher:
        await app.state.dispatcher.stop()
    await app.state.traccar.stop()
    await fetcher.aclose()
    await close_pool()


app = FastAPI(title="RASAD control room", lifespan=lifespan)
app.include_router(beckn_routes.router)


def _iso(v):
    return v.isoformat() if isinstance(v, datetime) else v


@app.get("/health")
async def health():
    s = get_settings()
    pool = app.state.pool
    now = datetime.now(UTC)
    rows = {r["source"]: r for r in await pool.fetch("SELECT * FROM source_status")}
    feeds = []
    for src in SOURCES:
        r = rows.get(src)
        if src == "worldpop":
            wp = await pool.fetchrow(
                """SELECT count(*) FILTER (WHERE population IS NOT NULL) ok_n, count(*) FILTER (WHERE pop_error IS NOT NULL) err_n,
                          max(computed_at) FILTER (WHERE population IS NOT NULL) last_ok,
                          (array_agg(pop_error ORDER BY computed_at DESC) FILTER (WHERE pop_error IS NOT NULL))[1] last_err
                   FROM catchment""")
            state = "waiting" if not (wp["ok_n"] or wp["err_n"]) else ("ok" if wp["ok_n"] and not wp["err_n"] else ("degraded" if wp["ok_n"] else "down"))
            feeds.append({"source": src, "state": state, "last_success": _iso(wp["last_ok"]), "items_last_run": wp["ok_n"],
                          "last_error": wp["last_err"], "consecutive_failures": wp["err_n"]})
            continue
        if src == "tomtom" and not s.tomtom_key:
            feeds.append({"source": src, "state": "off", "last_success": None, "last_error": "no TomTom key set"})
            continue
        if r is None:
            feeds.append({"source": src, "state": "waiting", "last_success": None})
            continue
        age = (now - r["last_success"]).total_seconds() if r["last_success"] else None
        if age is None:
            state = "down"
        elif age > s.stale_after * (8 if src == "river_watch" else 1):
            state = "stale"
        elif r["consecutive_failures"]:
            state = "degraded"
        else:
            state = "ok"
        feeds.append(
            {
                "source": src,
                "state": state,
                "last_success": _iso(r["last_success"]),
                "last_attempt": _iso(r["last_attempt"]),
                "age_seconds": int(age) if age is not None else None,
                "consecutive_failures": r["consecutive_failures"],
                "last_error": r["last_error"],
                "items_last_run": r["items_last_run"],
                "last_raw_fetch_id": r["last_raw_fetch_id"],
            }
        )
    try:
        redis_ok = bool(await get_redis().ping())
    except Exception:
        redis_ok = False
    return {
        "now": now.isoformat(),
        "database": True,
        "redis": redis_ok,
        "worker_embedded": app.state.sentinel is not None,
        "feeds": feeds,
        "traccar": app.state.traccar.health(),
    }


@app.get("/api/hazards")
async def hazards(include_inactive: bool = False):
    pool = app.state.pool
    rows = await pool.fetch(
        """SELECT h.id, h.kind, h.title, h.severity, h.certainty, h.urgency, h.onset, h.expires, h.located_by,
                  h.area_desc, h.status, h.first_seen, h.last_updated, h.attrs,
                  ST_AsGeoJSON(h.geom, 5)::json AS geometry,
                  ST_AsGeoJSON(ST_PointOnSurface(h.geom), 5)::json AS label_point,
                  COALESCE(json_agg(json_build_object(
                      'source', s.source, 'ref', s.source_ref, 'url', s.source_url, 'sender', s.sender,
                      'sent', s.sent, 'msg_type', s.msg_type, 'raw_fetch_id', s.raw_fetch_id)
                    ORDER BY s.sent DESC NULLS LAST) FILTER (WHERE s.source IS NOT NULL), '[]') AS sources
           FROM hazard h LEFT JOIN hazard_source s ON s.hazard_id = h.id
           WHERE ($1 OR h.status='active')
           GROUP BY h.id
           ORDER BY h.severity DESC, h.last_updated DESC
           LIMIT 500""",
        include_inactive,
    )
    features = []
    for r in rows:
        attrs = r["attrs"] or {}
        features.append(
            {
                "type": "Feature",
                "id": str(r["id"]),
                "geometry": r["geometry"],
                "properties": {
                    "id": str(r["id"]),
                    "kind": r["kind"],
                    "title": r["title"],
                    "severity": r["severity"],
                    "certainty": r["certainty"],
                    "urgency": r["urgency"],
                    "onset": _iso(r["onset"]),
                    "expires": _iso(r["expires"]),
                    "located_by": r["located_by"],
                    "area_desc": r["area_desc"],
                    "status": r["status"],
                    "first_seen": _iso(r["first_seen"]),
                    "last_updated": _iso(r["last_updated"]),
                    "label_point": r["label_point"],
                    "river_watch": attrs.get("river_watch"),
                    "matched_areas": [m.get("matched_areas") for m in (attrs.get("messages") or {}).values() if m.get("matched_areas")],
                    "sources": r["sources"],
                },
            }
        )
    return {"type": "FeatureCollection", "features": features, "generated_at": datetime.now(UTC).isoformat()}


@app.get("/api/hazards/{hazard_id}")
async def hazard_detail(hazard_id: str):
    pool = app.state.pool
    h = await pool.fetchrow("SELECT id, attrs FROM hazard WHERE id::text=$1", hazard_id)
    if h is None:
        raise HTTPException(404, "hazard not found")
    points = await pool.fetch(
        """SELECT p.id, p.name, p.road_class, p.osm_ref, ST_Y(p.geom) AS lat, ST_X(p.geom) AS lon,
                  p.threshold_m3s, p.warning_m3s, p.threshold_method, p.threshold_raw_fetch_id, p.no_river, p.forecast_at,
                  COALESCE(json_agg(json_build_object('day', f.day, 'discharge', f.discharge, 'median', f.q_median,
                      'p25', f.q_p25, 'p75', f.q_p75, 'min', f.q_min, 'max', f.q_max, 'exceed_prob', f.exceed_prob,
                      'raw_fetch_id', f.raw_fetch_id) ORDER BY f.day) FILTER (WHERE f.day IS NOT NULL), '[]') AS forecast
           FROM river_point_hazard rh JOIN river_point p ON p.id = rh.river_point_id
           LEFT JOIN river_forecast f ON f.river_point_id = p.id AND f.day >= current_date
           WHERE rh.hazard_id = $1
           GROUP BY p.id ORDER BY p.road_class NULLS LAST""",
        h["id"],
    )
    return {
        "id": str(h["id"]),
        "messages": (h["attrs"] or {}).get("messages", {}),
        "river_points": [
            {**{k: _iso(v) for k, v in dict(p).items() if k != "forecast"}, "forecast": p["forecast"]} for p in points
        ],
    }


@app.get("/api/river-points")
async def river_points():
    rows = await app.state.pool.fetch(
        """SELECT p.id, p.name, p.road_class, ST_Y(p.geom) AS lat, ST_X(p.geom) AS lon, p.threshold_m3s, p.no_river,
                  (SELECT max(exceed_prob) FROM river_forecast f WHERE f.river_point_id=p.id
                     AND f.day BETWEEN current_date AND current_date + 3) AS p3
           FROM river_point p
           WHERE EXISTS (SELECT 1 FROM river_point_hazard rh JOIN hazard h ON h.id=rh.hazard_id
                         WHERE rh.river_point_id=p.id AND h.status='active')"""
    )
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
                "properties": {"id": r["id"], "name": r["name"], "road_class": r["road_class"],
                               "threshold_m3s": r["threshold_m3s"], "no_river": r["no_river"], "p3": r["p3"]},
            }
            for r in rows
        ],
    }


@app.get("/api/vehicles")
async def vehicles():
    rows = await app.state.pool.fetch(
        """SELECT DISTINCT ON (p.device_id) p.device_id, v.name, p.fix_time, p.speed_kmh, p.course,
                  ST_Y(p.geom) AS lat, ST_X(p.geom) AS lon
           FROM vehicle_position p LEFT JOIN vehicle v ON v.device_id = p.device_id
           ORDER BY p.device_id, p.fix_time DESC"""
    )
    now = datetime.now(UTC)
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
                "properties": {
                    "device_id": r["device_id"],
                    "name": r["name"] or f"Device {r['device_id']}",
                    "fix_time": _iso(r["fix_time"]),
                    "age_seconds": int((now - r["fix_time"]).total_seconds()),
                    "speed_kmh": r["speed_kmh"],
                    "course": r["course"],
                },
            }
            for r in rows
        ],
    }


@app.get("/api/raw/{raw_id}")
async def raw(raw_id: int, meta: bool = False):
    got = await raw_body(app.state.pool, raw_id)
    if got is None:
        raise HTTPException(404, "no such fetch")
    body, ctype, info = got
    if meta:
        return info
    return Response(
        content=body,
        media_type=(ctype or "application/octet-stream").split(";")[0] if body else "text/plain",
        headers={"X-Rasad-Source": info["source"], "X-Rasad-Url": info["url"], "X-Rasad-Fetched-At": info["fetched_at"]},
    )


@app.get("/api/events")
async def events(since_id: int = 0, limit: int = 100):
    rows = await app.state.pool.fetch(
        "SELECT id, at, type, payload FROM event_log WHERE id > $1 ORDER BY id DESC LIMIT $2", since_id, min(limit, 500)
    )
    return [{"id": r["id"], "at": _iso(r["at"]), "type": r["type"], "payload": r["payload"]} for r in rows]


@app.post("/api/refresh/{source}")
async def refresh(source: str):
    if source not in REFRESHABLE:
        raise HTTPException(400, f"unknown source; use one of {REFRESHABLE}")
    if source == "procurer":
        if app.state.procurer is not None:
            app.state.procurer.trigger()
            return {"triggered": True, "via": "in-process"}
        await publish(app.state.pool, "procurement.approved", {"reason": "manual refresh"})
        return {"triggered": True, "via": "event"}
    if source == "dispatcher":
        if app.state.dispatcher is not None:
            app.state.dispatcher.trigger()
            return {"triggered": True, "via": "in-process"}
        await publish(app.state.pool, "driver.updated", {"reason": "manual refresh"})
        return {"triggered": True, "via": "event"}
    if source == "quartermaster":
        if app.state.quartermaster is not None:
            app.state.quartermaster.trigger()
            return {"triggered": True, "via": "in-process"}
        await publish(app.state.pool, "headcount.updated", {"reason": "manual refresh"})
        return {"triggered": True, "via": "event"}
    if source == "cartographer":
        if app.state.cartographer is not None:
            app.state.cartographer.trigger()
            return {"triggered": True, "via": "in-process"}
        await publish(app.state.pool, "depot.updated", {"reason": "manual refresh"})
        return {"triggered": True, "via": "event"}
    if app.state.sentinel is not None:
        return {"triggered": app.state.sentinel.trigger(source), "via": "in-process"}
    return {"triggered": await send_command({"cmd": "refresh", "source": source}), "via": "redis"}


# ---------------- Cartographer: depots, shelters, closures, risk ----------------

class PointIn(BaseModel):
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    name: str = Field(min_length=1, max_length=120)
    note: str | None = Field(default=None, max_length=500)


class ReportIn(BaseModel):
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    reporter: str = Field(min_length=1, max_length=80)
    note: str | None = Field(default=None, max_length=500)
    channel: str = Field(default="control_room", max_length=40)


def _point_fc(rows, props):
    return {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
         "properties": {k: _iso(r[k]) for k in props}} for r in rows]}


@app.get("/api/depots")
async def list_depots():
    rows = await app.state.pool.fetch("SELECT id, name, note, created_at, ST_X(geom) lon, ST_Y(geom) lat FROM depot WHERE active ORDER BY id")
    return _point_fc(rows, ["id", "name", "note", "created_at"])


@app.post("/api/depots")
async def add_depot(d: PointIn):
    did = await app.state.pool.fetchval(
        "INSERT INTO depot (name, geom, note) VALUES ($1, ST_SetSRID(ST_MakePoint($2,$3),4326), $4) RETURNING id",
        d.name.strip(), d.lon, d.lat, d.note,
    )
    await publish(app.state.pool, "depot.updated", {"depot_id": did, "action": "added", "name": d.name})
    return {"id": did}


@app.delete("/api/depots/{depot_id}")
async def remove_depot(depot_id: int):
    n = await app.state.pool.execute("UPDATE depot SET active=false WHERE id=$1 AND active", depot_id)
    if not n.endswith("1"):
        raise HTTPException(404, "no such depot")
    await publish(app.state.pool, "depot.updated", {"depot_id": depot_id, "action": "removed"})
    return {"ok": True}


@app.post("/api/shelters")
async def add_shelter(sh: PointIn):
    sid = await app.state.pool.fetchval(
        "INSERT INTO shelter (name, kind, geom, source) VALUES ($1, 'officer', ST_SetSRID(ST_MakePoint($2,$3),4326), 'officer') RETURNING id",
        sh.name.strip(), sh.lon, sh.lat,
    )
    await app.state.pool.execute(
        """INSERT INTO shelter_hazard (shelter_id, hazard_id) SELECT $1, h.id FROM hazard h
           WHERE h.status='active' AND h.geom IS NOT NULL AND ST_Within(ST_SetSRID(ST_MakePoint($2,$3),4326), h.geom)
           ON CONFLICT DO NOTHING""",
        sid, sh.lon, sh.lat,
    )
    await publish(app.state.pool, "shelter.updated", {"shelter_id": sid, "action": "added", "name": sh.name})
    return {"id": sid}


@app.get("/api/shelters")
async def shelters():
    rows = await app.state.pool.fetch(
        """SELECT s.id, s.name, s.kind, s.source, s.osm_ref, s.raw_fetch_id, ST_X(s.geom) lon, ST_Y(s.geom) lat,
                  a.status, a.closes_at, a.computed_at, a.eta_min, a.distance_km, a.max_risk, a.crossed, a.blocked_by,
                  a.slices, a.note, d.name AS depot_name,
                  (SELECT max(people) FROM need n WHERE n.shelter_id=s.id) AS people,
                  (SELECT max(people_basis) FROM need n WHERE n.shelter_id=s.id) AS people_basis,
                  (SELECT max(priority) FROM need n WHERE n.shelter_id=s.id) AS priority
           FROM shelter s
           LEFT JOIN access_window a ON a.shelter_id = s.id
           LEFT JOIN depot d ON d.id = a.depot_id
           WHERE s.active AND (s.source='officer' OR EXISTS (
               SELECT 1 FROM shelter_hazard sh JOIN hazard h ON h.id=sh.hazard_id WHERE sh.shelter_id=s.id AND h.status='active'))
           ORDER BY CASE a.status WHEN 'cut_off' THEN 0 WHEN 'closing' THEN 1 WHEN 'unknown' THEN 2 WHEN 'unroutable' THEN 3 WHEN 'open' THEN 4 ELSE 5 END,
                    a.closes_at NULLS LAST, s.id"""
    )
    return _point_fc(rows, ["id", "name", "kind", "source", "osm_ref", "raw_fetch_id", "status", "closes_at", "computed_at",
                            "eta_min", "distance_km", "max_risk", "crossed", "blocked_by", "slices", "note", "depot_name",
                            "people", "people_basis", "priority"])


@app.get("/api/shelters/{shelter_id}/route")
async def shelter_route(shelter_id: int):
    row = await app.state.pool.fetchrow(
        "SELECT ST_AsGeoJSON(route, 5)::json AS g, status, eta_min FROM access_window WHERE shelter_id=$1", shelter_id
    )
    if row is None or row["g"] is None:
        raise HTTPException(404, "no route computed")
    return {"type": "Feature", "geometry": row["g"], "properties": {"status": row["status"], "eta_min": row["eta_min"]}}


@app.get("/api/closures")
async def closures():
    rows = await app.state.pool.fetch(
        """SELECT id, source, confirmed, reason, reporters, valid_from, valid_to, raw_fetch_id,
                  ST_AsGeoJSON(geom, 5)::json AS g FROM closure
           WHERE valid_to IS NULL OR valid_to > now() ORDER BY updated_at DESC LIMIT 300"""
    )
    return {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": r["g"], "properties": {
            "id": r["id"], "source": r["source"], "confirmed": r["confirmed"], "reason": r["reason"],
            "reports": len(r["reporters"] or []), "valid_to": _iso(r["valid_to"]), "raw_fetch_id": r["raw_fetch_id"]}}
        for r in rows]}


@app.post("/api/reports")
async def report(rep: ReportIn):
    """A field report that a road is blocked. One report = suspected closure; a second
    reporter nearby, a TomTom closure or an officer confirmation makes it hard."""
    return await closures_mod.add_report(app.state.pool, rep.lat, rep.lon, rep.note, rep.reporter, rep.channel)


@app.post("/api/closures/{closure_id}/confirm")
async def confirm_closure(closure_id: int):
    if not await closures_mod.set_closure(app.state.pool, closure_id, confirmed=True):
        raise HTTPException(404, "no such closure")
    return {"ok": True}


@app.post("/api/closures/{closure_id}/clear")
async def clear_closure(closure_id: int):
    if not await closures_mod.set_closure(app.state.pool, closure_id, clear=True):
        raise HTTPException(404, "no such closure")
    return {"ok": True}


# ---------------- Quartermaster: needs, headcounts, ledger ----------------

class HeadcountIn(BaseModel):
    people: int = Field(ge=0, le=200000)
    households: int | None = Field(default=None, ge=0, le=100000)
    reporter: str = Field(min_length=1, max_length=80)
    note: str | None = Field(default=None, max_length=500)
    channel: str = Field(default="control_room", max_length=40)


@app.post("/api/shelters/{shelter_id}/headcount")
async def headcount(shelter_id: int, h: HeadcountIn):
    if not await app.state.pool.fetchval("SELECT 1 FROM shelter WHERE id=$1 AND active", shelter_id):
        raise HTTPException(404, "no such shelter")
    hid = await record_headcount(app.state.pool, shelter_id, h.people, h.households, h.reporter, h.channel, h.note)
    return {"id": hid}


@app.get("/api/shelters/{shelter_id}/needs")
async def shelter_needs(shelter_id: int):
    pool = app.state.pool
    rows = await pool.fetch(
        """SELECT item, unit, quantity, committed, people, people_basis, days, rate, needed_by, priority, access_status, basis, computed_at
           FROM need WHERE shelter_id=$1 ORDER BY item""", shelter_id)
    hc = await pool.fetch("SELECT people, households, reporter, channel, note, at FROM headcount WHERE shelter_id=$1 ORDER BY at DESC LIMIT 5", shelter_id)
    cats = await pool.fetch(
        """SELECT c.hazard_id, h.title, h.kind, c.population, c.pop_year, c.pop_raw_fetch_id, c.pop_error,
                  round((ST_Area(c.geom::geography) / 1e6)::numeric, 1) AS area_km2
           FROM catchment c JOIN hazard h ON h.id=c.hazard_id WHERE c.shelter_id=$1""", shelter_id)
    return {
        "needs": [{k: _iso(v) for k, v in dict(r).items()} for r in rows],
        "headcounts": [{k: _iso(v) for k, v in dict(r).items()} for r in hc],
        "catchments": [{**{k: _iso(v) for k, v in dict(r).items()}, "hazard_id": str(r["hazard_id"]),
                        "area_km2": float(r["area_km2"]) if r["area_km2"] is not None else None} for r in cats],
        "kcal_per_person_day": KCAL_PER_PERSON_DAY,
    }


@app.get("/api/ledger")
async def ledger():
    """The public Needs Ledger: what is still short, where, and by when. Aggregated by hazard, then by item."""
    pool = app.state.pool
    rows = await pool.fetch(
        """SELECT n.item, n.unit, n.quantity, n.committed, n.people, n.people_basis, n.needed_by, n.priority, n.access_status,
                  s.id AS shelter_id, s.name AS shelter, ST_X(s.geom) lon, ST_Y(s.geom) lat,
                  (SELECT h.title FROM shelter_hazard sh JOIN hazard h ON h.id=sh.hazard_id
                   WHERE sh.shelter_id=s.id AND h.status='active' ORDER BY h.severity DESC LIMIT 1) AS hazard,
                  (SELECT h.area_desc FROM shelter_hazard sh JOIN hazard h ON h.id=sh.hazard_id
                   WHERE sh.shelter_id=s.id AND h.status='active' ORDER BY h.severity DESC LIMIT 1) AS area
           FROM need n JOIN shelter s ON s.id=n.shelter_id
           ORDER BY n.priority DESC, n.needed_by"""
    )
    labels = {r.item: r for r in SPHERE}
    items: dict[str, dict] = {}
    shelters: dict[int, dict] = {}
    for r in rows:
        short = max(0.0, r["quantity"] - r["committed"])
        it = items.setdefault(r["item"], {"item": r["item"], "unit": r["unit"], "needed": 0.0, "committed": 0.0, "short": 0.0,
                                          "earliest_needed_by": None, "rate": labels[r["item"]].label if r["item"] in labels else None})
        it["needed"] += r["quantity"]; it["committed"] += r["committed"]; it["short"] += short
        if r["needed_by"] and (it["earliest_needed_by"] is None or r["needed_by"] < it["earliest_needed_by"]):
            it["earliest_needed_by"] = r["needed_by"]
        sh = shelters.setdefault(r["shelter_id"], {"shelter_id": r["shelter_id"], "shelter": r["shelter"], "hazard": r["hazard"],
                                                   "area": r["area"], "people": r["people"], "people_basis": r["people_basis"],
                                                   "access_status": r["access_status"], "needed_by": _iso(r["needed_by"]),
                                                   "lat": r["lat"], "lon": r["lon"], "items": []})
        sh["items"].append({"item": r["item"], "unit": r["unit"], "needed": r["quantity"], "short": short})
    for it in items.values():
        it["earliest_needed_by"] = _iso(it["earliest_needed_by"])
        it["status"] = "covered" if it["short"] <= 0 else "short"
    people = sum(sh["people"] for sh in shelters.values())
    measured = sum(sh["people"] for sh in shelters.values() if sh["people_basis"] == "headcount")
    last = await pool.fetchval("SELECT max(computed_at) FROM need")
    awaiting = await pool.fetchval(
        """SELECT count(*) FROM shelter s WHERE s.active
             AND NOT EXISTS (SELECT 1 FROM need n WHERE n.shelter_id=s.id)
             AND (s.source='officer' OR EXISTS (SELECT 1 FROM shelter_hazard sh JOIN hazard h ON h.id=sh.hazard_id
                                                WHERE sh.shelter_id=s.id AND h.status='active'))""")
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "needs_computed_at": _iso(last),
        "people": people,
        "people_from_headcounts": measured,
        "shelters_awaiting_count": awaiting,
        "shelters": list(shelters.values()),
        "items": sorted(items.values(), key=lambda x: -x["short"]),
        "method": "people x Sphere minimum standards x supply days, minus what is already ordered or delivered",
    }


@app.get("/ledger")
async def ledger_page():
    return FileResponse(WEB_DIR / "ledger.html")


# ---------------- Procurer: suppliers, orders, Beckn ----------------

class SupplierIn(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    phone: str | None = Field(default=None, max_length=20)
    kind: str = Field(default="shop", pattern="^(shop|wholesaler|pharmacy|ngo_warehouse|government)$")
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    serves_km: float = Field(default=150, gt=0, le=1000)


def _supplier_link(token: str) -> str:
    return f"{get_settings().public_url.rstrip('/')}/s/{token}"


@app.post("/api/suppliers")
async def add_supplier(sp: SupplierIn):
    token = new_token()
    try:
        sid = await app.state.pool.fetchval(
            """INSERT INTO supplier (name, phone, kind, geom, token, serves_km)
               VALUES ($1,$2,$3, ST_SetSRID(ST_MakePoint($4,$5),4326), $6, $7) RETURNING id""",
            sp.name.strip(), normalise_phone(sp.phone), sp.kind, sp.lon, sp.lat, token, sp.serves_km)
    except Exception as exc:
        if "unique" in str(exc).lower():
            raise HTTPException(409, "a supplier with this phone is already registered") from exc
        raise
    link = _supplier_link(token)
    await messaging.send(app.state.pool, sid, normalise_phone(sp.phone),
                         f"Welcome to RASAD, {sp.name}. Update your relief stock here: {link}\nOr reply: STOCK water 5000 @ 2 (send HELP for all commands).")
    await publish(app.state.pool, "supplier.added", {"supplier_id": sid, "name": sp.name})
    return {"id": sid, "link": link}


@app.get("/api/suppliers")
async def suppliers():
    rows = await app.state.pool.fetch(
        """SELECT s.id, s.name, s.phone, s.kind, s.token, s.serves_km, ST_X(s.geom) lon, ST_Y(s.geom) lat,
                  COALESCE(json_agg(json_build_object('item', i.item, 'available', i.available, 'reserved', i.reserved,
                      'unit_price', i.unit_price, 'updated_at', i.updated_at, 'via', i.updated_via)) FILTER (WHERE i.item IS NOT NULL), '[]') AS items
           FROM supplier s LEFT JOIN supplier_item i ON i.supplier_id=s.id WHERE s.active GROUP BY s.id ORDER BY s.id""")
    return {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
         "properties": {"id": r["id"], "name": r["name"], "kind": r["kind"], "phone": r["phone"], "serves_km": r["serves_km"],
                        "link": _supplier_link(r["token"]), "items": r["items"]}} for r in rows]}


async def _supplier_by_token(token: str):
    sp = await app.state.pool.fetchrow("SELECT id, name, phone, kind FROM supplier WHERE token=$1 AND active", token)
    if sp is None:
        raise HTTPException(404, "unknown supplier link")
    return sp


@app.get("/s/{token}")
async def supplier_page(token: str):
    await _supplier_by_token(token)
    return FileResponse(WEB_DIR / "supplier.html")


@app.get("/api/supplier/{token}")
async def supplier_view(token: str):
    sp = await _supplier_by_token(token)
    pool = app.state.pool
    items = {r["item"]: r for r in await pool.fetch("SELECT * FROM supplier_item WHERE supplier_id=$1", sp["id"])}
    orders = await pool.fetch(
        "SELECT id, item, quantity, status, deliver_to, reason, created_at, updated_at FROM bpp_order WHERE supplier_id=$1 ORDER BY id DESC LIMIT 30",
        sp["id"])
    msgs = await pool.fetch("SELECT at, direction, channel, text FROM supplier_message WHERE supplier_id=$1 ORDER BY id DESC LIMIT 20", sp["id"])
    return {
        "supplier": {"name": sp["name"], "kind": sp["kind"]},
        "items": [{"item": code, "name": meta["name"], "msg_unit": meta["msg_unit"], "factor": meta["factor"],
                   "available": items[code]["available"] if code in items else None,
                   "reserved": items[code]["reserved"] if code in items else 0,
                   "unit_price": items[code]["unit_price"] if code in items else None,
                   "updated_at": _iso(items[code]["updated_at"]) if code in items else None} for code, meta in ITEMS.items()],
        "orders": [{**{k: _iso(v) for k, v in dict(o).items()}, "what": describe(o['item'], o['quantity'])} for o in orders],
        "messages": [{k: _iso(v) for k, v in dict(m).items()} for m in msgs],
    }


class SupplierText(BaseModel):
    text: str = Field(min_length=1, max_length=500)


@app.post("/api/supplier/{token}/message")
async def supplier_message(token: str, m: SupplierText):
    """The supplier page sends the same commands a supplier would type on WhatsApp."""
    sp = await _supplier_by_token(token)
    reply = await handle_text(app.state.pool, sp["id"], sp["phone"], m.text, "page")
    await app.state.pool.execute(
        "INSERT INTO supplier_message (supplier_id, phone, direction, channel, text, delivered) VALUES ($1,$2,'out','page',$3,true)",
        sp["id"], sp["phone"], reply)
    return {"reply": reply}


@app.get("/webhooks/whatsapp")
async def whatsapp_verify(request: Request):
    q = request.query_params
    s = get_settings()
    if q.get("hub.mode") == "subscribe" and s.whatsapp_verify_token and q.get("hub.verify_token") == s.whatsapp_verify_token:
        return PlainTextResponse(q.get("hub.challenge", ""))
    raise HTTPException(403, "verification failed")


@app.post("/webhooks/whatsapp")
async def whatsapp_inbound(request: Request):
    raw = await request.body()
    if not messaging.verify_webhook_signature(raw, request.headers.get("X-Hub-Signature-256")):
        raise HTTPException(401, "bad signature")
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        raise HTTPException(400, "not JSON") from exc
    for phone, text in messaging.inbound_texts(payload):
        drv = await fleet.by_phone(app.state.pool, phone)
        if drv is not None:   # drivers and suppliers share the one WhatsApp number
            reply = await fleet.handle_text(app.state.pool, drv, text, "whatsapp")
            await fleet.notify(app.state.pool, drv, reply)
            continue
        reply = await handle_text(app.state.pool, None, phone, text, "whatsapp")
        sid = await app.state.pool.fetchval("SELECT id FROM supplier WHERE phone=$1", normalise_phone(phone))
        await messaging.send(app.state.pool, sid, normalise_phone(phone), reply)
    return {"ok": True}


@app.get("/api/procurements")
async def procurements(limit: int = 100):
    rows = await app.state.pool.fetch(
        """SELECT p.*, s.name AS shelter_name FROM procurement p JOIN shelter s ON s.id=p.shelter_id
           ORDER BY CASE p.status WHEN 'awaiting_approval' THEN 0 WHEN 'awaiting_supplier' THEN 1 WHEN 'confirming' THEN 2
                    WHEN 'approved' THEN 2 WHEN 'accepted' THEN 3 WHEN 'packed' THEN 4 ELSE 5 END, p.updated_at DESC LIMIT $1""",
        min(limit, 300))
    return [{k: _iso(v) for k, v in dict(r).items()} for r in rows]


class Decision(BaseModel):
    by: str = Field(min_length=1, max_length=80)
    reason: str | None = Field(default=None, max_length=300)


@app.post("/api/procurements/{pid}/approve")
async def approve(pid: int, d: Decision):
    n = await app.state.pool.execute(
        "UPDATE procurement SET status='approved', approved_by=$2, approved_at=now(), updated_at=now() WHERE id=$1 AND status='awaiting_approval'",
        pid, d.by)
    if not n.endswith("1"):
        raise HTTPException(409, "not awaiting approval")
    await publish(app.state.pool, "procurement.approved", {"procurement_id": pid, "by": d.by})
    if app.state.procurer is not None:
        app.state.procurer.trigger()
    return {"ok": True}


@app.post("/api/procurements/{pid}/reject")
async def reject(pid: int, d: Decision):
    n = await app.state.pool.execute(
        "UPDATE procurement SET status='cancelled', approved_by=$2, note=$3, updated_at=now() WHERE id=$1 AND status='awaiting_approval'",
        pid, d.by, f"rejected by {d.by}: {d.reason or 'no reason given'}")
    if not n.endswith("1"):
        raise HTTPException(409, "not awaiting approval")
    await publish(app.state.pool, "procurement.updated", {"procurement_id": pid, "status": "cancelled", "note": f"officer {d.by} rejected"})
    return {"ok": True}


@app.get("/api/beckn/identity")
async def beckn_identity():
    """What another Beckn network or registry needs to trust this node."""
    ident = await identity(app.state.pool)
    return {"subscriber_id": ident.subscriber_id, "unique_key_id": ident.unique_key_id, "signing_public_key": ident.public_key,
            "algorithm": "ed25519", "bap_uri": ident.bap_uri, "bpp_uri": ident.bpp_uri}


class PeerIn(BaseModel):
    subscriber_id: str = Field(min_length=1, max_length=200)
    unique_key_id: str = Field(min_length=1, max_length=200)
    role: str = Field(pattern="^(BAP|BPP)$")
    url: str = Field(min_length=8, max_length=500)
    public_key: str = Field(min_length=40, max_length=100)


@app.post("/api/beckn/peers")
async def add_peer(p: PeerIn):
    await app.state.pool.execute(
        """INSERT INTO beckn_peer (subscriber_id, unique_key_id, role, url, public_key) VALUES ($1,$2,$3,$4,$5)
           ON CONFLICT (subscriber_id, unique_key_id, role) DO UPDATE SET url=EXCLUDED.url, public_key=EXCLUDED.public_key, active=true""",
        p.subscriber_id, p.unique_key_id, p.role, p.url, p.public_key)
    return {"ok": True}


@app.get("/api/beckn/peers")
async def peers():
    rows = await app.state.pool.fetch("SELECT subscriber_id, unique_key_id, role, url, active FROM beckn_peer ORDER BY role, subscriber_id")
    return [dict(r) for r in rows]


@app.get("/api/beckn/messages")
async def beckn_messages(transaction_id: str | None = None, limit: int = 100):
    rows = await app.state.pool.fetch(
        """SELECT id, at, direction, role, action, transaction_id, message_id, peer, signature_ok, http_status, ack, error, body
           FROM beckn_message WHERE ($1::text IS NULL OR transaction_id=$1) ORDER BY id DESC LIMIT $2""",
        transaction_id, min(limit, 500))
    return [{k: _iso(v) for k, v in dict(r).items()} for r in rows]


@app.get("/api/risk-zones")
async def risk_zones(hours: int = 0):
    """Risk zones as they stand `hours` from now: what the routing engine is told to avoid."""
    s = get_settings()
    if hours < 0 or hours > 240:
        raise HTTPException(400, "hours must be 0..240")
    road_impact = s.road_impact_map
    hazards, crossings, cls = await load_inputs(app.state.pool, road_impact)
    sl = build_slices(datetime.now(UTC), [hours], hazards, crossings, cls, road_impact, s.hard_flood_prob)[0]
    feats = [{"type": "Feature", "geometry": z.geom.__geo_interface__, "properties": z.as_ref()}
             for z in sorted(sl.zones, key=lambda z: (z.hard, z.risk))]
    return {"type": "FeatureCollection", "at": sl.at.isoformat(), "hours": hours, "features": feats}


@app.websocket("/ws")
async def ws(socket: WebSocket):
    """Pushes every event to the browser. Uses Redis Streams; if Redis is down,
    tails event_log in Postgres instead."""
    await socket.accept()
    pool = app.state.pool
    last_db_id = await pool.fetchval("SELECT COALESCE(max(id), 0) FROM event_log")
    last_stream = "$"
    try:
        while True:
            sent_any = False
            try:
                resp = await get_redis().xread({STREAM: last_stream}, block=5000, count=50)
                for _s, entries in resp or []:
                    for entry_id, f in entries:
                        last_stream = entry_id
                        last_db_id = max(last_db_id, int(f.get("id", 0)))
                        await socket.send_text(json.dumps({"id": int(f.get("id", 0)), "type": f["type"], "payload": json.loads(f["payload"])}))
                        sent_any = True
            except Exception:
                rows = await pool.fetch(
                    "SELECT id, type, payload FROM event_log WHERE id > $1 ORDER BY id LIMIT 50", last_db_id
                )
                for r in rows:
                    last_db_id = r["id"]
                    await socket.send_text(json.dumps({"id": r["id"], "type": r["type"], "payload": r["payload"]}, default=str))
                    sent_any = True
                if not sent_any:
                    await asyncio.sleep(3)
            if not sent_any:
                await socket.send_text(json.dumps({"type": "heartbeat"}))
    except WebSocketDisconnect:
        return


# ---------------- Dispatcher: drivers and trips ----------------

class DriverIn(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    phone: str | None = Field(default=None, max_length=20)
    vehicle: str = Field(default="mini truck", max_length=60)
    capacity_kg: float = Field(gt=0, le=40000)
    traccar_unique_id: str | None = Field(default=None, max_length=64)
    depot_id: int | None = None


@app.post("/api/drivers")
async def add_driver(d: DriverIn):
    pool = app.state.pool
    if d.depot_id is not None and not await pool.fetchval("SELECT 1 FROM depot WHERE id=$1", d.depot_id):
        raise HTTPException(400, "no such depot")
    if d.depot_id is None and not d.traccar_unique_id:
        raise HTTPException(400, "give a depot or a tracking-app id, so the planner knows where the driver starts")
    res = await fleet.register(pool, d.name.strip(), d.phone, d.capacity_kg, d.vehicle.strip(), d.traccar_unique_id, d.depot_id)
    drv = await pool.fetchrow("SELECT * FROM driver WHERE id=$1", res["id"])
    await fleet.notify(pool, drv, f"Welcome to RASAD, {d.name}. Your relief trips will arrive here. Your page: {res['link']}\n"
                                  "Reply START when you set off, DONE <stop no.> at each stop, BLOCKED <where> if a road is shut.")
    if app.state.dispatcher is not None:
        app.state.dispatcher.trigger()
    return res


@app.get("/api/drivers")
async def drivers():
    rows = await app.state.pool.fetch(
        f"""SELECT d.id, d.name, d.phone, d.vehicle, d.capacity_kg, d.traccar_unique_id, d.depot_id, d.token, dep.name AS depot,
                   pos.pos_lon, pos.pos_lat, pos.fix_time,
                   (SELECT t.id FROM trip t WHERE t.driver_id=d.id AND t.status IN ('planned','en_route') ORDER BY t.id DESC LIMIT 1) AS trip_id
            FROM driver d LEFT JOIN depot dep ON dep.id=d.depot_id {fleet.POS_JOIN} WHERE d.active ORDER BY d.id""")
    base = get_settings().public_url.rstrip("/")
    return [{**{k: _iso(v) for k, v in dict(r).items() if k != "token"}, "link": f"{base}/d/{r['token']}"} for r in rows]


@app.get("/api/trips")
async def trips(active: bool = True, limit: int = 30):
    rows = await app.state.pool.fetch(
        "SELECT id FROM trip WHERE ($1 = false OR status IN ('planned','en_route') OR updated_at > now() - interval '6 hours') ORDER BY id DESC LIMIT $2",
        active, min(limit, 100))
    out = []
    for r in rows:
        t = await fleet.trip_detail(app.state.pool, r["id"])
        t["stops"] = [{k: _iso(v) for k, v in st.items()} for st in t["stops"]]
        out.append({k: _iso(v) for k, v in t.items()})
    return out


@app.post("/api/trips/{trip_id}/reroute")
async def reroute_trip(trip_id: int):
    if app.state.dispatcher is not None:
        res = await app.state.dispatcher.reroute(trip_id, "requested by the control room")
        if res is None:
            raise HTTPException(409, "trip is not active, or the routing engine is unreachable")
        return res
    await publish(app.state.pool, "trip.updated", {"trip_id": trip_id, "reroute": True, "reason": "requested by the control room"})
    return {"queued": True}


@app.post("/api/trips/{trip_id}/cancel")
async def cancel_trip(trip_id: int, d: Decision):
    pool = app.state.pool
    t = await pool.fetchrow("SELECT t.*, d.phone, d.name FROM trip t JOIN driver d ON d.id=t.driver_id WHERE t.id=$1", trip_id)
    if t is None or t["status"] not in fleet.ACTIVE:
        raise HTTPException(409, "trip is not active")
    if await pool.fetchval("SELECT 1 FROM trip_stop WHERE trip_id=$1 AND kind='pickup' AND status='done'", trip_id):
        raise HTTPException(409, "goods are already on this vehicle; reroute it instead")
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute("UPDATE trip_stop SET status='released', note=$2 WHERE trip_id=$1 AND status IN ('pending','blocked')",
                           trip_id, f"trip cancelled by {d.by}")
        await conn.execute("UPDATE trip SET status='cancelled', updated_at=now() WHERE id=$1", trip_id)
    await fleet.notify(pool, {"id": t["driver_id"], "phone": t["phone"]}, f"Trip #{trip_id} was cancelled by the control room. Please wait for a new one.")
    await publish(pool, "trip.updated", {"trip_id": trip_id, "status": "cancelled", "by": d.by, "reason": d.reason})
    if app.state.dispatcher is not None:
        app.state.dispatcher.trigger()
    return {"ok": True}


async def _driver_by_token(token: str):
    drv = await fleet.by_token(app.state.pool, token)
    if drv is None:
        raise HTTPException(404, "unknown driver link")
    return drv


@app.get("/d/{token}")
async def driver_page(token: str):
    await _driver_by_token(token)
    return FileResponse(WEB_DIR / "driver.html")


@app.get("/api/driver/{token}")
async def driver_view(token: str):
    pool = app.state.pool
    drv = await _driver_by_token(token)
    t = await fleet.active_trip(pool, drv["id"])
    if t is None:
        t = await pool.fetchrow("SELECT * FROM trip WHERE driver_id=$1 ORDER BY id DESC LIMIT 1", drv["id"])
    detail = await fleet.trip_detail(pool, t["id"]) if t else None
    if detail:
        detail["stops"] = [{k: _iso(v) for k, v in st.items()} for st in detail["stops"]]
        detail = {k: _iso(v) for k, v in detail.items()}
    pos = await fleet.position(pool, drv["id"])
    msgs = await pool.fetch("SELECT at, direction, channel, text FROM supplier_message WHERE driver_id=$1 ORDER BY id DESC LIMIT 20", drv["id"])
    return {
        "driver": {"name": drv["name"], "vehicle": drv["vehicle"], "capacity_kg": drv["capacity_kg"], "tracking": bool(drv["traccar_unique_id"])},
        "position": {"lat": pos["pos_lat"], "lon": pos["pos_lon"], "fix_time": _iso(pos["fix_time"])} if pos else None,
        "trip": detail,
        "messages": [{k: _iso(v) for k, v in dict(m).items()} for m in msgs],
    }


class StopAction(BaseModel):
    action: str = Field(pattern="^(done|failed|blocked)$")
    note: str | None = Field(default=None, max_length=300)
    lat: float | None = Field(default=None, ge=-90, le=90)
    lon: float | None = Field(default=None, ge=-180, le=180)


@app.post("/api/driver/{token}/start")
async def driver_start(token: str):
    drv = await _driver_by_token(token)
    ok, msg = await fleet.start(app.state.pool, drv)
    return {"ok": ok, "message": msg}


@app.post("/api/driver/{token}/stops/{stop_id}")
async def driver_stop(token: str, stop_id: int, a: StopAction):
    drv = await _driver_by_token(token)
    ok, msg = await fleet.act(app.state.pool, drv, stop_id, a.action, a.note, a.lat, a.lon, channel="page")
    if not ok:
        raise HTTPException(409, msg)
    return {"ok": ok, "message": msg}


@app.post("/api/driver/{token}/blocked")
async def driver_blocked(token: str, a: StopAction):
    drv = await _driver_by_token(token)
    ok, msg = await fleet.report_blocked(app.state.pool, drv, a.note, a.lat, a.lon, "page")
    if not ok:
        raise HTTPException(409, msg)
    return {"ok": ok, "message": msg}


class PositionIn(BaseModel):
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    speed: float | None = Field(default=None, ge=0, le=100)
    heading: float | None = Field(default=None, ge=0, le=360)


@app.post("/api/driver/{token}/position")
async def driver_position(token: str, p: PositionIn):
    drv = await _driver_by_token(token)
    await fleet.record_page_position(app.state.pool, drv, p.lat, p.lon, p.speed, p.heading)
    return {"ok": True}


@app.post("/api/driver/{token}/message")
async def driver_message(token: str, m: SupplierText):
    """The driver page accepts the same text commands as WhatsApp."""
    drv = await _driver_by_token(token)
    reply = await fleet.handle_text(app.state.pool, drv, m.text, "page")
    await app.state.pool.execute(
        "INSERT INTO supplier_message (driver_id, phone, direction, channel, text, delivered) VALUES ($1,$2,'out','page',$3,true)",
        drv["id"], drv["phone"], reply)
    return {"reply": reply}


@app.get("/")
async def index():
    return FileResponse(WEB_DIR / "index.html")


app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")


@app.exception_handler(Exception)
async def unhandled(_req, exc: Exception):
    log.exception("unhandled error")
    return JSONResponse({"error": type(exc).__name__, "detail": str(exc)}, status_code=500)
