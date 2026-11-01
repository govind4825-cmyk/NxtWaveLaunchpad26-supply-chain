"""Cartographer agent: keeps a time-sliced risk model of the road network and computes,
for every shelter in an alert area, its access window: how long a safe road from a
depot will stay open.

Runs every RASAD_CARTOGRAPHER_EVERY seconds and, sooner, 30 seconds after any
hazard, river forecast, closure or depot change.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import asyncpg
import shapely
from shapely import wkb as shp_wkb

from .. import store
from ..closures import sync_tomtom
from ..config import get_settings
from ..connectors.graphhopper import GraphHopper
from ..connectors.shelters import find_shelters
from ..engine.risk import (
    ClosureIn,
    CrossingIn,
    HazardIn,
    Zone,
    custom_model,
    exposure,
    haversine_km,
    zones_at,
)
from ..events import STREAM, get_redis, publish
from ..provenance import Fetcher

log = logging.getLogger(__name__)

WAKE_ON = {"hazard.updated", "river.updated", "closure.updated", "depot.updated", "shelter.updated"}
DEBOUNCE_S = 30
DEPOTS_PER_SHELTER = 3
ROUTE_CONCURRENCY = 8
MAX_HAZARDS_SEARCHED_PER_RUN = 5
LAST_MILE_SLACK_M = 150


@dataclass
class Slice:
    hours: int
    at: datetime
    zones: list[Zone]
    model: dict | None


def _geom(b):
    return shp_wkb.loads(bytes(b)) if b is not None else None


async def load_inputs(pool: asyncpg.Pool, road_impact: dict[str, float]):
    hz_rows = await pool.fetch(
        """SELECT id, kind, title, severity, certainty, onset, expires, ST_AsBinary(geom) AS wkb FROM hazard
           WHERE status='active' AND geom IS NOT NULL AND kind = ANY($1::text[])""",
        list(road_impact),
    )
    hazards = [HazardIn(str(r["id"]), r["kind"], r["title"], r["severity"], r["certainty"], r["onset"], r["expires"], _geom(r["wkb"])) for r in hz_rows]

    cr_rows = await pool.fetch(
        """SELECT p.id, p.name, ST_Y(p.geom) AS lat, ST_X(p.geom) AS lon,
                  COALESCE(json_object_agg(f.day, f.exceed_prob) FILTER (WHERE f.day IS NOT NULL AND f.exceed_prob IS NOT NULL), '{}') AS probs
           FROM river_point p
           LEFT JOIN river_forecast f ON f.river_point_id = p.id AND f.day >= current_date - 1
           WHERE NOT p.no_river AND EXISTS (
               SELECT 1 FROM river_point_hazard rh JOIN hazard h ON h.id = rh.hazard_id
               WHERE rh.river_point_id = p.id AND h.status='active')
           GROUP BY p.id"""
    )
    crossings = []
    for r in cr_rows:
        probs = r["probs"] or {}
        crossings.append(CrossingIn(r["id"], r["name"], r["lat"], r["lon"],
                                    {datetime.fromisoformat(k).date(): float(v) for k, v in probs.items()}))

    cl_rows = await pool.fetch(
        """SELECT id, source, ST_AsBinary(geom) AS wkb, confirmed, reason, valid_from, valid_to FROM closure
           WHERE valid_to IS NULL OR valid_to > now() - interval '1 hour'"""
    )
    closures = [ClosureIn(r["id"], r["source"], _geom(r["wkb"]), r["confirmed"], r["reason"], r["valid_from"], r["valid_to"]) for r in cl_rows]
    return hazards, crossings, closures


def build_slices(t0: datetime, hours: list[int], hazards, crossings, closures, road_impact, hard_prob) -> list[Slice]:
    out = []
    for h in hours:
        at = t0 + timedelta(hours=h)
        z = zones_at(at, hazards, crossings, closures, road_impact, hard_prob)
        out.append(Slice(h, at, z, custom_model(z)))
    return out


class Cartographer:
    def __init__(self, pool: asyncpg.Pool, fetcher: Fetcher, gh: GraphHopper | None = None):
        self.s = get_settings()
        self.pool = pool
        self.fetcher = fetcher
        self.gh = gh or GraphHopper(self.s.graphhopper_url)
        self._wake = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._sem = asyncio.Semaphore(ROUTE_CONCURRENCY)

    async def start(self) -> None:
        self._tasks.append(asyncio.create_task(self._loop(), name="cartographer"))
        self._tasks.append(asyncio.create_task(self._listen(), name="cartographer:events"))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.gh.aclose()

    def trigger(self) -> None:
        self._wake.set()

    async def _loop(self) -> None:
        await asyncio.sleep(15)
        while True:
            try:
                await self.run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("cartographer run crashed")
                await store.mark_source(self.pool, "cartographer", False, None, None, f"crash: {exc}")
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.s.cartographer_every)
                await asyncio.sleep(DEBOUNCE_S)   # let a burst of updates settle
            except TimeoutError:
                pass
            self._wake.clear()

    async def _listen(self) -> None:
        last = "$"
        while True:
            try:
                resp = await get_redis().xread({STREAM: last}, block=15000, count=50)
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(10)
                continue
            for _s, entries in resp or []:
                for entry_id, f in entries:
                    last = entry_id
                    if f.get("type") in WAKE_ON:
                        self._wake.set()

    # ------------------------------------------------------------------

    async def run(self) -> dict:
        s = self.s
        road_impact = s.road_impact_map

        # 1. TomTom closures inside active logistics hazards (optional).
        areas = await self._hazard_areas(road_impact, limit=5)
        if s.tomtom_key and areas:
            tt = await sync_tomtom(self.pool, self.fetcher, s.tomtom_key, areas)
            await store.mark_source(self.pool, "tomtom", tt["ok"], tt["raw_id"], tt["added"], tt["error"])

        # 2. Candidate shelters for hazards not searched yet.
        await self._discover_shelters(road_impact)

        # 3. Routing engine and depots must be there.
        if not await self.gh.healthy():
            await store.mark_source(self.pool, "cartographer", False, None, None, f"routing engine unreachable at {s.graphhopper_url}")
            await self.pool.execute("UPDATE access_window SET status='unknown', note='routing engine unreachable; showing last route'")
            return {"ok": False}
        depots = await self.pool.fetch("SELECT id, name, ST_X(geom) AS lon, ST_Y(geom) AS lat FROM depot WHERE active")
        shelters = await self.pool.fetch(
            """SELECT DISTINCT s.id, s.name, ST_X(s.geom) AS lon, ST_Y(s.geom) AS lat FROM shelter s
               LEFT JOIN shelter_hazard sh ON sh.shelter_id = s.id
               LEFT JOIN hazard h ON h.id = sh.hazard_id
               WHERE s.active AND (s.source = 'officer' OR h.status = 'active')"""
        )
        if not depots:
            await store.mark_source(self.pool, "cartographer", True, None, len(shelters), "no depots registered: add one on the map")
            return {"ok": True, "shelters": len(shelters), "depots": 0}

        # 4. Risk zones per slice, then access window per shelter.
        hazards, crossings, closures = await load_inputs(self.pool, road_impact)
        t0 = datetime.now(UTC)
        slices = build_slices(t0, s.slices, hazards, crossings, closures, road_impact, s.hard_flood_prob)

        results = await asyncio.gather(*(self._window(sh, depots, slices) for sh in shelters))
        changed = 0
        for sh, res in zip(shelters, results):
            if await self._save(sh, res):
                changed += 1
        counts = {}
        for r in results:
            counts[r["status"]] = counts.get(r["status"], 0) + 1
        note = ", ".join(f"{v} {k.replace('_', ' ')}" for k, v in sorted(counts.items())) or "no shelters in alert areas"
        await store.mark_source(self.pool, "cartographer", True, None, len(shelters), note)
        await publish(self.pool, "cartographer.run", {"shelters": len(shelters), "changed": changed, "counts": counts,
                                                      "zones_now": len(slices[0].zones) if slices else 0})
        log.info("cartographer: %d shelters, %d changed, %s", len(shelters), changed, counts)
        return {"ok": True, "shelters": len(shelters), "counts": counts, "changed": changed}

    async def _hazard_areas(self, road_impact, limit: int):
        rows = await self.pool.fetch(
            """SELECT ST_XMin(geom) w, ST_YMin(geom) s, ST_XMax(geom) e, ST_YMax(geom) n,
                      ST_X(ST_PointOnSurface(geom)) cx, ST_Y(ST_PointOnSurface(geom)) cy
               FROM hazard WHERE status='active' AND geom IS NOT NULL AND kind = ANY($1::text[]) AND severity >= 2
               ORDER BY severity DESC, last_updated DESC LIMIT $2""",
            list(road_impact), limit,
        )
        return [((r["w"], r["s"], r["e"], r["n"]), (r["cx"], r["cy"])) for r in rows]

    async def _discover_shelters(self, road_impact) -> None:
        rows = await self.pool.fetch(
            """SELECT id, ST_AsBinary(geom) AS wkb FROM hazard
               WHERE status='active' AND geom IS NOT NULL AND kind = ANY($1::text[]) AND severity >= 2
                 AND NOT (attrs ? 'shelters_searched')
               ORDER BY severity DESC LIMIT $2""",
            list(road_impact), MAX_HAZARDS_SEARCHED_PER_RUN,
        )
        for r in rows:
            geom = _geom(r["wkb"])
            c = geom.representative_point()
            cands, raw_id, err = await find_shelters(self.fetcher, self.s.overpass_url, geom.bounds, (c.x, c.y), self.s.max_shelters_per_hazard)
            if err:
                log.warning("shelter search failed for %s: %s", r["id"], err)
                await store.mark_source(self.pool, "cartographer", False, raw_id, None, f"shelter search: {err}")
                continue   # retried next run
            for cand in cands:
                if not geom.contains(shapely.Point(cand.lon, cand.lat)):
                    continue
                sid = await self.pool.fetchval(
                    """INSERT INTO shelter (osm_ref, name, kind, geom, source, raw_fetch_id)
                       VALUES ($1,$2,$3, ST_SetSRID(ST_MakePoint($4,$5),4326), 'osm', $6)
                       ON CONFLICT (osm_ref) DO UPDATE SET name=COALESCE(EXCLUDED.name, shelter.name) RETURNING id""",
                    cand.osm_ref, cand.name, cand.kind, cand.lon, cand.lat, raw_id,
                )
                await self.pool.execute("INSERT INTO shelter_hazard VALUES ($1,$2) ON CONFLICT DO NOTHING", sid, r["id"])
            # Officer-added shelters inside the footprint belong to this hazard too.
            await self.pool.execute(
                """INSERT INTO shelter_hazard (shelter_id, hazard_id)
                   SELECT s.id, h.id FROM shelter s, hazard h
                   WHERE h.id=$1 AND s.active AND ST_Within(s.geom, h.geom) ON CONFLICT DO NOTHING""",
                r["id"],
            )
            await self.pool.execute(
                "UPDATE hazard SET attrs = attrs || jsonb_build_object('shelters_searched', $2::bigint) WHERE id=$1",
                r["id"], raw_id,
            )

    async def _route(self, start, end, model):
        async with self._sem:
            return await self.gh.route(start, end, model)

    async def _window(self, sh, depots, slices: list[Slice]) -> dict:
        s = self.s
        near = sorted(
            ((haversine_km(d["lon"], d["lat"], sh["lon"], sh["lat"]), d) for d in depots),
            key=lambda x: x[0],
        )
        near = [d for km, d in near if km <= s.max_depot_km][:DEPOTS_PER_SHELTER]
        if not near:
            return {"status": "unroutable", "note": f"no depot within {s.max_depot_km:.0f} km", "slices": []}

        dest = (sh["lon"], sh["lat"])
        best_now = None
        slice_log = []
        closes_at = None
        blocked_by: list[dict] = []
        snap_fail = 0
        # Unconstrained route: how close does the road network get to this shelter?
        # If a risk-aware route stops much further away, the shelter's own road is blocked.
        base = await self._route((near[0]["lon"], near[0]["lat"]), dest, None)
        base_gap = _end_gap_m(base, dest) if base.ok else None

        for i, sl in enumerate(slices):
            found = None
            risky = None
            for d in near:
                r = await self._route((d["lon"], d["lat"]), dest, sl.model)
                if r.snap_failed:
                    snap_fail += 1
                    continue
                if not r.ok and not r.no_route:
                    return {"status": "unknown", "note": f"routing error: {r.error}", "slices": slice_log}
                if not r.ok or r.line is None:
                    continue
                if base_gap is not None and _end_gap_m(r, dest) > base_gap + LAST_MILE_SLACK_M:
                    continue  # reached a different road: the last stretch to the shelter is closed
                worst, hard, crossed = exposure(r.line, sl.zones)
                cand = {"depot": d, "route": r, "risk": worst, "crossed": crossed}
                if not hard and worst < s.safe_risk:
                    found = cand
                    break
                if risky is None or worst < risky["risk"]:
                    risky = cand
            if i == 0 and found is None and risky is None and snap_fail >= len(near):
                return {"status": "unroutable", "note": "shelter is not near a road the routing graph knows", "slices": []}
            slice_log.append({"hours": sl.hours, "at": sl.at.isoformat(), "safe": found is not None,
                              "eta_min": round(found["route"].time_s / 60, 1) if found else None,
                              "risk": round(found["risk"], 3) if found else (round(risky["risk"], 3) if risky else None)})
            if i == 0:
                best_now = found or risky
            if found is None:
                closes_at = sl.at
                blocked_by = await self._why_blocked(near[0], dest, sl, risky)
                break

        if closes_at is None:
            status = "open"
        elif closes_at <= slices[0].at:
            status = "cut_off"
        else:
            status = "closing"
        res = {"status": status, "closes_at": closes_at, "slices": slice_log, "blocked_by": blocked_by}
        if best_now:
            r = best_now["route"]
            res.update({"depot_id": best_now["depot"]["id"], "eta_min": round(r.time_s / 60, 1), "distance_km": round(r.distance_m / 1000, 2),
                        "max_risk": round(best_now["risk"], 3), "route": r.line, "crossed": best_now["crossed"][:12]})
            if status == "cut_off":
                res["note"] = "least-risk route shown; it crosses risk above the safe limit" if not any(c["hard"] for c in best_now["crossed"]) else None
        return res

    async def _why_blocked(self, depot, dest, sl: Slice, risky) -> list[dict]:
        """Explain a closed window: zones on the risky route, or on the plain road route."""
        if risky is not None:
            return [c for c in risky["crossed"] if c["hard"] or c["risk"] >= self.s.safe_risk][:8]
        plain = await self._route((depot["lon"], depot["lat"]), dest, None)
        if plain.ok and plain.line is not None:
            _w, _h, crossed = exposure(plain.line, sl.zones)
            hits = [c for c in crossed if c["hard"] or c["risk"] >= self.s.safe_risk]
            if hits:
                return hits[:8]
        return [{"type": "network", "id": None, "reason": "no road connection from the nearest depots", "risk": 1.0, "hard": True}]

    async def _save(self, sh, res: dict) -> bool:
        prev = await self.pool.fetchrow("SELECT status, closes_at, eta_min FROM access_window WHERE shelter_id=$1", sh["id"])
        route = res.get("route")
        await self.pool.execute(
            """INSERT INTO access_window (shelter_id, computed_at, status, closes_at, depot_id, eta_min, distance_km, max_risk, route, crossed, blocked_by, slices, note)
               VALUES ($1, now(), $2, $3, $4, $5, $6, $7, CASE WHEN $8::bytea IS NULL THEN NULL ELSE ST_SetSRID(ST_GeomFromWKB($8::bytea),4326) END, $9, $10, $11, $12)
               ON CONFLICT (shelter_id) DO UPDATE SET computed_at=now(), status=EXCLUDED.status, closes_at=EXCLUDED.closes_at,
                 depot_id=EXCLUDED.depot_id, eta_min=EXCLUDED.eta_min, distance_km=EXCLUDED.distance_km, max_risk=EXCLUDED.max_risk,
                 route=COALESCE(EXCLUDED.route, access_window.route), crossed=EXCLUDED.crossed, blocked_by=EXCLUDED.blocked_by,
                 slices=EXCLUDED.slices, note=EXCLUDED.note""",
            sh["id"], res["status"], res.get("closes_at"), res.get("depot_id"), res.get("eta_min"), res.get("distance_km"),
            res.get("max_risk"), shapely.to_wkb(route) if route is not None else None,
            res.get("crossed", []), res.get("blocked_by", []), res.get("slices", []), res.get("note"),
        )
        status_changed = prev is not None and prev["status"] != res["status"]
        earlier = _earlier_by(res.get("closes_at"), prev["closes_at"] if prev else None, hours=3)
        rerouted = (prev is not None and not status_changed and prev["eta_min"] and res.get("eta_min")
                    and res["eta_min"] >= prev["eta_min"] * 1.5 and res["eta_min"] - prev["eta_min"] >= 10)
        new_risk = prev is None and res["status"] in ("closing", "cut_off")
        changed = prev is None or status_changed or earlier or rerouted
        if status_changed or new_risk or (earlier and res["status"] == "closing") or rerouted:
            await publish(self.pool, "access_window.changed", {
                "shelter_id": sh["id"], "name": sh["name"], "status": res["status"],
                "previous": prev["status"] if prev else None,
                "change": "rerouted" if rerouted else ("closes_earlier" if earlier and not status_changed else "status"),
                "eta_min": res.get("eta_min"), "previous_eta_min": prev["eta_min"] if prev else None,
                "closes_at": res["closes_at"].isoformat() if res.get("closes_at") else None,
                "blocked_by": [b.get("reason") for b in res.get("blocked_by", [])][:3],
            })
        return bool(changed)


def _end_gap_m(route, dest) -> float:
    lon, lat = route.line.coords[-1]
    return haversine_km(lon, lat, dest[0], dest[1]) * 1000


def _earlier_by(new, old, hours: int) -> bool:
    if new is None:
        return False
    if old is None:
        return True
    return (old - new) >= timedelta(hours=hours)

