"""Dispatcher agent: turns accepted orders into driver trips and keeps every trip on a safe road.

Planning (every RASAD_DISPATCHER_EVERY seconds, sooner when an order is accepted or a driver joins):
  1. Open work: accepted orders whose goods are not yet on a live trip.
  2. Free drivers start at their phone's live GPS when it is fresh, otherwise at their depot.
  3. Travel times between every start, supplier and shelter come from the risk-aware router
     with today's hazards, river crossings and closures. A leg with no safe road is forbidden.
  4. Pickup-and-delivery routing (OR-Tools): vehicle capacity; a hard deadline where the
     Cartographer says the shelter's last safe road closes; a soft "needed by" from the
     Quartermaster; orders left out cost a penalty weighted by the people they serve.
  5. Trips are saved and sent to drivers. Anything left unplanned is reported with the reason.

Monitoring (every RASAD_MONITOR_EVERY seconds): a driver off the planned road, a new hazard
or closure on the remaining legs, or a driver's BLOCKED report reroutes the rest of the trip
from where the driver is now. A stop with no safe road is released for another driver,
or held (goods on board) until the road reopens. A driver who stops moving is flagged.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import asyncpg
import shapely
from shapely import wkb as shp_wkb
from shapely.geometry import LineString

from .. import fleet, store
from ..config import get_settings
from ..connectors.graphhopper import GraphHopper
from ..engine import vrp
from ..engine.risk import buffer_m, exposure, haversine_km, point
from ..events import STREAM, get_redis, publish
from ..fleet import POS_JOIN, hm
from .cartographer import build_slices, load_inputs

log = logging.getLogger(__name__)

WAKE_ON = {"procurement.updated", "driver.updated"}
RECHECK_ON = {"closure.updated", "hazard.updated", "river.updated", "access_window.changed", "cartographer.run"}
MAX_JOBS = 24                  # orders per planning run; keeps the travel-time matrix small
MAX_CHUNKS = 120               # loads per run after splitting orders to fit vehicles
ROUTE_CONCURRENCY = 8
DEBOUNCE_S = 10
STRIKES_TO_REROUTE = 3         # consecutive off-route GPS fixes before rerouting
ZONE_RECHECK_S = 300
LAST_MILE_SLACK_M = 150        # see Cartographer: a route ending on a different road means the stop's road is shut
ORIGIN_CLEAR_M = 250           # a hazard at the driver's own spot is behind them, not on the route ahead
STARTED_M = 300                # a planned trip counts as started once the driver moves this far
STOPPED_M = 150


@dataclass
class Leg:
    ok: bool
    time_s: int
    dist_m: float
    line: LineString | None
    risk: float | None = None
    why: str | None = None
    error: bool = False


def _key(p) -> tuple[float, float]:
    return (round(p[0], 5), round(p[1], 5))


def _gap_m(line: LineString, dest) -> float:
    end = line.coords[-1]
    return haversine_km(end[0], end[1], dest[0], dest[1]) * 1000


def _wkb(line: LineString | None):
    return shapely.to_wkb(line) if line is not None else None


def _service(kind: str) -> int:
    return vrp.PICKUP_SERVICE_S if kind == "pickup" else vrp.DROP_SERVICE_S


def _join(lines: list[LineString | None]) -> LineString | None:
    coords: list = []
    for ln in lines:
        if ln is None:
            continue
        c = list(ln.coords)
        coords.extend(c if not coords else c[1:])
    return LineString(coords) if len(coords) >= 2 else None


class Router:
    """Risk-aware legs for one moment: the current risk zones, with a per-run cache."""

    def __init__(self, gh: GraphHopper, zones, model, safe_risk: float):
        self.gh, self.zones, self.model, self.safe_risk = gh, zones, model, safe_risk
        self._legs: dict = {}
        self._snap: dict = {}
        self._sem = asyncio.Semaphore(ROUTE_CONCURRENCY)

    def unsafe(self, line: LineString, ignore_near=None) -> tuple[bool, float, str | None]:
        zones = self.zones
        if ignore_near is not None:
            near = buffer_m(point(*ignore_near), ORIGIN_CLEAR_M)
            zones = [z for z in zones if not z.geom.intersects(near)]
        worst, hard, crossed = exposure(line, zones)
        if hard or worst >= self.safe_risk:
            bad = [c["reason"] for c in crossed if c["hard"] or c["risk"] >= self.safe_risk] or [c["reason"] for c in crossed]
            return True, worst, "crosses " + "; ".join(dict.fromkeys(bad[:2]))
        return False, worst, None

    async def _snap_gap(self, p):
        k = _key(p)
        if k not in self._snap:
            self._snap[k] = await self.gh.nearest(p)
        return self._snap[k]

    async def leg(self, a, b, from_vehicle: bool = False) -> Leg:
        if _key(a) == _key(b):
            return Leg(True, 0, 0.0, None, 0.0)
        k = (_key(a), _key(b), from_vehicle)
        if k in self._legs:
            return self._legs[k]
        async with self._sem:
            r = await self.gh.route(a, b, self.model)
        if not r.ok or r.line is None:
            if r.no_route:
                leg = Leg(False, vrp.UNREACHABLE_S, 0.0, None, why="no open road")
            elif r.snap_failed:
                leg = Leg(False, vrp.UNREACHABLE_S, 0.0, None, why="not near a road the router knows")
            else:
                leg = Leg(False, vrp.UNREACHABLE_S, 0.0, None, why=f"routing error: {r.error}", error=True)
        else:
            base = await self._snap_gap(b)
            bad, worst, why = self.unsafe(r.line, a if from_vehicle else None)
            if base is not None and _gap_m(r.line, b) > base + LAST_MILE_SLACK_M:
                leg = Leg(False, vrp.UNREACHABLE_S, r.distance_m or 0.0, r.line, worst, "the last stretch of road to this stop is closed")
            elif bad:
                leg = Leg(False, vrp.UNREACHABLE_S, r.distance_m or 0.0, r.line, worst, why)
            else:
                leg = Leg(True, int(round(r.time_s or 0)), r.distance_m or 0.0, r.line, worst)
        self._legs[k] = leg
        return leg


async def make_router(pool: asyncpg.Pool, gh: GraphHopper) -> Router:
    s = get_settings()
    hazards, crossings, closures = await load_inputs(pool, s.road_impact_map)
    sl = build_slices(datetime.now(UTC), [0], hazards, crossings, closures, s.road_impact_map, s.hard_flood_prob)[0]
    return Router(gh, sl.zones, sl.model, s.safe_risk)


class Dispatcher:
    def __init__(self, pool: asyncpg.Pool, gh: GraphHopper | None = None):
        self.s = get_settings()
        self.pool = pool
        self.gh = gh or GraphHopper(self.s.graphhopper_url)
        self._wake = asyncio.Event()
        self._mon_wake = asyncio.Event()
        self._zones_dirty = True
        self._last_zone_check = 0.0
        self._reroute_q: dict[int, str] = {}
        self._lock = asyncio.Lock()
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        self._tasks.append(asyncio.create_task(self._loop(), name="dispatcher"))
        self._tasks.append(asyncio.create_task(self._monitor_loop(), name="dispatcher:monitor"))
        self._tasks.append(asyncio.create_task(self._listen(), name="dispatcher:events"))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.gh.aclose()

    def trigger(self) -> None:
        self._wake.set()

    async def _loop(self) -> None:
        await asyncio.sleep(40)
        while True:
            try:
                await self.run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("dispatcher run crashed")
                await store.mark_source(self.pool, "dispatcher", False, None, None, f"crash: {exc}")
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.s.dispatcher_every)
                await asyncio.sleep(DEBOUNCE_S)
            except TimeoutError:
                pass
            self._wake.clear()

    async def _monitor_loop(self) -> None:
        await asyncio.sleep(20)
        while True:
            try:
                await self.monitor()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("dispatcher monitor crashed")
            try:
                await asyncio.wait_for(self._mon_wake.wait(), timeout=self.s.monitor_every)
                await asyncio.sleep(2)
            except TimeoutError:
                pass
            self._mon_wake.clear()

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
                    typ = f.get("type")
                    if typ in WAKE_ON:
                        self._wake.set()
                    if typ in RECHECK_ON:
                        self._zones_dirty = True
                        self._mon_wake.set()
                    if typ == "trip.updated":
                        try:
                            p = json.loads(f.get("payload") or "{}")
                        except ValueError:
                            continue
                        if p.get("reroute") and p.get("trip_id"):
                            self._reroute_q[int(p["trip_id"])] = p.get("reason") or "requested"
                            self._mon_wake.set()

    # ------------------------------------------------------------------ planning

    async def run(self) -> dict:
        async with self._lock:
            return await self._plan()

    async def _open_work(self):
        return await self.pool.fetch(
            f"""WITH placed AS (
                   SELECT ts.procurement_id, sum(ts.quantity) AS q FROM trip_stop ts JOIN trip t ON t.id = ts.trip_id
                   WHERE ts.kind='drop' AND (ts.status='done' OR (ts.status IN ('pending','blocked') AND t.status IN ('planned','en_route')))
                   GROUP BY ts.procurement_id)
                SELECT p.id, p.item, p.unit, p.quantity, p.quantity - COALESCE(pl.q, 0) AS remaining, p.needed_by,
                       p.pickup_lat, p.pickup_lon, p.provider_name, p.shelter_id, p.dispatch_note, s.name AS shelter_name,
                       ST_X(s.geom) AS lon, ST_Y(s.geom) AS lat, COALESCE(n.priority, 1) AS priority, COALESCE(n.people, 0) AS people,
                       aw.status AS access_status, aw.closes_at
                FROM procurement p JOIN shelter s ON s.id = p.shelter_id
                LEFT JOIN placed pl ON pl.procurement_id = p.id
                LEFT JOIN need n ON n.shelter_id = p.shelter_id AND n.item = p.item
                LEFT JOIN access_window aw ON aw.shelter_id = p.shelter_id
                WHERE p.status IN ('accepted','packed','in_transit') AND p.pickup_lat IS NOT NULL
                  AND p.quantity - COALESCE(pl.q, 0) > 1e-6
                ORDER BY COALESCE(n.priority, 1) DESC, p.needed_by NULLS LAST, p.id
                LIMIT {MAX_JOBS}""")

    async def _free_drivers(self, now: datetime) -> list[dict]:
        rows = await self.pool.fetch(
            f"""SELECT d.id, d.name, d.phone, d.token, d.capacity_kg, d.vehicle,
                       ST_X(dep.geom) AS dep_lon, ST_Y(dep.geom) AS dep_lat, pos.pos_lon, pos.pos_lat, pos.fix_time
                FROM driver d LEFT JOIN depot dep ON dep.id = d.depot_id {POS_JOIN}
                WHERE d.active AND NOT EXISTS (SELECT 1 FROM trip t WHERE t.driver_id = d.id AND t.status IN ('planned','en_route'))
                ORDER BY d.id""")
        out = []
        fresh_after = now - timedelta(minutes=self.s.gps_fresh_minutes)
        for r in rows:
            d = dict(r)
            if r["pos_lon"] is not None and r["fix_time"] >= fresh_after:
                d["start"], d["start_from"] = (r["pos_lon"], r["pos_lat"]), "gps"
            elif r["dep_lon"] is not None:
                d["start"], d["start_from"] = (r["dep_lon"], r["dep_lat"]), "depot"
            else:
                d["start"], d["start_from"] = None, None
            out.append(d)
        return out

    @staticmethod
    def _deadlines(w, now: datetime) -> tuple[int | None, int | None, datetime | None]:
        """Hard: the shelter's last safe road closes. Soft: when the Quartermaster wants it."""
        hard = soft = None
        due = w["needed_by"]
        if w["access_status"] == "closing" and w["closes_at"] is not None:
            hard = max(0, int((w["closes_at"] - now).total_seconds()))
            due = min(due, w["closes_at"]) if due else w["closes_at"]
        if w["needed_by"] is not None:
            soft = max(0, int((w["needed_by"] - now).total_seconds()))
        return hard, soft, due

    async def _plan(self) -> dict:
        s = self.s
        now = datetime.now(UTC)
        work = await self._open_work()
        drivers = await self._free_drivers(now)
        if not work:
            await store.mark_source(self.pool, "dispatcher", True, None, 0, f"no orders waiting for a driver; {len(drivers)} driver(s) free")
            return {"ok": True, "jobs": 0, "trips": 0, "unplanned": 0}
        usable = [d for d in drivers if d["start"] is not None]
        if not usable:
            why = "no free driver: register one, or wait for a trip to finish" if not drivers else "free drivers have no live GPS and no depot"
            reasons = {w["id"]: why for w in work}
            if await self.gh.healthy():
                # A missing driver is not the whole story if the shelter can't be reached at all: say so.
                router = await make_router(self.pool, self.gh)
                legs = await asyncio.gather(*(router.leg((w["pickup_lon"], w["pickup_lat"]), (w["lon"], w["lat"])) for w in work))
                for w, lg in zip(work, legs):
                    if not lg.ok and not lg.error:
                        reasons[w["id"]] = f"no safe road from the supplier to the shelter ({lg.why})"
            await self._report_unplanned(reasons, {w["id"]: w for w in work})
            await store.mark_source(self.pool, "dispatcher", True, None, len(work), f"{len(work)} orders waiting: {why}")
            return {"ok": True, "jobs": len(work), "trips": 0, "unplanned": len(work)}
        if not await self.gh.healthy():
            await store.mark_source(self.pool, "dispatcher", False, None, None, f"routing engine unreachable at {s.graphhopper_url}")
            return {"ok": False}
        router = await make_router(self.pool, self.gh)

        locs: list[tuple[float, float]] = []
        index: dict = {}

        def at(p) -> int:
            k = _key(p)
            if k not in index:
                index[k] = len(locs)
                locs.append((float(p[0]), float(p[1])))
            return index[k]

        vehicles = [vrp.Vehicle(str(d["id"]), at(d["start"]), float(d["capacity_kg"])) for d in usable]
        gps_starts = {v.start for v, d in zip(vehicles, usable) if d["start_from"] == "gps"}
        max_cap = max(v.capacity_kg for v in vehicles)
        # Split big orders into loads the smallest free vehicle can carry, so every driver can help;
        # fall back to the largest vehicle's size if that would make too many loads to plan well.
        min_cap = min(v.capacity_kg for v in vehicles)
        small = sum(len(vrp.split_by_capacity(float(w["remaining"]), s.item_kg_map.get(w["item"], 1.0), min_cap)) for w in work)
        chunk_cap = min_cap if small <= MAX_CHUNKS else max_cap
        kg_map = s.item_kg_map
        jobs: list[vrp.Job] = []
        meta: dict[str, tuple] = {}
        for w in work:
            kg = kg_map.get(w["item"], 1.0)
            pu, dr = at((w["pickup_lon"], w["pickup_lat"])), at((w["lon"], w["lat"]))
            hard, soft, due = self._deadlines(w, now)
            value = float(w["priority"]) * max(1, int(w["people"]))
            for i, qty in enumerate(vrp.split_by_capacity(float(w["remaining"]), kg, chunk_cap)):
                key = f"{w['id']}.{i + 1}"
                jobs.append(vrp.Job(key, pu, dr, qty * kg, hard, value, soft_deadline_s=soft))
                meta[key] = (w, qty, due)
        job_locs = {j.pickup for j in jobs} | {j.drop for j in jobs}
        pairs = [(a, b) for a in range(len(locs)) for b in range(len(locs)) if a != b and b in job_locs]
        legs = await asyncio.gather(*(router.leg(locs[a], locs[b], from_vehicle=a in gps_starts) for a, b in pairs))
        broken = next((lg for lg in legs if lg.error), None)
        if broken:
            await store.mark_source(self.pool, "dispatcher", False, None, None, broken.why)
            return {"ok": False, "error": broken.why}
        leg_of = dict(zip(pairs, legs))
        n = len(locs)
        matrix = [[0 if a == b else vrp.UNREACHABLE_S for b in range(n)] for a in range(n)]
        for (a, b), lg in leg_of.items():
            matrix[a][b] = lg.time_s if lg.ok else vrp.UNREACHABLE_S

        plan = await asyncio.to_thread(vrp.solve, matrix, vehicles, jobs, s.solver_seconds)

        by_vehicle = {str(d["id"]): (d, v) for d, v in zip(usable, vehicles)}
        trips = []
        for vkey, stops in plan.routes.items():
            d, v = by_vehicle[vkey]
            tid = await self._save_trip(d, v, stops, locs, leg_of, meta, now, plan.status, len(jobs))
            trips.append(tid)
            detail = await fleet.trip_detail(self.pool, tid)
            await fleet.notify(self.pool, d, fleet.trip_text(
                detail, d["token"], f"RASAD trip #{tid} for {d['name']}: {len(stops)} stops, about {detail['planned_min']:.0f} min. Reply START when you set off."))
            await publish(self.pool, "trip.planned", {"trip_id": tid, "driver": d["name"], "stops": len(stops),
                                                      "planned_min": detail["planned_min"], "planned_km": detail["planned_km"],
                                                      "start_from": d["start_from"]})

        jobs_by_key = {j.key: j for j in jobs}
        reasons: dict[int, str] = {}
        for key in plan.dropped:
            w, qty, due = meta[key]
            reasons.setdefault(w["id"], self._why_unplanned(jobs_by_key[key], vehicles, leg_of, due))
        planned_ids = {meta[st.job][0]["id"] for stops in plan.routes.values() for st in stops}
        await self._report_unplanned(reasons, {w["id"]: w for w in work}, cleared=planned_ids - set(reasons))

        note = f"{len(trips)} trips planned for {len(jobs) - len(plan.dropped)} loads; {len(reasons)} orders unplanned ({plan.status})"
        await store.mark_source(self.pool, "dispatcher", True, None, len(jobs), note)
        summary = {"ok": True, "jobs": len(jobs), "trips": len(trips), "trip_ids": trips, "unplanned": len(reasons),
                   "drivers_free": len(usable), "locations": n, "status": plan.status}
        await publish(self.pool, "dispatcher.run", summary)
        log.info("dispatcher: %s", note)
        return summary

    @staticmethod
    def _why_unplanned(job: vrp.Job, vehicles, leg_of, due) -> str:
        def ok(a, b):
            return a == b or leg_of[(a, b)].ok
        if not any(ok(v.start, job.pickup) for v in vehicles):
            lg = next((leg_of[(v.start, job.pickup)] for v in vehicles if v.start != job.pickup), None)
            return f"no safe road from any free driver to the supplier ({lg.why if lg else 'unknown'})"
        if not ok(job.pickup, job.drop):
            return f"no safe road from the supplier to the shelter ({leg_of[(job.pickup, job.drop)].why})"
        if job.deadline_s is not None:
            return f"cannot reach the shelter before its last safe road closes at {hm(due)} IST with the drivers free now"
        return "not enough free drivers or vehicle capacity right now"

    async def _report_unplanned(self, reasons: dict[int, str], work: dict, cleared: set[int] | None = None) -> None:
        for pid, why in reasons.items():
            w = work[pid]
            if w["dispatch_note"] == why:
                continue    # already reported; don't repeat on every run
            await self.pool.execute("UPDATE procurement SET dispatch_note=$2 WHERE id=$1", pid, why)
            await publish(self.pool, "dispatch.unassigned", {"procurement_id": pid, "shelter": w["shelter_name"], "item": w["item"],
                                                             "quantity": w["remaining"], "reason": why})
        if cleared:
            await self.pool.execute("UPDATE procurement SET dispatch_note=NULL WHERE id = ANY($1::bigint[])", list(cleared))

    async def _save_trip(self, d, v: vrp.Vehicle, stops: list[vrp.Stop], locs, leg_of, meta, now, status, n_jobs) -> int:
        rows = []
        cur = v.start
        km = 0.0
        lines = []
        for seq, st in enumerate(stops, 1):
            lg = leg_of.get((cur, st.location)) if cur != st.location else None
            if lg is not None:
                km += lg.dist_m / 1000
                lines.append(lg.line)
            w, qty, due = meta[st.job]
            name = (w["provider_name"] or "Supplier") if st.kind == "pickup" else w["shelter_name"]
            lon, lat = locs[st.location]
            rows.append((seq, st.kind, w["id"], name, lon, lat, w["item"], qty, now + timedelta(seconds=st.arrival_s),
                         due if st.kind == "drop" else None, st.job, _wkb(lg.line if lg else None)))
            cur = st.location
        geom = _join(lines)
        last = stops[-1]
        planned_min = (last.arrival_s + _service(last.kind)) / 60
        plan_info = {"start": list(locs[v.start]), "start_from": d["start_from"], "solver": status, "loads_considered": n_jobs,
                     "planned_at": now.isoformat()}
        async with self.pool.acquire() as conn, conn.transaction():
            tid = await conn.fetchval(
                """INSERT INTO trip (driver_id, status, geom, planned_km, planned_min, plan, last_progress_at)
                   VALUES ($1, 'planned', ST_SetSRID(ST_GeomFromWKB($2::bytea), 4326), $3, $4, $5, now()) RETURNING id""",
                d["id"], _wkb(geom), round(km, 2), round(planned_min, 1), plan_info)
            await conn.executemany(
                """INSERT INTO trip_stop (trip_id, seq, kind, procurement_id, name, geom, item, quantity, eta, due, chunk, leg)
                   VALUES ($1,$2,$3,$4,$5, ST_SetSRID(ST_MakePoint($6,$7),4326), $8,$9,$10,$11,$12, ST_SetSRID(ST_GeomFromWKB($13::bytea),4326))""",
                [(tid, *r) for r in rows])
        return tid

    # ------------------------------------------------------------------ monitoring

    async def monitor(self) -> dict:
        async with self._lock:
            return await self._monitor()

    async def _monitor(self) -> dict:
        s = self.s
        now = datetime.now(UTC)
        trips = await self.pool.fetch(
            f"""SELECT t.id, t.status, t.deviation_strikes, t.plan, d.id AS driver_id, d.name AS driver_name, d.phone, d.token,
                       pos.device_id, pos.pos_lon, pos.pos_lat, pos.fix_time,
                       (SELECT ST_Distance(ST_Collect(ts.leg)::geography, ST_SetSRID(ST_MakePoint(pos.pos_lon, pos.pos_lat), 4326)::geography)
                          FROM trip_stop ts WHERE ts.trip_id = t.id AND ts.status = 'pending' AND ts.leg IS NOT NULL) AS off_m,
                       EXISTS (SELECT 1 FROM trip_stop ts WHERE ts.trip_id = t.id AND ts.status = 'blocked') AS has_blocked
                FROM trip t JOIN driver d ON d.id = t.driver_id {POS_JOIN}
                WHERE t.status IN ('planned','en_route') ORDER BY t.id""")
        loop = asyncio.get_running_loop()
        zone_check = self._zones_dirty or loop.time() - self._last_zone_check > ZONE_RECHECK_S
        router = None
        if zone_check and trips:
            if await self.gh.healthy():
                router = await make_router(self.pool, self.gh)
                self._zones_dirty = False
                self._last_zone_check = loop.time()
        rerouted = deviated = stalled = 0
        fresh_after = now - timedelta(minutes=s.gps_fresh_minutes)
        for t in trips:
            reason = self._reroute_q.pop(t["id"], None)
            fresh = t["fix_time"] is not None and t["fix_time"] >= fresh_after
            here = (t["pos_lon"], t["pos_lat"]) if fresh else None
            plan = t["plan"] or {}

            if here and t["status"] == "planned" and plan.get("start"):
                if haversine_km(*here, *plan["start"]) * 1000 > STARTED_M:
                    await self.pool.execute("UPDATE trip SET status='en_route', updated_at=now() WHERE id=$1 AND status='planned'", t["id"])
                    await publish(self.pool, "trip.updated", {"trip_id": t["id"], "status": "en_route", "driver": t["driver_name"]})

            if here and t["off_m"] is not None:
                strikes = t["deviation_strikes"] + 1 if t["off_m"] > s.deviation_m else 0
                if strikes != t["deviation_strikes"]:
                    await self.pool.execute("UPDATE trip SET deviation_strikes=$2 WHERE id=$1", t["id"], strikes)
                if strikes >= STRIKES_TO_REROUTE and reason is None:
                    reason = f"driver is {t['off_m']:.0f} m off the planned road"
                    deviated += 1
                    await publish(self.pool, "trip.deviated", {"trip_id": t["id"], "driver": t["driver_name"], "off_m": round(t["off_m"])})

            if here and t["status"] == "en_route" and await self._stalled(t, here, now):
                stalled += 1

            if reason is None and router is not None:
                reason = await self._route_now_unsafe(t["id"], router)
            if reason is None and router is not None and t["has_blocked"]:
                reason = "checking held stops"    # a road may have reopened
            if reason:
                res = await self._reroute(t, reason, here, quiet=(reason == "checking held stops"))
                if res and res.get("changed"):
                    rerouted += 1
        return {"trips": len(trips), "rerouted": rerouted, "deviated": deviated, "stalled": stalled, "zones_checked": router is not None}

    async def _route_now_unsafe(self, trip_id: int, router: Router) -> str | None:
        legs = await self.pool.fetch(
            "SELECT seq, name, ST_AsBinary(leg) AS wkb FROM trip_stop WHERE trip_id=$1 AND status='pending' AND leg IS NOT NULL ORDER BY seq",
            trip_id)
        for i, r in enumerate(legs):
            line = shp_wkb.loads(bytes(r["wkb"]))
            # The first remaining leg starts where the driver already is; a hazard right there is behind them.
            bad, _worst, why = router.unsafe(line, line.coords[0] if i == 0 else None)
            if bad:
                return f"road to stop {r['seq']} ({r['name']}) now {why}"
        return None

    async def _stalled(self, t, here, now: datetime) -> bool:
        s = self.s
        window = timedelta(minutes=s.stall_minutes)
        old = await self.pool.fetchrow(
            """SELECT ST_X(geom) AS lon, ST_Y(geom) AS lat, fix_time FROM vehicle_position
               WHERE device_id=$1 AND fix_time <= $2 ORDER BY fix_time DESC LIMIT 1""", t["device_id"], now - window)
        if old is None or old["fix_time"] < now - 2 * window:
            return False
        if haversine_km(old["lon"], old["lat"], *here) * 1000 > STOPPED_M:
            return False
        recent = await self.pool.fetchrow(
            f"""SELECT max(done_at) AS last_done,
                       bool_or(status='pending' AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint($2,$3),4326)::geography, {STOPPED_M * 2})) AS at_stop
                FROM trip_stop WHERE trip_id=$1""", t["id"], here[0], here[1])
        if recent["at_stop"] or (recent["last_done"] and recent["last_done"] > now - window):
            return False     # loading or unloading
        plan = t["plan"] or {}
        last_alert = plan.get("stall_alert_at")
        if last_alert and datetime.fromisoformat(last_alert) > now - window:
            return True
        await self.pool.execute("UPDATE trip SET plan = plan || jsonb_build_object('stall_alert_at', $2::text) WHERE id=$1",
                                t["id"], now.isoformat())
        await publish(self.pool, "trip.stalled", {"trip_id": t["id"], "driver": t["driver_name"], "minutes": s.stall_minutes,
                                                  "lat": here[1], "lon": here[0]})
        return True

    async def reroute(self, trip_id: int, reason: str) -> dict | None:
        """Reroute one trip now (used by tests and the control room)."""
        async with self._lock:
            t = await self.pool.fetchrow(
                f"""SELECT t.id, t.status, t.plan, d.id AS driver_id, d.name AS driver_name, d.phone, d.token, pos.pos_lon, pos.pos_lat, pos.fix_time
                    FROM trip t JOIN driver d ON d.id = t.driver_id {POS_JOIN} WHERE t.id=$1""", trip_id)
            if t is None or t["status"] not in fleet.ACTIVE:
                return None
            fresh = t["fix_time"] is not None and t["fix_time"] >= datetime.now(UTC) - timedelta(minutes=self.s.gps_fresh_minutes)
            return await self._reroute(t, reason, (t["pos_lon"], t["pos_lat"]) if fresh else None)

    async def _reroute(self, t, reason: str, here, quiet: bool = False) -> dict | None:
        if not await self.gh.healthy():
            return None
        router = await make_router(self.pool, self.gh)
        now = datetime.now(UTC)
        stops = await self.pool.fetch(
            """SELECT id, seq, kind, chunk, procurement_id, name, ST_X(geom) AS lon, ST_Y(geom) AS lat, status, note, item, quantity, done_at
               FROM trip_stop WHERE trip_id=$1 ORDER BY seq""", t["id"])
        on_board = {st["chunk"] for st in stops if st["kind"] == "pickup" and st["status"] == "done"}
        todo = [st for st in stops if st["status"] == "pending" or (st["status"] == "blocked" and st["chunk"] in on_board)]
        if not todo:
            await fleet.finish_if_complete(self.pool, t["id"])
            return {"changed": False}
        if here is not None:
            origin, from_vehicle = here, True
        else:
            done = [st for st in stops if st["status"] == "done"]
            last = max(done, key=lambda st: st["done_at"]) if done else None
            origin = (last["lon"], last["lat"]) if last else tuple((t["plan"] or {}).get("start") or (todo[0]["lon"], todo[0]["lat"]))
            from_vehicle = not done and (t["plan"] or {}).get("start_from") == "gps"

        released: dict[str, str] = {}
        for _ in range(len(todo) + 1):
            cur, fv, clock, owed = origin, from_vehicle, 0, 0
            result: list[tuple] = []
            restart = False
            for st in todo:
                if st["chunk"] in released:
                    continue
                loc = (st["lon"], st["lat"])
                lg = await router.leg(cur, loc, from_vehicle=fv)
                if lg.error:
                    return None
                if not lg.ok:
                    if st["kind"] == "drop" and st["chunk"] in on_board:
                        result.append((st, "blocked", lg, None))
                        continue
                    released[st["chunk"]] = lg.why or "no safe road"
                    restart = True
                    break
                same_place = owed and _key(loc) == _key(cur)
                clock += vrp.SAME_PLACE_S if same_place else owed + lg.time_s
                result.append((st, "pending", lg, clock))
                owed = _service(st["kind"])
                cur, fv = loc, False
            if not restart:
                break

        before = {st["id"]: st["status"] for st in stops}
        async with self.pool.acquire() as conn, conn.transaction():
            for chunk, why in released.items():
                await conn.execute(
                    "UPDATE trip_stop SET status='released', note=$3, leg=NULL WHERE trip_id=$1 AND chunk=$2 AND status IN ('pending','blocked')",
                    t["id"], chunk, f"no safe road: {why}")
            for st, status, lg, clock in result:
                if status == "blocked":
                    await conn.execute("UPDATE trip_stop SET status='blocked', note=$2, eta=NULL, leg=NULL WHERE id=$1",
                                       st["id"], f"held: {lg.why}")
                else:
                    await conn.execute(
                        "UPDATE trip_stop SET status='pending', note=NULL, eta=$2, leg=ST_SetSRID(ST_GeomFromWKB($3::bytea),4326) WHERE id=$1",
                        st["id"], now + timedelta(seconds=clock), _wkb(lg.line))
            pending = [(st, lg, clock) for st, status, lg, clock in result if status == "pending"]
            geom = _join([lg.line for _st, lg, _c in pending])
            remaining_min = (pending[-1][2] + _service(pending[-1][0]["kind"])) / 60 if pending else 0   # last stop's own loading time
            await conn.execute(
                """UPDATE trip SET geom=ST_SetSRID(ST_GeomFromWKB($2::bytea),4326), planned_min=$3, planned_km=$4, deviation_strikes=0,
                       plan = plan || jsonb_build_object('rerouted_at', $5::text, 'reroute_reason', $6::text), updated_at=now() WHERE id=$1""",
                t["id"], _wkb(geom), round(remaining_min, 1), round(sum(lg.dist_m for _s, lg, _c in pending) / 1000, 2),
                now.isoformat(), reason)
        after = {r["id"]: r["status"] for r in await self.pool.fetch("SELECT id, status FROM trip_stop WHERE trip_id=$1", t["id"])}
        changed_status = [sid for sid in after if after[sid] != before.get(sid)]
        changed = bool(changed_status) or not quiet
        held = [st["name"] for st, status, _lg, _c in result if status == "blocked"]
        rel_names = sorted({st["name"] for st in todo if st["chunk"] in released})
        finished = await fleet.finish_if_complete(self.pool, t["id"])
        if changed:
            detail = await fleet.trip_detail(self.pool, t["id"])
            header = f"Route changed: {reason}."
            if rel_names:
                header += f" Removed (no safe road): {', '.join(rel_names)}."
            if held:
                header += f" On hold until a road opens: {', '.join(held)}."
            driver = {"id": t["driver_id"], "phone": t["phone"]}
            await fleet.notify(self.pool, driver, fleet.trip_text(detail, t["token"], header))
            await publish(self.pool, "trip.rerouted", {"trip_id": t["id"], "driver": t["driver_name"], "reason": reason,
                                                       "released": rel_names, "held": held, "remaining_min": round(remaining_min, 1),
                                                       "finished": finished})
        if released:
            self._wake.set()      # freed loads go back to the planner for another driver
        return {"changed": changed, "released": rel_names, "held": held, "remaining_min": remaining_min, "finished": finished}
