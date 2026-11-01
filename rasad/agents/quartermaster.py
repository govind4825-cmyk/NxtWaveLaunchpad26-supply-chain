"""Quartermaster agent: how much each shelter needs, by when, and why.

People per shelter come from the shelter in-charge's latest headcount when there is
one (within RASAD_HEADCOUNT_VALID_HOURS). Until then RASAD uses a planning figure:
WorldPop residents in the shelter's catchment (its Voronoi cell inside the hazard)
times the officer-set displacement rate for that hazard kind. If neither exists the
shelter gets no quantities at all, only a request for a headcount: RASAD never guesses.

Needs are those people x Sphere minimums x supply days, due before the shelter's
access window closes, and ranked by people x urgency.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import asyncpg
import shapely
from shapely import wkb as shp_wkb

from .. import store
from ..config import get_settings
from ..connectors.worldpop import population as worldpop_population
from ..engine.needs import catchments, geom_hash, need_lines, needed_by, supply_days, urgency
from ..events import STREAM, get_redis, publish
from ..provenance import Fetcher

log = logging.getLogger(__name__)

WAKE_ON = {"access_window.changed", "cartographer.run", "headcount.updated", "shelter.updated", "hazard.updated"}
DEBOUNCE_S = 20
WORLDPOP_CONCURRENCY = 3
RETRY_POP_AFTER = timedelta(hours=1)


class Quartermaster:
    def __init__(self, pool: asyncpg.Pool, fetcher: Fetcher, population_fn=None):
        self.s = get_settings()
        self.pool = pool
        self.fetcher = fetcher
        self._population = population_fn or (lambda g: worldpop_population(self.fetcher, self.s.worldpop_base, g, self.s.worldpop_year))
        self._wake = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        self._tasks.append(asyncio.create_task(self._loop(), name="quartermaster"))
        self._tasks.append(asyncio.create_task(self._listen(), name="quartermaster:events"))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def trigger(self) -> None:
        self._wake.set()

    async def _loop(self) -> None:
        await asyncio.sleep(25)
        while True:
            try:
                await self.run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("quartermaster run crashed")
                await store.mark_source(self.pool, "quartermaster", False, None, None, f"crash: {exc}")
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.s.quartermaster_every)
                await asyncio.sleep(DEBOUNCE_S)
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
        kinds = list(s.displacement_map)
        await self._refresh_catchments(kinds)
        pop_errors = await self._fill_population()
        summary = await self._compute_needs()
        note = f"{summary['shelters_with_needs']} shelters with needs, {summary['awaiting']} awaiting headcount"
        if pop_errors:
            note += f"; WorldPop: {pop_errors[0][:120]}"
        await store.mark_source(self.pool, "quartermaster", not pop_errors or summary["shelters_with_needs"] > 0, None,
                                summary["shelters_with_needs"], note)
        await publish(self.pool, "quartermaster.run", summary)
        log.info("quartermaster: %s", summary)
        return summary

    async def _refresh_catchments(self, kinds: list[str]) -> None:
        hazards = await self.pool.fetch(
            """SELECT h.id, ST_AsBinary(h.geom) AS wkb FROM hazard h
               WHERE h.status='active' AND h.geom IS NOT NULL AND h.kind = ANY($1::text[])
                 AND EXISTS (SELECT 1 FROM shelter_hazard sh JOIN shelter s ON s.id=sh.shelter_id
                             WHERE sh.hazard_id=h.id AND s.active)""",
            kinds,
        )
        live: set[tuple[int, str]] = set()
        for h in hazards:
            area = shp_wkb.loads(bytes(h["wkb"]))
            pts = await self.pool.fetch(
                """SELECT s.id, ST_X(s.geom) lon, ST_Y(s.geom) lat FROM shelter s
                   JOIN shelter_hazard sh ON sh.shelter_id=s.id WHERE sh.hazard_id=$1 AND s.active""",
                h["id"],
            )
            cells = catchments({p["id"]: (p["lon"], p["lat"]) for p in pts}, area)
            for sid, cell in cells.items():
                live.add((sid, str(h["id"])))
                gh = geom_hash(cell)
                await self.pool.execute(
                    """INSERT INTO catchment (shelter_id, hazard_id, geom, geom_hash)
                       VALUES ($1, $2, ST_Multi(ST_SetSRID(ST_GeomFromWKB($3::bytea),4326)), $4)
                       ON CONFLICT (shelter_id, hazard_id) DO UPDATE SET
                         geom = EXCLUDED.geom, geom_hash = EXCLUDED.geom_hash, computed_at = now(),
                         population = CASE WHEN catchment.geom_hash = EXCLUDED.geom_hash THEN catchment.population END,
                         pop_raw_fetch_id = CASE WHEN catchment.geom_hash = EXCLUDED.geom_hash THEN catchment.pop_raw_fetch_id END,
                         pop_error = CASE WHEN catchment.geom_hash = EXCLUDED.geom_hash THEN catchment.pop_error END""",
                    sid, h["id"], shapely.to_wkb(cell), gh,
                )
        # Drop catchments whose hazard ended or whose shelter left it.
        rows = await self.pool.fetch("SELECT shelter_id, hazard_id FROM catchment")
        for r in rows:
            if (r["shelter_id"], str(r["hazard_id"])) not in live:
                await self.pool.execute("DELETE FROM catchment WHERE shelter_id=$1 AND hazard_id=$2", r["shelter_id"], r["hazard_id"])

    async def _fill_population(self) -> list[str]:
        rows = await self.pool.fetch(
            """SELECT shelter_id, hazard_id, ST_AsBinary(geom) AS wkb FROM catchment
               WHERE population IS NULL AND (pop_error IS NULL OR computed_at < now() - $1::interval)
               ORDER BY computed_at LIMIT $2""",
            RETRY_POP_AFTER, self.s.worldpop_calls_per_run,
        )
        sem = asyncio.Semaphore(WORLDPOP_CONCURRENCY)
        errors: list[str] = []

        async def one(r):
            async with sem:
                pop, raw_id, err = await self._population(shp_wkb.loads(bytes(r["wkb"])))
            if pop is None:
                errors.append(err or "unknown error")
            await self.pool.execute(
                """UPDATE catchment SET population=$3, pop_year=$4, pop_raw_fetch_id=$5, pop_error=$6, computed_at=now()
                   WHERE shelter_id=$1 AND hazard_id=$2""",
                r["shelter_id"], r["hazard_id"], pop, self.s.worldpop_year if pop is not None else None, raw_id, err,
            )

        await asyncio.gather(*(one(r) for r in rows))
        return errors

    async def _compute_needs(self) -> dict:
        s = self.s
        now = datetime.now(UTC)
        rates = s.displacement_map
        shelters = await self.pool.fetch(
            """SELECT s.id, s.name, a.status AS access_status, a.closes_at, a.computed_at AS access_at
               FROM shelter s LEFT JOIN access_window a ON a.shelter_id = s.id
               WHERE s.active AND (s.source='officer' OR EXISTS (
                   SELECT 1 FROM shelter_hazard sh JOIN hazard h ON h.id=sh.hazard_id
                   WHERE sh.shelter_id=s.id AND h.status='active'))"""
        )
        with_needs = awaiting = 0
        totals: dict[str, float] = {}
        for sh in shelters:
            hc = await self.pool.fetchrow(
                "SELECT id, people, households, at FROM headcount WHERE shelter_id=$1 AND at > now() - $2::interval ORDER BY at DESC LIMIT 1",
                sh["id"], timedelta(hours=s.headcount_valid_hours),
            )
            cats = await self.pool.fetch(
                """SELECT c.hazard_id, c.population, c.pop_year, c.pop_raw_fetch_id, h.kind, h.expires, h.title
                   FROM catchment c JOIN hazard h ON h.id=c.hazard_id
                   WHERE c.shelter_id=$1 AND h.status='active'""",
                sh["id"],
            )
            days = max([supply_days(c["expires"], now, s.default_days, s.max_days) for c in cats] or
                       [s.default_days])
            basis: dict = {"catchments": [], "access_window_at": sh["access_at"].isoformat() if sh["access_at"] else None}
            people, people_basis = None, None
            if hc:
                people, people_basis = hc["people"], "headcount"
                basis["headcount"] = {"id": hc["id"], "people": hc["people"], "at": hc["at"].isoformat()}
            planning = []
            for c in cats:
                rate = rates.get(c["kind"])
                entry = {"hazard_id": str(c["hazard_id"]), "hazard": c["title"], "residents": c["population"],
                         "year": c["pop_year"], "raw_fetch_id": c["pop_raw_fetch_id"], "displacement_rate": rate}
                basis["catchments"].append(entry)
                if c["population"] is not None and rate:
                    planning.append(c["population"] * rate)
            if people is None and planning:
                people, people_basis = round(max(planning)), "planning"

            existing = {r["item"]: r for r in await self.pool.fetch(
                "SELECT item, quantity, committed FROM need WHERE shelter_id=$1", sh["id"])}
            if not people:
                awaiting += 1
                if existing:
                    await self.pool.execute("DELETE FROM need WHERE shelter_id=$1", sh["id"])
                continue

            lines = need_lines(people, days, s.household_size)
            prio = people * urgency(sh["access_status"], sh["closes_at"], now)
            due = needed_by(sh["access_status"], sh["closes_at"], now)
            for ln in lines:
                await self.pool.execute(
                    """INSERT INTO need (shelter_id, item, unit, quantity, people, people_basis, days, rate, needed_by, priority, access_status, basis, computed_at)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, now())
                       ON CONFLICT (shelter_id, item) DO UPDATE SET unit=EXCLUDED.unit, quantity=EXCLUDED.quantity,
                         people=EXCLUDED.people, people_basis=EXCLUDED.people_basis, days=EXCLUDED.days, rate=EXCLUDED.rate,
                         needed_by=EXCLUDED.needed_by, priority=EXCLUDED.priority, access_status=EXCLUDED.access_status,
                         basis=EXCLUDED.basis, computed_at=now()""",
                    sh["id"], ln["item"], ln["unit"], ln["quantity"], people, people_basis, days, ln["rate"],
                    due, prio, sh["access_status"], basis,
                )
                committed = existing[ln["item"]]["committed"] if ln["item"] in existing else 0.0
                totals[ln["item"]] = totals.get(ln["item"], 0.0) + max(0.0, ln["quantity"] - committed)
            with_needs += 1
            old = existing.get("drinking_water")
            new_q = next(ln["quantity"] for ln in lines if ln["item"] == "drinking_water")
            if old is None or abs(new_q - old["quantity"]) > 0.2 * max(old["quantity"], 1):
                await publish(self.pool, "need.updated", {
                    "shelter_id": sh["id"], "name": sh["name"], "people": people, "people_basis": people_basis,
                    "days": days, "water_litres": new_q, "previous_water_litres": old["quantity"] if old else None,
                    "needed_by": due.isoformat(), "access_status": sh["access_status"],
                })
        return {"shelters_with_needs": with_needs, "awaiting": awaiting, "outstanding": {k: round(v, 1) for k, v in totals.items()}}


async def record_headcount(pool: asyncpg.Pool, shelter_id: int, people: int, households: int | None,
                           reporter: str, channel: str, note: str | None) -> int:
    hid = await pool.fetchval(
        "INSERT INTO headcount (shelter_id, people, households, reporter, channel, note) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id",
        shelter_id, people, households, reporter.strip()[:80], channel, note,
    )
    await publish(pool, "headcount.updated", {"shelter_id": shelter_id, "people": people, "reporter": reporter, "headcount_id": hid})
    return hid
