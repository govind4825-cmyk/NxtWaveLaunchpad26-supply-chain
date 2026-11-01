"""River watch: finds road bridges over rivers inside flood-prone hazards and tracks
the GloFAS probability that each river exceeds its own flood threshold.

This is the first input to access windows: a bridge whose river is likely to
exceed its 2-year flood level on day N is a road that may fail on day N.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date

import asyncpg
from shapely import wkb as shp_wkb

from ..config import get_settings
from ..connectors import openmeteo_flood as om
from ..connectors.overpass import river_crossings
from ..engine.classify import RIVER_KINDS
from ..engine.flood import exceedance_probability, thresholds_from_history
from ..provenance import Fetcher

log = logging.getLogger(__name__)

MAX_HAZARDS_PER_RUN = 8
REFRESH_HOURS = 6
CONCURRENCY = 4


class RiverWatch:
    def __init__(self, pool: asyncpg.Pool, fetcher: Fetcher):
        self.s = get_settings()
        self.pool = pool
        self.fetcher = fetcher

    async def run(self) -> dict:
        hazards = await self.pool.fetch(
            """SELECT h.id, ST_AsBinary(h.geom) AS wkb,
                      EXISTS (SELECT 1 FROM river_point_hazard r WHERE r.hazard_id=h.id) AS has_points
               FROM hazard h
               WHERE h.status='active' AND h.geom IS NOT NULL AND h.kind = ANY($1::text[]) AND h.severity >= 2
               ORDER BY h.severity DESC, h.last_updated DESC LIMIT $2""",
            list(RIVER_KINDS), MAX_HAZARDS_PER_RUN,
        )
        errors: list[str] = []
        for h in hazards:
            if h["has_points"]:
                continue
            geom = shp_wkb.loads(bytes(h["wkb"]))
            centre = geom.representative_point()
            crossings, sampled, err = await river_crossings(
                self.fetcher, self.s.overpass_url, geom.bounds, (centre.x, centre.y)
            )
            if err:
                errors.append(f"overpass: {err}")
                continue
            for c in crossings:
                if not geom.buffer(0.02).contains(shp_point(c.lon, c.lat)):
                    continue
                pid = await self.pool.fetchval(
                    """INSERT INTO river_point (osm_ref, name, road_class, geom)
                       VALUES ($1,$2,$3, ST_SetSRID(ST_MakePoint($4,$5),4326))
                       ON CONFLICT (osm_ref) DO UPDATE SET name=EXCLUDED.name RETURNING id""",
                    c.osm_ref, c.name, c.road_class, c.lon, c.lat,
                )
                await self.pool.execute(
                    "INSERT INTO river_point_hazard VALUES ($1,$2) ON CONFLICT DO NOTHING", pid, h["id"]
                )
            if sampled:
                await self.pool.execute(
                    """UPDATE hazard SET attrs = attrs || jsonb_build_object('river_watch_sampled', true) WHERE id=$1""",
                    h["id"],
                )

        points = await self.pool.fetch(
            f"""SELECT DISTINCT p.id, ST_Y(p.geom) AS lat, ST_X(p.geom) AS lon, p.threshold_m3s
                FROM river_point p
                JOIN river_point_hazard rh ON rh.river_point_id = p.id
                JOIN hazard h ON h.id = rh.hazard_id AND h.status='active'
                WHERE NOT p.no_river
                  AND (p.forecast_at IS NULL OR p.forecast_at < now() - interval '{REFRESH_HOURS} hours')"""
        )
        sem = asyncio.Semaphore(CONCURRENCY)
        last_raw: list[int | None] = [None]

        async def one(p):
            async with sem:
                try:
                    last_raw[0] = await self._refresh_point(p) or last_raw[0]
                except Exception as exc:
                    log.exception("river point %s failed", p["id"])
                    errors.append(f"point {p['id']}: {exc}")

        await asyncio.gather(*(one(p) for p in points))
        summaries = await self._summaries([str(h["id"]) for h in hazards])
        ok = not errors or len(errors) < max(1, len(points))
        return {
            "ok": ok,
            "points": len(points),
            "raw_id": last_raw[0],
            "error": "; ".join(errors[:5]) or None,
            "hazards": summaries,
        }

    async def _refresh_point(self, p) -> int | None:
        threshold = p["threshold_m3s"]
        if threshold is None:
            raw_id, times, values = await om.history(self.fetcher, self.s.openmeteo_flood_base, p["lat"], p["lon"])
            th = thresholds_from_history(times, values) if times else None
            if th is None:
                if times and all(v is None for v in values):
                    await self.pool.execute("UPDATE river_point SET no_river=true WHERE id=$1", p["id"])
                return raw_id
            threshold = th["flood_m3s"]
            await self.pool.execute(
                """UPDATE river_point SET threshold_m3s=$2, warning_m3s=$3, threshold_method=$4, threshold_raw_fetch_id=$5
                   WHERE id=$1""",
                p["id"], th["flood_m3s"], th["warning_m3s"], th["method"], raw_id,
            )

        fc = await om.forecast(self.fetcher, self.s.openmeteo_flood_base, p["lat"], p["lon"])
        if fc is None:
            return None
        if fc.no_river:
            await self.pool.execute("UPDATE river_point SET no_river=true WHERE id=$1", p["id"])
            return fc.raw_id
        for d in fc.days:
            prob = exceedance_probability(threshold, d["q_min"], d["q_p25"], d["q_median"], d["q_p75"], d["q_max"])
            await self.pool.execute(
                """INSERT INTO river_forecast (river_point_id, day, discharge, q_min, q_p25, q_median, q_p75, q_max, exceed_prob, raw_fetch_id, fetched_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now())
                   ON CONFLICT (river_point_id, day) DO UPDATE SET discharge=EXCLUDED.discharge, q_min=EXCLUDED.q_min,
                     q_p25=EXCLUDED.q_p25, q_median=EXCLUDED.q_median, q_p75=EXCLUDED.q_p75, q_max=EXCLUDED.q_max,
                     exceed_prob=EXCLUDED.exceed_prob, raw_fetch_id=EXCLUDED.raw_fetch_id, fetched_at=now()""",
                p["id"], date.fromisoformat(d["day"]), d["discharge"], d["q_min"], d["q_p25"], d["q_median"],
                d["q_p75"], d["q_max"], prob, fc.raw_id,
            )
        await self.pool.execute(
            "UPDATE river_point SET forecast_at=now(), model_lat=$2, model_lon=$3 WHERE id=$1",
            p["id"], fc.model_lat, fc.model_lon,
        )
        return fc.raw_id

    async def _summaries(self, hazard_ids: list[str]) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for hid in hazard_ids:
            row = await self.pool.fetchrow(
                """SELECT count(DISTINCT p.id) AS points,
                          max(f.exceed_prob) FILTER (WHERE f.day <= current_date + 3) AS p3,
                          max(f.exceed_prob) FILTER (WHERE f.day <= current_date + 7) AS p7,
                          count(DISTINCT p.id) FILTER (WHERE f.exceed_prob >= 0.5 AND f.day <= current_date + 3) AS at_risk_3d
                   FROM river_point_hazard rh
                   JOIN river_point p ON p.id = rh.river_point_id AND NOT p.no_river
                   LEFT JOIN river_forecast f ON f.river_point_id = p.id AND f.day >= current_date
                   WHERE rh.hazard_id = $1::uuid""",
                hid,
            )
            if not row or not row["points"]:
                continue
            rw = {
                "points": row["points"],
                "max_prob_3d": row["p3"],
                "max_prob_7d": row["p7"],
                "crossings_at_risk_3d": row["at_risk_3d"],
            }
            await self.pool.execute(
                "UPDATE hazard SET attrs = attrs || jsonb_build_object('river_watch', $2::jsonb), last_updated=now() WHERE id=$1::uuid",
                hid, rw,
            )
            out[hid] = rw
        return out


def shp_point(lon: float, lat: float):
    from shapely.geometry import Point

    return Point(lon, lat)
