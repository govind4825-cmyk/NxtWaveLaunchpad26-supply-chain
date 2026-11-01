"""Road closures from TomTom and from field reports, with a corroboration rule.

One report from the field creates a *suspected* closure: routes are penalised
but not forbidden. It becomes *confirmed* (routes forbidden) when a second,
different reporter reports within 300 m and 6 hours, when TomTom shows a closure
there, or when the officer confirms it. Every report is kept in road_report.
"""

from __future__ import annotations

import asyncpg

from .connectors.overpass import clamp_bbox
from .connectors.tomtom import closures_in
from .events import publish
from .provenance import Fetcher

CORROBORATE_M = 300
CORROBORATE_HOURS = 6
SUSPECT_TTL_HOURS = 12
TOMTOM_MAX_SPAN_DEG = 0.9   # keeps a query box under TomTom's area limit


async def add_report(pool: asyncpg.Pool, lat: float, lon: float, note: str | None, reporter: str, channel: str) -> dict:
    reporter = reporter.strip().lower()[:80] or "anonymous"
    async with pool.acquire() as conn, conn.transaction():
        report_id = await conn.fetchval(
            "INSERT INTO road_report (geom, note, reporter, channel) VALUES (ST_SetSRID(ST_MakePoint($1,$2),4326), $3, $4, $5) RETURNING id",
            lon, lat, note, reporter, channel,
        )
        near = await conn.fetchrow(
            f"""SELECT id, source, confirmed, reporters FROM closure
                WHERE (valid_to IS NULL OR valid_to > now())
                  AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint($1,$2),4326)::geography, {CORROBORATE_M})
                  AND (source <> 'report' OR updated_at > now() - interval '{CORROBORATE_HOURS} hours')
                ORDER BY confirmed DESC, ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint($1,$2),4326)::geography)
                LIMIT 1""",
            lon, lat,
        )
        if near:
            reporters = list(near["reporters"] or [])
            if reporter not in reporters:
                reporters.append(reporter)
            confirm = near["confirmed"] or len(reporters) >= 2
            await conn.execute(
                f"""UPDATE closure SET reporters=$2, confirmed=$3, updated_at=now(),
                       valid_to = CASE WHEN source <> 'report' THEN valid_to
                                       WHEN $3 THEN NULL   -- confirmed by the field: stays until cleared
                                       ELSE now() + interval '{SUSPECT_TTL_HOURS} hours' END
                    WHERE id=$1""",
                near["id"], reporters, confirm,
            )
            await conn.execute("UPDATE road_report SET closure_id=$1 WHERE id=$2", near["id"], report_id)
            action = "already_confirmed" if near["confirmed"] else ("confirmed" if confirm else "corroborating")
            closure_id = near["id"]
        else:
            closure_id = await conn.fetchval(
                f"""INSERT INTO closure (source, source_ref, geom, confirmed, reason, reporters, valid_to)
                    VALUES ('report', $1, ST_SetSRID(ST_MakePoint($2,$3),4326), false, $4, $5, now() + interval '{SUSPECT_TTL_HOURS} hours')
                    RETURNING id""",
                f"report:{report_id}", lon, lat, note or "road reported blocked", [reporter],
            )
            await conn.execute("UPDATE road_report SET closure_id=$1 WHERE id=$2", closure_id, report_id)
            action = "suspected"
    payload = {"closure_id": closure_id, "report_id": report_id, "action": action, "lat": lat, "lon": lon, "note": note}
    await publish(pool, "closure.updated", payload)
    return payload


async def set_closure(pool: asyncpg.Pool, closure_id: int, confirmed: bool | None = None, clear: bool = False) -> bool:
    if clear:
        n = await pool.execute("UPDATE closure SET valid_to=now(), updated_at=now() WHERE id=$1", closure_id)
    else:
        n = await pool.execute(
            """UPDATE closure SET confirmed=$2, updated_at=now(),
                 valid_to = CASE WHEN $2 AND source='report' THEN NULL ELSE valid_to END
               WHERE id=$1""",
            closure_id, bool(confirmed),
        )
    changed = n.endswith("1")
    if changed:
        await publish(pool, "closure.updated", {"closure_id": closure_id, "action": "cleared" if clear else ("confirmed" if confirmed else "unconfirmed"), "by": "officer"})
    return changed


async def sync_tomtom(pool: asyncpg.Pool, fetcher: Fetcher, key: str, areas: list[tuple]) -> dict:
    """areas: list of (bounds, centre). Upserts closing incidents; ends TomTom closures no longer reported."""
    summary = {"ok": True, "added": 0, "ended": 0, "raw_id": None, "error": None}
    for bounds, centre in areas:
        bbox, _ = clamp_bbox(bounds, centre, TOMTOM_MAX_SPAN_DEG)
        incidents, raw_id, err = await closures_in(fetcher, key, bbox)
        summary["raw_id"] = raw_id or summary["raw_id"]
        if err:
            summary["ok"], summary["error"] = False, err
            continue
        seen = []
        for inc in incidents:
            seen.append(inc.ref)
            new = await pool.fetchval(
                """INSERT INTO closure (source, source_ref, geom, confirmed, reason, valid_from, valid_to, raw_fetch_id)
                   VALUES ('tomtom', $1, ST_SetSRID(ST_GeomFromText($2),4326), true, $3, COALESCE($4, now()), $5, $6)
                   ON CONFLICT (source, source_ref) DO UPDATE SET geom=EXCLUDED.geom, reason=EXCLUDED.reason,
                     valid_to=EXCLUDED.valid_to, raw_fetch_id=EXCLUDED.raw_fetch_id, updated_at=now()
                   RETURNING (xmax = 0)""",
                inc.ref, inc.geom.wkt, inc.reason, inc.start, inc.end, raw_id,
            )
            summary["added"] += 1 if new else 0
        w, s, e, n = bbox
        ended = await pool.execute(
            """UPDATE closure SET valid_to=now(), updated_at=now()
               WHERE source='tomtom' AND (valid_to IS NULL OR valid_to > now()) AND NOT (source_ref = ANY($1::text[]))
                 AND ST_Intersects(geom, ST_MakeEnvelope($2,$3,$4,$5,4326))""",
            seen, w, s, e, n,
        )
        summary["ended"] += int(ended.split()[-1])
    if summary["added"] or summary["ended"]:
        await publish(pool, "closure.updated", {"source": "tomtom", "added": summary["added"], "ended": summary["ended"]})
    return summary

