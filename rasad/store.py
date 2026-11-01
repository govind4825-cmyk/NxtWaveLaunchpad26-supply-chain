"""Writes hazards to PostGIS, merging duplicates and applying CAP updates and cancels."""

from __future__ import annotations

from datetime import datetime

import asyncpg
import shapely
from shapely import wkb as shp_wkb

from .engine.fusion import LOCATION_RANK, Candidate, best_match
from .models import HazardRecord

GEOM_SQL = "ST_Multi(ST_MakeValid(ST_SetSRID(ST_GeomFromWKB($1::bytea), 4326)))"


def _wkb(geom) -> bytes | None:
    if geom is None:
        return None
    return shapely.to_wkb(geom.simplify(0.002, preserve_topology=True))


def _message_attrs(rec: HazardRecord) -> dict:
    keep = {}
    for k, v in rec.attrs.items():
        if isinstance(v, str):
            keep[k] = v[:800]
        elif v is not None:
            keep[k] = v
    keep.update({"source": rec.source, "sent": rec.sent.isoformat() if rec.sent else None, "sender": rec.sender})
    return keep


async def _load(conn, hazard_id) -> asyncpg.Record:
    return await conn.fetchrow(
        "SELECT id, title, severity, certainty, urgency, onset, expires, located_by, area_desc, attrs, status FROM hazard WHERE id=$1",
        hazard_id,
    )


def _later(a: datetime | None, b: datetime | None):
    vals = [v for v in (a, b) if v is not None]
    return max(vals) if vals else None


def _earlier(a: datetime | None, b: datetime | None):
    vals = [v for v in (a, b) if v is not None]
    return min(vals) if vals else None


async def _write(conn, hazard_id, rec: HazardRecord, cur, replace: bool) -> None:
    """replace=True: same agency updated its own message, take its values.
    replace=False: another agency's message about the same hazard; keep the most
    severe reading, the widest time window and the most precise footprint."""
    attrs = dict(cur["attrs"] or {})
    msgs = dict(attrs.get("messages") or {})
    msgs[f"{rec.source}:{rec.source_ref}"] = _message_attrs(rec)
    attrs["messages"] = msgs

    new_rank = LOCATION_RANK.get(rec.located_by, 0)
    cur_rank = LOCATION_RANK.get(cur["located_by"], 0)
    take_geom = rec.geom is not None and (replace or new_rank > cur_rank)

    if replace:
        title, severity, certainty = rec.title, rec.severity, rec.certainty
        urgency, onset, expires = rec.urgency or cur["urgency"], rec.onset or cur["onset"], rec.expires or cur["expires"]
    else:
        title = rec.title if rec.severity > cur["severity"] else cur["title"]
        severity = max(rec.severity, cur["severity"])
        certainty = max(rec.certainty, cur["certainty"])
        urgency = cur["urgency"] or rec.urgency
        onset = _earlier(cur["onset"], rec.onset)
        expires = _later(cur["expires"], rec.expires)

    await conn.execute(
        f"""UPDATE hazard SET
              title=$2, severity=$3, certainty=$4, urgency=$5, onset=$6, expires=$7,
              geom = CASE WHEN $8 THEN {GEOM_SQL} ELSE geom END,
              located_by = CASE WHEN $8 THEN $9 ELSE located_by END,
              area_desc = COALESCE($10, area_desc),
              status='active', last_updated=now(), attrs=$11
            WHERE id=$12""",
        _wkb(rec.geom) if take_geom else None,
        title, severity, certainty, urgency, onset, expires,
        take_geom, rec.located_by, rec.area_desc, attrs, hazard_id,
    )


async def _add_source(conn, hazard_id, rec: HazardRecord) -> None:
    await conn.execute(
        """INSERT INTO hazard_source (hazard_id, source, source_ref, source_url, sender, sent, msg_type, raw_fetch_id)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
           ON CONFLICT (source, source_ref) DO UPDATE SET
             hazard_id=EXCLUDED.hazard_id, source_url=EXCLUDED.source_url, sent=EXCLUDED.sent,
             msg_type=EXCLUDED.msg_type, raw_fetch_id=EXCLUDED.raw_fetch_id""",
        hazard_id, rec.source, rec.source_ref, rec.source_url, rec.sender, rec.sent, rec.msg_type, rec.raw_fetch_id,
    )


async def upsert_hazard(pool: asyncpg.Pool, rec: HazardRecord) -> tuple[str | None, str]:
    """Returns (hazard_id, action) where action is created | merged | updated | cancelled | unchanged | ignored."""
    async with pool.acquire() as conn, conn.transaction():
        own = await conn.fetchrow(
            "SELECT hazard_id, sent FROM hazard_source WHERE source=$1 AND source_ref=$2", rec.source, rec.source_ref
        )
        if own:
            hid = own["hazard_id"]
            if rec.msg_type == "Cancel":
                await conn.execute("UPDATE hazard SET status='cancelled', last_updated=now() WHERE id=$1", hid)
                await _add_source(conn, hid, rec)
                return str(hid), "cancelled"
            if own["sent"] is not None and rec.sent is not None and rec.sent <= own["sent"]:
                await conn.execute(
                    "UPDATE hazard_source SET raw_fetch_id=$3 WHERE source=$1 AND source_ref=$2",
                    rec.source, rec.source_ref, rec.raw_fetch_id,
                )
                return str(hid), "unchanged"
            await _write(conn, hid, rec, await _load(conn, hid), replace=True)
            await _add_source(conn, hid, rec)
            return str(hid), "updated"

        if rec.references:
            ref = await conn.fetchrow(
                "SELECT hazard_id FROM hazard_source WHERE source_ref = ANY($1::text[]) LIMIT 1", rec.references
            )
            if ref:
                hid = ref["hazard_id"]
                if rec.msg_type == "Cancel":
                    await conn.execute("UPDATE hazard SET status='cancelled', last_updated=now() WHERE id=$1", hid)
                    await _add_source(conn, hid, rec)
                    return str(hid), "cancelled"
                await _write(conn, hid, rec, await _load(conn, hid), replace=True)
                await _add_source(conn, hid, rec)
                return str(hid), "updated"

        if rec.msg_type == "Cancel":
            return None, "ignored"

        rows = await conn.fetch(
            """SELECT id, kind, ST_AsBinary(geom) AS wkb, area_desc, onset, expires FROM hazard
               WHERE status='active' AND kind=$1
                 AND (geom IS NULL OR $2::bytea IS NULL
                      OR ST_Intersects(geom, ST_SetSRID(ST_GeomFromWKB($2::bytea), 4326)))""",
            rec.kind, _wkb(rec.geom),
        )
        cands = [
            Candidate(str(r["id"]), r["kind"], shp_wkb.loads(bytes(r["wkb"])) if r["wkb"] else None, r["area_desc"], r["onset"], r["expires"])
            for r in rows
        ]
        match = best_match(rec.kind, rec.geom, rec.area_desc, rec.onset, rec.expires, cands)
        if match:
            await _write(conn, match, rec, await _load(conn, match), replace=False)
            await _add_source(conn, match, rec)
            return match, "merged"

        attrs = {"messages": {f"{rec.source}:{rec.source_ref}": _message_attrs(rec)}}
        hid = await conn.fetchval(
            f"""INSERT INTO hazard (kind, title, severity, certainty, urgency, onset, expires, geom, area_desc, located_by, attrs)
                VALUES ($2,$3,$4,$5,$6,$7,$8,{GEOM_SQL},$9,$10,$11) RETURNING id""",
            _wkb(rec.geom), rec.kind, rec.title, rec.severity, rec.certainty, rec.urgency, rec.onset, rec.expires,
            rec.area_desc, rec.located_by, attrs,
        )
        await _add_source(conn, hid, rec)
        return str(hid), "created"


async def expire_old(pool: asyncpg.Pool) -> list[str]:
    rows = await pool.fetch(
        """UPDATE hazard SET status='expired', last_updated=now()
           WHERE status='active' AND expires IS NOT NULL AND expires < now() - interval '1 hour'
           RETURNING id"""
    )
    return [str(r["id"]) for r in rows]


async def mark_source(pool: asyncpg.Pool, source: str, ok: bool, raw_id: int | None, items: int | None, error: str | None) -> None:
    if ok:
        await pool.execute(
            """INSERT INTO source_status (source, last_attempt, last_success, last_error, consecutive_failures, last_raw_fetch_id, items_last_run)
               VALUES ($1, now(), now(), $4, 0, $2, $3)
               ON CONFLICT (source) DO UPDATE SET last_attempt=now(), last_success=now(), last_error=$4,
                 consecutive_failures=0, last_raw_fetch_id=$2, items_last_run=$3""",
            source, raw_id, items, error,
        )
    else:
        await pool.execute(
            """INSERT INTO source_status (source, last_attempt, last_error, consecutive_failures, last_raw_fetch_id)
               VALUES ($1, now(), $3, 1, $2)
               ON CONFLICT (source) DO UPDATE SET last_attempt=now(), last_error=$3,
                 consecutive_failures=source_status.consecutive_failures+1, last_raw_fetch_id=$2""",
            source, raw_id, error,
        )
