"""Drivers: registration, what their phone page shows, stop actions, and WhatsApp commands.

A driver needs nothing but a phone. The trip arrives as a WhatsApp message (or on the
outbox of their page at /d/<token>), and they answer with short commands:

    START                 begin the trip
    DONE 2                stop 2 finished (picked up or delivered)
    FAIL 2 no stock       stop 2 could not be done, with the reason
    BLOCKED near bridge   the road ahead is shut (placed at their live GPS position)
    STATUS                resend the remaining stops
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import asyncpg

from . import closures, messaging
from .config import get_settings
from .events import publish
from .suppliers import ITEMS, friendly_qty, new_token, normalise_phone

IST = ZoneInfo("Asia/Kolkata")
ACTIVE = ("planned", "en_route")
EPS = 1e-6

# Latest phone position for a driver row aliased "d" (Traccar Client app sends it).
POS_JOIN = """LEFT JOIN LATERAL (
    SELECT vp.device_id, ST_X(vp.geom) AS pos_lon, ST_Y(vp.geom) AS pos_lat, vp.fix_time, vp.speed_kmh
    FROM vehicle v JOIN vehicle_position vp ON vp.device_id = v.device_id
    WHERE d.traccar_unique_id IS NOT NULL AND v.unique_id = d.traccar_unique_id
    ORDER BY vp.fix_time DESC LIMIT 1) pos ON true"""


def hm(dt: datetime | None) -> str:
    if dt is None:
        return "?"
    local = dt.astimezone(IST)
    if local.date() != datetime.now(IST).date():
        return local.strftime("%d %b %H:%M")
    return local.strftime("%H:%M")


def load_text(item: str, q: float) -> str:
    """'3,000 litres drinking water', '240 water containers (10-20 L)', '12.5 kg bathing soap'."""
    meta = ITEMS.get(item)
    if meta is None:
        return f"{q:g} {item}"
    name = meta["name"][0].lower() + meta["name"][1:]
    if meta["unit"] == "containers":
        return f"{q:,.0f} {name}"
    return f"{friendly_qty(item, q)} {name}"


async def register(pool: asyncpg.Pool, name: str, phone: str | None, capacity_kg: float, vehicle: str,
                   traccar_unique_id: str | None, depot_id: int | None) -> dict:
    phone = normalise_phone(phone) if phone else None
    row = await pool.fetchrow(
        """INSERT INTO driver (name, phone, vehicle, capacity_kg, traccar_unique_id, depot_id, token)
           VALUES ($1,$2,$3,$4,$5,$6,$7)
           ON CONFLICT (phone) DO UPDATE SET name=EXCLUDED.name, vehicle=EXCLUDED.vehicle, capacity_kg=EXCLUDED.capacity_kg,
               traccar_unique_id=EXCLUDED.traccar_unique_id, depot_id=EXCLUDED.depot_id, active=true
           RETURNING id, token""",
        name, phone, vehicle, capacity_kg, traccar_unique_id or None, depot_id, new_token())
    await publish(pool, "driver.updated", {"driver_id": row["id"], "name": name, "capacity_kg": capacity_kg})
    return {"id": row["id"], "token": row["token"], "link": f"{get_settings().public_url.rstrip('/')}/d/{row['token']}"}


async def by_token(pool: asyncpg.Pool, token: str):
    return await pool.fetchrow("SELECT * FROM driver WHERE token=$1 AND active", token)


async def by_phone(pool: asyncpg.Pool, phone: str):
    return await pool.fetchrow("SELECT * FROM driver WHERE phone=$1 AND active", normalise_phone(phone))


async def position(pool: asyncpg.Pool, driver_id: int, fresh_minutes: int | None = None):
    row = await pool.fetchrow(f"SELECT pos.* FROM driver d {POS_JOIN} WHERE d.id=$1", driver_id)
    if row is None or row["pos_lon"] is None:
        return None
    if fresh_minutes is not None and row["fix_time"] < datetime.now(UTC) - timedelta(minutes=fresh_minutes):
        return None
    return row


async def active_trip(pool: asyncpg.Pool, driver_id: int):
    return await pool.fetchrow(
        "SELECT * FROM trip WHERE driver_id=$1 AND status = ANY($2::text[]) ORDER BY id DESC LIMIT 1", driver_id, list(ACTIVE))


async def trip_detail(pool: asyncpg.Pool, trip_id: int) -> dict | None:
    t = await pool.fetchrow(
        """SELECT t.id, t.status, t.planned_km, t.planned_min, t.plan, t.created_at, t.updated_at, t.deviation_strikes,
                  ST_AsGeoJSON(t.geom)::json AS geom, d.id AS driver_id, d.name AS driver_name, d.vehicle, d.capacity_kg
           FROM trip t JOIN driver d ON d.id = t.driver_id WHERE t.id=$1""", trip_id)
    if t is None:
        return None
    stops = await pool.fetch(
        """SELECT ts.id, ts.seq, ts.kind, ts.procurement_id, ts.name, ST_Y(ts.geom) AS lat, ST_X(ts.geom) AS lon, ts.item,
                  ts.quantity, ts.eta, ts.due, ts.status, ts.done_at, ts.note, ts.chunk, p.unit, p.provider_name, s.name AS shelter_name
           FROM trip_stop ts JOIN procurement p ON p.id = ts.procurement_id JOIN shelter s ON s.id = p.shelter_id
           WHERE ts.trip_id=$1 ORDER BY ts.seq""", trip_id)
    out = dict(t)
    out["stops"] = [dict(r) | {"item_name": ITEMS.get(r["item"], {}).get("name", r["item"]),
                               "qty_text": friendly_qty(r["item"], r["quantity"]) if r["item"] in ITEMS else f"{r['quantity']:g}",
                               "load_text": load_text(r["item"], r["quantity"])}
                    for r in stops]
    return out


def trip_text(detail: dict, token: str, header: str) -> str:
    lines = [header]
    for st in detail["stops"]:
        if st["status"] not in ("pending", "blocked"):
            continue
        if st["kind"] == "pickup":
            line = f"{st['seq']}. PICK UP {st['load_text']} at {st['name']} (order #{st['procurement_id']}), ETA {hm(st['eta'])}"
        else:
            line = f"{st['seq']}. DROP {st['load_text']} at {st['name']}, ETA {hm(st['eta'])}"
            if st["due"]:
                line += f", needed by {hm(st['due'])}"
        if st["status"] == "blocked":
            line += f". ON HOLD: {st['note'] or 'no safe road yet'}"
        lines.append(line)
    if len(lines) == 1:
        lines.append("No stops left. Thank you.")
    lines.append(f"Map and buttons: {get_settings().public_url.rstrip('/')}/d/{token}")
    lines.append("Reply DONE <stop no.>, FAIL <stop no.> <reason>, or BLOCKED <where> if a road is shut.")
    return "\n".join(lines)


async def notify(pool: asyncpg.Pool, driver, text: str) -> None:
    await messaging.send(pool, None, driver["phone"], text, driver_id=driver["id"])


async def finish_if_complete(pool: asyncpg.Pool, trip_id: int) -> str | None:
    r = await pool.fetchrow(
        """SELECT count(*) FILTER (WHERE status IN ('pending','blocked')) AS open,
                  count(*) FILTER (WHERE status='done') AS done FROM trip_stop WHERE trip_id=$1""", trip_id)
    if r["open"]:
        return None
    status = "done" if r["done"] else "cancelled"
    n = await pool.execute("UPDATE trip SET status=$2, updated_at=now() WHERE id=$1 AND status = ANY($3::text[])",
                           trip_id, status, list(ACTIVE))
    if n.endswith("1"):
        await publish(pool, "trip.updated", {"trip_id": trip_id, "status": status})
        return status
    return None


async def start(pool: asyncpg.Pool, driver) -> tuple[bool, str]:
    t = await active_trip(pool, driver["id"])
    if t is None:
        return False, "You have no trip right now. We will message you when there is one."
    if t["status"] == "planned":
        await pool.execute("UPDATE trip SET status='en_route', last_progress_at=now(), updated_at=now() WHERE id=$1", t["id"])
        await publish(pool, "trip.updated", {"trip_id": t["id"], "status": "en_route", "driver": driver["name"]})
    return True, f"Trip #{t['id']} started. Drive safe."


async def act(pool: asyncpg.Pool, driver, stop_id: int, action: str, note: str | None = None,
              lat: float | None = None, lon: float | None = None, channel: str = "page") -> tuple[bool, str]:
    t = await active_trip(pool, driver["id"])
    if t is None:
        return False, "You have no active trip."
    if action == "blocked":
        return await report_blocked(pool, driver, note, lat, lon, channel)
    st = await pool.fetchrow("SELECT * FROM trip_stop WHERE id=$1 AND trip_id=$2", stop_id, t["id"])
    if st is None:
        return False, "That stop is not on your current trip."
    if st["status"] not in ("pending", "blocked"):
        return False, f"Stop {st['seq']} is already {st['status']}."
    p = await pool.fetchrow("SELECT * FROM procurement WHERE id=$1", st["procurement_id"])
    what = load_text(st["item"], st["quantity"])
    note = (note or "").strip()[:300] or None

    if action == "done":
        if st["kind"] == "drop":
            pick = await pool.fetchrow("SELECT seq, status FROM trip_stop WHERE trip_id=$1 AND chunk=$2 AND kind='pickup'", t["id"], st["chunk"])
            if pick and pick["status"] != "done":
                return False, f"Collect it first: stop {pick['seq']} (pick up) is not marked done."
        supplier_note = None
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute("UPDATE trip_stop SET status='done', done_at=now(), note=COALESCE($2, note) WHERE id=$1", st["id"], note)
            await conn.execute("UPDATE trip SET status='en_route', last_progress_at=now(), updated_at=now() WHERE id=$1", t["id"])
            if st["kind"] == "pickup":
                await conn.execute("UPDATE procurement SET status='in_transit', updated_at=now() WHERE id=$1 AND status IN ('accepted','packed')", p["id"])
                supplier_note = await conn.fetchrow(
                    """UPDATE bpp_order b SET status='Collected', updated_at=now() FROM supplier s
                       WHERE b.transaction_id=$1 AND s.id=b.supplier_id RETURNING b.id, s.id AS supplier_id, s.phone""",
                    p["transaction_id"])
                new_status = "in_transit"
            else:
                delivered = await conn.fetchval(
                    "SELECT COALESCE(sum(quantity),0) FROM trip_stop WHERE procurement_id=$1 AND kind='drop' AND status='done'", p["id"])
                new_status = "in_transit"
                if delivered >= p["quantity"] - EPS:
                    await conn.execute(
                        "UPDATE procurement SET status='delivered', delivered_at=now(), updated_at=now() WHERE id=$1 AND status IN ('accepted','packed','in_transit')",
                        p["id"])
                    new_status = "delivered"
        if supplier_note:
            await messaging.send(pool, supplier_note["supplier_id"], supplier_note["phone"],
                                 f"Order #{supplier_note['id']} collected by RASAD driver {driver['name']}. Thank you.")
        kind = "picked_up" if st["kind"] == "pickup" else "delivered"
        await publish(pool, "procurement.updated", {"procurement_id": p["id"], "shelter_id": p["shelter_id"], "item": p["item"],
                                                    "status": new_status, "provider": p["provider_name"]})
        await publish(pool, f"delivery.{kind}", {"trip_id": t["id"], "stop_id": st["id"], "procurement_id": p["id"], "driver": driver["name"],
                                                  "place": st["name"], "item": st["item"], "quantity": st["quantity"]})
        finished = await finish_if_complete(pool, t["id"])
        msg = f"{'Picked up' if st['kind'] == 'pickup' else 'Delivered'} {what} at {st['name']}."
        nxt = await pool.fetchrow("SELECT seq, kind, name FROM trip_stop WHERE trip_id=$1 AND status='pending' ORDER BY seq LIMIT 1", t["id"])
        if nxt:
            msg += f" Next: stop {nxt['seq']}, {'pick up at' if nxt['kind'] == 'pickup' else 'drop at'} {nxt['name']}."
        elif finished:
            msg += " Trip complete. Thank you."
        return True, msg

    if action == "failed":
        if not note:
            return False, "Say what went wrong, e.g. FAIL 2 shop closed."
        async with pool.acquire() as conn, conn.transaction():
            if st["kind"] == "pickup":
                # The supplier could not hand the goods over: this order is dead. Release what was not collected
                # so the Procurer buys it elsewhere; anything already collected still gets delivered.
                picked = await conn.fetchval(
                    "SELECT COALESCE(sum(quantity),0) FROM trip_stop WHERE procurement_id=$1 AND kind='pickup' AND status='done'", p["id"])
                await conn.execute(
                    """UPDATE trip_stop ts SET status='failed', note=$2 FROM trip t
                       WHERE ts.trip_id=t.id AND ts.procurement_id=$1 AND ts.status IN ('pending','blocked') AND t.status IN ('planned','en_route')
                         AND (ts.kind='pickup' OR ts.chunk IN (SELECT chunk FROM trip_stop WHERE procurement_id=$1 AND kind='pickup' AND status<>'done'))""",
                    p["id"], f"pick up failed: {note}")
                was = p["status"]
                await conn.execute("UPDATE procurement SET status='failed', note=$2, updated_at=now() WHERE id=$1", p["id"],
                                   f"collection failed ({note}); {friendly_qty(p['item'], picked) if p['item'] in ITEMS else picked} collected")
                if was in ("accepted", "packed", "in_transit"):
                    await conn.execute("UPDATE need SET committed = GREATEST(0, committed - $3) WHERE shelter_id=$1 AND item=$2",
                                       p["shelter_id"], p["item"], max(0.0, p["quantity"] - picked))
            else:
                await conn.execute("UPDATE trip_stop SET status='failed', note=$2 WHERE id=$1", st["id"], note)
        if st["kind"] == "pickup":
            await publish(pool, "procurement.updated", {"procurement_id": p["id"], "shelter_id": p["shelter_id"], "item": p["item"],
                                                        "status": "failed", "provider": p["provider_name"], "note": note})
        else:
            await publish(pool, "trip.issue", {"trip_id": t["id"], "stop_id": st["id"], "driver": driver["name"], "place": st["name"],
                                               "note": f"could not deliver {what}: {note}. Goods are still on the vehicle."})
        await publish(pool, "trip.updated", {"trip_id": t["id"], "reroute": True, "reason": f"stop {st['seq']} failed"})
        await finish_if_complete(pool, t["id"])
        return True, f"Noted: stop {st['seq']} could not be done ({note}). The control room has been told."

    return False, f"Unknown action {action}."


async def report_blocked(pool: asyncpg.Pool, driver, note: str | None, lat: float | None, lon: float | None,
                         channel: str) -> tuple[bool, str]:
    if lat is None or lon is None:
        pos = await position(pool, driver["id"], fresh_minutes=get_settings().gps_fresh_minutes)
        if pos is None:
            return False, ("We need your location to mark the road. Open your RASAD page and tap 'Road blocked', "
                           "or switch on the tracking app.")
        lat, lon = pos["pos_lat"], pos["pos_lon"]
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return False, "That location is not valid."
    res = await closures.add_report(pool, lat, lon, (note or "").strip()[:200] or "blocked, reported by driver",
                                    f"driver:{driver['id']}", channel)
    t = await active_trip(pool, driver["id"])
    if t:
        await publish(pool, "trip.updated", {"trip_id": t["id"], "reroute": True, "reason": "driver reported a blocked road"})
    return True, f"Thanks, road marked as blocked ({res['action'].replace('_', ' ')}). Finding you a new route now."


_DONE = re.compile(r"^\s*(?:done|picked|delivered|ok|d)\s*#?(\d+)\s*(.*)$", re.I)
_FAIL = re.compile(r"^\s*(?:fail|failed|f)\s*#?(\d+)\s*(.*)$", re.I)
_BLOCK = re.compile(r"^\s*(?:blocked|block|band|road\s+blocked)\b\s*(.*)$", re.I)


async def handle_text(pool: asyncpg.Pool, driver, text: str, channel: str) -> str:
    await pool.execute("INSERT INTO supplier_message (driver_id, phone, direction, channel, text) VALUES ($1,$2,'in',$3,$4)",
                       driver["id"], driver["phone"], channel, text[:2000])
    t = (text or "").strip()
    low = t.lower()
    if low in ("start", "s", "go", "chalo"):
        return (await start(pool, driver))[1]
    if low in ("status", "next", "route", "?", "help", "stops"):
        trip = await active_trip(pool, driver["id"])
        if trip is None:
            return "You have no trip right now."
        return trip_text(await trip_detail(pool, trip["id"]), driver["token"], f"Trip #{trip['id']}, remaining stops:")
    m = _BLOCK.match(t)
    if m:
        return (await report_blocked(pool, driver, m.group(1), None, None, channel))[1]
    m = _DONE.match(t) or _FAIL.match(t)
    if m:
        trip = await active_trip(pool, driver["id"])
        if trip is None:
            return "You have no active trip."
        sid = await pool.fetchval("SELECT id FROM trip_stop WHERE trip_id=$1 AND seq=$2", trip["id"], int(m.group(1)))
        if sid is None:
            return f"Trip #{trip['id']} has no stop {m.group(1)}. Reply STATUS to see your stops."
        action = "done" if _DONE.match(t) else "failed"
        return (await act(pool, driver, sid, action, m.group(2), channel=channel))[1]
    return "Commands: START, DONE <stop no.>, FAIL <stop no.> <reason>, BLOCKED <where>, STATUS."


async def record_page_position(pool: asyncpg.Pool, driver, lat: float, lon: float, speed_ms: float | None, heading: float | None) -> None:
    """GPS from the driver's own page, for phones without the tracking app. Stored like a Traccar fix
    under a device id no Traccar device uses (negative driver id)."""
    device_id = -int(driver["id"])
    unique_id = driver["traccar_unique_id"] or f"rasad-page-{driver['id']}"
    async with pool.acquire() as conn, conn.transaction():
        if not driver["traccar_unique_id"]:
            await conn.execute("UPDATE driver SET traccar_unique_id=$2 WHERE id=$1", driver["id"], unique_id)
        await conn.execute(
            """INSERT INTO vehicle (device_id, name, unique_id, status, last_update) VALUES ($1,$2,$3,'online', now())
               ON CONFLICT (device_id) DO UPDATE SET status='online', last_update=now(), unique_id=EXCLUDED.unique_id""",
            device_id, f"{driver['name']} (page)", unique_id)
        await conn.execute(
            """INSERT INTO vehicle_position (device_id, fix_time, server_time, geom, speed_kmh, course, attrs)
               VALUES ($1, now(), now(), ST_SetSRID(ST_MakePoint($2,$3),4326), $4, $5, '{"source": "driver_page"}')""",
            device_id, lon, lat, speed_ms * 3.6 if speed_ms is not None else None, heading)
    await publish(pool, "vehicle.position", {"device_id": device_id, "name": driver["name"], "lat": lat, "lon": lon, "source": "driver_page"})
