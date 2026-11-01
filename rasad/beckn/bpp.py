"""BPP side: serves RASAD's supplier network on the Beckn network.

Handles search, select, init, confirm, status and cancel. Each call is ACKed at once and
answered asynchronously with the matching on_* callback to the BAP, as Beckn requires.
A confirmed order reserves the stock and waits for the supplier to ACCEPT or REJECT it
(over WhatsApp or the supplier page); the decision goes back to the BAP as an
unsolicited on_status or on_cancel.
"""

from __future__ import annotations

import asyncio
import logging
import math

import asyncpg

from .. import messaging
from ..config import get_settings
from ..suppliers import ITEMS, describe
from .node import context, identity, send

log = logging.getLogger(__name__)
_tasks: set[asyncio.Task] = set()


def spawn(coro) -> None:
    t = asyncio.create_task(coro)
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)


def _end_gps(order_or_intent: dict) -> tuple[float, float] | None:
    fl = order_or_intent.get("fulfillment") or (order_or_intent.get("fulfillments") or [{}])[0]
    for stop in fl.get("stops") or []:
        if stop.get("type") == "end":
            gps = (stop.get("location") or {}).get("gps")
            if gps:
                lat, lon = (float(x) for x in gps.split(","))
                return lat, lon
    return None


def _price(v: float | None) -> dict | None:
    return None if v is None else {"currency": "INR", "value": f"{v:.4f}"}


def _item_json(supplier_id: int, item: str, available: float, unit_price: float | None, updated_at, count: float | None = None) -> dict:
    meta = ITEMS[item]
    j = {
        "id": f"{supplier_id}:{item}",
        "descriptor": {"code": item, "name": meta["name"]},
        "quantity": {"available": {"count": math.floor(available), "measure": {"unit": meta["unit"], "value": "1"}}},
        "location_ids": [f"{supplier_id}:loc"],
        "time": {"timestamp": updated_at.isoformat()},
        "tags": [{"descriptor": {"code": "stock"}, "list": [
            {"descriptor": {"code": "price_stated"}, "value": "true" if unit_price is not None else "false"}]}],
    }
    if unit_price is not None:
        j["price"] = _price(unit_price)
    if count is not None:
        j["quantity"]["selected"] = {"count": count}
    return j


async def handle(pool: asyncpg.Pool, action: str, body: dict) -> None:
    """Called after the ACK has gone back; computes and sends the on_* callback."""
    try:
        fn = {"search": on_search, "select": on_select, "init": on_init, "confirm": on_confirm,
              "status": on_status, "cancel": on_cancel}.get(action)
        if fn:
            await fn(pool, body)
    except Exception:
        log.exception("BPP %s handling failed", action)


async def _reply(pool, body: dict, action: str, message: dict, error: dict | None = None) -> None:
    ident = await identity(pool)
    ctx = context(ident, action, body["context"]["transaction_id"], base=body["context"])
    out = {"context": ctx, "message": message}
    if error:
        out["error"] = error
    await send(pool, "BPP", body["context"]["bap_uri"], action, out, peer=body["context"].get("bap_id"))


async def on_search(pool, body):
    s = get_settings()
    intent = (body.get("message") or {}).get("intent") or {}
    code = ((intent.get("item") or {}).get("descriptor") or {}).get("code")
    end = _end_gps(intent)
    if code not in ITEMS or end is None:
        await _reply(pool, body, "on_search", {"catalog": {"descriptor": {"name": "RASAD supplier network"}, "providers": []}},
                     {"code": "40000", "message": "search needs item.descriptor.code and an end stop gps"})
        return
    lat, lon = end
    rows = await pool.fetch(
        """SELECT s.id, s.name, s.kind, ST_Y(s.geom) lat, ST_X(s.geom) lon, i.available - i.reserved AS free, i.unit_price, i.updated_at,
                  ST_Distance(s.geom::geography, ST_SetSRID(ST_MakePoint($2,$3),4326)::geography) / 1000 AS km
           FROM supplier s JOIN supplier_item i ON i.supplier_id = s.id
           WHERE s.active AND i.item = $1 AND i.available - i.reserved >= 1
             AND i.updated_at > now() - make_interval(hours => $4)
             AND ST_DWithin(s.geom::geography, ST_SetSRID(ST_MakePoint($2,$3),4326)::geography, s.serves_km * 1000)
           ORDER BY km LIMIT 25""",
        code, lon, lat, s.stock_fresh_hours,
    )
    providers = [{
        "id": str(r["id"]),
        "descriptor": {"name": r["name"], "short_desc": r["kind"]},
        "locations": [{"id": f"{r['id']}:loc", "gps": f"{r['lat']:.6f},{r['lon']:.6f}"}],
        "items": [_item_json(r["id"], code, r["free"], r["unit_price"], r["updated_at"])],
        "tags": [{"descriptor": {"code": "distance"}, "list": [{"descriptor": {"code": "km"}, "value": f"{r['km']:.1f}"}]}],
    } for r in rows]
    await _reply(pool, body, "on_search", {"catalog": {"descriptor": {"name": "RASAD supplier network"}, "providers": providers}})


async def _lookup(pool, order: dict):
    pid = int((order.get("provider") or {}).get("id"))
    it = (order.get("items") or [{}])[0]
    item = it["id"].split(":", 1)[1]
    count = float(((it.get("quantity") or {}).get("selected") or {}).get("count") or 0)
    row = await pool.fetchrow(
        """SELECT s.id, s.name, s.phone, ST_Y(s.geom) lat, ST_X(s.geom) lon, i.available - i.reserved AS free, i.unit_price, i.updated_at
           FROM supplier s JOIN supplier_item i ON i.supplier_id=s.id WHERE s.id=$1 AND i.item=$2 AND s.active""",
        pid, item,
    )
    return row, item, count


def _quote(row, item, count) -> dict:
    total = None if row["unit_price"] is None else row["unit_price"] * count
    q = {"breakup": [{"title": ITEMS[item]["name"], "item": {"id": f"{row['id']}:{item}", "quantity": {"selected": {"count": count}}},
                      "price": _price(total) or {"currency": "INR", "value": "unstated"}}]}
    q["price"] = _price(total) or {"currency": "INR", "value": "unstated"}
    return q


async def _order_reply(pool, body, action, extra: dict | None = None):
    order = (body.get("message") or {}).get("order") or {}
    row, item, count = await _lookup(pool, order)
    if row is None:
        await _reply(pool, body, action, {"order": order}, {"code": "30004", "message": "item not found"})
        return None
    if count > row["free"]:
        await _reply(pool, body, action, {"order": {**order, "items": [_item_json(row["id"], item, row["free"], row["unit_price"], row["updated_at"], count=math.floor(row["free"]))]}},
                     {"code": "40002", "message": f"only {math.floor(row['free'])} available"})
        return None
    msg = {"order": {**order, "provider": {"id": str(row["id"]), "descriptor": {"name": row["name"]}},
                     "items": [_item_json(row["id"], item, row["free"], row["unit_price"], row["updated_at"], count=count)],
                     "quote": _quote(row, item, count), **(extra or {})}}
    await _reply(pool, body, action, msg)
    return row, item, count


async def on_select(pool, body):
    await _order_reply(pool, body, "on_select")


async def on_init(pool, body):
    await _order_reply(pool, body, "on_init", {"payments": [{"type": "ON-FULFILLMENT", "status": "NOT-PAID", "collected_by": "BPP"}]})


async def on_confirm(pool, body):
    order = (body.get("message") or {}).get("order") or {}
    ctx = body["context"]
    row, item, count = await _lookup(pool, order)
    existing = await pool.fetchrow("SELECT id, status FROM bpp_order WHERE transaction_id=$1", ctx["transaction_id"])
    if existing is None:
        if row is None or count > row["free"]:
            await _reply(pool, body, "on_confirm", {"order": order}, {"code": "40002", "message": "stock no longer available"})
            return
        end = _end_gps(order)
        deliver_to = {"gps": f"{end[0]},{end[1]}" if end else None,
                      "name": ((order.get("billing") or {}).get("name")),
                      "contact": ((order.get("billing") or {}).get("phone")),
                      "instructions": _instructions(order)}
        async with pool.acquire() as conn, conn.transaction():
            oid = await conn.fetchval(
                """INSERT INTO bpp_order (transaction_id, bap_id, bap_uri, supplier_id, item, quantity, unit_price, deliver_to, status)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'Created') RETURNING id""",
                ctx["transaction_id"], ctx["bap_id"], ctx["bap_uri"], row["id"], item, count, row["unit_price"], deliver_to,
            )
            await conn.execute("UPDATE supplier_item SET reserved = reserved + $3 WHERE supplier_id=$1 AND item=$2", row["id"], item, count)
        await messaging.send(pool, row["id"], row["phone"], _order_text(oid, item, count, deliver_to))
        existing = {"id": oid, "status": "Created"}
    await _reply(pool, body, "on_confirm", {"order": {**order, "id": str(existing["id"]), "status": existing["status"],
                                                      "fulfillments": [{**(order.get("fulfillments") or [{}])[0],
                                                                        "state": {"descriptor": {"code": "AWAITING_SUPPLIER"}}}]}})


def _instructions(order) -> str | None:
    for fl in order.get("fulfillments") or []:
        for st in fl.get("stops") or []:
            if st.get("type") == "end" and st.get("instructions"):
                return (st["instructions"] or {}).get("short_desc") or (st["instructions"] or {}).get("name")
    return None


def _order_text(oid, item, count, deliver_to) -> str:
    s = get_settings()
    where = deliver_to.get("name") or "relief shelter"
    gps = deliver_to.get("gps")
    maps = f"https://www.google.com/maps?q={gps}" if gps else ""
    note = f"\n{deliver_to['instructions']}" if deliver_to.get("instructions") else ""
    return (f"RASAD relief order #{oid}: {describe(item, count)} for {where}.{note}\n"
            f"A RASAD driver will collect it from you and quote order #{oid}. Destination: {maps}\n"
            f"Reply ACCEPT {oid} or REJECT {oid} <reason>. Reply READY {oid} when it is packed.\n"
            f"Supplier page: {s.public_url.rstrip('/')}/supplier")


async def _order_state(pool, oid: int) -> dict | None:
    return await pool.fetchrow("SELECT * FROM bpp_order WHERE id=$1", oid)


def _order_json(o) -> dict:
    code = {"Created": "AWAITING_SUPPLIER", "Accepted": "ACCEPTED", "Packed": "PACKED", "Cancelled": "CANCELLED"}[o["status"]]
    return {"id": str(o["id"]), "status": o["status"], "provider": {"id": str(o["supplier_id"])},
            "items": [{"id": f"{o['supplier_id']}:{o['item']}", "quantity": {"selected": {"count": o["quantity"]}}}],
            "fulfillments": [{"state": {"descriptor": {"code": code, "short_desc": o["reason"] or ""}}}]}


async def on_status(pool, body):
    oid = ((body.get("message") or {}).get("order_id"))
    o = await pool.fetchrow("SELECT * FROM bpp_order WHERE id=$1 AND transaction_id=$2", int(oid or 0), body["context"]["transaction_id"])
    if o is None:
        await _reply(pool, body, "on_status", {}, {"code": "30004", "message": "order not found"})
        return
    await _reply(pool, body, "on_status", {"order": _order_json(o)})


async def on_cancel(pool, body):
    msg = body.get("message") or {}
    o = await pool.fetchrow("SELECT * FROM bpp_order WHERE id=$1 AND transaction_id=$2", int(msg.get("order_id") or 0), body["context"]["transaction_id"])
    if o is None:
        await _reply(pool, body, "on_cancel", {}, {"code": "30004", "message": "order not found"})
        return
    if o["status"] in ("Created", "Accepted"):
        await _cancel(pool, o, msg.get("descriptor", {}).get("short_desc") or "cancelled by buyer")
        o = await _order_state(pool, o["id"])
        sup = await pool.fetchrow("SELECT phone FROM supplier WHERE id=$1", o["supplier_id"])
        await messaging.send(pool, o["supplier_id"], sup["phone"], f"RASAD order #{o['id']} was cancelled by the buyer: {o['reason']}. No delivery needed.")
    await _reply(pool, body, "on_cancel", {"order": _order_json(o)})


async def _cancel(pool, o, reason: str) -> None:
    async with pool.acquire() as conn, conn.transaction():
        n = await conn.execute(
            "UPDATE bpp_order SET status='Cancelled', reason=$2, updated_at=now() WHERE id=$1 AND status IN ('Created','Accepted')", o["id"], reason)
        if n.endswith("1"):
            await conn.execute("UPDATE supplier_item SET reserved = GREATEST(0, reserved - $3) WHERE supplier_id=$1 AND item=$2",
                               o["supplier_id"], o["item"], o["quantity"])


async def supplier_decision(pool: asyncpg.Pool, supplier_id: int, order_id: int, decision: str, reason: str | None = None) -> str:
    """The supplier accepts, rejects or marks an order ready. Returns a reply for the supplier."""
    o = await pool.fetchrow("SELECT * FROM bpp_order WHERE id=$1 AND supplier_id=$2", order_id, supplier_id)
    if o is None:
        return f"Order #{order_id} not found for your account."
    if decision == "accept":
        if o["status"] != "Created":
            return f"Order #{order_id} is already {o['status'].lower()}."
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute("UPDATE bpp_order SET status='Accepted', updated_at=now() WHERE id=$1", order_id)
            # Accepted stock leaves the shelf: move it from reserved out of available.
            await conn.execute("""UPDATE supplier_item SET available = GREATEST(0, available - $3), reserved = GREATEST(0, reserved - $3)
                                  WHERE supplier_id=$1 AND item=$2""", supplier_id, o["item"], o["quantity"])
        reply = f"Thanks. Order #{order_id} accepted. A driver will be assigned; reply READY {order_id} when it is packed."
    elif decision == "reject":
        if o["status"] not in ("Created", "Accepted"):
            return f"Order #{order_id} is already {o['status'].lower()}."
        if o["status"] == "Accepted":   # put accepted stock back on the shelf before releasing
            await pool.execute("UPDATE supplier_item SET available = available + $3, reserved = reserved + $3 WHERE supplier_id=$1 AND item=$2",
                               supplier_id, o["item"], o["quantity"])
        await _cancel(pool, o, f"rejected by supplier: {reason or 'no reason given'}")
        reply = f"Order #{order_id} rejected. RASAD will source it elsewhere."
    elif decision == "ready":
        if o["status"] != "Accepted":
            return f"Order #{order_id} must be accepted before it can be marked ready."
        await pool.execute("UPDATE bpp_order SET status='Packed', updated_at=now() WHERE id=$1", order_id)
        reply = f"Order #{order_id} marked packed. Thank you."
    else:
        return "Unknown decision."
    spawn(_push_update(pool, order_id))
    return reply


async def _push_update(pool, order_id: int) -> None:
    """Unsolicited on_status (or on_cancel) to the BAP after a supplier decision."""
    o = await _order_state(pool, order_id)
    ident = await identity(pool)
    action = "on_cancel" if o["status"] == "Cancelled" else "on_status"
    ctx = context(ident, action, o["transaction_id"])
    ctx.update({"bap_id": o["bap_id"], "bap_uri": o["bap_uri"], "bpp_id": ident.subscriber_id, "bpp_uri": ident.bpp_uri})
    await send(pool, "BPP", o["bap_uri"], action, {"context": ctx, "message": {"order": _order_json(o)}}, peer=o["bap_id"])
