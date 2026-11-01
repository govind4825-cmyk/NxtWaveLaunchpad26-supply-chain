"""HTTP endpoints for both Beckn roles of this node.

/beckn/bpp/{search,select,init,confirm,status,cancel}   requests from any BAP
/beckn/bap/{on_search,on_select,...}                    callbacks from any BPP

Every request is signature-checked against the peer registry (RASAD_BECKN_VERIFY) and
logged in beckn_message before it is ACKed.
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from ..config import get_settings
from ..events import publish
from . import bpp
from .node import ack, check_signature, log_message, nack

log = logging.getLogger(__name__)
router = APIRouter()

BPP_ACTIONS = {"search", "select", "init", "confirm", "status", "cancel"}
BAP_ACTIONS = {"on_search", "on_select", "on_init", "on_confirm", "on_status", "on_cancel", "on_update"}


async def _receive(request: Request, role: str, action: str):
    pool = request.app.state.pool
    raw = await request.body()
    try:
        body = json.loads(raw)
        assert isinstance(body, dict) and isinstance(body.get("context"), dict)
    except (ValueError, AssertionError):
        await log_message(pool, "in", role, action, {"raw": raw[:2000].decode(errors="replace")}, error="malformed body")
        return None, JSONResponse(nack("10000", "malformed request body"), status_code=400)
    ok, err, signer = await check_signature(pool, raw, request.headers.get("Authorization"))
    if not ok and get_settings().beckn_verify:
        await log_message(pool, "in", role, action, body, peer=signer, signature_ok=False, ack="NACK", error=err)
        return None, JSONResponse(nack("10001", f"invalid signature: {err}"), status_code=401)
    await log_message(pool, "in", role, action, body, peer=signer, signature_ok=ok, ack="ACK", error=None if ok else err)
    return body, None


@router.post("/beckn/bpp/{action}")
async def bpp_entry(action: str, request: Request):
    if action not in BPP_ACTIONS:
        return JSONResponse(nack("10002", f"unsupported action {action}"), status_code=404)
    body, err = await _receive(request, "BPP", action)
    if err:
        return err
    bpp.spawn(bpp.handle(request.app.state.pool, action, body))
    return ack()


@router.post("/beckn/bap/{action}")
async def bap_entry(action: str, request: Request):
    if action not in BAP_ACTIONS:
        return JSONResponse(nack("10002", f"unsupported action {action}"), status_code=404)
    body, err = await _receive(request, "BAP", action)
    if err:
        return err
    if action in ("on_status", "on_cancel", "on_update"):
        bpp.spawn(apply_order_update(request.app.state.pool, body))
    return ack()


async def apply_order_update(pool, body: dict) -> None:
    """A BPP reports an order state change: update the procurement and the need it fills."""
    order = (body.get("message") or {}).get("order") or {}
    txn = (body.get("context") or {}).get("transaction_id")
    if not order or not txn:
        return
    status = (order.get("status") or "").lower()
    state = (((order.get("fulfillments") or [{}])[0].get("state") or {}).get("descriptor") or {})
    new = {"accepted": "accepted", "packed": "packed", "cancelled": "rejected"}.get(status)
    if state.get("code") == "PACKED":
        new = "packed"
    if new is None:
        return
    async with pool.acquire() as conn, conn.transaction():
        p = await conn.fetchrow("SELECT * FROM procurement WHERE transaction_id=$1 FOR UPDATE", txn)
        if p is None or p["status"] == new:
            return
        was_committed = p["status"] in ("accepted", "packed", "in_transit", "delivered")
        now_committed = new in ("accepted", "packed")
        note = state.get("short_desc") or None
        if p["status"] == "cancelled" and new == "rejected":
            return   # we cancelled it ourselves; the BPP is just confirming
        if p["status"] in ("in_transit", "delivered", "failed"):
            return   # our driver already collected it (or it failed on collection); late BPP updates don't move it back
        await conn.execute("UPDATE procurement SET status=$2, note=COALESCE($3, note), updated_at=now() WHERE id=$1", p["id"], new, note)
        delta = (p["quantity"] if now_committed else 0) - (p["quantity"] if was_committed else 0)
        if delta:
            await conn.execute("UPDATE need SET committed = GREATEST(0, committed + $3) WHERE shelter_id=$1 AND item=$2",
                               p["shelter_id"], p["item"], delta)
    await publish(pool, "procurement.updated", {"procurement_id": p["id"], "shelter_id": p["shelter_id"], "item": p["item"],
                                                "status": new, "previous": p["status"], "provider": p["provider_name"],
                                                "quantity": p["quantity"], "note": note})
