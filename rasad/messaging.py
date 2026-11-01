"""Messages to suppliers and drivers: WhatsApp Cloud API when configured, otherwise the outbox.

Every message, sent or not, is stored in supplier_message, and the supplier page shows the
outbox, so a demo works before WhatsApp Business approval comes through.
"""

from __future__ import annotations

import hashlib
import hmac
import logging

import asyncpg
import httpx

from .config import get_settings

log = logging.getLogger(__name__)


async def send(pool: asyncpg.Pool, supplier_id: int | None, phone: str | None, text: str, driver_id: int | None = None) -> bool:
    s = get_settings()
    delivered, err, channel = None, None, "outbox"
    if s.whatsapp_token and s.whatsapp_phone_id and phone:
        channel = "whatsapp"
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.post(
                    f"{s.whatsapp_api}/{s.whatsapp_phone_id}/messages",
                    headers={"Authorization": f"Bearer {s.whatsapp_token}"},
                    json={"messaging_product": "whatsapp", "to": phone, "type": "text", "text": {"body": text[:4000]}},
                )
            delivered = r.status_code == 200
            if not delivered:
                err = f"HTTP {r.status_code}: {r.text[:300]}"
        except httpx.HTTPError as exc:
            delivered, err = False, f"{type(exc).__name__}: {exc}"
    await pool.execute(
        "INSERT INTO supplier_message (supplier_id, driver_id, phone, direction, channel, text, delivered, error) VALUES ($1,$2,$3,'out',$4,$5,$6,$7)",
        supplier_id, driver_id, phone, channel, text, delivered, err,
    )
    if err:
        log.warning("whatsapp send failed: %s", err)
    return bool(delivered) if channel == "whatsapp" else True


def verify_webhook_signature(raw: bytes, header: str | None) -> bool:
    """Meta signs webhook bodies with the app secret (X-Hub-Signature-256: sha256=<hex>)."""
    secret = get_settings().whatsapp_app_secret
    if not secret:
        return True
    if not header or not header.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header[7:])


def inbound_texts(payload: dict) -> list[tuple[str, str]]:
    """(phone, text) pairs from a WhatsApp Cloud API webhook payload."""
    out = []
    for entry in payload.get("entry") or []:
        for ch in entry.get("changes") or []:
            for m in (ch.get("value") or {}).get("messages") or []:
                if m.get("type") == "text" and m.get("from"):
                    out.append((m["from"], (m.get("text") or {}).get("body", "")))
    return out
