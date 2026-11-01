"""One entry point for everything a supplier sends: WhatsApp text or the supplier page."""

from __future__ import annotations

import asyncpg

from .beckn.bpp import supplier_decision
from .events import publish
from .suppliers import HELP, ITEMS, describe, normalise_phone, parse, set_stock


async def handle_text(pool: asyncpg.Pool, supplier_id: int | None, phone: str | None, text: str, channel: str) -> str:
    phone = normalise_phone(phone)
    if supplier_id is None and phone:
        supplier_id = await pool.fetchval("SELECT id FROM supplier WHERE phone=$1 AND active", phone)
    await pool.execute(
        "INSERT INTO supplier_message (supplier_id, phone, direction, channel, text) VALUES ($1,$2,'in',$3,$4)",
        supplier_id, phone, channel, text[:2000])
    if supplier_id is None:
        return "This number is not registered as a RASAD supplier. Please contact the district control room."
    cmd = parse(text)
    if cmd.cmd == "help":
        return HELP
    if cmd.cmd == "unknown":
        return f"Sorry, {cmd.error}.\n{HELP}"
    if cmd.cmd == "stock":
        await set_stock(pool, supplier_id, cmd.item, cmd.quantity, cmd.unit_price, channel)
        await publish(pool, "supplier.stock", {"supplier_id": supplier_id, "item": cmd.item, "available": cmd.quantity,
                                               "unit_price": cmd.unit_price, "via": channel})
        price = "" if cmd.unit_price is None else (
            f" at Rs {cmd.unit_price * 1000:,.2f}/kg" if ITEMS[cmd.item]["unit"] == "g" else f" at Rs {cmd.unit_price:,.2f}/{ITEMS[cmd.item]['unit'].rstrip('s')}")
        return f"Stock updated: {describe(cmd.item, cmd.quantity)}{price}."
    return await supplier_decision(pool, supplier_id, cmd.order_id, cmd.cmd, cmd.reason)
