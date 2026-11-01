"""Supplier stock updates and order replies, from WhatsApp text or the supplier page.

Text grammar (case-insensitive, Hindi item words accepted):
  STOCK water 5000            -> 5,000 litres of drinking water
  stock rice 800 kg @ 42      -> 800 kg cereal at Rs 42 per kg
  stock sabun 30 kg @ 120     -> 30 kg (30,000 g) bathing soap at Rs 120 per kg
  stock balti 200 @ 90        -> 200 water containers at Rs 90 each
  ACCEPT 12 / REJECT 12 out of stock / READY 12
  HELP
"""

from __future__ import annotations

import re
import secrets
from dataclasses import dataclass

import asyncpg

ITEMS = {
    "drinking_water": {"name": "Drinking water", "unit": "litres", "msg_unit": "L", "factor": 1.0,
                       "aliases": ["water", "drinking water", "pani", "paani", "jal", "bottled water"]},
    "cereal_ration": {"name": "Cereal ration (rice / atta)", "unit": "kg", "msg_unit": "kg", "factor": 1.0,
                      "aliases": ["rice", "atta", "chawal", "cereal", "ration", "wheat", "flour", "grain"]},
    "bathing_soap": {"name": "Bathing soap", "unit": "g", "msg_unit": "kg", "factor": 1000.0,
                     "aliases": ["soap", "sabun", "saabun", "bathing soap"]},
    "water_containers": {"name": "Water containers (10-20 L)", "unit": "containers", "msg_unit": "pcs", "factor": 1.0,
                         "aliases": ["container", "containers", "bucket", "buckets", "jerrycan", "jerrycans", "can", "cans", "balti"]},
}
# Units a supplier may type, as a multiplier into the need unit of each item family.
UNIT_FACTORS = {
    "litres": {"l": 1, "ltr": 1, "ltrs": 1, "litre": 1, "litres": 1, "liter": 1, "liters": 1, "ml": 0.001},
    "kg": {"kg": 1, "kgs": 1, "g": 0.001, "gm": 0.001, "q": 100, "quintal": 100, "t": 1000, "tonne": 1000, "tonnes": 1000},
    "g": {"g": 1, "gm": 1, "kg": 1000, "kgs": 1000},
    "containers": {"pcs": 1, "pc": 1, "pieces": 1, "nos": 1, "no": 1},
}

HELP = ("RASAD supplier commands:\n"
        "STOCK <item> <quantity> [unit] [@ price per unit]  e.g. STOCK water 5000 @ 2, STOCK rice 800 kg @ 42\n"
        "ACCEPT <order no>  |  REJECT <order no> [reason]  |  READY <order no>\n"
        "Items: water, rice/atta, soap, containers.")


@dataclass
class Command:
    cmd: str                   # stock | accept | reject | ready | help | unknown
    item: str | None = None
    quantity: float | None = None      # in the need unit
    unit_price: float | None = None    # INR per need unit
    order_id: int | None = None
    reason: str | None = None
    error: str | None = None


def item_from_word(text: str) -> str | None:
    t = text.lower().strip()
    for code, meta in ITEMS.items():
        if t == code or t in meta["aliases"]:
            return code
    return None


_NUM = r"(\d+(?:[.,]\d+)?)"
_STOCK = re.compile(rf"^\s*stock\s+(?P<item>[a-z_ ]+?)\s+{_NUM}\s*(?P<unit>[a-z]+)?\s*(?:(?:@|rs\.?|₹|at)\s*{_NUM}\s*(?:/\s*(?P<punit>[a-z]+))?)?\s*$", re.I)
_ORDER = re.compile(r"^\s*(?P<cmd>accept|yes|reject|no|ready|packed)\s+#?(?P<id>\d+)\s*(?P<reason>.*)$", re.I)


def parse(text: str) -> Command:
    t = (text or "").strip()
    if not t:
        return Command("unknown", error="empty message")
    if t.lower() in ("help", "?", "menu"):
        return Command("help")
    m = _ORDER.match(t)
    if m:
        c = m.group("cmd").lower()
        cmd = {"yes": "accept", "no": "reject", "packed": "ready"}.get(c, c)
        return Command(cmd, order_id=int(m.group("id")), reason=m.group("reason").strip() or None)
    m = _STOCK.match(t)
    if not m:
        return Command("unknown", error="could not read that; send HELP for the format")
    item = item_from_word(m.group("item"))
    if item is None:
        return Command("unknown", error=f"unknown item '{m.group('item').strip()}'")
    meta = ITEMS[item]
    qty = float(m.group(2).replace(",", ""))
    unit = (m.group("unit") or meta["msg_unit"]).lower()
    factors = UNIT_FACTORS[meta["unit"]]
    if unit not in factors and unit != meta["msg_unit"].lower():
        return Command("unknown", error=f"unit '{unit}' does not fit {meta['name'].lower()}")
    f = factors.get(unit, meta["factor"])
    price = None
    if m.group(4):
        p = float(m.group(4).replace(",", ""))
        punit = (m.group("punit") or unit).lower()
        pf = factors.get(punit, meta["factor"] if punit == meta["msg_unit"].lower() else None)
        if pf is None:
            return Command("unknown", error=f"price unit '{punit}' does not fit {meta['name'].lower()}")
        price = p / pf
    return Command("stock", item=item, quantity=round(qty * f, 3), unit_price=price)


def describe(item: str, q: float) -> str:
    """'20,000 litres of drinking water', '150 water containers (10-20 L)', '10.0 kg of bathing soap'."""
    if item == "water_containers":
        return f"{q:,.0f} water containers (10-20 L)"
    return f"{friendly_qty(item, q)} of {ITEMS[item]['name'][0].lower()}{ITEMS[item]['name'][1:]}"


def friendly_qty(item: str, q: float) -> str:
    meta = ITEMS[item]
    if meta["unit"] == "g" and q >= 1000:
        return f"{q / 1000:,.1f} kg"
    return f"{q:,.0f} {meta['unit']}"


async def set_stock(pool: asyncpg.Pool, supplier_id: int, item: str, quantity: float, unit_price: float | None, via: str) -> None:
    await pool.execute(
        """INSERT INTO supplier_item (supplier_id, item, available, unit_price, updated_at, updated_via)
           VALUES ($1,$2,$3,$4, now(), $5)
           ON CONFLICT (supplier_id, item) DO UPDATE SET available=EXCLUDED.available,
             unit_price=COALESCE(EXCLUDED.unit_price, supplier_item.unit_price), updated_at=now(), updated_via=EXCLUDED.updated_via""",
        supplier_id, item, quantity, unit_price, via,
    )


def new_token() -> str:
    return secrets.token_urlsafe(18)


def normalise_phone(p: str | None) -> str | None:
    if not p:
        return None
    digits = re.sub(r"\D", "", p)
    if len(digits) == 10:          # Indian mobile without country code
        digits = "91" + digits
    return digits or None
