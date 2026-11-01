"""Warden: checks every order against the officer's mandate before RASAD commits to it.

Beckn `confirm` is the irreversible step, so this runs immediately before it. Each check
returns pass, approval (the officer must approve) or block (never placed). Results are
stored on the procurement so the officer sees exactly why an order waited or stopped.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import asyncpg

from .config import get_settings


@dataclass
class Offer:
    item: str
    quantity: float
    unit_price: float | None
    route_safe: bool | None      # None = could not verify the route
    eta_min: float | None
    stock_at: datetime | None
    search_message_id: str | None
    need_basis: dict | None


def _c(name: str, outcome: str, detail: str) -> dict:
    return {"check": name, "outcome": outcome, "detail": detail}


async def review(pool: asyncpg.Pool, offer: Offer, short_remaining: float, needed_by: datetime | None, now: datetime) -> tuple[str, list[dict]]:
    s = get_settings()
    checks: list[dict] = []
    allowed = {x.strip() for x in s.mandate_items.split(",") if x.strip()}
    checks.append(_c("item_in_mandate", "pass" if offer.item in allowed else "block",
                     f"{offer.item} {'is' if offer.item in allowed else 'is not'} in the mandate"))
    limit = short_remaining * s.mandate_max_over_need
    checks.append(_c("quantity_within_need", "pass" if offer.quantity <= limit + 1e-6 else "block",
                     f"{offer.quantity:g} ordered vs {short_remaining:g} still short (limit {limit:g})"))
    checks.append(_c("provenance", "pass" if offer.search_message_id and offer.need_basis else "block",
                     "need basis and supplier catalog message are on record" if offer.search_message_id and offer.need_basis
                     else "missing need basis or catalog message"))
    if offer.stock_at is None or (now - offer.stock_at).total_seconds() > s.stock_fresh_hours * 3600:
        checks.append(_c("stock_fresh", "block", f"supplier stock older than {s.stock_fresh_hours} h"))
    else:
        checks.append(_c("stock_fresh", "pass", f"stock updated {int((now - offer.stock_at).total_seconds() // 60)} min ago"))

    if offer.unit_price is None:
        checks.append(_c("price_stated", "approval", "supplier has not stated a price"))
        total = None
    else:
        total = offer.unit_price * offer.quantity
        checks.append(_c("order_cap", "pass" if total <= s.mandate_order_cap_inr else "approval",
                         f"Rs {total:,.0f} vs order cap Rs {s.mandate_order_cap_inr:,.0f}"))
        spent = await pool.fetchval(
            """SELECT COALESCE(sum(total_price), 0) FROM procurement
               WHERE status IN ('confirming','awaiting_supplier','accepted','packed') AND created_at > date_trunc('day', now())""")
        checks.append(_c("daily_cap", "pass" if spent + total <= s.mandate_daily_cap_inr else "approval",
                         f"Rs {spent + total:,.0f} today incl. this order vs daily cap Rs {s.mandate_daily_cap_inr:,.0f}"))

    if offer.route_safe is True:
        checks.append(_c("safe_route", "pass", f"safe road route, about {offer.eta_min:.0f} min"))
    elif offer.route_safe is None:
        checks.append(_c("safe_route", "approval", "route from supplier could not be verified"))
    else:
        checks.append(_c("safe_route", "approval", "no safe road route to the shelter; needs boat, air or escort"))

    if needed_by is not None and offer.eta_min is not None:
        mins_left = (needed_by - now).total_seconds() / 60
        on_time = offer.eta_min <= max(mins_left, 0)
        checks.append(_c("on_time", "pass" if on_time else "approval",
                         f"ETA {offer.eta_min:.0f} min vs {max(mins_left, 0):.0f} min until needed"))

    outcomes = {c["outcome"] for c in checks}
    decision = "block" if "block" in outcomes else ("approval" if "approval" in outcomes else "pass")
    return decision, checks
