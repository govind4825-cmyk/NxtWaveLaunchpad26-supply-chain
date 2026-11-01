"""Procurer agent: turns open need lines into confirmed supplier orders over Beckn.

For the most urgent shelters it searches the network for each short item, ranks the
offers (safe road route from the supplier first, then ETA, price and the supplier's
track record), sends the best through the Warden, and runs select -> init -> confirm.
Orders the supplier has not accepted within RASAD_SUPPLIER_ACCEPT_MINUTES are cancelled
and re-sourced elsewhere; rejected orders are re-sourced at once.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import asyncpg

from .. import store
from ..beckn.node import context, identity, send
from ..config import get_settings
from ..connectors.graphhopper import GraphHopper
from ..engine.risk import exposure
from ..events import STREAM, get_redis, publish
from ..suppliers import ITEMS
from ..warden import Offer, review
from .cartographer import build_slices, load_inputs

log = logging.getLogger(__name__)

WAKE_ON = {"need.updated", "quartermaster.run", "procurement.updated", "procurement.approved"}
INFLIGHT = ("awaiting_approval", "approved", "confirming", "awaiting_supplier")
COMMITTED = ("accepted", "packed", "in_transit", "delivered")
MAX_ORDERS_PER_LINE_PER_RUN = 3
IST = ZoneInfo("Asia/Kolkata")
DEBOUNCE_S = 10


@dataclass
class CatalogOffer:
    bpp_id: str
    bpp_uri: str
    provider_id: str
    provider_name: str
    item_id: str
    available: float
    unit_price: float | None
    lat: float
    lon: float
    stock_at: datetime | None
    search_message_id: str


async def wait_for(pool: asyncpg.Pool, message_id: str, action: str, timeout: float, expect: int = 1) -> list[dict]:
    """Collect callbacks for a message id until `expect` arrive or the timeout passes."""
    deadline = asyncio.get_running_loop().time() + timeout
    rows: list = []
    while True:
        rows = await pool.fetch(
            "SELECT body FROM beckn_message WHERE direction='in' AND role='BAP' AND action=$1 AND message_id=$2 ORDER BY id",
            action, message_id)
        if len(rows) >= expect or asyncio.get_running_loop().time() >= deadline:
            return [r["body"] for r in rows]
        await asyncio.sleep(0.25)


def parse_catalog(body: dict, message_id: str) -> list[CatalogOffer]:
    ctx = body.get("context") or {}
    out = []
    for p in ((body.get("message") or {}).get("catalog") or {}).get("providers") or []:
        locs = {loc.get("id"): loc.get("gps") for loc in p.get("locations") or []}
        for it in p.get("items") or []:
            gps = locs.get((it.get("location_ids") or [None])[0]) or next(iter(locs.values()), None)
            if not gps:
                continue
            lat, lon = (float(x) for x in gps.split(","))
            price = it.get("price") or {}
            try:
                unit_price = float(price["value"]) if price.get("value") not in (None, "", "unstated") else None
            except (TypeError, ValueError):
                unit_price = None
            ts = (it.get("time") or {}).get("timestamp")
            out.append(CatalogOffer(
                ctx.get("bpp_id"), ctx.get("bpp_uri"), str(p.get("id")), (p.get("descriptor") or {}).get("name") or str(p.get("id")),
                it.get("id"), float((((it.get("quantity") or {}).get("available") or {}).get("count")) or 0),
                unit_price, lat, lon, datetime.fromisoformat(ts.replace("Z", "+00:00")) if ts else None, message_id))
    return out


class Procurer:
    def __init__(self, pool: asyncpg.Pool, gh: GraphHopper | None = None):
        self.s = get_settings()
        self.pool = pool
        self.gh = gh or GraphHopper(self.s.graphhopper_url)
        self._wake = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        self._tasks.append(asyncio.create_task(self._loop(), name="procurer"))
        self._tasks.append(asyncio.create_task(self._listen(), name="procurer:events"))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.gh.aclose()

    def trigger(self) -> None:
        self._wake.set()

    async def _loop(self) -> None:
        await asyncio.sleep(35)
        while True:
            try:
                await self.run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("procurer run crashed")
                await store.mark_source(self.pool, "procurer", False, None, None, f"crash: {exc}")
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.s.procurer_every)
                await asyncio.sleep(DEBOUNCE_S)
            except TimeoutError:
                pass
            self._wake.clear()

    async def _listen(self) -> None:
        last = "$"
        while True:
            try:
                resp = await get_redis().xread({STREAM: last}, block=15000, count=50)
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(10)
                continue
            for _s, entries in resp or []:
                for entry_id, f in entries:
                    last = entry_id
                    if f.get("type") in WAKE_ON:
                        self._wake.set()

    # ------------------------------------------------------------------

    async def run(self) -> dict:
        async with self._lock:
            await identity(self.pool)
            timed_out = await self._expire_unaccepted()
            for r in await self.pool.fetch("SELECT id FROM procurement WHERE status='approved' ORDER BY approved_at"):
                await self.execute(r["id"])
            lines = await self._open_lines()
            zones = await self._zones()
            placed = waiting = no_offer = 0
            for ln in lines:
                r = await self._procure_line(ln, zones)
                placed += r["placed"]
                waiting += r["waiting"]
                no_offer += r["no_offer"]
            note = f"{placed} orders placed, {waiting} awaiting approval, {no_offer} lines with no offer, {timed_out} timed out"
            await store.mark_source(self.pool, "procurer", True, None, len(lines), note)
            summary = {"lines": len(lines), "placed": placed, "awaiting_approval": waiting, "no_offer": no_offer, "timed_out": timed_out}
            await publish(self.pool, "procurer.run", summary)
            return summary

    async def _zones(self):
        try:
            hz, cr, cl = await load_inputs(self.pool, self.s.road_impact_map)
            return build_slices(datetime.now(UTC), [0], hz, cr, cl, self.s.road_impact_map, self.s.hard_flood_prob)[0]
        except Exception:
            log.exception("could not build risk zones")
            return None

    async def _open_lines(self) -> list:
        return await self.pool.fetch(
            """SELECT n.shelter_id, n.item, n.unit, n.quantity, n.committed, n.needed_by, n.priority, n.basis, n.people,
                       s.name AS shelter_name, ST_X(s.geom) lon, ST_Y(s.geom) lat,
                       COALESCE((SELECT sum(p.quantity) FROM procurement p WHERE p.shelter_id=n.shelter_id AND p.item=n.item
                                 AND p.status = ANY($1::text[])), 0) AS inflight
                FROM need n JOIN shelter s ON s.id = n.shelter_id
                WHERE n.shelter_id IN (SELECT shelter_id FROM need GROUP BY shelter_id ORDER BY max(priority) DESC LIMIT $2)
                ORDER BY n.priority DESC, n.needed_by""",
            list(INFLIGHT), self.s.procure_top_shelters,
        )

    async def _reliability(self, provider_id: str) -> float:
        r = await self.pool.fetchrow(
            """SELECT count(*) FILTER (WHERE status = ANY($2::text[])) ok,
                      count(*) FILTER (WHERE status IN ('rejected','cancelled')) bad
               FROM procurement WHERE provider_id=$1""", provider_id, list(COMMITTED))
        return (r["ok"] + 1) / (r["ok"] + r["bad"] + 2)

    async def _last_refusals(self, item: str) -> dict[str, datetime]:
        """When each supplier last rejected or let an order for this item time out (any shelter, last 6 h)."""
        rows = await self.pool.fetch(
            """SELECT bpp_id || '|' || provider_id AS k, max(updated_at) AS at FROM procurement
               WHERE item=$1 AND status IN ('rejected','cancelled') AND approved_by IS NULL
                 AND updated_at > now() - interval '6 hours' GROUP BY 1""", item)
        return {r["k"]: r["at"] for r in rows}

    async def _search(self, item: str, lat: float, lon: float) -> tuple[list[CatalogOffer], str]:
        ident = await identity(self.pool)
        txn, mid = str(uuid.uuid4()), str(uuid.uuid4())
        body = {"context": context(ident, "search", txn, message_id=mid),
                "message": {"intent": {"item": {"descriptor": {"code": item, "name": ITEMS[item]["name"]}},
                                       "fulfillment": {"type": "Delivery", "stops": [{"type": "end", "location": {"gps": f"{lat:.6f},{lon:.6f}"}}]}}}}
        if self.s.beckn_gateway_url:
            targets = [(self.s.beckn_gateway_url, "gateway")]
        else:
            targets = [(r["url"], r["subscriber_id"]) for r in await self.pool.fetch("SELECT url, subscriber_id FROM beckn_peer WHERE role='BPP' AND active")]
        acked = 0
        for url, peer in targets:
            ok, _ = await send(self.pool, "BAP", url, "search", body, peer=peer)
            acked += 1 if ok else 0
        if not acked:
            return [], mid
        responses = await wait_for(self.pool, mid, "on_search", self.s.beckn_wait_s, expect=acked if not self.s.beckn_gateway_url else 99)
        offers = []
        for r in responses:
            offers += parse_catalog(r, mid)
        return offers, mid

    async def _rank(self, offers: list[CatalogOffer], ln, zones) -> list[dict]:
        ranked = []
        gh_ok = await self.gh.healthy()
        for o in offers:
            safe, eta = None, None
            if gh_ok:
                r = await self.gh.route((o.lon, o.lat), (ln["lon"], ln["lat"]), zones.model if zones else None)
                if r.ok and r.line is not None:
                    worst, hard, _ = exposure(r.line, zones.zones if zones else [])
                    safe = (not hard) and worst < self.s.safe_risk
                    eta = round(r.time_s / 60, 1)
                elif r.no_route:
                    safe = False
            rel = await self._reliability(o.provider_id)
            ranked.append({"offer": o, "safe": safe, "eta": eta, "reliability": round(rel, 2)})
        order = {True: 0, None: 1, False: 2}
        ranked.sort(key=lambda x: (order[x["safe"]], x["eta"] if x["eta"] is not None else 1e9,
                                   x["offer"].unit_price if x["offer"].unit_price is not None else 1e9, -x["reliability"]))
        return ranked

    async def _procure_line(self, ln, zones) -> dict:
        res = {"placed": 0, "waiting": 0, "no_offer": 0}
        remaining = ln["quantity"] - ln["committed"] - ln["inflight"]
        if remaining <= max(1.0, 0.05 * ln["quantity"]):
            return res
        offers, search_mid = await self._search(ln["item"], ln["lat"], ln["lon"])
        # A supplier that refused this item is skipped until its catalog shows a stock update made after the refusal.
        refused = await self._last_refusals(ln["item"])
        offers = [o for o in offers if o.available >= 1 and not (
            f"{o.bpp_id}|{o.provider_id}" in refused and (o.stock_at is None or o.stock_at <= refused[f"{o.bpp_id}|{o.provider_id}"]))]
        if not offers:
            res["no_offer"] = 1
            await publish(self.pool, "procurement.no_offer", {"shelter_id": ln["shelter_id"], "shelter": ln["shelter_name"],
                                                              "item": ln["item"], "short": remaining, "search_message_id": search_mid})
            return res
        ranked = await self._rank(offers, ln, zones)
        now = datetime.now(UTC)
        for i, cand in enumerate(ranked[:MAX_ORDERS_PER_LINE_PER_RUN]):
            if remaining <= max(1.0, 0.05 * ln["quantity"]):
                break
            o: CatalogOffer = cand["offer"]
            qty = float(min(remaining, o.available))
            qty = float(int(qty)) if qty >= 1 else qty
            offer = Offer(ln["item"], qty, o.unit_price, cand["safe"], cand["eta"], o.stock_at, search_mid, ln["basis"])
            decision, checks = await review(self.pool, offer, remaining, ln["needed_by"], now)
            reasons = {"rank": i + 1, "safe_route": cand["safe"], "eta_min": cand["eta"], "unit_price": o.unit_price,
                       "reliability": cand["reliability"], "search_message_id": search_mid,
                       "alternatives": [{"provider": c["offer"].provider_name, "safe_route": c["safe"], "eta_min": c["eta"],
                                         "unit_price": c["offer"].unit_price, "available": c["offer"].available}
                                        for c in ranked if c is not cand][:4]}
            status = {"pass": "confirming", "approval": "awaiting_approval", "block": "failed"}[decision]
            pid = await self.pool.fetchval(
                """INSERT INTO procurement (shelter_id, item, quantity, unit, status, transaction_id, bpp_id, bpp_uri, provider_id,
                     provider_name, item_id, unit_price, total_price, eta_min, route_safe, needed_by, checks, reasons, note, pickup_lat, pickup_lon)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21) RETURNING id""",
                ln["shelter_id"], ln["item"], qty, ln["unit"], status, str(uuid.uuid4()), o.bpp_id, o.bpp_uri, o.provider_id,
                o.provider_name, o.item_id, o.unit_price, (o.unit_price * qty) if o.unit_price is not None else None,
                cand["eta"], cand["safe"], ln["needed_by"], checks, reasons,
                None if decision == "pass" else "; ".join(c["detail"] for c in checks if c["outcome"] != "pass"),
                o.lat, o.lon,
            )
            await publish(self.pool, "procurement.updated", {"procurement_id": pid, "shelter_id": ln["shelter_id"], "item": ln["item"],
                                                             "status": status, "provider": o.provider_name, "quantity": qty})
            if decision == "block":
                continue
            if decision == "approval":
                res["waiting"] += 1
                remaining -= qty   # held for the officer; do not double-order around it
                continue
            if await self.execute(pid):
                res["placed"] += 1
                remaining -= qty
        return res

    async def execute(self, procurement_id: int) -> bool:
        """select -> init -> confirm for one procurement. Returns True once the BPP has the order."""
        p = await self.pool.fetchrow("SELECT p.*, s.name AS shelter_name, ST_X(s.geom) lon, ST_Y(s.geom) lat FROM procurement p JOIN shelter s ON s.id=p.shelter_id WHERE p.id=$1", procurement_id)
        ident = await identity(self.pool)
        end = {"type": "end", "location": {"gps": f"{p['lat']:.6f},{p['lon']:.6f}"},
               "instructions": {"name": "Relief delivery", "short_desc": f"Needed by {p['needed_by'].astimezone(IST):%d %b %I:%M %p} IST" if p["needed_by"] else None}}
        order = {"provider": {"id": p["provider_id"]},
                 "items": [{"id": p["item_id"], "quantity": {"selected": {"count": p["quantity"]}}}],
                 # RASAD's own fleet collects from the supplier; the end stop tells the supplier where it is going.
                 "fulfillments": [{"type": "Self-Pickup", "stops": [end]}]}
        steps = [("select", "on_select", {"order": order}),
                 ("init", "on_init", {"order": {**order, "billing": {"name": p["shelter_name"] or "Relief shelter", "phone": ""}}}),
                 ("confirm", "on_confirm", {"order": {**order, "billing": {"name": p["shelter_name"] or "Relief shelter", "phone": ""},
                                                       "payments": [{"type": "ON-FULFILLMENT", "status": "NOT-PAID"}]}})]
        for action, cb, message in steps:
            mid = str(uuid.uuid4())
            body = {"context": context(ident, action, p["transaction_id"], bpp_id=p["bpp_id"], bpp_uri=p["bpp_uri"], message_id=mid),
                    "message": message}
            ok, err = await send(self.pool, "BAP", p["bpp_uri"], action, body, peer=p["bpp_id"])
            if not ok:
                return await self._fail(p, f"{action} not acknowledged: {err}")
            resp = await wait_for(self.pool, mid, cb, self.s.beckn_wait_s)
            if not resp:
                return await self._fail(p, f"no {cb} within {self.s.beckn_wait_s:.0f} s")
            if resp[0].get("error"):
                e = resp[0]["error"]
                return await self._fail(p, f"{cb} error {e.get('code')}: {e.get('message')}")
            if action == "confirm":
                o = (resp[0].get("message") or {}).get("order") or {}
                await self.pool.execute(
                    "UPDATE procurement SET status='awaiting_supplier', bpp_order_id=$2, updated_at=now() WHERE id=$1 AND status IN ('confirming','approved')",
                    p["id"], o.get("id"))
                await publish(self.pool, "procurement.updated", {"procurement_id": p["id"], "shelter_id": p["shelter_id"], "item": p["item"],
                                                                 "status": "awaiting_supplier", "provider": p["provider_name"],
                                                                 "quantity": p["quantity"], "bpp_order_id": o.get("id")})
        return True

    async def _fail(self, p, note: str) -> bool:
        await self.pool.execute("UPDATE procurement SET status='failed', note=$2, updated_at=now() WHERE id=$1", p["id"], note)
        await publish(self.pool, "procurement.updated", {"procurement_id": p["id"], "shelter_id": p["shelter_id"], "item": p["item"],
                                                         "status": "failed", "provider": p["provider_name"], "note": note})
        return False

    async def cancel(self, procurement_id: int, reason: str) -> None:
        p = await self.pool.fetchrow("SELECT * FROM procurement WHERE id=$1", procurement_id)
        if p is None:
            return
        if p["bpp_order_id"]:
            ident = await identity(self.pool)
            body = {"context": context(ident, "cancel", p["transaction_id"], bpp_id=p["bpp_id"], bpp_uri=p["bpp_uri"]),
                    "message": {"order_id": p["bpp_order_id"], "descriptor": {"short_desc": reason}}}
            await send(self.pool, "BAP", p["bpp_uri"], "cancel", body, peer=p["bpp_id"])
        await self.pool.execute("UPDATE procurement SET status='cancelled', note=$2, updated_at=now() WHERE id=$1", procurement_id, reason)
        await publish(self.pool, "procurement.updated", {"procurement_id": procurement_id, "shelter_id": p["shelter_id"], "item": p["item"],
                                                         "status": "cancelled", "provider": p["provider_name"], "note": reason})

    async def _expire_unaccepted(self) -> int:
        rows = await self.pool.fetch(
            "SELECT id FROM procurement WHERE status='awaiting_supplier' AND updated_at < now() - make_interval(mins => $1)",
            self.s.supplier_accept_minutes)
        for r in rows:
            await self.cancel(r["id"], f"supplier did not accept within {self.s.supplier_accept_minutes} min")
        return len(rows)

