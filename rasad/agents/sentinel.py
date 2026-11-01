"""Sentinel agent: polls hazard feeds, normalises them into hazards, and wakes the rest of the swarm.

Each source runs on its own cadence with backoff on failure. A failed source never
stops the others; it is marked stale on the health page and its sibling feed
(SACHET and IMD CAP back each other up) keeps alerts flowing.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable

import asyncpg

from .. import store
from ..config import get_settings
from ..connectors.boundaries import Boundaries
from ..connectors.cap_feeds import read_cap_feed
from ..connectors.gdacs import read_gdacs
from ..engine.classify import LOGISTICS_KINDS
from ..events import COMMANDS, get_redis, publish
from ..models import HazardRecord
from ..provenance import Fetcher
from .river_watch import RiverWatch

log = logging.getLogger(__name__)


class Sentinel:
    def __init__(self, pool: asyncpg.Pool, fetcher: Fetcher, boundaries: Boundaries):
        self.s = get_settings()
        self.pool = pool
        self.fetcher = fetcher
        self.boundaries = boundaries
        self.river = RiverWatch(pool, fetcher)
        self._wake: dict[str, asyncio.Event] = {}
        self._tasks: list[asyncio.Task] = []

    # ---------- public ----------

    def jobs(self) -> dict[str, tuple[int, Callable[[], Awaitable[None]]]]:
        return {
            "sachet": (self.s.sachet_every, self.run_sachet),
            "imd_cap": (self.s.imd_cap_every, self.run_imd_cap),
            "gdacs": (self.s.gdacs_every, self.run_gdacs),
            "river_watch": (self.s.river_watch_every, self.run_river_watch),
        }

    def trigger(self, source: str) -> bool:
        ev = self._wake.get(source)
        if ev is None:
            return False
        ev.set()
        return True

    async def start(self) -> None:
        for name, (every, fn) in self.jobs().items():
            self._wake[name] = asyncio.Event()
            self._tasks.append(asyncio.create_task(self._loop(name, every, fn), name=f"sentinel:{name}"))
        self._tasks.append(asyncio.create_task(self._commands(), name="sentinel:commands"))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    # ---------- scheduling ----------

    async def _loop(self, name: str, every: int, fn) -> None:
        failures = 0
        # River watch waits for the first hazard pass to land.
        if name == "river_watch":
            await asyncio.sleep(20)
        while True:
            try:
                await fn()
                failures = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures += 1
                log.exception("%s run crashed", name)
                await store.mark_source(self.pool, name, False, None, None, f"crash: {exc}")
            # Back off on repeated failure, capped at 4x the normal interval.
            delay = every * min(4, 2 ** max(0, failures - 1)) if failures else every
            try:
                await asyncio.wait_for(self._wake[name].wait(), timeout=delay)
            except TimeoutError:
                pass
            self._wake[name].clear()

    async def _commands(self) -> None:
        """Listen for refresh commands sent by the API through Redis."""
        last = "$"
        while True:
            try:
                resp = await get_redis().xread({COMMANDS: last}, block=15000, count=10)
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(10)
                continue
            for _stream, entries in resp or []:
                for entry_id, fields in entries:
                    last = entry_id
                    try:
                        cmd = json.loads(fields.get("cmd", "{}"))
                    except ValueError:
                        continue
                    if cmd.get("cmd") == "refresh":
                        self.trigger(cmd.get("source", ""))

    # ---------- runs ----------

    async def _ingest(self, records: list[HazardRecord]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for rec in records:
            if rec.geom is None and rec.source != "gdacs":
                geom, matched = self.boundaries.locate(rec.area_desc, rec.attrs.get("geocodes"))
                if geom is not None:
                    rec.geom, rec.located_by = geom, "district"
                    rec.attrs["matched_areas"] = matched
            try:
                hid, action = await store.upsert_hazard(self.pool, rec)
            except Exception as exc:
                log.exception("store failed for %s %s", rec.source, rec.source_ref)
                counts["store_error"] = counts.get("store_error", 0) + 1
                await publish(self.pool, "hazard.store_error", {"source": rec.source, "ref": rec.source_ref, "error": str(exc)})
                continue
            counts[action] = counts.get(action, 0) + 1
            if action in ("created", "merged", "updated", "cancelled"):
                await publish(
                    self.pool,
                    "hazard.updated",
                    {
                        "hazard_id": hid,
                        "action": action,
                        "kind": rec.kind,
                        "severity": rec.severity,
                        "title": rec.title,
                        "source": rec.source,
                        "source_ref": rec.source_ref,
                        "raw_fetch_id": rec.raw_fetch_id,
                        "logistics_relevant": rec.kind in LOGISTICS_KINDS,
                    },
                )
        return counts

    async def _run_cap(self, source: str, feeds: list[str]) -> None:
        all_records, errors, ok_any, raw_id, seen = [], [], False, None, 0
        for url in feeds:
            run = await read_cap_feed(self.fetcher, self.pool, source, url)
            ok_any = ok_any or run.feed_ok
            raw_id = raw_id or run.index_raw_id
            seen += run.items_seen
            all_records += run.records
            errors += run.errors
        counts = await self._ingest(all_records)
        err = "; ".join(errors[:5]) if errors else None
        await store.mark_source(self.pool, source, ok_any, raw_id, seen, err)
        log.info("%s: %d feed items, %d new messages, %s", source, seen, len(all_records), counts)

    async def run_sachet(self) -> None:
        await self._run_cap("sachet", self.s.sachet_feeds)

    async def run_imd_cap(self) -> None:
        await self._run_cap("imd_cap", [self.s.imd_cap_feed])

    async def run_gdacs(self) -> None:
        ok, raw_id, records, errors = await read_gdacs(self.fetcher, self.s.gdacs_base, self.s.bbox)
        counts = await self._ingest(records) if ok else {}
        await store.mark_source(self.pool, "gdacs", ok, raw_id, len(records), "; ".join(errors[:5]) or None)
        for hid in await store.expire_old(self.pool):
            await publish(self.pool, "hazard.updated", {"hazard_id": hid, "action": "expired"})
        log.info("gdacs: %d events near India, %s", len(records), counts)

    async def run_river_watch(self) -> None:
        summary = await self.river.run()
        await store.mark_source(self.pool, "river_watch", summary["ok"], summary.get("raw_id"), summary["points"], summary.get("error"))
        for hid, rw in summary["hazards"].items():
            await publish(self.pool, "river.updated", {"hazard_id": hid, **rw})
