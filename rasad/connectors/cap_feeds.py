"""SACHET and IMD CAP feeds: read the RSS index, then fetch and parse each new CAP file."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

import asyncpg

from ..models import HazardRecord
from ..provenance import Fetcher
from .cap import CapParseError, parse_cap
from .rss import parse_feed

log = logging.getLogger(__name__)

MAX_NEW_PER_RUN = 80
CONCURRENCY = 6


@dataclass
class FeedRun:
    feed_ok: bool
    index_raw_id: int | None
    records: list[HazardRecord] = field(default_factory=list)
    items_seen: int = 0
    already_known: int = 0
    errors: list[str] = field(default_factory=list)


async def _known(pool: asyncpg.Pool | None, source: str, url: str) -> bool:
    if pool is None:
        return False
    return bool(
        await pool.fetchval(
            "SELECT 1 FROM hazard_source WHERE source=$1 AND source_url=$2 LIMIT 1", source, url
        )
    )


async def read_cap_feed(fetcher: Fetcher, pool: asyncpg.Pool | None, source: str, feed_url: str) -> FeedRun:
    idx = await fetcher.get(source, feed_url)
    run = FeedRun(feed_ok=idx.ok, index_raw_id=idx.raw_id)
    if not idx.ok:
        run.errors.append(f"index: {idx.error}")
        return run
    try:
        items = parse_feed(idx.body)
    except Exception as exc:  # malformed feed
        run.feed_ok = False
        run.errors.append(f"index parse: {exc}")
        return run
    run.items_seen = len(items)

    todo = []
    for it in items:
        if not it.link:
            continue
        if await _known(pool, source, it.link):
            run.already_known += 1
            continue
        todo.append(it)
    todo = todo[:MAX_NEW_PER_RUN]

    sem = asyncio.Semaphore(CONCURRENCY)

    async def one(item):
        async with sem:
            res = await fetcher.get(source, item.link, retries=1)
            if not res.ok:
                run.errors.append(f"{item.link}: {res.error}")
                return
            try:
                rec = parse_cap(res.body, source=source, source_url=item.link, raw_fetch_id=res.raw_id)
            except CapParseError as exc:
                run.errors.append(f"{item.link}: {exc}")
                return
            if rec.sent is None and item.published:
                rec.sent = item.published
            run.records.append(rec)

    await asyncio.gather(*(one(it) for it in todo))
    return run
