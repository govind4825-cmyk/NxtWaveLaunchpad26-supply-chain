"""Event bus: Redis Streams for live fan-out, event_log table as the durable audit copy.

If Redis is down the event is still written to Postgres and the API falls back to
polling event_log, so a Redis outage slows the UI instead of breaking it.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import asyncpg
import redis.asyncio as aioredis

from .config import get_settings

log = logging.getLogger(__name__)

STREAM = "rasad:events"
COMMANDS = "rasad:commands"

_redis: aioredis.Redis | None = None
_redis_loop = None


def get_redis() -> aioredis.Redis:
    """One client per event loop (a client bound to a closed loop cannot be reused)."""
    global _redis, _redis_loop
    loop = asyncio.get_running_loop()
    if _redis is None or _redis_loop is not loop:
        _redis = aioredis.from_url(get_settings().redis_url, decode_responses=True)
        _redis_loop = loop
    return _redis


async def publish(pool: asyncpg.Pool, type_: str, payload: dict[str, Any]) -> int:
    event_id = await pool.fetchval(
        "INSERT INTO event_log (type, payload) VALUES ($1, $2) RETURNING id", type_, payload
    )
    try:
        await get_redis().xadd(
            STREAM,
            {"id": str(event_id), "type": type_, "payload": json.dumps(payload, default=str)},
            maxlen=5000,
            approximate=True,
        )
    except Exception as exc:  # Redis down: durable copy already stored
        log.warning("redis publish failed (%s); event %s kept in event_log", exc, event_id)
    return event_id


async def send_command(cmd: dict[str, Any]) -> bool:
    try:
        await get_redis().xadd(COMMANDS, {"cmd": json.dumps(cmd)}, maxlen=500, approximate=True)
        return True
    except Exception as exc:
        log.warning("redis command failed: %s", exc)
        return False
