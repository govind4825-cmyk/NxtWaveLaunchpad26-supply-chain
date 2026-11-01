"""Live phone GPS: subscribes to the Traccar server's WebSocket and stores positions.

Drivers install Traccar Client (Android/iOS) and point it at the Traccar server
(port 5055, OsmAnd protocol). This module logs into Traccar's REST API, then
listens on /api/socket. If the socket drops it falls back to polling /api/positions.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime

import asyncpg
import httpx
import websockets

from .config import get_settings
from .events import publish

log = logging.getLogger(__name__)


def _ts(v: str | None) -> datetime | None:
    if not v:
        return None
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None


class TraccarLink:
    def __init__(self, pool: asyncpg.Pool):
        self.s = get_settings()
        self.pool = pool
        self._task: asyncio.Task | None = None
        self.connected = False
        self.last_message: datetime | None = None
        self.last_error: str | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.s.traccar_email and self.s.traccar_password)

    async def start(self) -> None:
        if not self.enabled:
            log.info("Traccar credentials not set; live GPS disabled")
            return
        self._task = asyncio.create_task(self._run(), name="traccar")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _login(self, client: httpx.AsyncClient) -> str:
        r = await client.post(
            f"{self.s.traccar_url}/api/session",
            data={"email": self.s.traccar_email, "password": self.s.traccar_password},
        )
        r.raise_for_status()
        cookie = r.cookies.get("JSESSIONID")
        if not cookie:
            raise RuntimeError("Traccar login returned no session cookie")
        devices = await client.get(f"{self.s.traccar_url}/api/devices", cookies={"JSESSIONID": cookie})
        devices.raise_for_status()
        await self._store_devices(devices.json())
        return cookie

    async def _run(self) -> None:
        backoff = 2
        async with httpx.AsyncClient(timeout=15) as client:
            while True:
                try:
                    cookie = await self._login(client)
                    ws_url = self.s.traccar_url.replace("http", "ws", 1) + "/api/socket"
                    async with websockets.connect(
                        ws_url, additional_headers={"Cookie": f"JSESSIONID={cookie}"}, ping_interval=30
                    ) as ws:
                        self.connected, self.last_error, backoff = True, None, 2
                        log.info("Traccar socket connected")
                        async for msg in ws:
                            await self._handle(json.loads(msg))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.connected = False
                    self.last_error = f"{type(exc).__name__}: {exc}"
                    log.warning("Traccar socket down (%s); polling once, retrying in %ss", exc, backoff)
                    await self._poll_once(client)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)

    async def _poll_once(self, client: httpx.AsyncClient) -> None:
        try:
            cookie = await self._login(client)
            r = await client.get(f"{self.s.traccar_url}/api/positions", cookies={"JSESSIONID": cookie})
            r.raise_for_status()
            await self._handle({"positions": r.json()})
        except Exception as exc:
            self.last_error = f"poll: {exc}"

    async def _handle(self, msg: dict) -> None:
        self.last_message = datetime.now().astimezone()
        if msg.get("devices"):
            await self._store_devices(msg["devices"])
        for p in msg.get("positions") or []:
            fix = _ts(p.get("fixTime") or p.get("deviceTime"))
            if fix is None or p.get("latitude") is None:
                continue
            speed_kmh = (p.get("speed") or 0) * 1.852  # Traccar reports knots
            inserted = await self.pool.fetchval(
                """INSERT INTO vehicle_position (traccar_id, device_id, fix_time, server_time, geom, speed_kmh, course, attrs)
                   VALUES ($1,$2,$3,$4, ST_SetSRID(ST_MakePoint($5,$6),4326), $7,$8,$9)
                   ON CONFLICT (traccar_id) DO NOTHING RETURNING id""",
                p.get("id"), p.get("deviceId"), fix, _ts(p.get("serverTime")), p.get("longitude"), p.get("latitude"),
                speed_kmh, p.get("course"), {"accuracy": p.get("accuracy"), "valid": p.get("valid"), **(p.get("attributes") or {})},
            )
            if inserted:
                await publish(
                    self.pool,
                    "vehicle.position",
                    {"device_id": p.get("deviceId"), "lat": p.get("latitude"), "lon": p.get("longitude"),
                     "speed_kmh": round(speed_kmh, 1), "fix_time": fix.isoformat()},
                )

    async def _store_devices(self, devices: list[dict]) -> None:
        for d in devices:
            await self.pool.execute(
                """INSERT INTO vehicle (device_id, name, unique_id, status, last_update) VALUES ($1,$2,$3,$4,$5)
                   ON CONFLICT (device_id) DO UPDATE SET name=EXCLUDED.name, unique_id=EXCLUDED.unique_id,
                     status=EXCLUDED.status, last_update=EXCLUDED.last_update""",
                d.get("id"), d.get("name"), d.get("uniqueId"), d.get("status"), _ts(d.get("lastUpdate")),
            )

    def health(self) -> dict:
        return {
            "enabled": self.enabled,
            "connected": self.connected,
            "last_message": self.last_message.isoformat() if self.last_message else None,
            "last_error": self.last_error,
        }
