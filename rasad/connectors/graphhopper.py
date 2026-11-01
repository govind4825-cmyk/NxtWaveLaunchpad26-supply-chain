"""Client for a self-hosted GraphHopper (car profile with landmarks, per-request custom models)."""

from __future__ import annotations

from dataclasses import dataclass

import httpx
from shapely.geometry import LineString


@dataclass
class Route:
    ok: bool
    distance_m: float | None = None
    time_s: float | None = None
    line: LineString | None = None
    no_route: bool = False        # GraphHopper ran but found no way through
    snap_failed: bool = False     # a point is too far from any road
    error: str | None = None


class GraphHopper:
    def __init__(self, base_url: str, client: httpx.AsyncClient | None = None, profile: str = "car"):
        self.base = base_url.rstrip("/")
        self.profile = profile
        self.client = client or httpx.AsyncClient(timeout=30)

    async def aclose(self) -> None:
        await self.client.aclose()

    async def healthy(self) -> bool:
        try:
            r = await self.client.get(f"{self.base}/health", timeout=5)
            return r.status_code == 200
        except httpx.HTTPError:
            return False

    async def nearest(self, point: tuple[float, float]) -> float | None:
        """Metres from a point to the nearest routable road (no custom model applied)."""
        try:
            r = await self.client.get(f"{self.base}/nearest", params={"point": f"{point[1]},{point[0]}"}, timeout=10)
            if r.status_code == 200:
                return float(r.json().get("distance"))
        except (httpx.HTTPError, ValueError, TypeError):
            pass
        return None

    async def route(self, start: tuple[float, float], end: tuple[float, float], custom_model: dict | None = None) -> Route:
        body = {
            "points": [list(start), list(end)],
            "profile": self.profile,
            "points_encoded": False,
            "instructions": False,
            "calc_points": True,
        }
        if custom_model:
            body["custom_model"] = custom_model
        try:
            r = await self.client.post(f"{self.base}/route", json=body)
        except httpx.HTTPError as exc:
            return Route(ok=False, error=f"{type(exc).__name__}: {exc}")
        try:
            data = r.json()
        except ValueError:
            return Route(ok=False, error=f"HTTP {r.status_code}: not JSON")
        if r.status_code == 200 and data.get("paths"):
            p = data["paths"][0]
            coords = p.get("points", {}).get("coordinates") or []
            line = LineString(coords) if len(coords) >= 2 else None
            return Route(ok=True, distance_m=p.get("distance"), time_s=(p.get("time") or 0) / 1000, line=line)
        msg = str(data.get("message", f"HTTP {r.status_code}"))
        low = msg.lower()
        if "connection between locations not found" in low:
            return Route(ok=False, no_route=True, error=msg)
        if "cannot find point" in low or "point not found" in low or "point_not_found" in low:
            return Route(ok=False, snap_failed=True, error=msg)
        return Route(ok=False, error=msg)
