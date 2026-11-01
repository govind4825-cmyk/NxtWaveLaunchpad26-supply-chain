"""OpenStreetMap Overpass queries: road bridges over rivers inside a hazard area."""

from __future__ import annotations

from dataclasses import dataclass

from ..provenance import Fetcher

ROAD_RANK = {"motorway": 0, "trunk": 1, "primary": 2, "secondary": 3, "tertiary": 4}

# Overpass gets slow on huge boxes; larger hazards are sampled around their centre.
MAX_SPAN_DEG = 1.0


@dataclass
class Crossing:
    osm_ref: str
    lat: float
    lon: float
    name: str | None
    road_class: str | None


def clamp_bbox(bounds: tuple[float, float, float, float], centre: tuple[float, float], max_span: float = MAX_SPAN_DEG) -> tuple[tuple[float, float, float, float], bool]:
    w, s, e, n = bounds
    if (e - w) <= max_span and (n - s) <= max_span:
        return bounds, False
    cx, cy = centre
    h = max_span / 2
    return (max(w, cx - h), max(s, cy - h), min(e, cx + h), min(n, cy + h)), True


def bridges_query(w: float, s: float, e: float, n: float, limit: int = 120) -> str:
    bb = f"{s:.4f},{w:.4f},{n:.4f},{e:.4f}"
    return (
        "[out:json][timeout:60];"
        f'way["waterway"="river"]({bb})->.rivers;'
        f'way["bridge"]["highway"~"^(motorway|trunk|primary|secondary|tertiary)$"](around.rivers:30)({bb});'
        f"out center tags {limit};"
    )


def parse_crossings(data: dict, keep: int) -> list[Crossing]:
    out = []
    for el in data.get("elements", []):
        c = el.get("center") or {}
        if "lat" not in c:
            continue
        tags = el.get("tags") or {}
        out.append(
            Crossing(
                osm_ref=f"{el.get('type', 'way')}/{el.get('id')}",
                lat=c["lat"],
                lon=c["lon"],
                name=tags.get("name") or tags.get("bridge:name") or tags.get("ref"),
                road_class=tags.get("highway"),
            )
        )
    out.sort(key=lambda x: ROAD_RANK.get(x.road_class or "", 9))
    # Bridges on dual carriageways come in pairs; keep one per ~1 km.
    picked: list[Crossing] = []
    for c in out:
        if all(abs(c.lat - p.lat) > 0.01 or abs(c.lon - p.lon) > 0.01 for p in picked):
            picked.append(c)
        if len(picked) >= keep:
            break
    return picked


async def river_crossings(fetcher: Fetcher, url: str, bounds, centre, keep: int = 20) -> tuple[list[Crossing], bool, str | None]:
    bbox, sampled = clamp_bbox(bounds, centre)
    res = await fetcher.get("overpass", url, method="POST", data={"data": bridges_query(*bbox)}, retries=1)
    if not res.ok:
        return [], sampled, res.error
    try:
        return parse_crossings(res.json(), keep), sampled, None
    except ValueError as exc:
        return [], sampled, f"not JSON: {exc}"
