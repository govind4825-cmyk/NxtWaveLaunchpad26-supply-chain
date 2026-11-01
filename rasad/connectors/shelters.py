"""Candidate relief shelters from OpenStreetMap inside a hazard area.

Ranked by how likely a place is to be used as a relief point: designated
shelters and assembly points first, then community halls and town halls,
hospitals (critical destinations), then schools, which are the most common
relief camps in India but also the most numerous.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..provenance import Fetcher
from .overpass import clamp_bbox

RANK = {"designated": 0, "community_centre": 1, "townhall": 1, "hospital": 1, "school": 2}
GRID_DEG = 0.02   # keep the best candidate per ~2 km cell


@dataclass
class ShelterCandidate:
    osm_ref: str
    name: str | None
    kind: str
    lat: float
    lon: float


def shelters_query(w: float, s: float, e: float, n: float, limit: int = 600) -> str:
    bb = f"{s:.4f},{w:.4f},{n:.4f},{e:.4f}"
    return (
        "[out:json][timeout:60];("
        f'nwr["emergency"="assembly_point"]({bb});'
        f'nwr["social_facility"="shelter"]({bb});'
        f'nwr["building"="cyclone_shelter"]({bb});'
        f'nwr["amenity"="community_centre"]({bb});'
        f'nwr["amenity"="townhall"]({bb});'
        f'nwr["amenity"="hospital"]({bb});'
        f'nwr["amenity"="school"]({bb});'
        f");out center tags {limit};"
    )


def kind_of(tags: dict) -> str | None:
    if tags.get("emergency") == "assembly_point" or tags.get("social_facility") == "shelter" or tags.get("building") == "cyclone_shelter":
        return "designated"
    a = tags.get("amenity")
    return a if a in RANK else None


def parse_shelters(data: dict, keep: int) -> list[ShelterCandidate]:
    cells: dict[tuple[int, int], ShelterCandidate] = {}
    for el in data.get("elements", []):
        tags = el.get("tags") or {}
        k = kind_of(tags)
        if k is None:
            continue
        lat = el.get("lat") or (el.get("center") or {}).get("lat")
        lon = el.get("lon") or (el.get("center") or {}).get("lon")
        if lat is None or lon is None:
            continue
        cand = ShelterCandidate(f"{el.get('type')}/{el.get('id')}", tags.get("name") or tags.get("name:en"), k, lat, lon)
        cell = (int(lat / GRID_DEG), int(lon / GRID_DEG))
        cur = cells.get(cell)
        if cur is None or RANK[k] < RANK[cur.kind] or (RANK[k] == RANK[cur.kind] and cand.name and not cur.name):
            cells[cell] = cand
    out = sorted(cells.values(), key=lambda c: (RANK[c.kind], c.name is None))
    return out[:keep]


async def find_shelters(fetcher: Fetcher, url: str, bounds, centre, keep: int) -> tuple[list[ShelterCandidate], int | None, str | None]:
    bbox, _ = clamp_bbox(bounds, centre)
    res = await fetcher.get("overpass", url, method="POST", data={"data": shelters_query(*bbox)}, retries=1)
    if not res.ok:
        return [], res.raw_id, res.error
    try:
        return parse_shelters(res.json(), keep), res.raw_id, None
    except ValueError as exc:
        return [], res.raw_id, f"not JSON: {exc}"
