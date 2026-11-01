"""Need per shelter from people x Sphere minimum standards x days.

Rates are the published minimums from the Sphere Handbook (2018) and Sphere training
material; they are standards, not observations, and every need line carries the rate
it used in words so an officer can see and challenge it.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import datetime, timedelta

from shapely import voronoi_polygons
from shapely.geometry import MultiPoint, MultiPolygon, Point, Polygon
from shapely.geometry.base import BaseGeometry


@dataclass(frozen=True)
class Rate:
    item: str
    unit: str
    per_person_day: float | None   # consumables
    per_household: float | None    # one-off items
    label: str


SPHERE = [
    Rate("drinking_water", "litres", 15.0, None,
         "15 L per person per day for drinking, cooking and hygiene (Sphere water quantity standard)"),
    Rate("cereal_ration", "kg", 14.0 / 30, None,
         "14 kg cereal per person per month, about 0.47 kg a day (Sphere food basket guidance)"),
    Rate("bathing_soap", "g", 250.0 / 30, None,
         "250 g bathing soap per person per month (Sphere hygiene standard)"),
    Rate("water_containers", "containers", None, 2.0,
         "two 10-20 L containers per household, one to carry and one to store (Sphere)"),
]
KCAL_PER_PERSON_DAY = 2100   # Sphere energy requirement, reported with the food line


def need_lines(people: int, days: int, household_size: float) -> list[dict]:
    if people <= 0:
        return []
    households = math.ceil(people / household_size)
    out = []
    for r in SPHERE:
        if r.per_person_day is not None:
            qty = people * r.per_person_day * days
        else:
            qty = households * (r.per_household or 0)
        out.append({"item": r.item, "unit": r.unit, "quantity": round_up(qty), "rate": r.label})
    return out


def round_up(q: float) -> float:
    """Round up to a sensible procurement unit; never round a need down."""
    q = round(q, 6)   # absorb float noise so 6000.0000001 does not become 6010
    if q >= 1000:
        return float(math.ceil(q / 10) * 10)
    if q >= 10:
        return float(math.ceil(q))
    return math.ceil(q * 10) / 10


def supply_days(hazard_expires: datetime | None, now: datetime, default_days: int, max_days: int) -> int:
    """Days to supply for: until the hazard expires plus two days to reopen roads, capped."""
    if hazard_expires is None:
        return default_days
    hours = (hazard_expires + timedelta(hours=48) - now).total_seconds() / 3600
    return max(1, min(max_days, math.ceil(hours / 24)))


def urgency(access_status: str | None, closes_at: datetime | None, now: datetime) -> float:
    if access_status == "cut_off":
        return 3.0
    if access_status == "closing" and closes_at is not None:
        return 2.5 if (closes_at - now) <= timedelta(hours=24) else 1.8
    if access_status in ("unknown", "unroutable"):
        return 1.5
    return 1.0


def needed_by(access_status: str | None, closes_at: datetime | None, now: datetime) -> datetime:
    if access_status == "cut_off":
        return now
    if access_status == "closing" and closes_at is not None:
        return closes_at - timedelta(hours=3)   # land before the last safe road goes
    return now + timedelta(hours=24)


def catchments(points: dict[int, tuple[float, float]], area: BaseGeometry) -> dict[int, MultiPolygon]:
    """Voronoi cell of each shelter, clipped to the hazard footprint."""
    if not points or area.is_empty:
        return {}
    if len(points) == 1:
        (sid,) = points
        return {sid: _multi(area)}
    ids = list(points)
    mp = MultiPoint([points[i] for i in ids])
    minx, miny, maxx, maxy = area.bounds
    pad = max(maxx - minx, maxy - miny) + 1
    cells = voronoi_polygons(mp, extend_to=area.envelope.buffer(pad))
    out: dict[int, MultiPolygon] = {}
    for cell in cells.geoms:
        owner = next((i for i in ids if cell.covers(Point(points[i]))), None)
        if owner is None:
            continue
        clipped = _multi(cell.intersection(area))
        if clipped is not None:
            out[owner] = clipped
    return out


def _multi(g: BaseGeometry) -> MultiPolygon | None:
    if g.is_empty:
        return None
    if isinstance(g, Polygon):
        return MultiPolygon([g])
    if isinstance(g, MultiPolygon):
        return g
    polys = [p for p in getattr(g, "geoms", []) if isinstance(p, Polygon)]
    return MultiPolygon(polys) if polys else None


def geom_hash(g: BaseGeometry) -> str:
    return hashlib.sha1(g.simplify(0.001).wkb).hexdigest()[:16]
