"""Road risk over time.

At each time slice (now, +6 h, ... +72 h) RASAD builds a set of risk zones:

* hazard footprints, soft risk = playbook road impact x severity x certainty,
  only while the hazard is in force at that time;
* river crossings, soft risk = GloFAS probability of exceeding the local flood
  level on that day; hard closure when the probability reaches hard_flood_prob;
* road closures, hard when confirmed (TomTom or corroborated field reports),
  soft when a single unconfirmed report exists.

Zones become a GraphHopper custom model: soft zones lower route priority in
buckets, hard zones get priority 0 so routes cannot pass. After routing, the
route is checked against the same zones to report what it crosses.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from shapely.geometry import LineString, Point, Polygon, mapping
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

# Soft risk -> GraphHopper priority multiplier. Below the first bucket a zone is ignored.
BUCKETS = [(0.7, 0.1), (0.4, 0.3), (0.15, 0.6)]
IGNORE_BELOW = 0.15
CROSSING_RADIUS_M = 250
CLOSURE_BUFFER_M = 60
UNCONFIRMED_CLOSURE_RISK = 0.6


@dataclass
class Zone:
    geom: BaseGeometry
    risk: float          # 0..1, ignored when hard
    hard: bool
    reason: str
    ref_type: str        # hazard | river_point | closure
    ref_id: str

    def as_ref(self) -> dict:
        return {"type": self.ref_type, "id": self.ref_id, "reason": self.reason,
                "risk": round(self.risk, 3), "hard": self.hard}


@dataclass
class HazardIn:
    id: str
    kind: str
    title: str
    severity: int
    certainty: float
    onset: datetime | None
    expires: datetime | None
    geom: BaseGeometry


@dataclass
class CrossingIn:
    id: int
    name: str | None
    lat: float
    lon: float
    prob_by_day: dict = field(default_factory=dict)   # date -> exceedance probability


@dataclass
class ClosureIn:
    id: int
    source: str
    geom: BaseGeometry
    confirmed: bool
    reason: str | None
    valid_from: datetime
    valid_to: datetime | None


def metres_to_deg(m: float, lat: float) -> tuple[float, float]:
    dlat = m / 111_320
    dlon = dlat / max(math.cos(math.radians(lat)), 0.01)
    return dlon, dlat


def circle(lon: float, lat: float, radius_m: float, n: int = 16) -> Polygon:
    dlon, dlat = metres_to_deg(radius_m, lat)
    return Polygon([(lon + dlon * math.cos(2 * math.pi * i / n), lat + dlat * math.sin(2 * math.pi * i / n)) for i in range(n)])


def buffer_m(geom: BaseGeometry, metres: float) -> BaseGeometry:
    c = geom.centroid
    dlon, dlat = metres_to_deg(metres, c.y)
    return geom.buffer((dlon + dlat) / 2, quad_segs=4)


def active_at(t: datetime, start: datetime | None, end: datetime | None) -> bool:
    if start and t < start - timedelta(hours=1):  # allow an hour of lead before stated onset
        return False
    if end and t > end:
        return False
    return True


def zones_at(
    t: datetime,
    hazards: list[HazardIn],
    crossings: list[CrossingIn],
    closures: list[ClosureIn],
    road_impact: dict[str, float],
    hard_flood_prob: float,
) -> list[Zone]:
    zones: list[Zone] = []
    for h in hazards:
        w = road_impact.get(h.kind)
        if not w or not active_at(t, h.onset, h.expires):
            continue
        risk = w * max(h.severity, 1) / 4 * h.certainty
        if risk >= IGNORE_BELOW:
            zones.append(Zone(h.geom, risk, False, f"{h.kind.replace('_', ' ')}: {h.title}", "hazard", h.id))
    day = t.date()
    for c in crossings:
        p = c.prob_by_day.get(day)
        if p is None or p < IGNORE_BELOW:
            continue
        hard = p >= hard_flood_prob
        zones.append(Zone(circle(c.lon, c.lat, CROSSING_RADIUS_M), p, hard,
                          f"river at {c.name or 'unnamed bridge'}: {round(p * 100)}% chance above flood level on {day}",
                          "river_point", str(c.id)))
    for cl in closures:
        if not active_at(t, cl.valid_from, cl.valid_to):
            continue
        g = buffer_m(cl.geom, CLOSURE_BUFFER_M)
        if cl.confirmed:
            zones.append(Zone(g, 1.0, True, f"closed ({cl.source}): {cl.reason or 'road closed'}", "closure", str(cl.id)))
        else:
            zones.append(Zone(g, UNCONFIRMED_CLOSURE_RISK, False,
                              f"unconfirmed report: {cl.reason or 'road blocked'}", "closure", str(cl.id)))
    return zones


def _polys(geom: BaseGeometry) -> list[Polygon]:
    if geom.is_empty:
        return []
    if isinstance(geom, Polygon):
        return [geom]
    return [g for g in getattr(geom, "geoms", []) if isinstance(g, Polygon)]


def custom_model(zones: list[Zone], simplify_deg: float = 0.0005, max_areas: int = 150) -> dict | None:
    """Merge zones into priority buckets and emit a GraphHopper custom model."""
    groups: dict[float, list[BaseGeometry]] = {}
    for z in zones:
        if z.hard:
            groups.setdefault(0.0, []).append(z.geom)
            continue
        for floor, mult in BUCKETS:
            if z.risk >= floor:
                groups.setdefault(mult, []).append(z.geom)
                break
    if not groups:
        return None
    features, priority = [], []
    # Hard first so it wins when areas overlap; GraphHopper multiplies matching rules.
    for mult in sorted(groups):
        merged = unary_union(groups[mult]).simplify(simplify_deg, preserve_topology=True)
        for i, poly in enumerate(_polys(merged)):
            if len(features) >= max_areas:
                break
            aid = f"z{str(mult).replace('.', '')}_{i}"
            features.append({"type": "Feature", "id": aid, "properties": {}, "geometry": mapping(poly)})
            priority.append({"if": f"in_{aid}", "multiply_by": str(mult)})
    return {"priority": priority, "areas": {"type": "FeatureCollection", "features": features}}


def exposure(route: LineString, zones: list[Zone]) -> tuple[float, bool, list[dict]]:
    """Worst soft risk on the route, whether it touches a hard zone, and the zones crossed."""
    worst, hard, crossed = 0.0, False, []
    for z in zones:
        if route.intersects(z.geom):
            crossed.append(z.as_ref())
            if z.hard:
                hard = True
            else:
                worst = max(worst, z.risk)
    crossed.sort(key=lambda r: (not r["hard"], -r["risk"]))
    return worst, hard, crossed


def haversine_km(lon1, lat1, lon2, lat2) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def point(lon: float, lat: float) -> Point:
    return Point(lon, lat)
