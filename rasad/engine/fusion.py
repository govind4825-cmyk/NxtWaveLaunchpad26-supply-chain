"""Decide whether a new agency message describes a hazard RASAD already tracks.

SACHET relays IMD, CWC and state messages, IMD publishes its own CAP feed, and
GDACS models the same cyclones. Without merging, one storm becomes three markers
and three sets of orders.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from shapely.geometry.base import BaseGeometry

from ..connectors.boundaries import norm

# Precision of the footprint; a better-located message replaces a worse one.
LOCATION_RANK = {"polygon": 4, "circle": 3, "gdacs": 3, "district": 2, "none": 0}

IOU_MATCH = 0.4
CONTAIN_MATCH = 0.8


@dataclass
class Candidate:
    id: str
    kind: str
    geom: BaseGeometry | None
    area_desc: str | None
    onset: datetime | None
    expires: datetime | None


def _overlap_in_time(a0, a1, b0, b1) -> bool:
    if a0 and b1 and a0 > b1:
        return False
    if b0 and a1 and b0 > a1:
        return False
    return True


def spatial_match(a: BaseGeometry, b: BaseGeometry) -> float:
    """Return a match score: IoU, or containment share when one footprint sits inside the other."""
    if not a.intersects(b):
        return 0.0
    inter = a.intersection(b).area
    if inter == 0:
        return 0.0
    union = a.union(b).area
    iou = inter / union if union else 0.0
    contain = inter / min(a.area, b.area) if min(a.area, b.area) else 0.0
    if contain >= CONTAIN_MATCH:
        return max(iou, IOU_MATCH)
    return iou


def best_match(kind: str, geom, area_desc, onset, expires, candidates: list[Candidate]) -> str | None:
    best, best_score = None, 0.0
    for c in candidates:
        if c.kind != kind:
            continue
        if not _overlap_in_time(onset, expires, c.onset, c.expires):
            continue
        if geom is not None and c.geom is not None:
            score = spatial_match(geom, c.geom)
            if score >= IOU_MATCH and score > best_score:
                best, best_score = c.id, score
        elif geom is None and c.geom is None and area_desc and c.area_desc:
            if norm(area_desc) == norm(c.area_desc):
                return c.id
    return best
