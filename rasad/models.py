"""Plain data records passed between connectors, the fusion step and storage."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from shapely.geometry.base import BaseGeometry


@dataclass
class HazardRecord:
    source: str                 # sachet | imd_cap | gdacs
    source_ref: str             # unique per source (CAP identifier, GDACS TC:1001234:7)
    kind: str                   # cyclone | flood | heavy_rain | landslide | ...
    title: str
    severity: int               # 0..4
    certainty: float            # 0..1
    urgency: str | None
    onset: datetime | None
    expires: datetime | None
    geom: BaseGeometry | None   # MultiPolygon in EPSG:4326, or None
    located_by: str             # polygon | circle | district | gdacs | none
    area_desc: str | None = None
    sender: str | None = None
    sent: datetime | None = None
    msg_type: str = "Alert"     # Alert | Update | Cancel
    references: list[str] = field(default_factory=list)  # CAP identifiers this message replaces
    source_url: str | None = None
    raw_fetch_id: int | None = None
    attrs: dict = field(default_factory=dict)
