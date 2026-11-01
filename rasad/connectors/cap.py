"""Parser for Common Alerting Protocol (CAP 1.1 / 1.2) messages.

SACHET (NDMA) and IMD both publish CAP. One CAP alert can hold several <info>
blocks (one per language) and several <area> blocks; RASAD keeps the English
info block when present and unions every area into one footprint.
"""

from __future__ import annotations

import math
from datetime import datetime

from defusedxml import ElementTree as ET
from shapely import make_valid
from shapely.geometry import MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from ..engine.classify import certainty_of, classify, severity_of
from ..models import HazardRecord


class CapParseError(ValueError):
    pass


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _child(el, name: str):
    for c in el:
        if _local(c.tag) == name:
            return c
    return None


def _children(el, name: str):
    return [c for c in el if _local(c.tag) == name]


def _text(el, name: str) -> str | None:
    c = _child(el, name)
    if c is None or c.text is None:
        return None
    t = c.text.strip()
    return t or None


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    v = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(v)
    except ValueError:
        return None


def parse_polygon(text: str) -> Polygon | None:
    """CAP polygon: space-separated 'lat,lon' pairs, first equals last."""
    pts = []
    for pair in text.split():
        try:
            lat_s, lon_s = pair.split(",")[:2]
            pts.append((float(lon_s), float(lat_s)))
        except ValueError:
            continue
    if len(pts) < 3:
        return None
    if pts[0] != pts[-1]:
        pts.append(pts[0])
    if len(pts) < 4:
        return None
    poly = Polygon(pts)
    if not poly.is_valid:
        fixed = make_valid(poly)
        polys = [g for g in getattr(fixed, "geoms", [fixed]) if isinstance(g, Polygon)]
        if not polys:
            return None
        poly = max(polys, key=lambda g: g.area)
    return poly if poly.area > 0 else None


def parse_circle(text: str) -> Polygon | None:
    """CAP circle: 'lat,lon radius_km'. Built as a 48-point ring in degrees,
    stretched in longitude by 1/cos(lat) so it stays round on the ground."""
    try:
        centre, radius = text.split()
        lat, lon = (float(x) for x in centre.split(",")[:2])
        r_km = float(radius)
    except ValueError:
        return None
    if r_km <= 0:
        return None
    dlat = r_km / 111.32
    dlon = dlat / max(math.cos(math.radians(lat)), 0.01)
    ring = [
        (lon + dlon * math.cos(2 * math.pi * i / 48), lat + dlat * math.sin(2 * math.pi * i / 48))
        for i in range(48)
    ]
    return Polygon(ring)


def to_multipolygon(geom: BaseGeometry | None) -> MultiPolygon | None:
    if geom is None or geom.is_empty:
        return None
    if isinstance(geom, Polygon):
        return MultiPolygon([geom])
    if isinstance(geom, MultiPolygon):
        return geom
    polys = [g for g in getattr(geom, "geoms", []) if isinstance(g, Polygon)]
    return MultiPolygon(polys) if polys else None


def _pick_info(alert):
    infos = _children(alert, "info")
    if not infos:
        return None
    for info in infos:
        lang = (_text(info, "language") or "en").lower()
        if lang.startswith("en"):
            return info
    return infos[0]


def parse_cap(xml: bytes | str, source: str, source_url: str | None = None, raw_fetch_id: int | None = None) -> HazardRecord:
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as exc:
        raise CapParseError(f"not XML: {exc}") from exc
    if _local(root.tag) != "alert":
        raise CapParseError(f"root element is <{_local(root.tag)}>, not <alert>")

    identifier = _text(root, "identifier")
    if not identifier:
        raise CapParseError("CAP alert has no identifier")
    msg_type = _text(root, "msgType") or "Alert"
    refs_raw = _text(root, "references") or ""
    references = []
    for triple in refs_raw.split():
        parts = triple.split(",")
        if len(parts) >= 2:
            references.append(parts[1])

    info = _pick_info(root)
    if info is None:
        raise CapParseError("CAP alert has no <info> block")

    event = _text(info, "event")
    headline = _text(info, "headline")
    description = _text(info, "description")

    shapes: list[Polygon] = []
    located_by = "none"
    area_descs: list[str] = []
    geocodes: list[dict] = []
    for area in _children(info, "area"):
        if (d := _text(area, "areaDesc")):
            area_descs.append(d)
        for p in _children(area, "polygon"):
            if p.text and (poly := parse_polygon(p.text)):
                shapes.append(poly)
                located_by = "polygon"
        for c in _children(area, "circle"):
            if c.text and (circ := parse_circle(c.text)):
                shapes.append(circ)
                if located_by != "polygon":
                    located_by = "circle"
        for g in _children(area, "geocode"):
            name, value = _text(g, "valueName"), _text(g, "value")
            if name and value:
                geocodes.append({"name": name, "value": value})

    geom = to_multipolygon(unary_union(shapes)) if shapes else None
    if geom is None:
        located_by = "none"

    title = headline or event or (description or "")[:120] or "Untitled alert"
    return HazardRecord(
        source=source,
        source_ref=identifier,
        kind=classify(event, headline, description),
        title=title,
        severity=severity_of(_text(info, "severity")),
        certainty=certainty_of(_text(info, "certainty")),
        urgency=_text(info, "urgency"),
        onset=parse_time(_text(info, "onset")) or parse_time(_text(info, "effective")),
        expires=parse_time(_text(info, "expires")),
        geom=geom,
        located_by=located_by,
        area_desc="; ".join(area_descs) or None,
        sender=_text(info, "senderName") or _text(root, "sender"),
        sent=parse_time(_text(root, "sent")),
        msg_type=msg_type,
        references=references,
        source_url=source_url,
        raw_fetch_id=raw_fetch_id,
        attrs={
            "event": event,
            "description": description,
            "instruction": _text(info, "instruction"),
            "status": _text(root, "status"),
            "geocodes": geocodes,
            "category": _text(info, "category"),
            "web": _text(info, "web"),
        },
    )
