"""GDACS (UN / European Commission) cyclone and flood events with footprint polygons."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from shapely.geometry import box, shape
from shapely.ops import unary_union

from ..engine.classify import GDACS_KIND, GDACS_SEVERITY
from ..models import HazardRecord
from ..provenance import Fetcher
from .cap import parse_time, to_multipolygon

log = logging.getLogger(__name__)


def _truthy(v) -> bool:
    return str(v).strip().lower() in ("true", "1", "yes")


def relevant(feature: dict, bbox: tuple[float, float, float, float]) -> bool:
    p = feature.get("properties", {})
    if (p.get("iso3") or "").upper() == "IND":
        return True
    for c in p.get("affectedcountries") or []:
        if (c.get("iso3") or "").upper() == "IND":
            return True
    fb = feature.get("bbox")
    region = box(*bbox)
    if fb and len(fb) == 4:
        return box(*fb).intersects(region)
    geom = feature.get("geometry")
    if geom:
        try:
            return shape(geom).intersects(region)
        except Exception:
            return False
    return False


def footprint(collection: dict):
    """Union of the polygon features GDACS publishes for an episode
    (cyclone wind bands and forecast cone, flood extents). Lines and points are ignored."""
    polys = []
    classes = []
    for f in collection.get("features", []):
        g = f.get("geometry") or {}
        if g.get("type") not in ("Polygon", "MultiPolygon"):
            continue
        try:
            geom = shape(g)
        except Exception:
            continue
        if not geom.is_valid:
            geom = geom.buffer(0)
        if not geom.is_empty:
            polys.append(geom)
            cls = (f.get("properties") or {}).get("Class") or (f.get("properties") or {}).get("polygonlabel")
            if cls:
                classes.append(str(cls))
    if not polys:
        return None, classes
    return to_multipolygon(unary_union(polys)), sorted(set(classes))


async def read_gdacs(fetcher: Fetcher, base: str, bbox) -> tuple[bool, int | None, list[HazardRecord], list[str]]:
    today = datetime.now(UTC).date()
    params = {
        "eventlist": "TC;FL",
        "fromdate": (today - timedelta(days=14)).isoformat(),
        "todate": today.isoformat(),
        "alertlevel": "Green;Orange;Red",
    }
    idx = await fetcher.get("gdacs", f"{base}/events/geteventlist/SEARCH", params=params)
    errors: list[str] = []
    if not idx.ok:
        return False, idx.raw_id, [], [f"event list: {idx.error}"]
    try:
        data = idx.json()
    except ValueError as exc:
        return False, idx.raw_id, [], [f"event list not JSON: {exc}"]

    records: list[HazardRecord] = []
    for feat in data.get("features", []):
        p = feat.get("properties", {})
        if not relevant(feat, bbox):
            continue
        etype = p.get("eventtype")
        eid = p.get("eventid")
        ep = p.get("episodeid")
        if not etype or eid is None:
            continue
        current = _truthy(p.get("iscurrent", "true"))

        geom_url = (p.get("url") or {}).get("geometry") or (
            f"{base}/polygons/getgeometry?eventtype={etype}&eventid={eid}&episodeid={ep}"
        )
        g = await fetcher.get("gdacs", geom_url, retries=1)
        geom, classes = (None, [])
        if g.ok:
            try:
                geom, classes = footprint(g.json())
            except ValueError as exc:
                errors.append(f"{etype}{eid} geometry not JSON: {exc}")
        else:
            errors.append(f"{etype}{eid} geometry: {g.error}")

        level = (p.get("alertlevel") or "").lower()
        onset = parse_time(p.get("fromdate"))
        last = parse_time(p.get("todate"))
        if onset and onset.tzinfo is None:
            onset = onset.replace(tzinfo=UTC)
        if last and last.tzinfo is None:
            last = last.replace(tzinfo=UTC)
        sev = p.get("severitydata") or {}
        records.append(
            HazardRecord(
                source="gdacs",
                source_ref=f"{etype}:{eid}",
                kind=GDACS_KIND.get(etype, "other"),
                title=p.get("name") or p.get("eventname") or f"GDACS {etype} {eid}",
                severity=GDACS_SEVERITY.get(level, 1),
                certainty=0.7 if etype == "TC" else 0.9,
                urgency="Expected" if etype == "TC" else "Immediate",
                onset=onset,
                # GDACS has no expiry; keep an event alive two days past its last update.
                expires=(last + timedelta(days=2)) if last else None,
                geom=geom,
                located_by="gdacs" if geom is not None else "none",
                area_desc=p.get("country"),
                sender="GDACS",
                sent=parse_time(p.get("datemodified")) or last,
                msg_type="Alert" if current else "Cancel",
                source_url=(p.get("url") or {}).get("report"),
                raw_fetch_id=g.raw_id if g.ok else idx.raw_id,
                attrs={
                    "episode": ep,
                    "alertlevel": p.get("alertlevel"),
                    "severity_text": sev.get("severitytext"),
                    "severity_value": sev.get("severity"),
                    "severity_unit": sev.get("severityunit"),
                    "polygon_classes": classes,
                    "description": p.get("description"),
                    "iscurrent": current,
                },
            )
        )
    return True, idx.raw_id, records, errors
