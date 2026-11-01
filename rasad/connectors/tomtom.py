"""TomTom Traffic Incident Details: live road closures and flooded roads.

Optional: needs RASAD_TOMTOM_KEY (free tier). Only incidents that close a road
are kept: TomTom iconCategory 8 (road closed) and 11 (flooding), plus any
incident whose description says the road is closed or flooded.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from shapely.geometry import shape

from ..provenance import Fetcher

URL = "https://api.tomtom.com/traffic/services/5/incidentDetails"
FIELDS = "{incidents{type,geometry{type,coordinates},properties{id,iconCategory,startTime,endTime,from,to,events{description}}}}"
CLOSING_CATEGORIES = {8, 11}


@dataclass
class Incident:
    ref: str
    geom: object
    reason: str
    start: datetime | None
    end: datetime | None


def _ts(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_incidents(data: dict) -> list[Incident]:
    out = []
    for inc in data.get("incidents") or []:
        p = inc.get("properties") or {}
        descs = " ".join((e.get("description") or "") for e in p.get("events") or [])
        cat = p.get("iconCategory")
        closing = cat in CLOSING_CATEGORIES or any(w in descs.lower() for w in ("closed", "flood"))
        if not closing or not inc.get("geometry"):
            continue
        try:
            geom = shape(inc["geometry"])
        except Exception:
            continue
        where = " to ".join(x for x in (p.get("from"), p.get("to")) if x)
        out.append(Incident(str(p.get("id")), geom, f"{descs or 'road closed'}{' (' + where + ')' if where else ''}",
                            _ts(p.get("startTime")), _ts(p.get("endTime"))))
    return out


async def closures_in(fetcher: Fetcher, key: str, bbox: tuple[float, float, float, float]):
    w, s, e, n = bbox
    params = {"key": key, "bbox": f"{w:.4f},{s:.4f},{e:.4f},{n:.4f}", "fields": FIELDS,
              "language": "en-GB", "timeValidityFilter": "present"}
    res = await fetcher.get("tomtom", URL, params=params, retries=1)
    if not res.ok:
        return [], res.raw_id, res.error
    try:
        return parse_incidents(res.json()), res.raw_id, None
    except ValueError as exc:
        return [], res.raw_id, f"not JSON: {exc}"
