"""WorldPop population inside a polygon (100 m gridded estimates, University of Southampton).

The stats service runs as an asynchronous task: submit the polygon, then poll
/v1/tasks/{taskid} until it reports finished. An error can come back inside an
HTTP 200, so the task body is always checked.
"""

from __future__ import annotations

import asyncio
import json

from shapely.geometry import mapping
from shapely.geometry.base import BaseGeometry

from ..provenance import Fetcher

POLL_SECONDS = 2.0
POLL_TRIES = 30


def _round(coords, nd=4):
    if isinstance(coords, (int, float)):
        return round(coords, nd)
    return [_round(c, nd) for c in coords]


def to_geojson_param(geom: BaseGeometry, as_feature_collection: bool) -> str:
    g = geom.simplify(0.002, preserve_topology=True)
    gj = mapping(g)
    gj = {"type": gj["type"], "coordinates": _round(gj["coordinates"])}
    if as_feature_collection:
        gj = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {}, "geometry": gj}]}
    return json.dumps(gj, separators=(",", ":"))


async def population(fetcher: Fetcher, base: str, geom: BaseGeometry, year: int) -> tuple[float | None, int | None, str | None]:
    """Returns (total_population, raw_fetch_id of the finished task, error)."""
    last_err = None
    for as_fc in (True, False):   # the documented FeatureCollection form first, bare geometry as fallback
        params = {"dataset": "wpgppop", "year": str(year), "geojson": to_geojson_param(geom, as_fc), "runasync": "true"}
        sub = await fetcher.get("worldpop", f"{base}/services/stats", params=params, retries=1)
        if not sub.ok:
            last_err = sub.error
            continue
        try:
            body = sub.json()
        except ValueError:
            last_err = "submit response not JSON"
            continue
        pop, raw_id, err = await _follow(fetcher, base, body, sub.raw_id)
        if pop is not None:
            return pop, raw_id, None
        last_err = err
        if err and "geojson" not in err.lower():
            break   # not a format problem, retrying the other form will not help
    return None, None, last_err


async def _follow(fetcher: Fetcher, base: str, body: dict, raw_id: int | None):
    task = body.get("taskid")
    for _ in range(POLL_TRIES + 1):
        if body.get("error"):
            return None, raw_id, str(body.get("error_message") or "WorldPop task error")
        if body.get("status") == "finished":
            pop = (body.get("data") or {}).get("total_population")
            if pop is None:
                return None, raw_id, "finished without total_population"
            return float(pop), raw_id, None
        task = body.get("taskid") or task
        if not task:
            return None, raw_id, f"unexpected response: {str(body)[:200]}"
        await asyncio.sleep(POLL_SECONDS)
        res = await fetcher.get("worldpop", f"{base}/tasks/{task}", retries=1)
        if not res.ok:
            return None, res.raw_id, res.error
        raw_id = res.raw_id
        try:
            body = res.json()
        except ValueError:
            return None, raw_id, "task response not JSON"
    return None, raw_id, "WorldPop task did not finish in time"
