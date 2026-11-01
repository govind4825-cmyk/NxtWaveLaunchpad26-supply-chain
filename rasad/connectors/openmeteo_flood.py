"""Open-Meteo Global Flood API (GloFAS v4): river discharge forecast and reanalysis."""

from __future__ import annotations

from dataclasses import dataclass

from ..provenance import Fetcher

ENSEMBLE_VARS = [
    "river_discharge",
    "river_discharge_min",
    "river_discharge_p25",
    "river_discharge_median",
    "river_discharge_p75",
    "river_discharge_max",
]


@dataclass
class Forecast:
    raw_id: int | None
    model_lat: float | None
    model_lon: float | None
    days: list[dict]          # one dict per day with discharge and ensemble summary
    has_ensemble: bool
    no_river: bool


def _daily_rows(data: dict) -> list[dict]:
    daily = data.get("daily") or {}
    times = daily.get("time") or []
    rows = []
    for i, day in enumerate(times):
        def g(name):
            arr = daily.get(name)
            return arr[i] if arr and i < len(arr) else None

        rows.append(
            {
                "day": day,
                "discharge": g("river_discharge"),
                "q_min": g("river_discharge_min"),
                "q_p25": g("river_discharge_p25"),
                "q_median": g("river_discharge_median"),
                "q_p75": g("river_discharge_p75"),
                "q_max": g("river_discharge_max"),
            }
        )
    return rows


async def forecast(fetcher: Fetcher, base: str, lat: float, lon: float, days: int = 16) -> Forecast | None:
    params = {"latitude": f"{lat:.5f}", "longitude": f"{lon:.5f}", "daily": ",".join(ENSEMBLE_VARS), "forecast_days": days}
    res = await fetcher.get("openmeteo_flood", base, params=params)
    has_ens = True
    if not res.ok and res.status == 400:
        # Fall back to the deterministic run if ensemble summaries are refused.
        params["daily"] = "river_discharge"
        res = await fetcher.get("openmeteo_flood", base, params=params)
        has_ens = False
    if not res.ok:
        return None
    data = res.json()
    rows = _daily_rows(data)
    no_river = all(r["discharge"] is None for r in rows)
    return Forecast(res.raw_id, data.get("latitude"), data.get("longitude"), rows, has_ens, no_river)


async def history(fetcher: Fetcher, base: str, lat: float, lon: float) -> tuple[int | None, list[str], list]:
    params = {
        "latitude": f"{lat:.5f}",
        "longitude": f"{lon:.5f}",
        "daily": "river_discharge",
        "start_date": "1984-01-01",
        "end_date": "2022-07-31",
    }
    res = await fetcher.get("openmeteo_flood", base, params=params)
    if not res.ok:
        return res.raw_id, [], []
    daily = res.json().get("daily") or {}
    return res.raw_id, daily.get("time") or [], daily.get("river_discharge") or []
