"""River flood maths on GloFAS discharge.

Thresholds come from each river cell's own reanalysis history (1984 onward), so
nothing is typed in by hand. The flood threshold is the median of annual maximum
daily discharge, the usual estimate of the 2-year return level (close to
bankfull). The warning level is the 95th percentile of daily discharge.
"""

from __future__ import annotations

import statistics
from collections import defaultdict


def thresholds_from_history(times: list[str], values: list[float | None]) -> dict | None:
    by_year: dict[str, list[float]] = defaultdict(list)
    daily: list[float] = []
    for t, v in zip(times, values):
        if v is None:
            continue
        by_year[t[:4]].append(v)
        daily.append(v)
    full_years = [max(vs) for vs in by_year.values() if len(vs) >= 300]
    if len(full_years) < 10 or not daily:
        return None
    if max(daily) <= 0:
        return None
    daily.sort()
    p95 = daily[min(len(daily) - 1, int(0.95 * len(daily)))]
    return {
        "flood_m3s": statistics.median(full_years),
        "warning_m3s": p95,
        "years": len(full_years),
        "method": f"median annual max ({len(full_years)} yrs) / p95 daily, GloFAS reanalysis",
    }


def exceedance_probability(
    threshold: float | None,
    q_min: float | None,
    q_p25: float | None,
    q_median: float | None,
    q_p75: float | None,
    q_max: float | None,
) -> float | None:
    """P(discharge > threshold) from the ensemble summary Open-Meteo returns.

    The empirical CDF is taken as piecewise linear through
    (min, 0), (p25, 0.25), (median, 0.5), (p75, 0.75), (max, 1).
    Returns None when any input is missing; never guesses.
    """
    pts = [q_min, q_p25, q_median, q_p75, q_max]
    if threshold is None or any(v is None for v in pts):
        return None
    xs = [float(v) for v in pts]  # type: ignore[arg-type]
    ps = [0.0, 0.25, 0.5, 0.75, 1.0]
    # Guard against non-monotone summaries.
    for i in range(1, 5):
        xs[i] = max(xs[i], xs[i - 1])
    if threshold <= xs[0]:
        return 1.0
    if threshold >= xs[-1]:
        return 0.0
    for i in range(1, 5):
        if threshold <= xs[i]:
            x0, x1 = xs[i - 1], xs[i]
            if x1 == x0:
                cdf = ps[i]
            else:
                cdf = ps[i - 1] + (threshold - x0) / (x1 - x0) * (ps[i] - ps[i - 1])
            return round(1.0 - cdf, 4)
    return 0.0
