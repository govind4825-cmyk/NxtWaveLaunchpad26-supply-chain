"""Map free-text alert events to a small set of hazard kinds RASAD reasons about."""

from __future__ import annotations

import re

# Order matters: the first matching kind wins, so specific phrases come before broad ones.
_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("tsunami", ("tsunami",)),
    ("cyclone", ("cyclone", "cyclonic", "deep depression", "depression over", "storm surge", "typhoon")),
    ("landslide", ("landslide", "land slide", "mudslide", "debris flow")),
    ("flood", ("flood", "inundation", "river level", "dam release", "water level", "glof")),
    ("heavy_rain", ("extremely heavy", "very heavy rain", "heavy rain", "heavy rainfall", "cloudburst", "rainfall")),
    ("thunderstorm", ("thunderstorm", "lightning", "squall", "hailstorm", "hail", "gusty wind")),
    ("high_wave", ("high wave", "swell", "rough sea", "high tide", "sea state")),
    ("heatwave", ("heat wave", "heatwave", "hot and humid", "hot day", "warm night")),
    ("cold", ("cold wave", "cold day", "ground frost", "frost")),
    ("fog", ("dense fog", "fog")),
    ("avalanche", ("avalanche",)),
    ("fire", ("forest fire", "wildfire", "fire")),
    ("earthquake", ("earthquake",)),
]

# Kinds that can cut roads, strand people or destroy stock: the ones logistics must react to.
LOGISTICS_KINDS = {"cyclone", "flood", "heavy_rain", "landslide", "tsunami", "high_wave", "avalanche"}

# Kinds for which river crossings are worth watching with GloFAS.
RIVER_KINDS = {"cyclone", "flood", "heavy_rain"}

SEVERITY = {"extreme": 4, "severe": 3, "moderate": 2, "minor": 1, "unknown": 0}
CERTAINTY = {"observed": 1.0, "likely": 0.7, "possible": 0.4, "unlikely": 0.1, "unknown": 0.3}

GDACS_KIND = {"TC": "cyclone", "FL": "flood", "EQ": "earthquake", "DR": "drought", "WF": "fire", "VO": "volcano"}
GDACS_SEVERITY = {"red": 4, "orange": 3, "green": 1}


def classify(*texts: str | None) -> str:
    blob = " ".join(t for t in texts if t).lower()
    blob = re.sub(r"\s+", " ", blob)
    for kind, words in _RULES:
        if any(w in blob for w in words):
            return kind
    return "other"


def severity_of(word: str | None) -> int:
    return SEVERITY.get((word or "unknown").strip().lower(), 0)


def certainty_of(word: str | None) -> float:
    return CERTAINTY.get((word or "unknown").strip().lower(), 0.3)
