"""District and state boundaries, used to place CAP alerts that name areas but carry no polygon.

Source file: GADM-derived India districts published at github.com/geohacker/india,
downloaded by scripts/fetch_boundaries.py. Properties used: NAME_1 (state), NAME_2 (district).
"""

from __future__ import annotations

import json
import logging
import re
from difflib import get_close_matches
from pathlib import Path

from shapely.geometry import shape
from shapely.ops import unary_union

from .cap import to_multipolygon

log = logging.getLogger(__name__)

_STOP = re.compile(r"\b(districts?|dist\.?|and|of|the|parts?|areas?|isolated|places|in|over|state|ut)\b")


def norm(name: str) -> str:
    n = name.lower()
    n = re.sub(r"[^a-z0-9\s]", " ", n)
    n = _STOP.sub(" ", n)
    return re.sub(r"\s+", " ", n).strip()


# Current official or common spellings -> the spelling in the boundary file.
# Alerts use today's names; the GADM file uses older transliterations.
ALIASES = {
    "howrah": "haora", "hooghly": "hugli", "paschim medinipur": "west midnapore", "paschim midnapore": "west midnapore",
    "purba medinipur": "east midnapore", "purba midnapore": "east midnapore", "bardhaman": "barddhaman",
    "burdwan": "barddhaman", "purba bardhaman": "barddhaman", "paschim bardhaman": "barddhaman",
    "darjeeling": "darjiling", "cooch behar": "kochbihar", "coochbehar": "kochbihar", "koch bihar": "kochbihar",
    "malda": "maldah", "purulia": "puruliya", "kanyakumari": "kanniyakumari", "tiruchirappalli": "tiruchchirappalli",
    "trichy": "tiruchchirappalli", "tirunelveli": "tirunelveli kattabo", "tuticorin": "thoothukudi",
    "kanchipuram": "kancheepuram", "tiruvallur": "thiruvallur", "tiruvarur": "thiruvarur", "nilgiri": "nilgiris",
    "pathanamthitta": "pattanamtitta", "kasargod": "kasaragod", "balasore": "baleshwar", "bargarh": "baragarh",
    "balangir": "bolangir", "khurda": "khordha", "subarnapur": "sonepur", "nawarangpur": "nabarangpur",
    "dhubri": "dhuburi", "morigaon": "marigaon", "sivasagar": "sibsagar", "dima hasao": "north cachar hills",
    "odisha": "orissa", "uttarakhand": "uttaranchal", "pondicherry": "puducherry",
}


class Boundaries:
    def __init__(self, path: str | Path):
        self.districts: dict[str, list] = {}
        self.states: dict[str, list] = {}
        self.loaded = False
        p = Path(path)
        if not p.exists():
            log.warning("boundary file %s not found; alerts without polygons stay unlocated", p)
            return
        data = json.loads(p.read_text())
        for f in data.get("features", []):
            props = f.get("properties") or {}
            try:
                geom = shape(f["geometry"])
            except Exception:
                continue
            if not geom.is_valid:
                geom = geom.buffer(0)
            d, s = props.get("NAME_2"), props.get("NAME_1")
            if d:
                self.districts.setdefault(norm(d), []).append(geom)
            if s:
                self.states.setdefault(norm(s), []).append(geom)
        self.loaded = bool(self.districts)
        log.info("loaded %d districts, %d states", len(self.districts), len(self.states))

    def locate(self, area_desc: str | None, geocodes: list[dict] | None = None):
        """Return (MultiPolygon, matched names) for names in area_desc / geocodes, or (None, [])."""
        if not self.loaded:
            return None, []
        names: list[str] = []
        if area_desc:
            names += re.split(r"[,;/]|\band\b|\n", area_desc)
        for g in geocodes or []:
            if g.get("value") and not str(g["value"]).isdigit():
                names.append(str(g["value"]))
        shapes, matched = [], []
        for raw in names:
            key = norm(raw)
            if not key:
                continue
            key = ALIASES.get(key, key)
            if key in self.districts:
                shapes += self.districts[key]
                matched.append(raw.strip())
            elif key in self.states:
                shapes += self.states[key]
                matched.append(raw.strip())
            elif len(key) >= 5 and (close := get_close_matches(key, list(self.districts), n=1, cutoff=0.85)):
                shapes += self.districts[close[0]]
                matched.append(f"{raw.strip()} (~{close[0]})")
        if not shapes:
            return None, []
        return to_multipolygon(unary_union(shapes).simplify(0.01, preserve_topology=True)), matched
