"""Download India district boundaries used to place CAP alerts that only name districts.

Source: github.com/geohacker/india (GADM-derived), properties NAME_1 = state, NAME_2 = district.
Run once:  python scripts/fetch_boundaries.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx

URL = "https://raw.githubusercontent.com/geohacker/india/master/district/india_district.geojson"
OUT = Path(__file__).resolve().parents[1] / "data" / "india_district.geojson"


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    print(f"downloading {URL}")
    with httpx.Client(timeout=120, follow_redirects=True) as c:
        r = c.get(URL)
        r.raise_for_status()
    data = r.json()
    feats = data.get("features", [])
    if not feats or "NAME_2" not in (feats[0].get("properties") or {}):
        print("unexpected file format; not saved", file=sys.stderr)
        return 1
    OUT.write_text(json.dumps(data))
    print(f"saved {len(feats)} districts to {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
