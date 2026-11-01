"""Enter a demo scenario through the public API, the same way an officer would on the map.

    python scripts/demo_setup.py demo/andorra.json [--api http://localhost:8000] [--accept] [--drivers]

Without flags it adds depots, shelters with headcounts and suppliers with stock, then
asks the Cartographer and Quartermaster to run. Orders appear on their own; accept them
live from each supplier page (links are printed), or pass --accept to accept every open
order. --drivers registers the drivers, which makes the Dispatcher plan trips.
Every step goes through the API, so nothing is written to the database directly.
"""

import argparse
import json
import sys

import httpx


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("scenario")
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--accept", action="store_true", help="accept every order waiting on these suppliers")
    ap.add_argument("--drivers", action="store_true", help="register the scenario's drivers")
    a = ap.parse_args()
    sc = json.load(open(a.scenario))
    c = httpx.Client(base_url=a.api, timeout=60)

    def post(path, body):
        r = c.post(path, json=body)
        if r.status_code >= 400:
            print(f"  ! {path}: {r.status_code} {r.text[:200]}")
            return None
        return r.json()

    if c.get("/health").status_code != 200:
        print("API not reachable at", a.api)
        return 1
    links = json.load(open(".demo_links.json")) if a.accept or a.drivers else {}
    if not (a.accept or a.drivers):
        depot_ids = []
        for d in sc["depots"]:
            r = post("/api/depots", d)
            depot_ids.append(r and r["id"])
            print("depot", d["name"], r)
        for s in sc["shelters"]:
            r = post("/api/shelters", {"name": s["name"], "lat": s["lat"], "lon": s["lon"]})
            print("shelter", s["name"], r)
            if r and s.get("people"):
                post(f"/api/shelters/{r['id']}/headcount", {"people": s["people"], "reporter": "camp in-charge"})
        links = {"suppliers": {}, "depot": depot_ids[0] if depot_ids else None}
        for sp in sc["suppliers"]:
            r = post("/api/suppliers", {k: sp[k] for k in ("name", "phone", "kind", "lat", "lon")})
            if not r:
                continue
            token = r["link"].rsplit("/", 1)[1]
            links["suppliers"][sp["name"]] = token
            print("supplier", sp["name"], r["link"])
            for line in sp.get("stock", []):
                rep = post(f"/api/supplier/{token}/message", {"text": line})
                print("   ", rep and rep["reply"])
        json.dump(links, open(".demo_links.json", "w"))
        for agent in ("cartographer", "quartermaster"):
            post(f"/api/refresh/{agent}", {})
        print("\nAccess windows and needs compute in about a minute; orders follow within a few minutes.")

    if a.accept:
        for name, token in links["suppliers"].items():
            view = c.get(f"/api/supplier/{token}").json()
            for o in view["orders"]:
                if o["status"] == "Created":
                    rep = post(f"/api/supplier/{token}/message", {"text": f"ACCEPT #{o['id']}"})
                    print(name, rep and rep["reply"])
    if a.drivers:
        for d in sc["drivers"]:
            r = post("/api/drivers", {**d, "depot_id": links.get("depot")})
            print("driver", d["name"], r and r["link"])
        print("\nThe Dispatcher plans trips within about a minute; watch the Trips tab.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
