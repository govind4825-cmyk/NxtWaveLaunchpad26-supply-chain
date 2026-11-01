# Live demo runbook (about 6 minutes)

## Before the slot (30 minutes ahead)

```bash
cp .env.example .env            # set OSM_PBF_URL to your state's Geofabrik extract
docker compose -f infra/docker-compose.yml up -d
python scripts/fetch_boundaries.py
curl localhost:8989/health      # GraphHopper builds the road graph on first start: wait for OK
curl localhost:8000/health      # every feed should say ok or waiting, none down
```

Copy `demo/andorra.json` to `demo/<state>.json` and move the depot, shelters and suppliers onto roads in your extract (check each point with `curl "localhost:8989/nearest?point=LAT,LON"`: distance under 100 m). Then:

```bash
python scripts/demo_setup.py demo/<state>.json
```

Open three windows: the control room (`localhost:8000`), one supplier page and one driver page on a phone (links are printed by the script and the Trips tab).

## On stage

| # | Do | Say | Judges see |
|---|---|---|---|
| 1 | Show the Live feeds panel and click a hazard | “Live NDMA, IMD and GDACS alerts. Every number links to the raw fetch.” | Provenance drawer with the source CAP file |
| 2 | Access windows tab, drag the time bar to +24 h | “Each shelter gets a clock: when its last safe road closes.” | Shelters change colour as zones spread |
| 3 | Open `/ledger` | “Needs from headcounts and Sphere standards, public for donors.” | What is still short, where and by when |
| 4 | Orders tab | “The Procurer bought from local shops over signed Beckn messages.” | Orders, one held by the Warden over the Rs 50,000 cap |
| 5 | Approve the held order; on the supplier phone type `ACCEPT #<id>` | “Suppliers only need WhatsApp.” | Order turns accepted |
| 6 | `python scripts/demo_setup.py demo/<state>.json --drivers`, then the Trips tab | “The Dispatcher splits loads across trucks and plans around closing roads.” | Trips on the map and the driver’s phone |
| 7 | Report blocked road on a trip’s route twice (two reporter names) | “Two reports confirm a closure. Watch the trucks.” | Reroute in the ticker, new route on the driver page |
| 8 | On the driver page tap Picked up, then Delivered | “Delivery closes the loop.” | Order delivered; ledger updates |

## If something goes wrong

- A feed shows down: say so, it is the point. RASAD never fills gaps with made-up data.
- No orders after 3 minutes: `curl -XPOST localhost:8000/api/refresh/procurer`.
- No trips: check the Trips tab’s “Waiting for a truck” reasons; `curl -XPOST localhost:8000/api/refresh/dispatcher`.
- Stuck accepts: `python scripts/demo_setup.py demo/<state>.json --accept`.
