# RASAD: anticipatory relief logistics swarm

RASAD (रसद, "provisions") reads India's official hazard feeds, works out which communities are about to be cut off, and moves relief to them before the last safe road closes. This repository is the build for Theme 2, Problem 4: Autonomous Emergency Response & Supply Chain Logistics Swarm.

**Status:** all five agents run end to end. Live alerts become risk zones on the road network, every shelter in an alert area gets an access window, needs are published on a public Needs Ledger, the Procurer orders what is short from local suppliers over Beckn (checked by the Warden), and the Dispatcher turns accepted orders into driver trips that are rerouted live when roads close. The LLM layer is next; see [Roadmap](#roadmap). For the judges' walkthrough, see [DEMO.md](DEMO.md).

## What runs today

- **Sentinel** polls NDMA SACHET and IMD CAP feeds every 5 minutes and GDACS every 15 minutes, parses CAP 1.1/1.2 and GDACS footprints, and turns them into hazards in PostGIS.
- **Fusion** merges the same event reported by several agencies (same kind, overlapping time, footprint IoU ≥ 0.4 or containment ≥ 80%), applies CAP `Update` and `Cancel` messages through their `references`, and keeps the most precise footprint.
- **District fallback** places CAP alerts that only name districts, using GADM district boundaries with alias and fuzzy matching. These are drawn dashed and flagged "placed by district name".
- **River watch** finds road bridges over rivers inside moderate-or-worse flood, rain and cyclone hazards (OpenStreetMap Overpass), derives each river cell's own flood level from GloFAS reanalysis (median annual maximum, 1984–2022), and tracks the share of the GloFAS ensemble forecast above it for the next 16 days.
- **Provenance**: every HTTP call is stored in `raw_fetch` with URL, time, status and payload. Every hazard, source and forecast row points at the fetch it came from, and the UI links to `/api/raw/{id}`.
- **Live GPS** from drivers' phones through a Traccar server and its WebSocket, with REST polling as fallback.
- **Cartographer** builds risk zones for now, +6, +12, +24, +48 and +72 hours from hazard footprints (playbook weight × severity × certainty), river crossings (GloFAS exceedance probability; hard closure at 80%), TomTom closures and field reports. It sends them to a self-hosted GraphHopper as a per-request custom model: soft zones lower route priority, hard zones forbid passage.
- **Access windows**: for each shelter, routes from the three nearest depots in every time slice. The window closes at the first slice with no safe route (no hard zone, worst soft risk under 0.5). Each result says which zones the route crosses or what blocked it, with the hazard, river point or closure it came from. Shelters come from OpenStreetMap inside each alert (designated shelters, community halls, town halls, hospitals, schools) or are added by the officer; depots are added by the officer.
- **Field reports with corroboration**: one report of a blocked road is a suspected closure (penalised, not forbidden). A second, different reporter within 300 m and 6 hours, a TomTom closure, or the officer confirms it, and routes are recomputed around it. A reroute that adds 50% and 10 minutes or more raises an event.
- **Quartermaster** splits each hazard into shelter catchments (Voronoi cells clipped to the footprint) and asks WorldPop how many people live in each. People per shelter = the in-charge's latest headcount, or until one arrives, residents × an officer-set displacement planning rate (shown as "estimate"). If neither exists the shelter gets no quantities, only a request for a headcount. Need = people × Sphere minimums (15 L water, 14 kg cereal a month, 250 g soap a month, two water containers per household) × days until the alert ends plus two, due 3 hours before the access window closes, ranked by people × urgency. Every line keeps the WorldPop fetch, headcount and access window it came from.
- **Public Needs Ledger** at `/ledger`: what is still short, where and by when, which items are covered and should not be sent, and how many shelters are still waiting for a count. Built for donors and volunteers on a phone; the counter to the surplus that piled up in Wayanad.
- **Procurer** searches the Beckn network for each short item at the most urgent shelters, ranks offers (safe road route from the supplier first, then ETA, price, and the supplier's track record), splits an order across suppliers when one cannot fill it, and runs select → init → confirm. Orders a supplier does not accept within 20 minutes are cancelled and re-sourced; a supplier that rejects is skipped until its catalog shows a stock update made after the rejection.
- **Warden** checks every order immediately before Beckn `confirm` against the officer's mandate: item allowed, quantity within 1.5× of what is still short, need basis and catalog on record, stock updated within 48 h, price stated, order and daily spend caps, a verified safe road route, and arrival before the need is due. Failing a hard rule blocks the order; failing a soft one sends it to the officer's approval queue with the reasons.
- **Beckn node**: RASAD is both a BAP (buying) and a BPP (serving its own supplier network), speaking Beckn core 1.1 with ed25519 signatures over BLAKE2b-512 digests as in BECKN-006 (the digest matches the spec's published test vector). Every message in or out is signature-checked and logged. Other networks join as peers with their public key, or through a Beckn registry lookup. A second RASAD node runs as an independent partner network in the integration test.
- **Suppliers** (shops, wholesalers, NGO warehouses) register on the map and update stock by WhatsApp text (`STOCK rice 800 kg @ 42`, Hindi item words accepted) or a phone page at their private link. They get orders on WhatsApp (Cloud API) or in the page's outbox, and reply ACCEPT, REJECT or READY. Accepted stock is reserved, and the need's committed quantity updates on the ledger.
- **Control room** (MapLibre): hazards by severity, road-risk zones with a time slider, shelters coloured by access window, the chosen route, closures, depots, river crossings, vehicles, feed health, a provenance drawer and a live event ticker. Officers add depots and shelters and log field reports by clicking the map.

## Architecture

```mermaid
flowchart LR
  subgraph Live inputs
    S[SACHET CAP feed]
    I[IMD CAP feed]
    G[GDACS events + polygons]
    O[OSM Overpass bridges]
    F[GloFAS via Open-Meteo]
    T[Traccar phone GPS]
  end
  subgraph RASAD
    P[(raw_fetch provenance)]
    SE[Sentinel]
    FU[Fusion + district fallback]
    RW[River watch]
    CA[Cartographer]
    QM[Quartermaster]
    PR[Procurer + Warden]
    DI[Dispatcher + OR-Tools]
    GH[GraphHopper + risk custom model]
    DB[(PostGIS incident state)]
    EV[[Redis Streams + event_log]]
    API[FastAPI + WebSocket]
  end
  S & I & G --> P --> SE --> FU --> DB
  O & F --> P --> RW --> DB
  TT[TomTom closures] & FR[Field reports] --> DB
  DB --> CA <--> GH
  CA --> DB
  WP[WorldPop] & HC[Shelter headcounts] --> QM
  DB --> QM --> DB
  QM --> L[Public Needs Ledger]
  DB --> PR
  PR <-->|signed Beckn| BPP[RASAD supplier BPP and partner BPPs]
  SUP[Suppliers: WhatsApp / phone page] --> BPP
  PR --> DI
  DI <--> GH
  T --> DI
  DI <-->|WhatsApp / driver page| DRV[Drivers]
  T --> API
  DB --> EV --> API --> UI[Control room map]
```

Planned next: the LLM layer for messages in Indian languages and plain-language plan explanations.

## Run it

### With Docker (recommended)

```bash
cp .env.example .env
# Set OSM_PBF_URL in .env to the Geofabrik extract for the region you will demo.
# India is split into zones at https://download.geofabrik.de/asia/india.html; copy the .osm.pbf link for yours.
docker compose --env-file .env -f infra/docker-compose.yml up --build
```

Open http://localhost:8000 for the control room and http://localhost:8082 for Traccar. The first GraphHopper start downloads the extract and builds the routing graph; a regional extract takes several minutes and needs the memory set in `GH_JAVA_OPTS` (4 GB default, raise it for bigger regions). Later starts reuse the graph.

Then, in the control room, add at least one depot with **+ Depot** (where your trucks start). Access windows appear for shelters in alert areas within a minute.

### Without Docker

Needs Python 3.12+, PostgreSQL 16 with PostGIS, and Redis.

```bash
pip install -r requirements-dev.txt
python scripts/fetch_boundaries.py          # district boundaries, one time
createdb rasad                               # PostGIS extension is created on start
cp .env.example .env                         # edit the database URL if needed
uvicorn rasad.api.main:app --reload
```

For routing, run GraphHopper 11 yourself:

```bash
curl -fLO https://github.com/graphhopper/graphhopper/releases/download/11.0/graphhopper-web-11.0.jar
java -Xmx4g -Ddw.graphhopper.datareader.file=<region>.osm.pbf -jar graphhopper-web-11.0.jar server infra/graphhopper/config.yml
```

The schema is applied automatically on start. To run the agents in a separate process, set `RASAD_EMBED_WORKER=false` and run `python -m rasad.worker`.

### Suppliers and the Beckn network

1. In the control room, click **+ Supplier** and then the supplier's location. Enter its name and WhatsApp number. RASAD shows a private link for the supplier's phone page (and sends it on WhatsApp if configured).
2. The supplier enters stock on the page or texts `STOCK water 5000 @ 2`, `STOCK rice 800 kg @ 42`, `STOCK soap 10 kg`, `STOCK balti 150 @ 90`. Price `@ 0` means donated.
3. When shelters have needs, the Procurer places orders; the supplier gets them on WhatsApp or the page and replies ACCEPT, REJECT or READY.
4. Orders above the mandate (spend caps, unverified routes, missing prices) wait in the **Orders** tab for the officer.

To trade with another Beckn network, give it the output of `GET /api/beckn/identity` and add its BPP with `POST /api/beckn/peers`. Set `RASAD_PUBLIC_URL` to the address other nodes can reach.

For WhatsApp, create a Meta WhatsApp Business app, set `RASAD_WHATSAPP_TOKEN`, `RASAD_WHATSAPP_PHONE_ID`, `RASAD_WHATSAPP_VERIFY_TOKEN` and `RASAD_WHATSAPP_APP_SECRET`, and point the webhook at `/webhooks/whatsapp`. Without it, messages go to the supplier page's outbox.

### Live phone tracking

1. Open Traccar at http://localhost:8082 and create the admin account. Put the same email and password in `.env` as `RASAD_TRACCAR_EMAIL` and `RASAD_TRACCAR_PASSWORD`, then restart the API.
2. On each driver's phone install **Traccar Client** (Android or iOS). Set the server URL to `http://<your-server-ip>:5055` and note the device identifier it shows.
3. In Traccar, add a device with that identifier. Positions appear on the RASAD map within seconds.

## API

| Endpoint | Returns |
| --- | --- |
| `GET /health` | Each feed's state (ok, degraded, stale, down), last success, last error, item count; Redis and Traccar status |
| `GET /api/hazards` | Active hazards as GeoJSON, each with every agency message behind it |
| `GET /api/hazards/{id}` | Message texts and watched river crossings with 16-day forecasts |
| `GET /api/river-points` | Watched crossings with 3-day flood probability |
| `GET /api/vehicles` | Latest position per device |
| `GET /api/raw/{id}` | The exact payload of a stored fetch (`?meta=true` for URL, time, status, hash) |
| `GET /api/shelters` | Shelters in alert areas with access window, ETA, crossed zones, blockers and per-slice feasibility |
| `POST /api/shelters` | Add a shelter `{lat, lon, name}` |
| `GET /api/shelters/{id}/route` | Current best route to a shelter |
| `GET/POST /api/depots`, `DELETE /api/depots/{id}` | Depots where trucks start |
| `GET /api/risk-zones?hours=N` | Risk zones the router is told to avoid at now + N hours |
| `GET /api/closures` | Active closures (TomTom, field reports) |
| `POST /api/reports` | Field report `{lat, lon, reporter, note}`; returns suspected / corroborating / confirmed |
| `POST /api/closures/{id}/confirm`, `/clear` | Officer confirms or clears a closure |
| `POST /api/shelters/{id}/headcount` | Headcount from the shelter in-charge `{people, reporter}` |
| `GET /api/shelters/{id}/needs` | Need lines with their basis, recent headcounts and catchments |
| `GET /api/ledger` | The Needs Ledger as JSON |
| `GET /ledger` | The public Needs Ledger page |
| `POST /api/suppliers`, `GET /api/suppliers` | Register a supplier `{name, phone, kind, lat, lon}`; returns its private page link |
| `GET /s/{token}` | Supplier's phone page: stock, orders, messages |
| `POST /api/supplier/{token}/message` | A supplier command, same grammar as WhatsApp |
| `GET/POST /webhooks/whatsapp` | WhatsApp Cloud API webhook (verification and inbound messages) |
| `GET /api/procurements` | Orders with status, Warden checks, ranking reasons and alternatives |
| `POST /api/procurements/{id}/approve`, `/reject` | Officer decision on a held order `{by, reason}` |
| `GET /api/beckn/identity` | This node's subscriber id, key id, public key, BAP and BPP URIs (for registries and partners) |
| `GET/POST /api/beckn/peers` | Trusted Beckn peers |
| `GET /api/beckn/messages?transaction_id=` | Every Beckn message for an order, with signature results |
| `POST /beckn/bpp/{search,select,init,confirm,status,cancel}` | Beckn BPP endpoints |
| `POST /beckn/bap/{on_search,...}` | Beckn BAP callback endpoints |
| `GET /api/events` | Recent events from the audit log |
| `POST /api/refresh/{source}` | Run a source now (`sachet`, `imd_cap`, `gdacs`, `river_watch`, `cartographer`, `quartermaster`, `procurer`) |
| `WS /ws` | Live events; falls back to tailing Postgres if Redis is down |

## Data sources and the no-dummy-data rule

| Source | Used for | Access |
| --- | --- | --- |
| [NDMA SACHET CAP feed](https://sachet.ndma.gov.in/cap_public_website/rss/rss_india.xml) | Official alerts from IMD, CWC, INCOIS, FSI and states | Open RSS |
| [IMD CAP feed](https://cap-sources.s3.amazonaws.com/in-imd-en/rss.xml) | IMD warnings, backup to SACHET | Open RSS |
| [GDACS API](https://www.gdacs.org/gdacsapi/api) | Cyclone and flood footprints | Open |
| [Open-Meteo Flood API](https://open-meteo.com/en/docs/flood-api) (GloFAS v4) | River discharge forecast and reanalysis | Open |
| [Overpass API](https://overpass-api.de) (OpenStreetMap) | Road bridges over rivers | Open |
| [geohacker/india](https://github.com/geohacker/india) district boundaries (GADM) | Placing alerts that only name districts | Open, downloaded by script |
| [Traccar](https://www.traccar.org) | Live phone GPS | Self-hosted |
| [GraphHopper](https://github.com/graphhopper/graphhopper) on a [Geofabrik](https://download.geofabrik.de/asia/india.html) OSM extract | Road network routing with risk zones | Self-hosted |
| [TomTom Traffic Incident Details](https://docs.tomtom.com/traffic-api/) | Live road closures and flooded roads | Free key, optional |
| [WorldPop](https://www.worldpop.org/sdi/advancedapi) stats API (100 m, 2020) | Residents in each shelter catchment | Open, async tasks |
| [Sphere Handbook](https://spherestandards.org/handbook/) minimum standards | Water, food, hygiene quantities per person | Published standard |
| Headcounts from shelter in-charges | People actually at each shelter | Control room / API |
| Supplier stock and order replies (WhatsApp or phone page) | What can be bought, where, at what price | Live, from the suppliers themselves |
| [Beckn protocol](https://github.com/beckn/protocol-specifications) peers (RASAD's own BPP, partner networks, ONIX adapters) | Supplier catalogs and order lifecycle | Signed, open protocol |
| Field reports from drivers and staff | Blocked roads, corroborated before they become hard closures | Control room / API |

Nothing in `rasad/` or `web/` contains hazard, stock, population, road or position data. The only numbers in code are published Sphere standards (cited on every need line) and policy parameters (road-impact weights, the safe-risk limit, the 80% flood-probability closure, displacement planning rates, household size), all set in `.env`, documented and shown as such. Test files contain small format samples written to the CAP and RSS specs, used only to test the parsers. The app never loads them.

## How it handles failure

- A feed that errors is retried with backoff (up to 4x its interval) and shown as down or stale on the health strip; the other feeds keep running. SACHET and IMD CAP back each other up.
- Malformed CAP files are logged with the URL and skipped; the rest of the feed is still processed.
- Alerts without a polygon are placed by district name if possible, otherwise listed as "not on map", never dropped.
- GloFAS points off any modelled river are marked and skipped; missing ensemble values give no probability rather than a guess.
- Redis down: events still go to `event_log`, and the WebSocket tails Postgres.
- Traccar socket down: falls back to polling, reconnects with backoff.
- Repeated identical alerts are recognised by source and identifier and do not create duplicates.
- GraphHopper down: access windows are marked unknown with the last route kept, and the health strip says so. No guessed ETAs.
- A shelter whose own street is closed is detected even when the router reaches a nearby road instead: the risk-aware route must end as close to the shelter as the unconstrained one.
- No depot registered, or none within 250 km: the shelter is marked unroutable with the reason, instead of being dropped.
- API keys are stripped from stored URLs before they reach the provenance viewer.
- WorldPop down or a task failing: the error is stored on the catchment and retried after an hour; shelters without a headcount show "awaiting headcount" instead of a guessed number, on the control room and the public ledger.
- Moving or adding a shelter changes the catchments, which drops the cached population so it is fetched again.
- Quantities already ordered or delivered survive every recomputation.
- A BPP that does not ACK, does not call back within 8 s, or returns a Beckn error marks that order failed; the line is retried next run. Unsigned or wrongly signed Beckn messages get a 401 NACK and are logged.
- Supplier rejects or does not answer in 20 minutes: the order is cancelled over Beckn, the reservation is released, and the need is re-sourced from the next supplier.
- Routing engine down: offers are still ranked, but the Warden holds them for the officer because the route cannot be verified. No order goes out on a guessed route.
- Stock goes stale after 48 h without an update and drops out of search results; orders in flight are never double-counted against a need.

## Tests

```bash
pytest                                                        # parsers and engine
RASAD_TEST_DATABASE_URL=postgresql://rasad:rasad@localhost:5432/rasad_test pytest tests/test_db.py   # PostGIS integration
```

The Cartographer integration test needs a running GraphHopper on any OSM extract and a depot and shelter pair on its roads:

```bash
RASAD_TEST_DATABASE_URL=... RASAD_TEST_GRAPHHOPPER_URL=http://localhost:8989 \
RASAD_TEST_ROUTE="depot_lon,depot_lat;shelter_lon,shelter_lat" pytest tests/test_cartographer_gh.py
```

Two-node Beckn test (two empty PostGIS databases; it starts two RASAD servers with their own keys):

```bash
RASAD_TEST_DATABASE_URL=postgresql://.../rasad_net_a RASAD_TEST_DATABASE_URL_2=postgresql://.../rasad_net_b \
pytest tests/test_beckn_network.py
```

The Dispatcher test (`tests/test_dispatcher_gh.py`) uses the same GraphHopper. It plans trips for real orders, checks pickups come before drops and no truck is overloaded, reports an unreachable shelter with its reason, works a trip through driver commands, reroutes around a confirmed closure, and replans from a driver's GPS when they go off route.

Full suite: 61 tests.

The Beckn test checks that an unknown signer is refused, then runs search → select → init → confirm from node A to node B, the Warden holding orders when routes cannot be verified, officer approval and rejection, node B's supplier accepting, and node A committing the stock against the need.

The Cartographer test checks the whole lifecycle on real roads: open under soft risk, a forecast river flood forcing a detour from +48 h, a suspected then confirmed field closure causing a live reroute, the officer clearing it, and a flooded neighbourhood cutting a shelter off.

## Roadmap

1. **Done: Dispatcher**: OR-Tools pickup-and-delivery routing with access-window deadlines, WhatsApp trips, live rerouting.
2. **LLM layer**: message parsing in Indian languages (Hindi, Bengali, Tamil) and plain-language reasons for each plan.
3. **Pilot**: one district control room, with its own OSM extract, depots and supplier list.

## Dispatcher (trips, drivers, rerouting)

Accepted orders become driver trips. Every 10 minutes, or as soon as a supplier accepts or a driver joins:

1. Open work is every accepted order whose goods are not yet on a live trip. Big orders are split into loads the smallest free vehicle can carry.
2. Free drivers start from their phone's live GPS (Traccar Client, or the "share my location" switch on their page) if it is under 10 minutes old, otherwise from their depot.
3. Travel times between drivers, suppliers and shelters come from GraphHopper with today's hazards, river crossings and closures. A leg that crosses a hard closure or soft risk above `RASAD_SAFE_RISK`, or whose last stretch is shut, is forbidden.
4. OR-Tools solves a pickup-and-delivery problem: vehicle capacity, a hard deadline where the Cartographer says a shelter's last safe road closes, a soft "needed by" from the Quartermaster, a cost on the longest trip so work is spread across drivers, and a penalty weighted by people served for anything left out. Two first-plan strategies run and the cheaper result is kept.
5. Each driver gets the trip on WhatsApp (or their page's outbox) with a link to `/d/<token>`: map, stops, Done / Couldn't / Navigate buttons, and a "Road blocked here" button. Orders that could not be planned show the reason in the Trips tab.

Every 30 seconds the monitor checks live trips. Three GPS fixes more than 600 m off the planned road, a new hazard or closure on a remaining leg, or a driver's BLOCKED report reroutes the rest of the trip from where the driver is. A stop with no safe road goes back to the planner for another driver; a drop whose goods are already on board is held until a road opens. A driver who has not moved for 20 minutes away from any stop is flagged.

Driver commands (WhatsApp or the page): `START`, `DONE 2`, `FAIL 2 shop closed`, `BLOCKED near the bridge`, `STATUS`.

A failed pickup fails the order and releases the uncollected quantity, so the Procurer buys it elsewhere. A completed drop marks the order delivered once every load has arrived.
