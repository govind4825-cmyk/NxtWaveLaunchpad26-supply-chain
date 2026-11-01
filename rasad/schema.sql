-- RASAD incident state. Idempotent: safe to run on every start.
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Every external fetch is stored as-is. This is the provenance trail judges can inspect.
CREATE TABLE IF NOT EXISTS raw_fetch (
    id            BIGSERIAL PRIMARY KEY,
    source        TEXT        NOT NULL,
    url           TEXT        NOT NULL,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    http_status   INTEGER,
    ok            BOOLEAN     NOT NULL,
    content_type  TEXT,
    body          BYTEA,
    sha256        TEXT,
    error         TEXT,
    elapsed_ms    INTEGER
);
CREATE INDEX IF NOT EXISTS raw_fetch_source_idx ON raw_fetch (source, fetched_at DESC);
CREATE INDEX IF NOT EXISTS raw_fetch_sha_idx ON raw_fetch (source, sha256);

CREATE TABLE IF NOT EXISTS source_status (
    source               TEXT PRIMARY KEY,
    last_attempt         TIMESTAMPTZ,
    last_success         TIMESTAMPTZ,
    last_error           TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_raw_fetch_id    BIGINT REFERENCES raw_fetch (id),
    items_last_run       INTEGER
);

CREATE TABLE IF NOT EXISTS hazard (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kind          TEXT        NOT NULL,
    title         TEXT        NOT NULL,
    severity      SMALLINT    NOT NULL,          -- 0 unknown .. 4 extreme (CAP severity)
    certainty     REAL        NOT NULL,          -- 0..1 from CAP certainty
    urgency       TEXT,
    onset         TIMESTAMPTZ,
    expires       TIMESTAMPTZ,
    geom          geometry(MultiPolygon, 4326),  -- NULL when the alert could not be located
    area_desc     TEXT,
    located_by    TEXT        NOT NULL,          -- polygon | circle | district | gdacs | none
    status        TEXT        NOT NULL DEFAULT 'active',  -- active | expired | cancelled
    first_seen    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated  TIMESTAMPTZ NOT NULL DEFAULT now(),
    attrs         JSONB       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS hazard_geom_idx ON hazard USING GIST (geom);
CREATE INDEX IF NOT EXISTS hazard_status_idx ON hazard (status, kind);

-- One hazard can be backed by several agency messages (SACHET, IMD CAP, GDACS).
CREATE TABLE IF NOT EXISTS hazard_source (
    hazard_id     UUID        NOT NULL REFERENCES hazard (id) ON DELETE CASCADE,
    source        TEXT        NOT NULL,
    source_ref    TEXT        NOT NULL,          -- CAP identifier or GDACS type:event:episode
    source_url    TEXT,
    sender        TEXT,
    sent          TIMESTAMPTZ,
    msg_type      TEXT,
    raw_fetch_id  BIGINT REFERENCES raw_fetch (id),
    added_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, source_ref)
);
CREATE INDEX IF NOT EXISTS hazard_source_hazard_idx ON hazard_source (hazard_id);

-- River crossings watched with GloFAS forecasts.
CREATE TABLE IF NOT EXISTS river_point (
    id                     BIGSERIAL PRIMARY KEY,
    osm_ref                TEXT UNIQUE NOT NULL,
    name                   TEXT,
    road_class             TEXT,
    geom                   geometry(Point, 4326) NOT NULL,
    model_lat              DOUBLE PRECISION,
    model_lon              DOUBLE PRECISION,
    threshold_m3s          DOUBLE PRECISION,
    warning_m3s            DOUBLE PRECISION,
    threshold_method       TEXT,
    threshold_raw_fetch_id BIGINT REFERENCES raw_fetch (id),
    no_river               BOOLEAN NOT NULL DEFAULT false,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    forecast_at            TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS river_point_geom_idx ON river_point USING GIST (geom);

CREATE TABLE IF NOT EXISTS river_point_hazard (
    river_point_id BIGINT NOT NULL REFERENCES river_point (id) ON DELETE CASCADE,
    hazard_id      UUID   NOT NULL REFERENCES hazard (id) ON DELETE CASCADE,
    PRIMARY KEY (river_point_id, hazard_id)
);

CREATE TABLE IF NOT EXISTS river_forecast (
    river_point_id BIGINT NOT NULL REFERENCES river_point (id) ON DELETE CASCADE,
    day            DATE   NOT NULL,
    discharge      DOUBLE PRECISION,
    q_min          DOUBLE PRECISION,
    q_p25          DOUBLE PRECISION,
    q_median       DOUBLE PRECISION,
    q_p75          DOUBLE PRECISION,
    q_max          DOUBLE PRECISION,
    exceed_prob    REAL,
    raw_fetch_id   BIGINT REFERENCES raw_fetch (id),
    fetched_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (river_point_id, day)
);

-- Live phone GPS from Traccar.
CREATE TABLE IF NOT EXISTS vehicle (
    device_id    INTEGER PRIMARY KEY,
    name         TEXT,
    unique_id    TEXT,
    status       TEXT,
    last_update  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS vehicle_position (
    id           BIGSERIAL PRIMARY KEY,
    traccar_id   BIGINT UNIQUE,
    device_id    INTEGER     NOT NULL,
    fix_time     TIMESTAMPTZ NOT NULL,
    server_time  TIMESTAMPTZ,
    geom         geometry(Point, 4326) NOT NULL,
    speed_kmh    REAL,
    course       REAL,
    attrs        JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS vehicle_position_device_idx ON vehicle_position (device_id, fix_time DESC);

-- Append-only event log. Redis carries events live; this table is the durable copy and audit trail.
CREATE TABLE IF NOT EXISTS event_log (
    id       BIGSERIAL PRIMARY KEY,
    at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    type     TEXT        NOT NULL,
    payload  JSONB       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS event_log_at_idx ON event_log (at DESC);

-- ===================== Cartographer =====================

-- Where relief trucks start: registered by the district officer on the map.
CREATE TABLE IF NOT EXISTS depot (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    geom        geometry(Point, 4326) NOT NULL,
    note        TEXT,
    active      BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Places people shelter: OSM candidates inside hazards, or added by the officer.
CREATE TABLE IF NOT EXISTS shelter (
    id          BIGSERIAL PRIMARY KEY,
    osm_ref     TEXT UNIQUE,
    name        TEXT,
    kind        TEXT NOT NULL,            -- designated | community_centre | townhall | school | hospital | officer
    geom        geometry(Point, 4326) NOT NULL,
    source      TEXT NOT NULL,            -- osm | officer
    raw_fetch_id BIGINT REFERENCES raw_fetch (id),
    active      BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS shelter_geom_idx ON shelter USING GIST (geom);

CREATE TABLE IF NOT EXISTS shelter_hazard (
    shelter_id  BIGINT NOT NULL REFERENCES shelter (id) ON DELETE CASCADE,
    hazard_id   UUID   NOT NULL REFERENCES hazard (id) ON DELETE CASCADE,
    PRIMARY KEY (shelter_id, hazard_id)
);

-- Road closures: TomTom incidents, field reports, official notices.
CREATE TABLE IF NOT EXISTS closure (
    id            BIGSERIAL PRIMARY KEY,
    source        TEXT NOT NULL,           -- tomtom | report
    source_ref    TEXT,
    geom          geometry(Geometry, 4326) NOT NULL,
    confirmed     BOOLEAN NOT NULL DEFAULT false,
    reason        TEXT,
    reporters     TEXT[] NOT NULL DEFAULT '{}',
    valid_from    TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to      TIMESTAMPTZ,
    raw_fetch_id  BIGINT REFERENCES raw_fetch (id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_ref)
);
CREATE INDEX IF NOT EXISTS closure_geom_idx ON closure USING GIST (geom);

-- Every field report as received, before corroboration.
CREATE TABLE IF NOT EXISTS road_report (
    id          BIGSERIAL PRIMARY KEY,
    at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    geom        geometry(Point, 4326) NOT NULL,
    note        TEXT,
    reporter    TEXT NOT NULL,
    channel     TEXT NOT NULL DEFAULT 'control_room',
    closure_id  BIGINT REFERENCES closure (id)
);

-- Latest access window per shelter.
CREATE TABLE IF NOT EXISTS access_window (
    shelter_id   BIGINT PRIMARY KEY REFERENCES shelter (id) ON DELETE CASCADE,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    status       TEXT NOT NULL,            -- open | closing | cut_off | unroutable | unknown
    closes_at    TIMESTAMPTZ,
    depot_id     BIGINT REFERENCES depot (id) ON DELETE SET NULL,
    eta_min      REAL,
    distance_km  REAL,
    max_risk     REAL,
    route        geometry(LineString, 4326),
    crossed      JSONB NOT NULL DEFAULT '[]'::jsonb,   -- risk zones the route passes, with their sources
    blocked_by   JSONB NOT NULL DEFAULT '[]'::jsonb,   -- what closes the window
    slices       JSONB NOT NULL DEFAULT '[]'::jsonb,   -- feasibility per time slice
    note         TEXT
);

-- ===================== Quartermaster =====================

-- Area each shelter serves inside a hazard (Voronoi cell clipped to the footprint) and its WorldPop population.
CREATE TABLE IF NOT EXISTS catchment (
    shelter_id       BIGINT NOT NULL REFERENCES shelter (id) ON DELETE CASCADE,
    hazard_id        UUID   NOT NULL REFERENCES hazard (id) ON DELETE CASCADE,
    geom             geometry(MultiPolygon, 4326) NOT NULL,
    geom_hash        TEXT NOT NULL,
    population       DOUBLE PRECISION,
    pop_year         INTEGER,
    pop_raw_fetch_id BIGINT REFERENCES raw_fetch (id),
    pop_error        TEXT,
    computed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (shelter_id, hazard_id)
);
CREATE INDEX IF NOT EXISTS catchment_geom_idx ON catchment USING GIST (geom);

-- People actually at a shelter, reported by its in-charge. Overrides the planning estimate.
CREATE TABLE IF NOT EXISTS headcount (
    id          BIGSERIAL PRIMARY KEY,
    shelter_id  BIGINT NOT NULL REFERENCES shelter (id) ON DELETE CASCADE,
    people      INTEGER NOT NULL CHECK (people >= 0),
    households  INTEGER CHECK (households >= 0),
    reporter    TEXT NOT NULL,
    channel     TEXT NOT NULL DEFAULT 'control_room',
    note        TEXT,
    at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS headcount_shelter_idx ON headcount (shelter_id, at DESC);

-- One line per shelter and item: what is needed, by when, and on what basis.
CREATE TABLE IF NOT EXISTS need (
    shelter_id      BIGINT NOT NULL REFERENCES shelter (id) ON DELETE CASCADE,
    item            TEXT NOT NULL,
    unit            TEXT NOT NULL,
    quantity        DOUBLE PRECISION NOT NULL,     -- total for the period
    committed       DOUBLE PRECISION NOT NULL DEFAULT 0,   -- ordered or delivered (Procurer fills this)
    people          INTEGER NOT NULL,
    people_basis    TEXT NOT NULL,                 -- headcount | planning
    days            INTEGER NOT NULL,
    rate            TEXT NOT NULL,                 -- the standard used, in words
    needed_by       TIMESTAMPTZ,
    priority        DOUBLE PRECISION NOT NULL,
    access_status   TEXT,
    basis           JSONB NOT NULL DEFAULT '{}'::jsonb,   -- ids behind the number: catchments, headcount, access window
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (shelter_id, item)
);

-- ===================== Procurer, Beckn, suppliers =====================

-- This node's Beckn signing key (generated on first start) and the network peers it trusts.
CREATE TABLE IF NOT EXISTS beckn_key (
    subscriber_id   TEXT PRIMARY KEY,
    unique_key_id   TEXT NOT NULL,
    private_key     BYTEA NOT NULL,
    public_key      TEXT NOT NULL,          -- base64 raw ed25519 public key
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS beckn_peer (
    subscriber_id   TEXT NOT NULL,
    unique_key_id   TEXT NOT NULL,
    role            TEXT NOT NULL,          -- BAP | BPP
    url             TEXT NOT NULL,
    public_key      TEXT NOT NULL,
    active          BOOLEAN NOT NULL DEFAULT true,
    added_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (subscriber_id, unique_key_id, role)
);

-- Every Beckn message sent or received, with the signature check result.
CREATE TABLE IF NOT EXISTS beckn_message (
    id              BIGSERIAL PRIMARY KEY,
    at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    direction       TEXT NOT NULL,          -- out | in
    role            TEXT NOT NULL,          -- BAP | BPP (which side of this node handled it)
    action          TEXT NOT NULL,
    transaction_id  TEXT,
    message_id      TEXT,
    peer            TEXT,
    url             TEXT,
    body            JSONB NOT NULL,
    signature_ok    BOOLEAN,
    http_status     INTEGER,
    ack             TEXT,                   -- ACK | NACK | error text
    error           TEXT
);
CREATE INDEX IF NOT EXISTS beckn_message_msg_idx ON beckn_message (message_id, action);
CREATE INDEX IF NOT EXISTS beckn_message_txn_idx ON beckn_message (transaction_id);

-- RASAD's own supplier network, served to the Beckn network by this node's BPP.
CREATE TABLE IF NOT EXISTS supplier (
    id            BIGSERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    phone         TEXT UNIQUE,              -- E.164 without +, as WhatsApp sends it
    kind          TEXT NOT NULL DEFAULT 'shop',   -- shop | wholesaler | pharmacy | ngo_warehouse | government
    geom          geometry(Point, 4326) NOT NULL,
    token         TEXT UNIQUE NOT NULL,     -- secret link for the supplier page
    serves_km     REAL NOT NULL DEFAULT 150,
    active        BOOLEAN NOT NULL DEFAULT true,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS supplier_item (
    supplier_id   BIGINT NOT NULL REFERENCES supplier (id) ON DELETE CASCADE,
    item          TEXT NOT NULL,
    available     DOUBLE PRECISION NOT NULL CHECK (available >= 0),   -- in the need unit
    reserved      DOUBLE PRECISION NOT NULL DEFAULT 0,
    unit_price    DOUBLE PRECISION,         -- INR per need unit; NULL = not stated
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_via   TEXT NOT NULL,            -- whatsapp | page | api
    PRIMARY KEY (supplier_id, item)
);

CREATE TABLE IF NOT EXISTS supplier_message (
    id            BIGSERIAL PRIMARY KEY,
    at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    supplier_id   BIGINT REFERENCES supplier (id),
    phone         TEXT,
    direction     TEXT NOT NULL,            -- in | out
    channel       TEXT NOT NULL,            -- whatsapp | page | outbox
    text          TEXT NOT NULL,
    delivered     BOOLEAN,
    error         TEXT
);

-- Orders as the BPP sees them.
CREATE TABLE IF NOT EXISTS bpp_order (
    id              BIGSERIAL PRIMARY KEY,
    transaction_id  TEXT NOT NULL,
    bap_id          TEXT NOT NULL,
    bap_uri         TEXT NOT NULL,
    supplier_id     BIGINT NOT NULL REFERENCES supplier (id),
    item            TEXT NOT NULL,
    quantity        DOUBLE PRECISION NOT NULL,
    unit_price      DOUBLE PRECISION,
    deliver_to      JSONB NOT NULL,
    status          TEXT NOT NULL,          -- Created | Accepted | Packed | Cancelled
    reason          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (transaction_id)
);

-- Orders as RASAD (the buyer) sees them: one row per attempt to fill part of a need line.
CREATE TABLE IF NOT EXISTS procurement (
    id              BIGSERIAL PRIMARY KEY,
    shelter_id      BIGINT NOT NULL REFERENCES shelter (id) ON DELETE CASCADE,
    item            TEXT NOT NULL,
    quantity        DOUBLE PRECISION NOT NULL,
    unit            TEXT NOT NULL,
    status          TEXT NOT NULL,          -- awaiting_approval | confirming | awaiting_supplier | accepted | packed | rejected | cancelled | failed
    transaction_id  TEXT UNIQUE NOT NULL,
    bpp_id          TEXT,
    bpp_uri         TEXT,
    provider_id     TEXT,
    provider_name   TEXT,
    item_id         TEXT,
    unit_price      DOUBLE PRECISION,
    total_price     DOUBLE PRECISION,
    eta_min         REAL,
    route_safe      BOOLEAN,
    bpp_order_id    TEXT,
    needed_by       TIMESTAMPTZ,
    checks          JSONB NOT NULL DEFAULT '[]'::jsonb,   -- Warden results
    reasons         JSONB NOT NULL DEFAULT '{}'::jsonb,   -- why this offer: ranking inputs and alternatives
    approved_by     TEXT,
    approved_at     TIMESTAMPTZ,
    note            TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS procurement_need_idx ON procurement (shelter_id, item, status);

-- ===================== Dispatcher =====================

ALTER TABLE procurement ADD COLUMN IF NOT EXISTS pickup_lat DOUBLE PRECISION;
ALTER TABLE procurement ADD COLUMN IF NOT EXISTS pickup_lon DOUBLE PRECISION;
ALTER TABLE procurement ADD COLUMN IF NOT EXISTS delivered_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS driver (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    phone           TEXT UNIQUE,
    vehicle         TEXT NOT NULL DEFAULT 'mini truck',
    capacity_kg     DOUBLE PRECISION NOT NULL CHECK (capacity_kg > 0),
    traccar_unique_id TEXT,               -- the identifier shown in the driver's Traccar Client app
    depot_id        BIGINT REFERENCES depot (id),
    token           TEXT UNIQUE NOT NULL,
    active          BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS trip (
    id            BIGSERIAL PRIMARY KEY,
    driver_id     BIGINT NOT NULL REFERENCES driver (id),
    status        TEXT NOT NULL,            -- planned | en_route | done | cancelled
    geom          geometry(LineString, 4326),
    planned_km    REAL,
    planned_min   REAL,
    plan          JSONB NOT NULL DEFAULT '{}'::jsonb,   -- solver inputs and outcome for audit
    deviation_strikes INTEGER NOT NULL DEFAULT 0,
    last_progress_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS trip_stop (
    id              BIGSERIAL PRIMARY KEY,
    trip_id         BIGINT NOT NULL REFERENCES trip (id) ON DELETE CASCADE,
    seq             INTEGER NOT NULL,
    kind            TEXT NOT NULL,          -- pickup | drop
    procurement_id  BIGINT NOT NULL REFERENCES procurement (id),
    name            TEXT,
    geom            geometry(Point, 4326) NOT NULL,
    item            TEXT NOT NULL,
    quantity        DOUBLE PRECISION NOT NULL,
    eta             TIMESTAMPTZ,
    due             TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending | done | failed | blocked
    done_at         TIMESTAMPTZ,
    note            TEXT
);
CREATE INDEX IF NOT EXISTS trip_stop_trip_idx ON trip_stop (trip_id, seq);
ALTER TABLE trip_stop ADD COLUMN IF NOT EXISTS chunk TEXT;      -- pairs a pickup with its drop
CREATE INDEX IF NOT EXISTS trip_stop_proc_idx ON trip_stop (procurement_id);
ALTER TABLE procurement ADD COLUMN IF NOT EXISTS dispatch_note TEXT;
ALTER TABLE supplier_message ADD COLUMN IF NOT EXISTS driver_id BIGINT REFERENCES driver (id);
ALTER TABLE trip_stop ADD COLUMN IF NOT EXISTS leg geometry(LineString, 4326);   -- road from the previous stop
