"""Runtime settings, read from environment variables or a .env file."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="RASAD_", extra="ignore")

    database_url: str = "postgresql://rasad:rasad@localhost:5432/rasad"
    redis_url: str = "redis://localhost:6379/0"

    # Hazard feeds. SACHET publishes a national feed plus state feeds; add state
    # feed URLs to SACHET_EXTRA_FEEDS (comma separated) to widen coverage.
    sachet_feed: str = "https://sachet.ndma.gov.in/cap_public_website/rss/rss_india.xml"
    sachet_extra_feeds: str = ""
    imd_cap_feed: str = "https://cap-sources.s3.amazonaws.com/in-imd-en/rss.xml"
    gdacs_base: str = "https://www.gdacs.org/gdacsapi/api"
    openmeteo_flood_base: str = "https://flood-api.open-meteo.com/v1/flood"
    overpass_url: str = "https://overpass-api.de/api/interpreter"
    worldpop_base: str = "https://api.worldpop.org/v1"

    # Poll cadence in seconds (section 3 of the design doc).
    sachet_every: int = 300
    imd_cap_every: int = 300
    gdacs_every: int = 900
    river_watch_every: int = 3600

    # A source older than this many seconds is shown as stale.
    stale_after: int = 1800

    # India bounding box (west, south, east, north) used to keep GDACS events relevant.
    region_bbox: str = "66.0,6.0,98.5,37.5"

    # District boundaries for CAP alerts that carry no polygon.
    boundaries_path: str = "data/india_district.geojson"

    # Traccar server for live phone GPS.
    traccar_url: str = "http://localhost:8082"
    traccar_email: str = ""
    traccar_password: str = ""

    # Cartographer: routing engine, road closures, access windows.
    graphhopper_url: str = "http://localhost:8989"
    tomtom_key: str = ""
    cartographer_every: int = 900
    slice_hours: str = "0,6,12,24,48,72"
    # A route is "safe" if the worst soft risk it crosses stays below this.
    safe_risk: float = 0.5
    # A river crossing becomes a hard closure at this flood probability.
    hard_flood_prob: float = 0.8
    max_depot_km: float = 250.0
    max_shelters_per_hazard: int = 60
    # Playbook weights: how strongly each hazard kind degrades roads inside its footprint (0..1).
    # These are policy parameters an officer can tune, not observations.
    road_impact: str = "flood:0.9,landslide:0.9,tsunami:1.0,avalanche:0.9,cyclone:0.7,heavy_rain:0.4,high_wave:0.3"

    # Quartermaster.
    quartermaster_every: int = 1800
    worldpop_year: int = 2020
    worldpop_calls_per_run: int = 40
    # Planning assumption until a shelter reports its headcount: share of the catchment's
    # residents expected to come to the shelter, by hazard kind. Officer-set, shown as an estimate.
    displacement_rate: str = "flood:0.25,cyclone:0.3,landslide:0.2,tsunami:0.4,heavy_rain:0.05,high_wave:0.1,avalanche:0.2"
    household_size: float = 5.0      # Sphere's worked example uses a family of five
    default_days: int = 3            # supply period when the hazard has no expiry
    max_days: int = 7
    headcount_valid_hours: int = 48

    # Beckn. This node is both a BAP (RASAD buying) and a BPP (serving RASAD's supplier network).
    public_url: str = "http://localhost:8000"      # how other Beckn nodes reach this one
    beckn_subscriber_id: str = "rasad.local"
    beckn_domain: str = "rasad:relief"
    beckn_city: str = "std:080"
    beckn_verify: bool = True                      # reject unsigned or wrongly signed messages
    beckn_gateway_url: str = ""                    # optional: send search through a gateway instead of to each BPP
    beckn_registry_url: str = ""                   # optional: look up unknown peers' keys here
    beckn_wait_s: float = 8.0

    # Procurer and the mandate the officer sets (Warden checks every order against it).
    procurer_every: int = 300
    supplier_accept_minutes: int = 20
    stock_fresh_hours: int = 48
    mandate_items: str = "drinking_water,cereal_ration,bathing_soap,water_containers"
    mandate_order_cap_inr: float = 50000
    mandate_daily_cap_inr: float = 200000
    mandate_max_over_need: float = 1.5
    procure_top_shelters: int = 10

    # Dispatcher.
    dispatcher_every: int = 600
    monitor_every: int = 30
    solver_seconds: int = 6
    gps_fresh_minutes: int = 10
    deviation_m: float = 600
    stall_minutes: int = 20
    # Load weight per need unit, for vehicle capacity. Water and cereal by mass; soap per gram;
    # an empty 10-20 L jerrycan is taken as about 1 kg (packing assumption, adjustable).
    item_kg: str = "drinking_water:1.0,cereal_ration:1.0,bathing_soap:0.001,water_containers:1.0"

    # WhatsApp Cloud API (optional). Without it, messages go to the outbox shown on supplier pages.
    whatsapp_token: str = ""
    whatsapp_phone_id: str = ""
    whatsapp_verify_token: str = ""
    whatsapp_app_secret: str = ""
    whatsapp_api: str = "https://graph.facebook.com/v21.0"

    # Run the Sentinel and Traccar link inside the API process (simplest deploy).
    embed_worker: bool = True

    http_timeout: float = 25.0
    user_agent: str = "RASAD-hackathon/0.1 (relief logistics research)"

    @property
    def bbox(self) -> tuple[float, float, float, float]:
        w, s, e, n = (float(x) for x in self.region_bbox.split(","))
        return w, s, e, n

    @property
    def slices(self) -> list[int]:
        return sorted({int(x) for x in self.slice_hours.split(",") if x.strip()})

    @property
    def road_impact_map(self) -> dict[str, float]:
        out = {}
        for pair in self.road_impact.split(","):
            if ":" in pair:
                k, v = pair.split(":", 1)
                out[k.strip()] = float(v)
        return out

    @property
    def item_kg_map(self) -> dict[str, float]:
        out = {}
        for pair in self.item_kg.split(","):
            if ":" in pair:
                k, v = pair.split(":", 1)
                out[k.strip()] = float(v)
        return out

    @property
    def displacement_map(self) -> dict[str, float]:
        out = {}
        for pair in self.displacement_rate.split(","):
            if ":" in pair:
                k, v = pair.split(":", 1)
                out[k.strip()] = float(v)
        return out

    @property
    def sachet_feeds(self) -> list[str]:
        extra = [u.strip() for u in self.sachet_extra_feeds.split(",") if u.strip()]
        return [self.sachet_feed, *extra]


@lru_cache
def get_settings() -> Settings:
    return Settings()
