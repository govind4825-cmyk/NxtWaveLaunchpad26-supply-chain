/* RASAD control room. Every number shown here comes from /api, which comes from a stored raw fetch. */
(() => {
  const LOGISTICS = new Set(["cyclone", "flood", "heavy_rain", "landslide", "tsunami", "high_wave", "avalanche"]);
  const FEED_NAMES = { sachet: "SACHET (NDMA)", imd_cap: "IMD CAP", gdacs: "GDACS", river_watch: "River watch", cartographer: "Cartographer", quartermaster: "Quartermaster", procurer: "Procurer", dispatcher: "Dispatcher", tomtom: "TomTom closures", worldpop: "WorldPop" };
  const ITEM_NAMES = { drinking_water: "Drinking water", cereal_ration: "Cereal ration", bathing_soap: "Bathing soap", water_containers: "Water containers" };
  const STATUS_COLOR = { cut_off: "#e0442b", closing: "#f0a24b", open: "#5cbf8a" };
  const STATUS_TEXT = { cut_off: "Cut off", closing: "Closing", open: "Open", unknown: "Unknown", unroutable: "Unroutable" };
  const SEV = ["Unknown", "Minor", "Moderate", "Severe", "Extreme"];
  const SEV_COLOR = ["#7b858c", "#7fb3a3", "#e9c46a", "#f08a4b", "#e0442b"];
  const state = { hazards: null, selected: null, onlyLogistics: true, lastEventId: 0, shelters: null, selectedShelter: null, riskHours: 0, mode: null };

  const $ = (s) => document.querySelector(s);
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const ago = (iso) => {
    if (!iso) return "never";
    const s = Math.max(0, (Date.now() - Date.parse(iso)) / 1000);
    if (s < 90) return `${Math.round(s)}s ago`;
    if (s < 5400) return `${Math.round(s / 60)}m ago`;
    if (s < 172800) return `${Math.round(s / 3600)}h ago`;
    return `${Math.round(s / 86400)}d ago`;
  };
  const when = (iso) => (iso ? new Date(iso).toLocaleString("en-IN", { timeZone: "Asia/Kolkata", day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit" }) : "—");
  const pct = (p) => (p == null ? "—" : `${Math.round(p * 100)}%`);
  const kindLabel = (k) => (k || "other").replace("_", " ");

  async function api(path, opts) {
    const r = await fetch(path, opts);
    if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`);
    return r.json();
  }

  // ---------- clock ----------
  setInterval(() => {
    $("#clock").textContent = new Date().toLocaleTimeString("en-IN", { timeZone: "Asia/Kolkata", hour12: false }) + " IST";
  }, 1000);

  // ---------- map ----------
  const map = new maplibregl.Map({
    container: "map",
    center: [82.8, 22.6],
    zoom: 4.2,
    attributionControl: { compact: true },
    style: {
      version: 8,
      sources: {
        osm: {
          type: "raster",
          tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
          tileSize: 256,
          maxzoom: 19,
          attribution: "© OpenStreetMap contributors",
        },
      },
      layers: [
        { id: "bg", type: "background", paint: { "background-color": "#0f1418" } },
        { id: "osm", type: "raster", source: "osm", paint: { "raster-saturation": -0.85, "raster-brightness-max": 0.55, "raster-contrast": 0.1 } },
      ],
    },
  });
  map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");

  const empty = { type: "FeatureCollection", features: [] };
  map.on("load", () => {
    map.addSource("hazards", { type: "geojson", data: empty, promoteId: "id" });
    map.addSource("rivers", { type: "geojson", data: empty });
    map.addSource("vehicles", { type: "geojson", data: empty, promoteId: "device_id" });

    map.addLayer({
      id: "hz-fill", type: "fill", source: "hazards",
      paint: {
        "fill-color": ["match", ["get", "severity"], 4, SEV_COLOR[4], 3, SEV_COLOR[3], 2, SEV_COLOR[2], 1, SEV_COLOR[1], SEV_COLOR[0]],
        "fill-opacity": ["case", ["boolean", ["feature-state", "selected"], false], 0.5, ["+", 0.12, ["*", 0.25, ["get", "certainty"]]]],
      },
    });
    map.addLayer({
      id: "hz-line", type: "line", source: "hazards",
      paint: {
        "line-color": ["match", ["get", "severity"], 4, SEV_COLOR[4], 3, SEV_COLOR[3], 2, SEV_COLOR[2], 1, SEV_COLOR[1], SEV_COLOR[0]],
        "line-width": ["case", ["boolean", ["feature-state", "selected"], false], 2.5, 1],
        "line-dasharray": ["case", ["==", ["get", "located_by"], "district"], ["literal", [2, 2]], ["literal", [1, 0]]],
      },
    });
    map.addLayer({
      id: "rivers", type: "circle", source: "rivers",
      paint: {
        "circle-radius": ["interpolate", ["linear"], ["zoom"], 4, 3, 10, 7],
        "circle-color": ["case", ["==", ["get", "p3"], null], "#7b858c", [">=", ["get", "p3"], 0.5], "#e0442b", [">=", ["get", "p3"], 0.2], "#e9c46a", "#5cbf8a"],
        "circle-stroke-color": "#0f1418", "circle-stroke-width": 1.5,
      },
    });
    map.addLayer({
      id: "vehicles", type: "circle", source: "vehicles",
      paint: { "circle-radius": 7, "circle-color": "#6fb7ff", "circle-stroke-color": "#fff", "circle-stroke-width": 2,
        "circle-opacity": ["case", [">", ["get", "age_seconds"], 900], 0.4, 1] },
    });

    map.addSource("risk", { type: "geojson", data: empty });
    map.addSource("closures", { type: "geojson", data: empty });
    map.addSource("route", { type: "geojson", data: empty });
    map.addSource("shelters", { type: "geojson", data: empty });
    map.addSource("depots", { type: "geojson", data: empty });
    map.addSource("suppliers", { type: "geojson", data: empty });
    map.addSource("trips", { type: "geojson", data: empty });
    map.addSource("trip-stops", { type: "geojson", data: empty });
    const before = "rivers";
    map.addLayer({ id: "risk-fill", type: "fill", source: "risk", paint: {
      "fill-color": ["case", ["get", "hard"], "#e0442b", "#f0a24b"],
      "fill-opacity": ["case", ["get", "hard"], 0.5, ["*", 0.35, ["get", "risk"]]] } }, before);
    map.addLayer({ id: "risk-line", type: "line", source: "risk", filter: ["get", "hard"],
      paint: { "line-color": "#e0442b", "line-width": 1.5 } }, before);
    map.addLayer({ id: "closures", type: "line", source: "closures", paint: {
      "line-color": ["case", ["get", "confirmed"], "#e0442b", "#e9c46a"], "line-width": 4,
      "line-dasharray": ["case", ["get", "confirmed"], ["literal", [1, 0]], ["literal", [1.5, 1.5]]] } });
    map.addLayer({ id: "closure-pts", type: "circle", source: "closures", filter: ["==", ["geometry-type"], "Point"], paint: {
      "circle-radius": 7, "circle-color": ["case", ["get", "confirmed"], "#e0442b", "#e9c46a"], "circle-stroke-color": "#0f1418", "circle-stroke-width": 2 } });
    map.addLayer({ id: "route", type: "line", source: "route", layout: { "line-cap": "round", "line-join": "round" },
      paint: { "line-color": "#6fb7ff", "line-width": 4, "line-opacity": 0.9 } });
    map.addLayer({ id: "trips", type: "line", source: "trips", layout: { "line-cap": "round", "line-join": "round" },
      paint: { "line-color": "#b98cff", "line-width": ["case", ["==", ["get", "status"], "en_route"], 5, 3.5], "line-opacity": 0.95,
        "line-dasharray": ["case", ["==", ["get", "status"], "planned"], ["literal", [2, 1.5]], ["literal", [1, 0]]] } });
    map.addLayer({ id: "shelters", type: "circle", source: "shelters", paint: {
      "circle-radius": ["interpolate", ["linear"], ["zoom"], 4, 3.5, 10, 7],
      "circle-color": ["match", ["get", "status"], "cut_off", STATUS_COLOR.cut_off, "closing", STATUS_COLOR.closing, "open", STATUS_COLOR.open, "#7b858c"],
      "circle-stroke-color": "#0f1418", "circle-stroke-width": 1.5 } });
    map.addLayer({ id: "depots", type: "circle", source: "depots", paint: {
      "circle-radius": 7, "circle-color": "#ffffff", "circle-stroke-color": "#6fb7ff", "circle-stroke-width": 3 } });

    map.addLayer({ id: "suppliers", type: "circle", source: "suppliers", paint: {
      "circle-radius": 6, "circle-color": "#0f1418", "circle-stroke-color": "#b98cff", "circle-stroke-width": 3 } });
    map.on("click", "suppliers", (e) => {
      if (state.mode) return;
      const p = e.features[0].properties;
      const items = JSON.parse(p.items || "[]");
      new maplibregl.Popup({ closeButton: false, maxWidth: "300px" }).setLngLat(e.lngLat).setHTML(
        `<b>${esc(p.name)}</b> <span style="color:var(--muted)">${esc(p.kind.replace("_", " "))}</span>
         <table class="needs">${items.map((i) => `<tr><td>${esc(ITEM_NAMES[i.item] || i.item)}</td><td>${fmtQty(Math.max(0, i.available - i.reserved), i.item === "bathing_soap" ? "g" : i.item === "drinking_water" ? "litres" : i.item === "cereal_ration" ? "kg" : "containers")}</td></tr>`).join("") || "<tr><td>No stock reported yet</td><td></td></tr>"}</table>
         <a href="${esc(p.link)}" target="_blank" style="color:var(--accent)">Supplier page</a>`).addTo(map);
    });
    map.addLayer({ id: "trip-stops", type: "circle", source: "trip-stops", paint: {
      "circle-radius": 5.5, "circle-stroke-color": "#fff", "circle-stroke-width": 2,
      "circle-color": ["match", ["get", "status"], "blocked", "#e9c46a", ["match", ["get", "kind"], "pickup", "#0f1418", "#b98cff"]] } });
    map.on("click", "trip-stops", (e) => {
      if (state.mode) return;
      const p = e.features[0].properties;
      new maplibregl.Popup({ closeButton: false }).setLngLat(e.lngLat).setHTML(
        `<b>Trip #${p.trip_id}, stop ${p.seq}</b> (${esc(p.driver)})<br>${p.kind === "pickup" ? "Pick up" : "Drop"} ${esc(p.load_text)}<br>${p.kind === "pickup" ? "at" : "to"} ${esc(p.name)}<br>ETA ${when(p.eta)}${p.note && p.note !== "null" ? `<br><span style="color:#e9c46a">${esc(p.note)}</span>` : ""}`).addTo(map);
    });
    map.on("click", "trips", (e) => {
      if (state.mode || map.queryRenderedFeatures(e.point, { layers: ["trip-stops", "shelters", "depots"] }).length) return;
      const p = e.features[0].properties;
      new maplibregl.Popup({ closeButton: false }).setLngLat(e.lngLat).setHTML(
        `<b>Trip #${p.id}</b> · ${esc(p.driver)}<br>${esc(TRIP_TEXT[p.status] || p.status)}, ${Math.round(p.planned_min)} min to go`).addTo(map);
    });
    map.on("click", (e) => { if (state.mode) { const m = state.mode; setTimeout(() => pick(e.lngLat, m), 0); } });
    map.on("click", "shelters", (e) => { if (!state.mode) selectShelter(e.features[0].properties.id, false); });
    map.on("click", "closure-pts", (e) => { if (!state.mode) closurePopup(e); });
    map.on("click", "closures", (e) => { if (!state.mode) closurePopup(e); });
    map.on("click", "depots", (e) => {
      if (state.mode) return;
      const p = e.features[0].properties;
      new maplibregl.Popup({ closeButton: false }).setLngLat(e.lngLat).setHTML(`<b>Depot: ${esc(p.name)}</b>${p.note ? "<br>" + esc(p.note) : ""}`).addTo(map);
    });
    map.on("click", "hz-fill", (e) => { if (!state.mode && !map.queryRenderedFeatures(e.point, { layers: ["shelters", "depots", "closure-pts"] }).length) select(e.features[0].properties.id, false); });
    map.on("click", "rivers", (e) => {
      if (state.mode) return;
      const p = e.features[0].properties;
      new maplibregl.Popup({ closeButton: false }).setLngLat(e.lngLat)
        .setHTML(`<b>${esc(p.name || "Unnamed bridge")}</b><br>${esc(p.road_class || "")} road<br>Flood probability, 3 days: <b>${pct(p.p3 === "null" ? null : Number(p.p3))}</b><br>Threshold ${p.threshold_m3s && p.threshold_m3s !== "null" ? Math.round(p.threshold_m3s) + " m³/s" : "pending"}`)
        .addTo(map);
    });
    map.on("click", "vehicles", (e) => {
      if (state.mode) return;
      const p = e.features[0].properties;
      new maplibregl.Popup({ closeButton: false }).setLngLat(e.lngLat)
        .setHTML(`<b>${esc(p.name)}</b><br>Last fix ${ago(p.fix_time)}<br>${p.speed_kmh != null ? Math.round(p.speed_kmh) + " km/h" : ""}`)
        .addTo(map);
    });
    for (const l of ["hz-fill", "rivers", "vehicles", "shelters", "depots", "closures", "closure-pts", "suppliers", "trips", "trip-stops"]) {
      map.on("mouseenter", l, () => { if (!state.mode) map.getCanvas().style.cursor = "pointer"; });
      map.on("mouseleave", l, () => { if (!state.mode) map.getCanvas().style.cursor = ""; });
    }
    refreshAll();
  });

  // ---------- feeds ----------
  async function loadHealth() {
    let h;
    try { h = await api("/health"); } catch (e) { $("#feeds").innerHTML = `<div class="empty">API unreachable: ${esc(e.message)}</div>`; return; }
    const cards = h.feeds.map((f) => `
      <div class="feed" title="${esc(f.last_error || "")}">
        <div class="row"><span class="st ${f.state}"></span><span class="nm">${esc(FEED_NAMES[f.source] || f.source)}</span></div>
        <div class="meta">${f.state === "waiting" ? "first run pending" : f.state === "off" ? esc(f.last_error || "off") : `${esc(f.state)} · ${ago(f.last_success)}`}${f.items_last_run != null && f.state !== "off" ? ` · ${f.items_last_run} ${f.source === "cartographer" ? "shelters" : f.source === "quartermaster" ? "with needs" : f.source === "procurer" ? "lines checked" : f.source === "worldpop" ? "areas" : "items"}` : ""}</div>
        ${["cartographer", "quartermaster", "procurer"].includes(f.source) && f.last_error ? `<div class="meta" style="color:${f.state === "ok" ? "var(--muted)" : "var(--warn)"}">${esc(f.last_error)}</div>` : ""}
        ${f.last_raw_fetch_id ? `<div class="meta"><a href="/api/raw/${f.last_raw_fetch_id}" target="_blank" style="color:var(--accent)">raw payload #${f.last_raw_fetch_id}</a></div>` : ""}
        ${f.source !== "tomtom" && f.source !== "worldpop" ? `<button data-refresh="${f.source}" title="Run now">↻</button>` : ""}
      </div>`);
    const t = h.traccar;
    cards.push(`
      <div class="feed"><div class="row"><span class="st ${!t.enabled ? "" : t.connected ? "ok" : "down"}"></span><span class="nm">Phone GPS (Traccar)</span></div>
      <div class="meta">${!t.enabled ? "not configured" : t.connected ? `live · ${ago(t.last_message)}` : esc(t.last_error || "connecting")}</div></div>`);
    cards.push(`
      <div class="feed"><div class="row"><span class="st ${h.redis ? "ok" : "degraded"}"></span><span class="nm">Event bus</span></div>
      <div class="meta">${h.redis ? "Redis live" : "Redis down · polling Postgres"}</div></div>`);
    $("#feeds").innerHTML = cards.join("");
  }
  $("#feeds").addEventListener("click", async (e) => {
    const b = e.target.closest("[data-refresh]");
    if (!b) return;
    b.textContent = "…";
    try { await api(`/api/refresh/${b.dataset.refresh}`, { method: "POST" }); } catch (_) {}
    setTimeout(loadHealth, 4000);
  });

  // ---------- hazards ----------
  async function loadHazards() {
    try { state.hazards = await api("/api/hazards"); } catch (e) { $("#hazards").innerHTML = `<div class="empty">Could not load hazards: ${esc(e.message)}</div>`; return; }
    renderHazards();
    if (map.getSource("hazards")) {
      map.getSource("hazards").setData({ type: "FeatureCollection", features: state.hazards.features.filter((f) => f.geometry && visible(f)) });
      if (state.selected) map.setFeatureState({ source: "hazards", id: state.selected }, { selected: true });
    }
  }
  const visible = (f) => !state.onlyLogistics || LOGISTICS.has(f.properties.kind);

  function renderHazards() {
    const feats = (state.hazards?.features || []).filter(visible);
    $("#hazard-count").textContent = feats.length;
    if (!feats.length) {
      $("#hazards").innerHTML = `<div class="empty">No ${state.onlyLogistics ? "logistics-relevant " : ""}alerts are active in the feeds right now.<br>RASAD keeps polling; this list fills the moment an agency issues one.</div>`;
      return;
    }
    $("#hazards").innerHTML = feats.map((f) => {
      const p = f.properties;
      const srcs = [...new Set(p.sources.map((s) => s.source))];
      const rw = p.river_watch;
      return `<div class="hz ${state.selected === p.id ? "active" : ""}" data-id="${p.id}">
        <div class="bar" style="background:${SEV_COLOR[p.severity]}"></div>
        <div>
          <div class="k"><span>${esc(kindLabel(p.kind))}</span><span>${SEV[p.severity]}</span><span>until ${when(p.expires)}</span></div>
          <div class="t">${esc(p.title)}</div>
          <div class="a">${esc(p.area_desc || "")}</div>
          <div class="chips">
            ${srcs.map((s) => `<span class="chip src">${esc(FEED_NAMES[s] || s)}</span>`).join("")}
            ${p.located_by === "district" ? `<span class="chip warn">placed by district name</span>` : ""}
            ${!f.geometry ? `<span class="chip warn">not on map</span>` : ""}
            ${rw && rw.crossings_at_risk_3d ? `<span class="chip risk">${rw.crossings_at_risk_3d} crossings ≥50% flood risk, 3 days</span>` : ""}
          </div>
        </div></div>`;
    }).join("");
  }
  $("#hazards").addEventListener("click", (e) => {
    const el = e.target.closest(".hz");
    if (el) select(el.dataset.id, true);
  });
  $("#only-logistics").addEventListener("change", (e) => { state.onlyLogistics = e.target.checked; loadHazards(); });

  function bounds(geom) {
    const b = new maplibregl.LngLatBounds();
    const walk = (c) => (typeof c[0] === "number" ? b.extend(c) : c.forEach(walk));
    walk(geom.coordinates);
    return b;
  }

  async function select(id, fly) {
    if (state.selected && map.getSource("hazards")) map.setFeatureState({ source: "hazards", id: state.selected }, { selected: false });
    state.selected = id;
    if (map.getSource("hazards")) map.setFeatureState({ source: "hazards", id }, { selected: true });
    renderHazards();
    const f = state.hazards.features.find((x) => x.properties.id === id);
    if (!f) return;
    if (fly && f.geometry) map.fitBounds(bounds(f.geometry), { padding: { top: 60, bottom: 60, left: 60, right: 460 }, maxZoom: 9, duration: 800 });
    renderDrawer(f, null);
    try { renderDrawer(f, await api(`/api/hazards/${id}`)); } catch (_) {}
  }

  function renderDrawer(f, detail) {
    const p = f.properties;
    const msgs = detail?.messages || {};
    const sources = p.sources.map((s) => {
      const m = msgs[`${s.source}:${s.ref}`] || {};
      return `<div class="src-row">
        <div class="top"><b>${esc(FEED_NAMES[s.source] || s.source)}</b><span>${when(s.sent)}</span></div>
        <div>${esc(s.sender || m.sender || "")} · ${esc(s.msg_type || "")}</div>
        <div class="mono" style="font-size:11px;color:var(--muted);word-break:break-all">${esc(s.ref)}</div>
        <div>${s.raw_fetch_id ? `<a href="/api/raw/${s.raw_fetch_id}" target="_blank">Raw payload #${s.raw_fetch_id}</a>` : ""}${s.url ? ` · <a href="${esc(s.url)}" target="_blank" rel="noopener">Agency page</a>` : ""}</div>
        ${m.description ? `<div class="desc">${esc(m.description)}</div>` : ""}
        ${m.instruction ? `<div class="desc"><b>Instruction:</b> ${esc(m.instruction)}</div>` : ""}
      </div>`;
    }).join("");
    const rps = detail?.river_points || [];
    const rpRows = rps.filter((r) => !r.no_river).map((r) => {
      const bars = (r.forecast || []).slice(0, 10).map((d) => {
        const v = d.exceed_prob;
        const color = v == null ? "#7b858c" : v >= 0.5 ? "#e0442b" : v >= 0.2 ? "#e9c46a" : "#5cbf8a";
        return `<i title="${d.day}: ${pct(v)}" style="height:${v == null ? 3 : Math.max(3, v * 22)}px;background:${color}"></i>`;
      }).join("");
      return `<tr><td>${esc(r.name || "Unnamed")}<br><span style="color:var(--muted)">${esc(r.road_class || "")}</span></td>
        <td>${r.threshold_m3s ? Math.round(r.threshold_m3s) + " m³/s" : "pending"}${r.threshold_raw_fetch_id ? `<br><a href="/api/raw/${r.threshold_raw_fetch_id}" target="_blank" style="color:var(--accent)">history</a>` : ""}</td>
        <td><div class="probs">${bars}</div></td></tr>`;
    }).join("");
    $("#drawer-body").innerHTML = `
      <div class="hz-k" style="color:${SEV_COLOR[p.severity]};font-size:12px;text-transform:uppercase;letter-spacing:.08em">${esc(kindLabel(p.kind))} · ${SEV[p.severity]}</div>
      <h3>${esc(p.title)}</h3>
      <dl class="kv">
        <dt>Area</dt><dd>${esc(p.area_desc || "—")}</dd>
        <dt>Onset</dt><dd>${when(p.onset)}</dd>
        <dt>Expires</dt><dd>${when(p.expires)}</dd>
        <dt>Certainty</dt><dd>${pct(p.certainty)} (from CAP certainty)</dd>
        <dt>Footprint</dt><dd>${esc({ polygon: "agency polygon", circle: "agency circle", gdacs: "GDACS model polygons", district: "matched district boundaries", none: "not located" }[p.located_by] || p.located_by)}</dd>
        <dt>First seen</dt><dd>${when(p.first_seen)}</dd>
      </dl>
      <h4>Why RASAD believes this (${p.sources.length} source${p.sources.length === 1 ? "" : "s"})</h4>
      ${sources}
      <h4>River crossings watched</h4>
      ${rpRows ? `<table class="rp"><tr><th>Bridge</th><th>Flood level</th><th>Exceedance, next 10 days</th></tr>${rpRows}</table>
        <div class="desc">Flood level = median annual maximum of GloFAS reanalysis (1984–2022) at that river cell. Bars = share of the GloFAS ensemble above it.</div>`
        : `<div class="desc">${detail ? (["flood", "heavy_rain", "cyclone"].includes(p.kind) ? "No crossings analysed yet; river watch runs hourly for moderate and worse alerts." : "Not a river hazard.") : "Loading…"}</div>`}
    `;
    $("#drawer").hidden = false;
  }
  $("#drawer-close").addEventListener("click", () => {
    $("#drawer").hidden = true;
    if (state.selected) map.setFeatureState({ source: "hazards", id: state.selected }, { selected: false });
    state.selected = null;
    renderHazards();
  });

  // ---------- tabs ----------
  document.querySelectorAll(".tab").forEach((t) => t.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((x) => x.classList.toggle("active", x === t));
    $("#pane-access").hidden = t.dataset.tab !== "access";
    $("#pane-hazards").hidden = t.dataset.tab !== "hazards";
    $("#pane-orders").hidden = t.dataset.tab !== "orders";
    $("#pane-trips").hidden = t.dataset.tab !== "trips";
  }));

  // ---------- access windows ----------
  const inHours = (iso) => (iso ? (Date.parse(iso) - Date.now()) / 3600000 : null);

  async function loadShelters() {
    try { state.shelters = await api("/api/shelters"); } catch (e) { $("#access").innerHTML = `<div class="empty">Could not load shelters: ${esc(e.message)}</div>`; return; }
    map.getSource("shelters")?.setData(state.shelters);
    renderAccess();
  }

  function sliceCells(slices) {
    const hours = [0, 6, 12, 24, 48, 72];
    const first = (slices || []).find((x) => x.safe && x.eta_min != null);
    return hours.map((h) => {
      const sl = (slices || []).find((x) => x.hours === h);
      if (!sl) return `<span title="+${h}h: not checked (window already closed)">${h ? "+" + h : "now"}</span>`;
      const slow = sl.safe && first && sl.eta_min > first.eta_min * 1.5;
      const cls = !sl.safe ? "unsafe" : slow ? "slow" : "safe";
      const tip = sl.safe ? `+${h}h: safe route, ${Math.round(sl.eta_min)} min${slow ? " (detour)" : ""}` : `+${h}h: no safe route`;
      return `<span class="${cls}" title="${tip}">${h ? "+" + h : "now"}</span>`;
    }).join("");
  }

  function windowText(p) {
    if (p.status === "cut_off") return "No safe road now";
    if (p.status === "closing") {
      const h = inHours(p.closes_at);
      return `Safe road closes by ~${when(p.closes_at)} (${h < 1 ? "under 1 h" : "in " + Math.round(h) + " h"})`;
    }
    if (p.status === "open") return "Safe road open for 72 h+";
    if (!p.status) return "Waiting for the first routing run";
    return esc(p.note || STATUS_TEXT[p.status] || p.status);
  }

  function renderAccess() {
    const feats = state.shelters?.features || [];
    $("#access-count").textContent = feats.length;
    if (!feats.length) {
      $("#access").innerHTML = `<div class="empty">No shelters in active alert areas yet.<br>RASAD looks up shelters from OpenStreetMap when a moderate or worse flood, cyclone, rain or landslide alert arrives. You can also add one with <b>+ Shelter</b>.<br><br>Access windows need at least one depot: add it with <b>+ Depot</b>.</div>`;
      return;
    }
    $("#access").innerHTML = feats.map((f) => {
      const p = f.properties;
      const st = p.status || "pending";
      const why = (p.blocked_by || []).slice(0, 2).map((b) => esc(b.reason)).join("<br>");
      const via = p.eta_min != null ? `${Math.round(p.eta_min)} min from ${esc(p.depot_name || "depot")}${p.distance_km != null ? ` · ${Number(p.distance_km).toFixed(1)} km` : ""}` : "";
      return `<div class="hz ${state.selectedShelter === p.id ? "active" : ""}" data-sid="${p.id}">
        <div class="bar" style="background:${STATUS_COLOR[st] || "#7b858c"}"></div>
        <div>
          <div class="k"><span class="status ${st}">${esc(STATUS_TEXT[st] || "Pending")}</span>${p.source === "officer" ? "<span>added by officer</span>" : `<span>${esc(p.kind.replace("_", " "))} · OSM</span>`}</div>
          <div class="t">${esc(p.name || "Unnamed " + p.kind.replace("_", " "))}</div>
          <div class="a">${windowText(p)}</div>
          ${via ? `<div class="a">${via}</div>` : ""}
          <div class="a">${p.people != null ? `<span class="people">${Number(p.people).toLocaleString("en-IN")} people</span> <span class="est">${p.people_basis === "headcount" ? "counted" : "estimate"}</span>` : `<span class="est">needs a headcount or population estimate</span>`}</div>
          ${why ? `<div class="why">${why}</div>` : ""}
          ${p.slices && p.slices.length ? `<div class="slices">${sliceCells(p.slices)}</div>` : ""}
        </div></div>`;
    }).join("");
  }
  $("#access").addEventListener("click", (e) => {
    const el = e.target.closest("[data-sid]");
    if (el) selectShelter(Number(el.dataset.sid), true);
  });

  async function selectShelter(id, fly) {
    state.selectedShelter = Number(id);
    renderAccess();
    const f = state.shelters?.features.find((x) => x.properties.id === Number(id));
    try {
      const route = await api(`/api/shelters/${id}/route`);
      map.getSource("route")?.setData(route);
      if (fly) map.fitBounds(bounds(route.geometry), { padding: 80, maxZoom: 13, duration: 700 });
    } catch (_) {
      map.getSource("route")?.setData(empty);
      if (fly && f) map.flyTo({ center: f.geometry.coordinates, zoom: 11 });
    }
    if (f) {
      const p = f.properties;
      const crossed = (p.crossed || []).slice(0, 4).map((c) => `• ${esc(c.reason)}`).join("<br>");
      const pop = new maplibregl.Popup({ closeButton: false, maxWidth: "320px" }).setLngLat(f.geometry.coordinates)
        .setHTML(`<b>${esc(p.name || "Unnamed")}</b><br><span class="status ${p.status}">${esc(STATUS_TEXT[p.status] || "Pending")}</span> · ${windowText(p)}
          ${crossed ? `<br><br><span style="color:var(--muted)">Route passes:</span><br>${crossed}` : ""}
          ${p.raw_fetch_id ? `<br><a href="/api/raw/${p.raw_fetch_id}" target="_blank" style="color:var(--accent)">OSM source</a>` : ""}
          <div id="pop-needs">Loading needs…</div>
          <button class="pop-btn" data-hc="${p.id}">Update headcount</button>`)
        .addTo(map);
      const el = pop.getElement();
      el.querySelector("[data-hc]").addEventListener("click", () => askHeadcount(p.id, p.name));
      try {
        const n = await api(`/api/shelters/${id}/needs`);
        const box = el.querySelector("#pop-needs");
        if (!n.needs.length) { box.innerHTML = `<br><span style="color:var(--muted)">No quantities yet: waiting for a headcount or a WorldPop estimate.</span>`; }
        else {
          const first = n.needs[0];
          const bc = ((first.basis || {}).catchments || []).filter((c) => c.residents != null && c.displacement_rate)
            .sort((x, y) => y.residents * y.displacement_rate - x.residents * x.displacement_rate)[0];
          const cat = bc && { population: bc.residents, displacement_rate: bc.displacement_rate, pop_raw_fetch_id: bc.raw_fetch_id,
            area_km2: (n.catchments.find((c) => c.hazard_id === bc.hazard_id) || {}).area_km2 };
          box.innerHTML = `<br><b>Needs for ${first.days} day${first.days === 1 ? "" : "s"}</b> · ${first.people.toLocaleString("en-IN")} people
            <span style="color:var(--muted)">(${first.people_basis === "headcount" ? "latest headcount" : `estimate: ${cat ? Math.round(cat.population).toLocaleString("en-IN") + " residents in " + cat.area_km2 + " km² × " + Math.round((cat.displacement_rate || 0) * 100) + "%" : "planning"}`})</span>
            <table class="needs">${n.needs.map((x) => `<tr><td title="${esc(x.rate)}">${esc(ITEM_NAMES[x.item] || x.item)}</td><td>${fmtQty(Math.max(0, x.quantity - x.committed), x.unit)}</td></tr>`).join("")}</table>
            <span style="color:var(--muted)">Due by ${when(first.needed_by)}</span>
            ${cat && cat.pop_raw_fetch_id ? ` · <a href="/api/raw/${cat.pop_raw_fetch_id}" target="_blank" style="color:var(--accent)">WorldPop source</a>` : ""}`;
        }
      } catch (_) {}
    }
  }

  function fmtQty(q, unit) {
    if (unit === "g" && q >= 1000) return `${(Math.ceil(q / 100) / 10).toLocaleString("en-IN")} kg`;
    return `${Math.ceil(q).toLocaleString("en-IN")} ${esc(unit)}`;
  }

  async function askHeadcount(id, name) {
    const people = prompt(`How many people are at ${name || "this shelter"} now?`);
    if (people === null || people.trim() === "" || isNaN(Number(people))) return;
    const reporter = prompt("Who is reporting? (shelter in-charge name or phone)");
    if (!reporter) return;
    try {
      await api(`/api/shelters/${id}/headcount`, { method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ people: Math.round(Number(people)), reporter, channel: "control_room" }) });
      pushEvent({ type: "headcount.updated", payload: { shelter_id: id, people: Number(people) } });
    } catch (e) { alert(`Could not save: ${e.message}`); }
  }

  async function loadSuppliers() {
    try { map.getSource("suppliers")?.setData(await api("/api/suppliers")); } catch (_) {}
  }

  // ---------- orders ----------
  const ORDER_TEXT = { awaiting_approval: "Needs your approval", approved: "Approved, placing", confirming: "Placing order",
    awaiting_supplier: "Waiting for supplier", accepted: "Accepted by supplier", packed: "Packed", rejected: "Rejected by supplier",
    cancelled: "Cancelled", failed: "Failed", in_transit: "On the truck", delivered: "Delivered" };
  async function loadOrders() {
    let rows;
    try { rows = await api("/api/procurements"); } catch (e) { $("#orders").innerHTML = `<div class="empty">Could not load orders: ${esc(e.message)}</div>`; return; }
    const waiting = rows.filter((r) => r.status === "awaiting_approval").length;
    $("#order-count").textContent = waiting || rows.length;
    $("#order-count").classList.toggle("warn-count", !!waiting);
    $("#order-count").title = waiting ? `${waiting} waiting for your approval` : "";
    if (!rows.length) {
      $("#orders").innerHTML = `<div class="empty">No orders yet. The Procurer orders from registered suppliers once shelters have needs and suppliers have reported stock. Register suppliers with <b>+ Supplier</b>.</div>`;
      return;
    }
    $("#orders").innerHTML = rows.map((r) => {
      const unit = r.unit === "g" && r.quantity >= 1000 ? `${(r.quantity / 1000).toFixed(1)} kg` : `${Math.round(r.quantity).toLocaleString("en-IN")} ${esc(r.unit)}`;
      const flags = (r.checks || []).filter((c) => c.outcome !== "pass");
      const alts = (r.reasons || {}).alternatives || [];
      return `<div class="hz" data-pid="${r.id}">
        <div class="bar" style="background:${["accepted", "packed", "delivered"].includes(r.status) ? "#5cbf8a" : r.status === "in_transit" ? "#b98cff" : r.status === "awaiting_approval" ? "#e9c46a" : ["rejected", "failed"].includes(r.status) ? "#e0442b" : "#6fb7ff"}"></div>
        <div>
          <div class="k"><span class="status ${r.status}">${esc(ORDER_TEXT[r.status] || r.status)}</span><span>#${r.id}</span>${r.bpp_order_id ? `<span>supplier order ${esc(r.bpp_order_id)}</span>` : ""}</div>
          <div class="t">${unit} ${esc(ITEM_NAMES[r.item] || r.item)} → ${esc(r.shelter_name || "shelter")}</div>
          <div class="a">from ${esc(r.provider_name || "?")}${r.total_price != null ? ` · Rs ${Math.round(r.total_price).toLocaleString("en-IN")}` : " · price not stated"}${r.eta_min != null ? ` · ${Math.round(r.eta_min)} min by road` : ""}</div>
          ${r.note && !flags.length ? `<div class="why">${esc(r.note)}</div>` : ""}
          ${r.dispatch_note && ["accepted", "packed", "in_transit"].includes(r.status) ? `<div class="checks"><div class="approval">Not on a truck yet: ${esc(r.dispatch_note)}</div></div>` : ""}
          ${r.status === "delivered" ? `<div class="why">Delivered ${when(r.delivered_at)}</div>` : ""}
          ${flags.length ? `<div class="checks">${flags.map((c) => `<div class="${c.outcome}">${c.outcome === "block" ? "Blocked" : "Needs approval"}: ${esc(c.detail)}</div>`).join("")}</div>` : ""}
          ${alts.length ? `<div class="why" title="${esc(alts.map((a) => `${a.provider}: ${a.eta_min ?? "?"} min, Rs ${a.unit_price ?? "?"}/unit, ${a.safe_route === false ? "unsafe road" : "ok"}`).join("\n"))}">Chosen over ${alts.length} other offer${alts.length === 1 ? "" : "s"} (hover)</div>` : ""}
          ${r.status === "awaiting_approval" ? `<div class="row-btns"><button class="go" data-approve="${r.id}">Approve</button><button data-deny="${r.id}">Reject</button>
            <a href="/api/beckn/messages?transaction_id=${esc(r.transaction_id)}" target="_blank" style="color:var(--accent);font-size:12px;align-self:center">Beckn log</a></div>`
            : `<div class="why"><a href="/api/beckn/messages?transaction_id=${esc(r.transaction_id)}" target="_blank" style="color:var(--accent)">Beckn messages for this order</a></div>`}
        </div></div>`;
    }).join("");
  }
  $("#orders").addEventListener("click", async (e) => {
    const a = e.target.closest("[data-approve]"), d = e.target.closest("[data-deny]");
    if (!a && !d) return;
    const by = prompt("Your name (recorded with the decision)");
    if (!by) return;
    const id = (a || d).dataset.approve || (a || d).dataset.deny;
    const reason = d ? prompt("Reason", "") : null;
    try { await api(`/api/procurements/${id}/${a ? "approve" : "reject"}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ by, reason }) }); }
    catch (err) { alert(err.message); }
    loadOrders();
  });


  // ---------- trips and drivers ----------
  const TRIP_TEXT = { planned: "Planned", en_route: "On the road", done: "Done", cancelled: "Cancelled" };
  const STOP_TEXT = { done: "done", blocked: "on hold", released: "released", failed: "failed" };
  const hhmm = (iso) => {
    if (!iso) return "—";
    const d = new Date(iso), day = (x) => x.toLocaleDateString("en-IN", { timeZone: "Asia/Kolkata" });
    const t = d.toLocaleTimeString("en-IN", { timeZone: "Asia/Kolkata", hour: "2-digit", minute: "2-digit", hour12: false });
    return day(d) === day(new Date()) ? t : `${d.toLocaleDateString("en-IN", { timeZone: "Asia/Kolkata", day: "numeric", month: "short" })} ${t}`;
  };
  state.trips = [];
  async function loadTrips() {
    let trips, drivers, orders;
    try { [trips, drivers, orders] = await Promise.all([api("/api/trips"), api("/api/drivers"), api("/api/procurements")]); }
    catch (e) { $("#trips").innerHTML = `<div class="empty">Could not load trips: ${esc(e.message)}</div>`; return; }
    state.trips = trips;
    const live = trips.filter((t) => ["planned", "en_route"].includes(t.status));
    $("#trip-count").textContent = live.length || "";
    const waiting = orders.filter((o) => ["accepted", "packed", "in_transit"].includes(o.status) && o.dispatch_note);
    $("#trip-count").classList.toggle("warn-count", waiting.length > 0);
    const link = Object.fromEntries(drivers.map((d) => [d.id, d.link]));
    map.getSource("trips")?.setData({ type: "FeatureCollection", features: live.filter((t) => t.geom).map((t) => ({
      type: "Feature", geometry: t.geom, properties: { id: t.id, status: t.status, driver: t.driver_name, planned_min: t.planned_min } })) });
    map.getSource("trip-stops")?.setData({ type: "FeatureCollection", features: live.flatMap((t) => t.stops
      .filter((st) => ["pending", "blocked"].includes(st.status))
      .map((st) => ({ type: "Feature", geometry: { type: "Point", coordinates: [st.lon, st.lat] },
        properties: { ...st, trip_id: t.id, driver: t.driver_name } }))) });

    const waitHtml = waiting.length ? `<div class="sub-head"><h3>Waiting for a truck (${waiting.length})</h3></div>` + waiting.map((o) =>
      `<div class="why">#${o.id} ${esc(ITEM_NAMES[o.item] || o.item)} → ${esc(o.shelter_name)}: <span style="color:#e9c46a">${esc(o.dispatch_note)}</span></div>`).join("") : "";
    const tripHtml = trips.length ? trips.map((t) => {
      const open = t.stops.filter((st) => ["pending", "blocked"].includes(st.status)).length;
      const rr = t.plan && t.plan.reroute_reason && t.plan.reroute_reason !== "checking held stops" ? t.plan.reroute_reason : null;
      return `<div class="hz" data-trip="${t.id}">
        <div class="bar" style="background:${t.status === "en_route" ? "#b98cff" : t.status === "planned" ? "#6fb7ff" : "#3a454d"}"></div>
        <div>
          <div class="k"><span class="status ${t.status}">${esc(TRIP_TEXT[t.status] || t.status)}</span><span>#${t.id}</span><span>${esc(t.driver_name)} · ${esc(t.vehicle)}</span></div>
          <div class="t">${open ? `${open} stops left · ${Math.round(t.planned_min || 0)} min · ${(t.planned_km || 0).toFixed(1)} km` : `${t.stops.length} stops`}</div>
          <ul class="trip-stops">${t.stops.map((st) => `<li class="${st.status}"><span>${st.seq}</span>
            <span>${st.kind === "pickup" ? "Pick up" : "Drop"} <b>${esc(st.load_text)}</b> ${st.kind === "pickup" ? "at" : "to"} ${esc(st.name)}${st.note ? ` <span title="${esc(st.note)}">ⓘ</span>` : ""}</span>
            <span>${STOP_TEXT[st.status] ? `<em>${STOP_TEXT[st.status]}</em>` : hhmm(st.eta)}</span></li>`).join("")}</ul>
          ${rr ? `<div class="why">Rerouted: ${esc(rr)}</div>` : ""}
          ${["planned", "en_route"].includes(t.status) ? `<div class="row-btns"><button data-reroute="${t.id}">Reroute now</button><button data-cancel-trip="${t.id}">Cancel</button>
            ${link[t.driver_id] ? `<a href="${esc(link[t.driver_id])}" target="_blank" style="color:var(--accent);font-size:12px;align-self:center">Driver page</a>` : ""}</div>` : ""}
        </div></div>`;
    }).join("") : `<div class="empty">No trips yet. When suppliers accept orders, the Dispatcher plans trips for free drivers on roads that are safe now and before each shelter's access window closes.</div>`;
    const drvHtml = `<div class="sub-head"><h3>Drivers (${drivers.length})</h3><button id="add-driver">+ Driver</button></div>` + (drivers.length ? drivers.map((d) =>
      `<div class="driver-row"><div>${esc(d.name)} <span>${esc(d.vehicle)}, ${Math.round(d.capacity_kg).toLocaleString("en-IN")} kg${d.depot ? " · " + esc(d.depot) : ""}</span></div>
       <div><span>${d.trip_id ? "trip #" + d.trip_id : "free"} · GPS ${d.fix_time ? ago(d.fix_time) : "none"}</span> <a href="${esc(d.link)}" target="_blank">page</a></div></div>`).join("")
      : `<div class="why">No drivers yet. Add one with a depot or a tracking-app id so the planner knows where they start.</div>`);
    $("#trips").innerHTML = waitHtml + tripHtml + drvHtml;
  }
  $("#trips").addEventListener("click", async (e) => {
    const rr = e.target.closest("[data-reroute]"), cx = e.target.closest("[data-cancel-trip]"), add = e.target.closest("#add-driver");
    try {
      if (rr) {
        const r = await api(`/api/trips/${rr.dataset.reroute}/reroute`, { method: "POST" });
        pushEvent({ type: "trip.rerouted", payload: { trip_id: rr.dataset.reroute, reason: "requested", released: r.released || [], held: r.held || [] } });
      } else if (cx) {
        const by = prompt("Your name (recorded with the cancellation)");
        if (!by) return;
        await api(`/api/trips/${cx.dataset.cancelTrip}/cancel`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ by, reason: prompt("Reason", "") }) });
      } else if (add) {
        await addDriver();
      } else {
        const card = e.target.closest("[data-trip]");
        const t = card && state.trips.find((x) => String(x.id) === card.dataset.trip);
        if (t && t.geom) { const b = new maplibregl.LngLatBounds(); t.geom.coordinates.forEach((c) => b.extend(c)); map.fitBounds(b, { padding: 60, maxZoom: 14 }); }
        return;
      }
    } catch (err) { alert(err.message); }
    loadTrips();
  });
  async function addDriver() {
    const depots = (await api("/api/depots")).features.map((f) => f.properties);
    const name = prompt("Driver name");
    if (!name) return;
    const phone = prompt("WhatsApp number (10 digits; the trip is sent here)") || null;
    const capacity = Number(prompt("Vehicle capacity in kg", "3000"));
    if (!(capacity > 0)) return alert("Capacity must be a number of kg.");
    const vehicle = prompt("Vehicle", "mini truck") || "mini truck";
    const tracking = prompt("Tracking-app id (Traccar Client 'device identifier'; leave empty if none)") || null;
    let depot_id = null;
    if (depots.length) {
      const pickTxt = depots.map((d, i) => `${i + 1}. ${d.name}`).join("\n");
      const n = Number(prompt(`Home depot (number):\n${pickTxt}`, "1"));
      if (n >= 1 && n <= depots.length) depot_id = depots[n - 1].id;
    }
    const r = await api("/api/drivers", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, phone, capacity_kg: capacity, vehicle, traccar_unique_id: tracking, depot_id }) });
    prompt("Driver registered. Their trip page (also sent on WhatsApp if configured):", r.link);
  }

  async function loadDepots() {
    try { map.getSource("depots")?.setData(await api("/api/depots")); } catch (_) {}
  }
  async function loadClosures() {
    try { map.getSource("closures")?.setData(await api("/api/closures")); } catch (_) {}
  }
  async function loadRisk() {
    try {
      const d = await api(`/api/risk-zones?hours=${state.riskHours}`);
      map.getSource("risk")?.setData(d);
      $("#tb-at").textContent = when(d.at) + " · " + d.features.length + " zones";
    } catch (_) { $("#tb-at").textContent = "unavailable"; }
  }
  document.querySelectorAll(".timebar [data-h]").forEach((b) => b.addEventListener("click", () => {
    document.querySelectorAll(".timebar [data-h]").forEach((x) => x.classList.toggle("active", x === b));
    state.riskHours = Number(b.dataset.h);
    loadRisk();
  }));

  function closurePopup(e) {
    const p = e.features[0].properties;
    const id = p.id;
    const html = `<b>${p.confirmed ? "Confirmed closure" : "Suspected closure"}</b> (${esc(p.source)})<br>${esc(p.reason || "")}<br>
      ${p.source === "report" ? `${p.reports} report${p.reports == 1 ? "" : "s"}<br>` : ""}
      ${!p.confirmed ? `<button data-cl="confirm" data-id="${id}">Confirm</button> ` : ""}<button data-cl="clear" data-id="${id}">Clear</button>`;
    const pop = new maplibregl.Popup({ closeButton: false }).setLngLat(e.lngLat).setHTML(html).addTo(map);
    pop.getElement().addEventListener("click", async (ev) => {
      const b = ev.target.closest("[data-cl]");
      if (!b) return;
      await api(`/api/closures/${b.dataset.id}/${b.dataset.cl}`, { method: "POST" });
      pop.remove();
      loadClosures(); loadRisk();
    });
  }

  // ---------- map tools: depot, shelter, field report ----------
  const HINTS = { depot: "Click where trucks start", shelter: "Click the shelter's location", report: "Click the blocked road", supplier: "Click the supplier's shop or warehouse" };
  document.querySelectorAll(".tools [data-mode]").forEach((b) => b.addEventListener("click", () => {
    state.mode = state.mode === b.dataset.mode ? null : b.dataset.mode;
    document.querySelectorAll(".tools [data-mode]").forEach((x) => x.classList.toggle("active", x.dataset.mode === state.mode));
    $("#mode-hint").textContent = state.mode ? HINTS[state.mode] + " · Esc to cancel" : "";
    document.querySelector(".map-wrap").classList.toggle("picking", !!state.mode);
    map.getCanvas().style.cursor = state.mode ? "crosshair" : "";
  }));
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && state.mode) document.querySelector(`.tools [data-mode="${state.mode}"]`).click();
  });

  async function pick(ll, mode) {
    // Runs after every click handler, so layer handlers have already seen pick mode and stood down.
    if (state.mode === mode) document.querySelector(`.tools [data-mode="${mode}"]`).click();   // leave pick mode
    const body = { lat: ll.lat, lon: ll.lng };
    try {
      if (mode === "depot" || mode === "shelter") {
        const name = prompt(mode === "depot" ? "Depot name (e.g. district warehouse)" : "Shelter name");
        if (!name) return;
        await api(`/api/${mode}s`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ...body, name }) });
        mode === "depot" ? loadDepots() : loadShelters();
      } else if (mode === "supplier") {
        const name = prompt("Supplier name");
        if (!name) return;
        const phone = prompt("WhatsApp number (10 digits; orders go here)") || null;
        const kind = (prompt("Type: shop, wholesaler, pharmacy, ngo_warehouse or government", "shop") || "shop").trim();
        const r = await api("/api/suppliers", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ...body, name, phone, kind }) });
        loadSuppliers();
        prompt("Supplier registered. Send them this link to update stock (also sent on WhatsApp if configured):", r.link);
      } else if (mode === "report") {
        const reporter = prompt("Who is reporting? (name or phone; a second, different reporter confirms the closure)");
        if (!reporter) return;
        const note = prompt("What is wrong with the road?", "road blocked") || "road blocked";
        const r = await api("/api/reports", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ...body, reporter, note, channel: "control_room" }) });
        pushEvent({ type: "closure.updated", payload: { action: r.action, note } });
        loadClosures(); loadRisk();
      }
    } catch (e) { alert(`Could not save: ${e.message}`); }
  }

  // ---------- rivers and vehicles ----------
  async function loadRivers() {
    try { const d = await api("/api/river-points"); map.getSource("rivers")?.setData(d); } catch (_) {}
  }
  async function loadVehicles() {
    try { const d = await api("/api/vehicles"); map.getSource("vehicles")?.setData(d); } catch (_) {}
  }

  // ---------- live events ----------
  const evList = [];
  function pushEvent(ev) {
    if (ev.type === "heartbeat") return;
    const p = ev.payload || {};
    let text = ev.type;
    if (ev.type === "hazard.updated") text = `<b>${esc(p.action)}</b> ${esc(p.kind || "")} ${esc(p.title || p.hazard_id || "")} <span>(${esc(p.source || "")})</span>`;
    else if (ev.type === "vehicle.position") text = `<b>GPS</b> device ${esc(p.device_id)} ${Number(p.lat).toFixed(4)}, ${Number(p.lon).toFixed(4)}`;
    else if (ev.type === "access_window.changed" && p.change === "rerouted") text = `<b>rerouted</b> ${esc(p.name || "shelter " + p.shelter_id)}: ${Math.round(p.previous_eta_min)} → ${Math.round(p.eta_min)} min around a closure`;
    else if (ev.type === "access_window.changed") text = `<b>access</b> ${esc(p.name || "shelter " + p.shelter_id)}: ${esc(STATUS_TEXT[p.previous] || "new")} → <b>${esc(STATUS_TEXT[p.status] || p.status)}</b>${p.blocked_by && p.blocked_by.length ? " (" + esc(p.blocked_by[0]) + ")" : ""}`;
    else if (ev.type === "closure.updated") text = `<b>closure</b> ${esc(p.action || "")} ${esc(p.note || p.source || "")}`;
    else if (ev.type === "cartographer.run") text = `<b>cartographer</b> ${esc(p.shelters)} shelters checked, ${esc(p.changed)} changed`;
    else if (ev.type === "need.updated") text = `<b>needs</b> ${esc(p.name || "shelter " + p.shelter_id)}: ${Number(p.people).toLocaleString("en-IN")} people (${p.people_basis === "headcount" ? "counted" : "estimate"}), ${Number(p.water_litres).toLocaleString("en-IN")} L water for ${p.days} d`;
    else if (ev.type === "headcount.updated") text = `<b>headcount</b> shelter ${esc(p.shelter_id)}: ${esc(p.people)} people`;
    else if (ev.type === "quartermaster.run") text = `<b>quartermaster</b> ${esc(p.shelters_with_needs)} shelters with needs, ${esc(p.awaiting)} awaiting counts`;
    else if (ev.type === "procurement.updated") text = `<b>order</b> ${esc(ORDER_TEXT[p.status] || p.status)}: ${p.quantity != null ? Math.round(p.quantity).toLocaleString("en-IN") + " " : ""}${esc(ITEM_NAMES[p.item] || p.item || "")} ${p.provider ? "from " + esc(p.provider) : ""}${p.note ? " (" + esc(p.note) + ")" : ""}`;
    else if (ev.type === "procurement.no_offer") text = `<b>no supplier</b> for ${esc(ITEM_NAMES[p.item] || p.item)} at ${esc(p.shelter || "shelter")}`;
    else if (ev.type === "supplier.stock") text = `<b>stock</b> supplier ${esc(p.supplier_id)} updated ${esc(ITEM_NAMES[p.item] || p.item)} via ${esc(p.via)}`;
    else if (ev.type === "procurer.run") text = `<b>procurer</b> ${esc(p.placed)} placed, ${esc(p.awaiting_approval)} to approve, ${esc(p.no_offer)} with no offer`;
    else if (ev.type === "depot.updated") text = `<b>depot</b> ${esc(p.action || "")} ${esc(p.name || "")}`;
    else if (ev.type === "trip.planned") text = `<b>trip #${esc(p.trip_id)}</b> planned for ${esc(p.driver)}: ${esc(p.stops)} stops, ${Math.round(p.planned_min)} min${p.start_from === "gps" ? " from live GPS" : ""}`;
    else if (ev.type === "trip.rerouted") text = `<b>reroute</b> trip #${esc(p.trip_id)}: ${esc(p.reason)}${p.released && p.released.length ? `; released ${esc(p.released.join(", "))}` : ""}${p.held && p.held.length ? `; holding ${esc(p.held.join(", "))}` : ""}`;
    else if (ev.type === "trip.deviated") text = `<b>off route</b> ${esc(p.driver)} is ${esc(p.off_m)} m from trip #${esc(p.trip_id)}'s road`;
    else if (ev.type === "trip.stalled") text = `<b>stopped</b> ${esc(p.driver)} has not moved for ${esc(p.minutes)} min (trip #${esc(p.trip_id)})`;
    else if (ev.type === "trip.issue") text = `<b>problem</b> trip #${esc(p.trip_id)} at ${esc(p.place)}: ${esc(p.note)}`;
    else if (ev.type === "trip.updated") text = `<b>trip #${esc(p.trip_id)}</b> ${p.status ? esc(TRIP_TEXT[p.status] || p.status) : esc(p.reason || "updated")}`;
    else if (ev.type === "dispatch.unassigned") text = `<b>no truck</b> for ${esc(ITEM_NAMES[p.item] || p.item)} → ${esc(p.shelter)}: ${esc(p.reason)}`;
    else if (ev.type === "delivery.picked_up") text = `<b>picked up</b> ${esc(p.driver)} at ${esc(p.place)}`;
    else if (ev.type === "delivery.delivered") text = `<b>delivered</b> ${esc(p.driver)}: ${Math.round(p.quantity).toLocaleString("en-IN")} ${esc(ITEM_NAMES[p.item] || p.item)} at ${esc(p.place)}`;
    else if (ev.type === "dispatcher.run") text = `<b>dispatcher</b> ${esc(p.trips)} trips planned, ${esc(p.unplanned)} orders waiting`;
    else if (ev.type === "driver.updated") text = `<b>driver</b> ${esc(p.name || "")} ${p.capacity_kg ? Math.round(p.capacity_kg) + " kg" : ""}`;
    else if (ev.type === "river.updated") text = `<b>river watch</b> ${esc(p.points)} crossings, max 3-day risk ${pct(p.max_prob_3d)}`;
    evList.unshift(`<span class="ev">${new Date().toLocaleTimeString("en-IN", { hour12: false })} ${text}</span>`);
    evList.length = Math.min(evList.length, 25);
    $("#events").innerHTML = evList.join("");
  }

  let hzTimer = null;
  function connect() {
    const ws = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`);
    ws.onopen = () => ($("#ws-state").className = "dot live");
    ws.onclose = () => { $("#ws-state").className = "dot down"; setTimeout(connect, 3000); };
    ws.onmessage = (m) => {
      const ev = JSON.parse(m.data);
      pushEvent(ev);
      if (ev.type === "hazard.updated" || ev.type === "river.updated") {
        clearTimeout(hzTimer);
        hzTimer = setTimeout(() => { loadHazards(); loadRivers(); }, 1500);
      } else if (ev.type === "vehicle.position") {
        loadVehicles();
      } else if (["access_window.changed", "cartographer.run", "need.updated", "quartermaster.run"].includes(ev.type)) {
        loadShelters(); loadHealth();
      } else if (ev.type === "closure.updated") {
        loadClosures(); loadRisk();
      } else if (["procurement.updated", "procurer.run", "procurement.no_offer"].includes(ev.type)) {
        loadOrders(); loadShelters(); loadHealth();
      } else if (ev.type.startsWith("trip.") || ev.type.startsWith("delivery.") || ["dispatch.unassigned", "dispatcher.run", "driver.updated"].includes(ev.type)) {
        loadTrips(); loadOrders(); loadHealth();
        if (ev.type.startsWith("delivery.")) loadShelters();
      } else if (["supplier.stock", "supplier.added"].includes(ev.type)) {
        loadSuppliers();
      } else if (ev.type === "depot.updated" || ev.type === "shelter.updated") {
        loadDepots(); loadShelters();
      }
    };
  }

  async function seedEvents() {
    try { (await api("/api/events?limit=15")).reverse().forEach(pushEvent); } catch (_) {}
  }

  function refreshAll() { loadHealth(); loadHazards(); loadRivers(); loadVehicles(); loadShelters(); loadDepots(); loadClosures(); loadRisk(); loadSuppliers(); loadOrders(); loadTrips(); }
  setInterval(loadHealth, 30000);
  setInterval(() => { loadHazards(); loadRivers(); }, 90000);
  setInterval(loadVehicles, 20000);
  setInterval(() => { loadShelters(); loadClosures(); loadRisk(); loadOrders(); loadTrips(); }, 60000);
  seedEvents();
  connect();
  refreshAll();   // lists fill even if the map (WebGL) fails; map layers fill again on its load event
})();
