import { WORLD, loadBand, openDirectory, queryBand } from "/fgba-browser.js";
import { cellEdgeLengthAvg, cellToBoundary } from "a5-js";

const { Deck, WebMercatorViewport, SolidPolygonLayer, GeoJsonLayer, PathLayer, TextLayer } = window.deck;
const DRAW_LIMIT = 50_000;
const SVG_NS = "http://www.w3.org/2000/svg";
const HOME_VIEW = { longitude: 20, latitude: 20, zoom: 0, pitch: 0, bearing: 0, minZoom: 0, maxZoom: 18 };
const state = {
  buckets: [], bucket: null, objects: [], currentObject: null, loading: false, uploading: false, queryRunning: false, queryId: 0, sqlObjectKey: null, outputNameTouched: false, loadId: 0, viewId: 0,
  bands: [], bandCache: new Map(), band: null, hits: [], features: [], mode: "aggregate", requests: [], selectedId: null, selectedFeature: null,
  view: { ...HOME_VIEW }, viewTimer: null, deck: null, bodyReadDepth: 0, updateChain: Promise.resolve(), pendingUpdate: false,
};
const $ = (id) => document.getElementById(id);

function html(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function svg(tag, attributes = {}) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [name, value] of Object.entries(attributes)) node.setAttribute(name, value);
  return node;
}

function icon(name) {
  const node = svg("svg", { "aria-hidden": "true" });
  node.append(svg("use", { href: `#icon-${name}` }));
  return node;
}

function formatBytes(bytes) {
  const units = ["B", "KB", "MB", "GB"];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) { value /= 1024; unit += 1; }
  return `${new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(value)} ${units[unit]}`;
}

function formatCount(count) {
  if (count >= 1_000_000) return `${(count / 1_000_000).toFixed(1)}M`;
  if (count >= 10_000) return `${(count / 1_000).toFixed(1)}K`;
  return count.toLocaleString("en-US");
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, options);
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error || `The local server answered ${response.status}.`);
  return body;
}

async function getJson(url) {
  return requestJson(url);
}

async function postJson(url, value) {
  return requestJson(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(value),
  });
}

function errorMessage(error) {
  if (error instanceof TypeError) return "The local server did not respond. Run `npm start` in example/, then try again.";
  return error.message || String(error);
}

const nativeFetch = window.fetch.bind(window);
window.fetch = async (input, init) => {
  const response = await nativeFetch(input, init);
  if (!state.bodyReadDepth) return response;
  const url = typeof input === "string" ? input : input.url;
  if (url !== state.currentObject?.url) return response;
  const headers = new Headers(input instanceof Request ? input.headers : init?.headers);
  const span = headers.get("Range");
  if (!span) return response;
  const loadId = state.loadId;
  const originalArrayBuffer = response.arrayBuffer.bind(response);
  response.arrayBuffer = async () => {
    const buffer = await originalArrayBuffer();
    if (loadId === state.loadId) {
      recordRange({ url, span, status: response.status, bytes: buffer.byteLength, contentRange: response.headers.get("Content-Range") || "" }, "FGB body");
    }
    return buffer;
  };
  return response;
};

function updateButtons() {
  document.querySelectorAll(".bucket-button, .object-button").forEach((button) => { button.disabled = state.loading; });
  $("refresh").disabled = state.loading || !state.bucket;
  $("upload-open").disabled = state.loading || state.uploading;
  $("upload-submit").disabled = state.uploading || !$("vector-files").files?.length;
  $("upload-cancel").disabled = state.uploading;
  $("upload-close").disabled = state.uploading;
  $("sql-open").disabled = state.loading || state.uploading || !state.currentObject;
  $("sql-run").disabled = state.queryRunning;
  $("sql-clear").disabled = state.queryRunning;
  const unavailable = state.loading || !state.bands.length;
  $("zoom-in").disabled = unavailable || state.view.zoom >= 18;
  $("zoom-out").disabled = unavailable || state.view.zoom <= 0;
  $("fit-data").disabled = unavailable;
}

function renderBuckets(buckets) {
  const list = $("bucket-list");
  list.replaceChildren();
  for (const bucket of buckets) {
    const button = html("button", "bucket-button");
    button.type = "button";
    button.setAttribute("aria-pressed", String(state.bucket === bucket.name));
    button.disabled = state.loading;
    button.title = `${bucket.objectCount} ${bucket.objectCount === 1 ? "object" : "objects"} · ${formatBytes(bucket.bytes)}`;
    button.append(icon("bucket"));
    const copy = html("span", "bucket-copy");
    copy.append(html("strong", "", bucket.name), html("small", "", `${bucket.objectCount} ${bucket.objectCount === 1 ? "object" : "objects"}`));
    button.append(copy);
    button.addEventListener("click", () => loadObjects(bucket.name, true));
    list.append(button);
  }
}

function updateBucketSummary(bucketName) {
  const bucket = state.buckets.find((item) => item.name === bucketName);
  if (!bucket) return;
  bucket.objectCount = state.objects.length;
  bucket.bytes = state.objects.reduce((sum, object) => sum + object.size, 0);
  renderBuckets(state.buckets);
}

function renderObjects() {
  const list = $("object-list");
  list.replaceChildren();
  $("object-path").textContent = state.bucket ? `${state.bucket} / ${state.objects.length} ${state.objects.length === 1 ? "object" : "objects"}` : "No bucket selected";
  if (!state.objects.length) {
    list.append(html("p", "empty-list", "No objects here. Add an .a.fgb file to this bucket and reload."));
    return;
  }
  for (const object of state.objects) {
    const button = html("button", "object-button");
    button.type = "button";
    button.disabled = state.loading;
    button.setAttribute("aria-pressed", String(state.currentObject?.key === object.key));
    button.title = `${object.key}\nLast modified ${new Date(object.lastModified).toLocaleString()}`;
    const copy = html("span", "object-copy");
    const extension = object.key.split(".").pop().toUpperCase();
    copy.append(html("strong", "", object.key), html("small", "", `${formatBytes(object.size)} · ${extension}`));
    button.append(icon("file"), copy, icon("arrow"));
    button.addEventListener("click", () => loadDataset(object));
    list.append(button);
  }
}

async function loadBuckets() {
  try {
    const { buckets } = await getJson("/api/buckets");
    state.buckets = buckets;
    $("bucket-count").textContent = String(buckets.length);
    renderBuckets(buckets);
    const first = buckets.find((bucket) => bucket.name === "india-buildings" && bucket.objectCount > 0)
      || buckets.find((bucket) => bucket.objectCount > 0)
      || buckets[0];
    if (!first) {
      $("bucket-list").append(html("p", "empty-list", "No local buckets are configured."));
      showMapMessage("No bucket available", "Configure a bucket in example/server.mjs, then reload.", false);
      return;
    }
    await loadObjects(first.name, true);
  } catch (error) {
    $("bucket-list").replaceChildren(html("p", "empty-list error-text", errorMessage(error)));
    showMapMessage("Object storage is unavailable", errorMessage(error), true);
  }
}

async function loadObjects(bucketName, previewFirst) {
  state.bucket = bucketName;
  state.objects = [];
  state.currentObject = null;
  renderBuckets(state.buckets);
  $("object-path").textContent = `${bucketName} / reading objects…`;
  const skeleton = html("div", "list-skeleton");
  skeleton.append(html("span"), html("span"));
  $("object-list").replaceChildren(skeleton);
  updateButtons();
  try {
    const { objects } = await getJson(`/api/buckets/${encodeURIComponent(bucketName)}/objects`);
    state.objects = objects;
    updateBucketSummary(bucketName);
    renderObjects();
    updateButtons();
    if (previewFirst && objects.length) {
      const preferred = objects.find((object) => object.key.endsWith(".a.fgb")) || objects[0];
      await loadDataset(preferred);
    } else if (!objects.length) {
      showMapMessage("This bucket is empty", "Add an FGBA object to this bucket and reload the object list.", false);
    }
  } catch (error) {
    $("object-list").replaceChildren(html("p", "empty-list error-text", errorMessage(error)));
    showMapMessage("Could not list this bucket", errorMessage(error), true);
  }
}

function resetDataset(object) {
  clearTimeout(state.viewTimer);
  state.bands = [];
  state.bandCache = new Map();
  state.band = null;
  state.hits = [];
  state.features = [];
  state.mode = "aggregate";
  state.requests = [];
  state.selectedId = null;
  state.selectedFeature = null;
  state.updateChain = Promise.resolve();
  state.pendingUpdate = false;
  state.queryId += 1;
  state.queryRunning = false;
  state.viewId += 1;
  state.view = { ...HOME_VIEW };
  $("dataset-title").textContent = object.key;
  $("dataset-uri").textContent = `s3://${state.bucket}/${object.key}`;
  $("dataset-uri").title = object.url;
  const india = object.key === "india-buildings.a.fgb";
  const uploaded = state.bucket === "uploads";
  const source = $("dataset-source-link");
  source.textContent = india ? "OpenStreetMap contributors · ODbL 1.0" : uploaded ? "Uploaded vector · converted locally" : "FGBA test vector";
  if (india) source.href = "https://www.openstreetmap.org/copyright";
  else source.removeAttribute("href");
  updateSqlObject(object);
  for (const id of ["feature-count", "cell-count", "byte-count"]) $(id).textContent = "—";
  $("resolution").textContent = "—";
  $("resolution-label").textContent = "A5 RES";
  $("feature-label").textContent = "Est. features";
  $("cell-label").textContent = "Visible cells";
  document.querySelector(".legend-ramp").hidden = false;
  $("zoom-value").textContent = "0.0";
  $("selected-cell").textContent = "Select a cell to inspect its count";
  $("legend-min").textContent = "Lower";
  $("legend-max").textContent = "Higher";
  $("request-list").replaceChildren();
  $("request-summary").textContent = "Waiting for the first request";
  $("band-status").hidden = true;
  state.deck.setProps({ initialViewState: state.view, layers: gridLayers() });
}

function appendRequest(detail, phase, index) {
  const row = html("tr", "new-row");
  row.title = detail.contentRange;
  const step = html("td", "step");
  step.append(html("span", "", String(index + 1).padStart(2, "0")), document.createTextNode(phase));
  const response = html("td", "response");
  response.append(html("span", "status-code", String(detail.status)), html("span", "status-text", " Partial"));
  row.append(step, html("td", "range", detail.span), response, html("td", "", formatBytes(detail.bytes)));
  $("request-list").append(row);
  const scroll = $("request-list").closest(".table-scroll");
  scroll.scrollTop = scroll.scrollHeight;
}

function updateTransferSummary() {
  const transferred = state.requests.reduce((sum, request) => sum + request.bytes, 0);
  $("byte-count").textContent = formatBytes(transferred);
  if (!state.requests.length) return;
  const percentage = state.currentObject?.size ? ((transferred / state.currentObject.size) * 100).toFixed(3) : "0";
  $("request-summary").textContent = `${state.requests.length} range requests · ${formatBytes(transferred)} payload · ${percentage}% of object`;
}

function recordRange(detail, phase) {
  state.requests.push({ ...detail, phase });
  appendRequest(detail, phase, state.requests.length - 1);
  const nextMessages = ["Reading the band directory…", "Streaming aggregate bands…", "Drawing the counted A5 cells…"];
  $("map-state-text").textContent = nextMessages[Math.min(state.requests.length - 1, 2)];
  updateTransferSummary();
}

async function ensureBand(resolution) {
  if (state.bandCache.has(resolution)) return state.bandCache.get(resolution);
  const object = state.currentObject;
  const loadId = state.loadId;
  const promise = loadBand(object.url, state.bands.filter((band) => band.res === resolution), (detail) => {
    if (loadId === state.loadId) recordRange(detail, `A5 res ${resolution}`);
  });
  state.bandCache.set(resolution, promise);
  try {
    return await promise;
  } catch (error) {
    state.bandCache.delete(resolution);
    throw error;
  }
}

async function loadDataset(object) {
  if (state.loading || !state.deck) return;
  await state.updateChain;
  state.loading = true;
  state.currentObject = object;
  const loadId = ++state.loadId;
  resetDataset(object);
  renderObjects();
  updateButtons();
  showMapMessage("Pulling vector object", "Requesting the FGBA footer…", false, true);
  try {
    const bands = await openDirectory(object.url, (detail) => {
      if (loadId === state.loadId) recordRange(detail, detail.span === "bytes=-32" ? "Footer" : "Directory");
    });
    if (loadId !== state.loadId) return;
    state.bands = bands.sort((a, b) => a.res - b.res);
    const rendered = await fitToData();
    if (loadId !== state.loadId) return;
    if (!rendered) return;
  } catch (error) {
    if (loadId !== state.loadId) return;
    state.loading = false;
    updateButtons();
    showMapMessage("Could not read this vector object", `${errorMessage(error)} Check that the object is FGBA and the server supports ranges, then try again.`, true);
    $("request-summary").textContent = state.requests.length ? `Stopped after ${state.requests.length} range requests` : "The first range request failed";
  }
}

function chooseResolution(view) {
  const metersPerPixel = 156543.03392 * Math.cos(view.latitude * Math.PI / 180) / (2 ** view.zoom);
  // Use the finest readable band; zoomed-out views fall back to the coarsest.
  // FGBA ladder: https://github.com/afterrealism/fgba/blob/main/SPEC.md#94-the-ladder-and-the-switch-rendering-guidance
  const match = state.bands.findLast((band) => cellEdgeLengthAvg(band.res) / metersPerPixel >= 48);
  return (match || state.bands[0]).res;
}

function viewportBounds(view) {
  const viewport = new WebMercatorViewport({
    width: $("map-shell").clientWidth,
    height: $("map-shell").clientHeight,
    longitude: view.longitude,
    latitude: view.latitude,
    zoom: view.zoom,
    pitch: 0,
    bearing: 0,
  });
  const [west, south, east, north] = viewport.getBounds();
  return [Math.max(-180, west), Math.max(-85.0511, south), Math.min(180, east), Math.min(85.0511, north)];
}

function fittedView(hits) {
  if (!hits.length) return { ...HOME_VIEW };
  let west = 180;
  let south = 85.0511;
  let east = -180;
  let north = -85.0511;
  for (const hit of hits) {
    for (const [longitude, latitude] of cellToBoundary(hit.cell)) {
      west = Math.min(west, longitude);
      south = Math.min(south, latitude);
      east = Math.max(east, longitude);
      north = Math.max(north, latitude);
    }
  }
  const viewport = new WebMercatorViewport({
    width: $("map-shell").clientWidth,
    height: $("map-shell").clientHeight,
    longitude: 0,
    latitude: 0,
    zoom: 0,
  });
  const fitted = viewport.fitBounds([[west, south], [east, north]], { padding: 48, maxZoom: 12 });
  return { ...HOME_VIEW, longitude: fitted.longitude, latitude: fitted.latitude, zoom: fitted.zoom };
}

async function fitToData() {
  await state.updateChain;
  const loadId = state.loadId;
  const band = await ensureBand(state.bands[0].res);
  if (loadId !== state.loadId) return false;
  const view = fittedView(queryBand(band, WORLD));
  setView(view, false);
  return queueViewportUpdate();
}

function queueViewportUpdate() {
  if (state.pendingUpdate) return state.updateChain;
  state.pendingUpdate = true;
  const drain = async () => {
    state.pendingUpdate = false;
    return updateViewport();
  };
  state.updateChain = state.updateChain.then(drain, drain);
  return state.updateChain;
}

async function loadBuildings(bounds) {
  const loadId = state.loadId;
  const viewId = state.viewId;
  const rect = { minX: bounds[0], minY: bounds[1], maxX: bounds[2], maxY: bounds[3] };
  // FlatGeobuf 4.6.1 spatial filter over HTTP ranges.
  // https://unpkg.com/flatgeobuf@4.6.1/lib/mjs/geojson.d.ts
  const iterator = window.flatgeobuf.deserialize(state.currentObject.url, { rect })[Symbol.asyncIterator]();
  const features = [];
  state.bodyReadDepth += 1;
  try {
    while (true) {
      const { value, done } = await iterator.next();
      if (done) break;
      if (loadId !== state.loadId || viewId !== state.viewId) {
        await iterator.return();
        return null;
      }
      features.push(value);
      if (features.length > DRAW_LIMIT) {
        await iterator.return();
        return null;
      }
    }
    return features;
  } finally {
    state.bodyReadDepth -= 1;
  }
}

async function updateViewport() {
  const view = state.view;
  const viewId = state.viewId;
  const loadId = state.loadId;
  const resolution = chooseResolution(view);
  if (state.band?.res !== resolution) {
    $("band-status").textContent = `Loading A5 res ${resolution}…`;
    $("band-status").hidden = false;
  }
  try {
    const band = await ensureBand(resolution);
    if (viewId !== state.viewId || loadId !== state.loadId) return false;
    const bounds = viewportBounds(view);
    const hits = queryBand(band, bounds);
    const estimated = hits.reduce((sum, hit) => sum + hit.count, 0);
    let features = null;
    if (estimated > 0 && estimated <= DRAW_LIMIT) {
      $("band-status").textContent = "Loading full-resolution buildings…";
      $("band-status").hidden = false;
      features = await loadBuildings(bounds);
      if (viewId !== state.viewId || loadId !== state.loadId) return false;
    }
    state.band = band;
    state.hits = hits;
    if (features) renderBuildings(features);
    else renderCells(hits);
    renderViewportStats(band, hits);
    $("band-status").hidden = true;
    if (state.loading) {
      state.loading = false;
      $("map-shell").setAttribute("aria-busy", "false");
      $("map-state").hidden = true;
      renderObjects();
      updateButtons();
    }
    return true;
  } catch (error) {
    if (viewId !== state.viewId || loadId !== state.loadId) return false;
    $("band-status").hidden = true;
    state.loading = false;
    updateButtons();
    showMapMessage("Could not load this zoom band", `${errorMessage(error)} Try another zoom level or reload the object.`, true);
    return false;
  }
}

function renderBuildings(features) {
  state.mode = "features";
  state.features = features;
  if (state.selectedFeature && !features.some((feature) => feature.id === state.selectedFeature.id)) state.selectedFeature = null;
  $("legend-min").textContent = "FGB body";
  $("legend-max").textContent = "Full resolution";
  document.querySelector(".legend-ramp").hidden = true;
  renderBuildingSelection();
  updateDeckLayers();
}

function renderCells(hits) {
  state.mode = "aggregate";
  state.features = [];
  state.selectedFeature = null;
  document.querySelector(".legend-ramp").hidden = false;
  const counts = hits.map((hit) => hit.count);
  const min = counts.length ? Math.min(...counts) : 0;
  const max = counts.length ? Math.max(...counts) : 0;
  const logMin = Math.log1p(min);
  const logRange = Math.log1p(max) - logMin || 1;
  const palette = [[38, 75, 56, 190], [62, 108, 66, 190], [98, 139, 72, 195], [140, 175, 92, 205], [183, 217, 118, 215]];
  for (const hit of hits) {
    const ratio = (Math.log1p(hit.count) - logMin) / logRange;
    hit.color = palette[Math.round(ratio * (palette.length - 1))];
  }
  if (!hits.some((hit) => hit.cell.toString(16) === state.selectedId)) {
    const largest = hits.reduce((best, hit) => !best || hit.count > best.count ? hit : best, null);
    state.selectedId = largest?.cell.toString(16) || null;
  }
  renderCellSelection();
  $("legend-min").textContent = `${formatCount(min)} features`;
  $("legend-max").textContent = `${formatCount(max)} features`;
  updateDeckLayers();
}

function updateDeckLayers() {
  if (state.mode === "features") {
    const selected = state.selectedFeature?.id;
    const layer = new GeoJsonLayer({
      id: "full-resolution-buildings",
      data: { type: "FeatureCollection", features: state.features },
      filled: true,
      stroked: true,
      getFillColor: (feature) => feature.id === selected ? [232, 185, 106, 160] : [98, 139, 72, 85],
      getLineColor: (feature) => feature.id === selected ? [232, 185, 106, 255] : [214, 232, 211, 190],
      getLineWidth: (feature) => feature.id === selected ? 3 : 1,
      lineWidthUnits: "pixels",
      pointType: "circle",
      getPointColor: [232, 185, 106, 220],
      getPointRadius: (feature) => feature.id === selected ? 6 : 4,
      pointRadiusUnits: "pixels",
      pickable: true,
      autoHighlight: true,
      highlightColor: [255, 247, 231, 55],
      updateTriggers: {
        getFillColor: [selected], getLineColor: [selected], getLineWidth: [selected], getPointRadius: [selected],
      },
    });
    state.deck.setProps({ layers: [...gridLayers(), layer] });
    return;
  }
  const selected = state.selectedId;
  const layer = new SolidPolygonLayer({
    id: `a5-cells-${state.band?.res ?? "pending"}`,
    data: state.hits,
    getPolygon: (hit) => cellToBoundary(hit.cell),
    getFillColor: (hit) => hit.color,
    getLineColor: (hit) => hit.cell.toString(16) === selected ? [232, 185, 106, 255] : [214, 232, 211, 150],
    getLineWidth: (hit) => hit.cell.toString(16) === selected ? 3 : 1,
    lineWidthUnits: "pixels",
    stroked: true,
    filled: true,
    extruded: false,
    pickable: true,
    autoHighlight: true,
    highlightColor: [255, 247, 231, 55],
    updateTriggers: { getLineColor: [selected], getLineWidth: [selected] },
  });
  state.deck.setProps({ layers: [...gridLayers(), layer] });
}

function renderCellSelection() {
  const hit = state.hits.find((item) => item.cell.toString(16) === state.selectedId);
  $("selected-cell").textContent = hit
    ? `0x${state.selectedId.padStart(16, "0")} / ${hit.count.toLocaleString("en-US")} features`
    : "No occupied cells cover this view";
}

function renderBuildingSelection() {
  const feature = state.selectedFeature;
  $("selected-cell").textContent = feature
    ? `${feature.geometry.type} / FGB feature ${feature.id}`
    : state.features.length
      ? `${state.features.length.toLocaleString("en-US")} full-resolution features`
      : "No building features cover this view";
}

function renderViewportStats(band, hits) {
  const estimated = hits.reduce((sum, hit) => sum + hit.count, 0);
  const body = state.mode === "features";
  $("feature-label").textContent = "Est. features";
  $("feature-count").textContent = formatCount(estimated);
  $("feature-count").title = `Approximately ${estimated.toLocaleString("en-US")} features in this viewport; ${band.total.toLocaleString("en-US")} in the full band`;
  $("cell-label").textContent = body ? "Buildings" : "Visible cells";
  $("cell-count").textContent = body ? formatCount(state.features.length) : formatCount(hits.length);
  $("cell-count").title = body ? "Actual full-resolution features read from the FlatGeobuf body" : `${band.ids.length.toLocaleString("en-US")} occupied cells in the loaded band`;
  $("resolution-label").textContent = body ? "FGB BODY" : "A5 RES";
  $("resolution").textContent = body ? "FULL" : String(band.res);
  $("deck-map").setAttribute("aria-label", body
    ? `Zoomable full-resolution building map with ${state.features.length} features. Use plus and minus keys to zoom and arrow keys to pan.`
    : `Zoomable A5 map at resolution ${band.res}: ${hits.length} visible cells and approximately ${estimated} features. Use plus and minus keys to zoom and arrow keys to pan.`);
  updateTransferSummary();
}

function selectCell(object) {
  if (state.mode === "features") {
    if (!object?.geometry) return;
    state.selectedFeature = object;
    renderBuildingSelection();
    updateDeckLayers();
    return;
  }
  if (!object?.cell) return;
  state.selectedId = object.cell.toString(16);
  renderCellSelection();
  updateDeckLayers();
}

function gridLayers() {
  const lines = [];
  const labels = [];
  for (let longitude = -180; longitude <= 180; longitude += 30) {
    lines.push({ path: [[longitude, -85.0511], [longitude, 85.0511]], color: [38, 51, 43, 190] });
    if (Math.abs(longitude) < 180 && longitude % 60 === 0) {
      labels.push({ position: [longitude + 1, 82], text: `${Math.abs(longitude)}${longitude === 0 ? "" : longitude < 0 ? "W" : "E"}` });
    }
  }
  for (let latitude = -60; latitude <= 60; latitude += 30) {
    lines.push({ path: [[-180, latitude], [180, latitude]], color: latitude === 0 ? [59, 75, 65, 190] : [38, 51, 43, 190] });
    labels.push({ position: [-178, latitude + 1], text: `${Math.abs(latitude)}${latitude === 0 ? "" : latitude < 0 ? "S" : "N"}` });
  }
  return [
    new PathLayer({ id: "graticule", data: lines, getPath: (line) => line.path, getColor: (line) => line.color, getWidth: 1, widthUnits: "pixels", pickable: false }),
    new TextLayer({ id: "grid-labels", data: labels, visible: window.innerWidth > 560, getPosition: (label) => label.position, getText: (label) => label.text, getSize: 11, getColor: [127, 141, 132, 230], fontFamily: "ui-monospace, monospace", billboard: true, pickable: false }),
  ];
}

function setView(view, schedule = true) {
  state.viewId += 1;
  state.view = view;
  state.deck.setProps({ initialViewState: view });
  $("zoom-value").textContent = view.zoom.toFixed(1);
  updateButtons();
  if (schedule) {
    clearTimeout(state.viewTimer);
    state.viewTimer = setTimeout(queueViewportUpdate, 90);
  }
}

function showMapMessage(title, message, isError, loading = false) {
  const panel = $("map-state");
  panel.hidden = false;
  panel.setAttribute("role", isError ? "alert" : "status");
  panel.querySelector("h3").textContent = title;
  $("map-state-text").textContent = message;
  panel.querySelector(".loading-grid").hidden = !loading;
  $("retry").hidden = !isError;
  $("map-shell").setAttribute("aria-busy", String(loading));
}

function initDeck() {
  const mobile = window.matchMedia("(max-width: 800px)").matches;
  // deck.gl 9.4: MapView interaction and polygon rendering.
  // Sources: https://deck.gl/docs/api-reference/core/controller, https://deck.gl/docs/api-reference/layers/solid-polygon-layer, and https://deck.gl/docs/api-reference/layers/geojson-layer
  state.deck = new Deck({
    parent: $("deck-map"),
    initialViewState: state.view,
    controller: {
      scrollZoom: { smooth: false },
      dragPan: !mobile,
      dragRotate: false,
      doubleClickZoom: true,
      touchZoom: true,
      multiTouchDrag: "pan",
      keyboard: true,
      maxBounds: [[-180, -85.0511], [180, 85.0511]],
    },
    touchAction: mobile ? "pan-y" : "none",
    pickingRadius: 4,
    layers: [],
    onViewStateChange: ({ viewState }) => {
      state.viewId += 1;
      state.view = viewState;
      $("zoom-value").textContent = viewState.zoom.toFixed(1);
      updateButtons();
      clearTimeout(state.viewTimer);
      state.viewTimer = setTimeout(queueViewportUpdate, 90);
      return viewState;
    },
    onClick: ({ object }) => selectCell(object),
    getTooltip: ({ object }) => {
      if (!object) return null;
      const text = state.mode === "features"
        ? `${object.geometry?.type || "Building"}\nFull-resolution feature`
        : `A5 cell 0x${object.cell.toString(16).padStart(16, "0")}\n${object.count.toLocaleString("en-US")} features`;
      return {
        text,
        style: { backgroundColor: "#19221d", color: "#e7ede8", border: "1px solid #415147", padding: "8px 10px", font: "11px ui-monospace, monospace" },
      };
    },
  });
  const canvas = $("deck-map").querySelector("canvas");
  if (canvas) {
    canvas.tabIndex = 0;
    canvas.setAttribute("role", "application");
    canvas.setAttribute("aria-label", "Zoomable vector aggregate map. Use plus and minus keys to zoom and arrow keys to pan.");
  }
}

function openDialog(dialog) {
  if (typeof dialog.showModal === "function") {
    if (!dialog.open) dialog.showModal();
  } else {
    dialog.setAttribute("open", "");
  }
}

function closeDialog(dialog) {
  if (typeof dialog.close === "function" && dialog.open) dialog.close();
  else dialog.removeAttribute("open");
}

function setFormStatus(id, message, kind = "") {
  const node = $(id);
  node.textContent = message;
  node.classList.toggle("error", kind === "error");
  node.classList.toggle("success", kind === "success");
}

function defaultOutputName(name) {
  const lower = name.toLowerCase();
  const base = lower.endsWith(".a.fgb")
    ? name.slice(0, -6)
    : lower.endsWith(".fgb")
      ? name.slice(0, -4)
      : name.replace(/\.[^.]+$/, "");
  return `${base || "upload"}.a.fgb`;
}

function renderSelectedFiles() {
  const files = Array.from($("vector-files").files || []);
  const list = $("selected-files");
  list.replaceChildren();
  for (const file of files) {
    const item = html("li");
    const name = html("span", "", file.name);
    name.title = file.name;
    item.append(name, html("span", "", formatBytes(file.size)));
    list.append(item);
  }
  if (files.length && !state.outputNameTouched) {
    const primary = files.find((file) => /\.(geojson|json|csv|tsv|shp|fgb)$/i.test(file.name) || /\.a\.fgb$/i.test(file.name));
    if (primary) $("output-name").value = defaultOutputName(primary.name);
  }
  updateButtons();
}

async function uploadVector() {
  const files = Array.from($("vector-files").files || []);
  if (!files.length) {
    setFormStatus("upload-status", "Choose at least one vector file first.", "error");
    return;
  }
  const outputName = $("output-name").value.trim();
  const chunkRows = Number($("chunk-rows").value);
  if (!outputName) {
    setFormStatus("upload-status", "Give the converted object a name.", "error");
    return;
  }
  if (!Number.isInteger(chunkRows) || chunkRows < 1 || chunkRows > 1_000_000) {
    setFormStatus("upload-status", "Rows per chunk must be an integer from 1 to 1,000,000.", "error");
    return;
  }
  const form = new FormData();
  form.append("analytics", String($("analytics").checked));
  form.append("chunkRows", String(chunkRows));
  form.append("outputName", outputName);
  for (const file of files) form.append("files", file, file.name);
  state.uploading = true;
  updateButtons();
  setFormStatus("upload-status", "Uploading and converting…");
  try {
    const response = await fetch("/api/uploads", { method: "POST", body: form });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(result.error || `The conversion server answered ${response.status}.`);
    const object = result.object;
    setFormStatus("upload-status", `Converted ${result.features} features to ${object.key}. Opening the map…`, "success");
    await loadObjects("uploads", false);
    const selected = state.objects.find((item) => item.key === object.key) || object;
    closeDialog($("upload-dialog"));
    $("vector-files").value = "";
    state.outputNameTouched = false;
    renderSelectedFiles();
    await loadDataset(selected);
  } catch (error) {
    setFormStatus("upload-status", errorMessage(error), "error");
  } finally {
    state.uploading = false;
    updateButtons();
  }
}

function updateSqlObject(object) {
  const objectId = `${state.bucket}/${object.key}`;
  $("sql-object-name").textContent = `s3://${objectId}`;
  if (state.sqlObjectKey !== objectId) {
    state.sqlObjectKey = objectId;
    $("sql-input").value = "SELECT * FROM features LIMIT 100";
    clearSqlResults();
    setFormStatus("sql-status", "");
  }
  updateSqlTables();
}

function updateSqlTables() {
  const tables = ["features", ...state.bands.map((band) => `a5_band_${band.res}`)];
  if (state.bands.length) tables.push("a5_viewport_approx (bbox required)");
  tables.push("attribute_stats (with analytics)");
  $("sql-tables").textContent = tables.join(" · ");
}

function clearSqlResults() {
  $("sql-results").replaceChildren(html("p", "results-empty", "Run a query to see its results here."));
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
  let quoted = false;
  let sawField = false;
  for (let index = 0; index < text.length; index += 1) {
    const character = text[index];
    if (quoted) {
      if (character === '"' && text[index + 1] === '"') {
        field += '"';
        sawField = true;
        index += 1;
      } else if (character === '"') {
        quoted = false;
      } else {
        field += character;
        sawField = true;
      }
    } else if (character === '"' && field === "") {
      quoted = true;
      sawField = true;
    } else if (character === ",") {
      row.push(field);
      field = "";
      sawField = false;
    } else if (character === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      sawField = false;
    } else if (character !== "\r") {
      field += character;
      sawField = true;
    }
  }
  if (sawField || row.length) {
    row.push(field);
    rows.push(row);
  }
  return rows;
}

function renderSqlResults(csv) {
  const rows = parseCsv(csv);
  const headers = rows.shift() || [];
  const table = html("table");
  const caption = html("caption", "sr-only", `SQL results for ${state.currentObject?.key || "the selected object"}`);
  const thead = html("thead");
  const headerRow = html("tr");
  for (const name of headers) {
    const cell = html("th", "", name || "column");
    cell.scope = "col";
    headerRow.append(cell);
  }
  thead.append(headerRow);
  const tbody = html("tbody");
  if (!rows.length) {
    const empty = html("tr", "empty-row");
    const cell = html("td", "", "No rows returned.");
    cell.colSpan = Math.max(headers.length, 1);
    empty.append(cell);
    tbody.append(empty);
  } else {
    for (const row of rows.slice(0, 1000)) {
      const line = html("tr");
      for (let index = 0; index < headers.length; index += 1) {
        const value = row[index] ?? "";
        const cell = html("td", "", value);
        cell.title = value;
        line.append(cell);
      }
      tbody.append(line);
    }
  }
  table.append(caption, thead, tbody);
  $("sql-results").replaceChildren(table);
  const suffix = rows.length > 1000 ? " · showing the first 1,000" : "";
  setFormStatus("sql-status", `${rows.length.toLocaleString("en-US")} rows · ${headers.length} columns${suffix}`, "success");
}

async function runSql() {
  const object = state.currentObject;
  const bucket = state.bucket;
  const sql = $("sql-input").value.trim();
  if (!object || !sql) {
    setFormStatus("sql-status", "Select an object and enter a SQL query.", "error");
    return;
  }
  const queryId = ++state.queryId;
  const objectId = `${bucket}/${object.key}`;
  state.queryRunning = true;
  clearSqlResults();
  updateButtons();
  setFormStatus("sql-status", "Running query…");
  try {
    const result = await postJson("/api/query", {
      bucket,
      key: object.key,
      sql,
      bbox: $("sql-bbox").value.trim() || null,
      res: $("sql-res").value.trim() ? Number($("sql-res").value) : null,
    });
    if (queryId !== state.queryId || state.sqlObjectKey !== objectId || state.bucket !== bucket || state.currentObject?.key !== object.key) return;
    renderSqlResults(result.csv);
  } catch (error) {
    if (queryId !== state.queryId) return;
    clearSqlResults();
    setFormStatus("sql-status", errorMessage(error), "error");
  } finally {
    if (queryId === state.queryId) {
      state.queryRunning = false;
      updateButtons();
    }
  }
}

const dropZone = $("drop-zone");
for (const eventName of ["dragenter", "dragover"]) {
  dropZone.addEventListener(eventName, (event) => {
    event.preventDefault();
    dropZone.classList.add("dragging");
  });
}
for (const eventName of ["dragleave", "drop", "dragend"]) {
  dropZone.addEventListener(eventName, () => dropZone.classList.remove("dragging"));
}
dropZone.addEventListener("drop", (event) => {
  event.preventDefault();
  if (event.dataTransfer?.files?.length) {
    $("vector-files").files = event.dataTransfer.files;
    renderSelectedFiles();
  }
});
$("vector-files").addEventListener("change", renderSelectedFiles);
$("output-name").addEventListener("input", () => { state.outputNameTouched = true; });
$("upload-open").addEventListener("click", () => {
  setFormStatus("upload-status", "");
  renderSelectedFiles();
  openDialog($("upload-dialog"));
});
$("upload-close").addEventListener("click", () => closeDialog($("upload-dialog")));
$("upload-cancel").addEventListener("click", () => closeDialog($("upload-dialog")));
$("upload-dialog").addEventListener("cancel", (event) => {
  if (state.uploading) event.preventDefault();
});
$("upload-form").addEventListener("submit", (event) => {
  event.preventDefault();
  uploadVector();
});
$("sql-open").addEventListener("click", () => {
  if (!state.currentObject) return;
  updateSqlObject(state.currentObject);
  setFormStatus("sql-status", "");
  openDialog($("sql-dialog"));
});
$("sql-close").addEventListener("click", () => closeDialog($("sql-dialog")));
$("sql-form").addEventListener("submit", (event) => {
  event.preventDefault();
  runSql();
});
$("sql-clear").addEventListener("click", () => {
  clearSqlResults();
  setFormStatus("sql-status", "Results cleared.");
});
document.querySelectorAll(".query-example").forEach((button) => {
  button.addEventListener("click", () => {
    if (button.dataset.needsBbox && !$("sql-bbox").value.trim()) {
      setFormStatus("sql-status", "Enter a viewport bbox before using the approximate covering table.", "error");
      $("sql-bbox").focus();
      return;
    }
    $("sql-input").value = button.dataset.query;
    setFormStatus("sql-status", "Example loaded. Run it when you are ready.");
    $("sql-input").focus();
  });
});
renderSelectedFiles();

$("refresh").addEventListener("click", () => state.bucket && loadObjects(state.bucket, true));
$("retry").addEventListener("click", () => {
  if (state.currentObject) loadDataset(state.currentObject);
  else if (state.bucket) loadObjects(state.bucket, true);
  else loadBuckets();
});
$("zoom-in").addEventListener("click", () => setView({ ...state.view, zoom: Math.min(18, state.view.zoom + 1) }));
$("zoom-out").addEventListener("click", () => setView({ ...state.view, zoom: Math.max(0, state.view.zoom - 1) }));
$("fit-data").addEventListener("click", () => fitToData().catch((error) => showMapMessage("Could not fit the dataset", errorMessage(error), true)));
try {
  initDeck();
  loadBuckets();
} catch (error) {
  showMapMessage("deck.gl could not start", `${errorMessage(error)} WebGL is required for the interactive map.`, true);
}
