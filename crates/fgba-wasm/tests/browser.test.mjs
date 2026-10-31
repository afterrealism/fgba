import assert from "node:assert/strict";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { after, afterEach, before, beforeEach, test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { FgbaDataset, initializeFgbaWasm } from "../js/client.js";
import { createSignedObjectStore } from "../js/object-store.js";

const WORLD = [-180, -85.0511, 180, 85.0511];
const fixture = await readFile(new URL("../../../testdata/cities_12.a.fgb", import.meta.url));
let objectServer;
let signServer;
let objectOrigin;
let signOrigin;
let requests = [];
let signingRequests = [];
let token = 0;
let authToken = 0;
let generation = "g1";
let rejectNextAuthorizedRead = false;
const openDatasets = [];

function listen(server) {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve(server.address().port));
  });
}

function closeServer(server) {
  return new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
}

function cors(response) {
  response.setHeader("Access-Control-Allow-Origin", "*");
  response.setHeader("Access-Control-Allow-Headers", "Range, Content-Type");
  response.setHeader("Access-Control-Expose-Headers", "Content-Range, Content-Length");
}

function sendPartial(response, start, end, mode) {
  let body = fixture.subarray(start, end + 1);
  let contentStart = start;
  let contentEnd = end;
  if (mode === "short") body = body.subarray(0, -1);
  if (mode === "wrong") {
    contentStart += 1;
    contentEnd += 1;
  }
  response.statusCode = 206;
  response.setHeader("Content-Range", `bytes ${contentStart}-${contentEnd}/${fixture.length}`);
  response.setHeader("Content-Length", String(body.length));
  response.end(body);
}

before(async () => {
  objectServer = createServer((request, response) => {
    cors(response);
    const url = new URL(request.url, "http://object.invalid");
    const range = request.headers.range || null;
    const mode = url.pathname.slice(1).replace(/\.fgb$/, "");
    requests.push({ path: url.pathname, range, token: url.searchParams.get("token") });
    if (url.pathname.includes("pmtiles")) {
      response.statusCode = 500;
      response.end("PMTiles must not be requested");
      return;
    }
    if (mode === "transport") {
      request.socket.destroy();
      return;
    }
    if (mode === "redirect") {
      response.statusCode = 302;
      response.setHeader("Location", `${objectOrigin}/cities_12.a.fgb?token=redirected`);
      response.end();
      return;
    }
    if (mode === "missing") {
      response.statusCode = 404;
      response.end("missing");
      return;
    }
    if (rejectNextAuthorizedRead || (mode === "auth" && url.searchParams.get("token") === "auth-1")) {
      rejectNextAuthorizedRead = false;
      response.statusCode = 401;
      response.end("expired");
      return;
    }
    if (!range) {
      response.statusCode = 400;
      response.end("a Range header is required");
      return;
    }
    if (mode === "full-object") {
      response.statusCode = 200;
      response.setHeader("Content-Length", String(fixture.length));
      response.end(fixture);
      return;
    }
    const match = /^bytes=(\d+)-(\d+)$/.exec(range);
    if (!match) {
      response.statusCode = 400;
      response.end("only explicit ranges are supported");
      return;
    }
    const start = Number(match[1]);
    const end = Number(match[2]);
    if (end >= fixture.length) {
      response.statusCode = 416;
      response.setHeader("Content-Range", `bytes */${fixture.length}`);
      response.end();
      return;
    }
    sendPartial(response, start, end, mode);
  });
  objectOrigin = `http://127.0.0.1:${await listen(objectServer)}`;

  signServer = createServer(async (request, response) => {
    cors(response);
    if (request.method !== "POST" || request.url !== "/sign") {
      response.statusCode = 404;
      response.end();
      return;
    }
    let body = "";
    for await (const chunk of request) body += chunk;
    const { key, generation: requestedGeneration } = JSON.parse(body);
    token += 1;
    const issuedToken = key === "auth.fgb" ? `auth-${++authToken}` : `token-${token}`;
    signingRequests.push({ key, generation: requestedGeneration });
    response.statusCode = 200;
    response.setHeader("Content-Type", "application/json");
    response.end(JSON.stringify({
      url: `${objectOrigin}/${key}?token=${issuedToken}`,
      expiresAt: Date.now() + 300_000,
      generation,
    }));
  });
  signOrigin = `http://127.0.0.1:${await listen(signServer)}`;
  const wasm = await readFile(new URL("../pkg/fgba_wasm_bg.wasm", import.meta.url));
  await initializeFgbaWasm(wasm);
});

after(async () => {
  await Promise.all([closeServer(objectServer), closeServer(signServer)]);
});

beforeEach(() => {
  requests = [];
  signingRequests = [];
  generation = "g1";
  authToken = 0;
  rejectNextAuthorizedRead = false;
});

afterEach(() => {
  for (const dataset of openDatasets.splice(0)) dataset.close();
});

async function open(key = "cities_12.a.fgb", options = {}) {
  const dataset = await FgbaDataset.open({
    key,
    signEndpoint: `${signOrigin}/sign`,
    generation: "g1",
    ...options,
  });
  openDatasets.push(dataset);
  return dataset;
}

test("sparse open, A5 GeoJSON, and exact cities use only explicit 206 ranges", async () => {
  const dataset = await open();
  assert.equal(requests[0].range, "bytes=0-0");
  assert.equal(requests[1].range, `bytes=${fixture.length - 32}-${fixture.length - 1}`);
  assert.ok(requests.every((request) => request.range && !request.range.startsWith("bytes=-")));

  const bands = dataset.bands();
  assert.ok(bands.length > 0);
  for (const band of bands) {
    assert.equal(typeof band.offset, "string");
    assert.equal(typeof band.size, "string");
    assert.ok(BigInt(band.offset) < BigInt(fixture.length));
  }

  const aggregates = await dataset.queryA5({ bbox: WORLD });
  assert.equal(aggregates.type, "FeatureCollection");
  assert.equal(aggregates.estimatedCount, "12");
  assert.ok(aggregates.features.length > 0);
  assert.ok(aggregates.features.every((feature) => typeof feature.properties.cell === "string"));
  assert.ok(aggregates.features.every((feature) => feature.geometry.type === "Polygon"));

  const cities = await dataset.queryFeatures({ bbox: WORLD, limit: 50_000 });
  assert.equal(cities.type, "FeatureCollection");
  assert.equal(cities.features.length, 12);
  assert.ok(cities.features.every((feature) => feature.geometry.type === "Point"));
  assert.ok(cities.features.some((feature) => {
    const [x, y] = feature.geometry.coordinates;
    return Math.abs(x - 2.36) < 1e-9 && Math.abs(y - 48.84) < 1e-9;
  }));
  const limited = await dataset.queryFeatures({ bbox: WORLD, limit: 3 });
  assert.equal(limited.features.length, 3);
  assert.ok(requests.every((request) => request.range));
  assert.ok(requests.every((request) => !request.path.includes("pmtiles")));
  assert.ok(signingRequests.every((request) => request.generation === "g1"));
});

test("404 is distinct from a transport failure and neither refreshes authorization", async () => {
  await assert.rejects(open("missing.fgb"), (error) => {
    assert.equal(error.name, "ObjectNotFound");
    assert.equal(error.status, 404);
    assert.doesNotMatch(error.message, /token=|signed|127\.0\.0\.1/);
    return true;
  });
  assert.equal(signingRequests.length, 1);

  signingRequests = [];
  await assert.rejects(open("transport.fgb"), (error) => {
    assert.equal(error.name, "ObjectTransportError");
    assert.doesNotMatch(error.message, /token=|signed|127\.0\.0\.1/);
    return true;
  });
  assert.equal(signingRequests.length, 1);
});

test("200 partial fallback, short reads, wrong spans, and redirects are rejected", async () => {
  await assert.rejects(open("full-object.fgb"), (error) => error.name === "RangeRequired");
  assert.equal(requests[0].range, "bytes=0-0");
  assert.equal(requests.filter((request) => !request.range).length, 0);

  await assert.rejects(open("short.fgb"), (error) => error.name === "ShortRead");
  await assert.rejects(open("wrong.fgb"), (error) => error.name === "ContentRangeError");
  await assert.rejects(open("redirect.fgb"), (error) => error.name === "ObjectTransportError");
});

test("401 causes exactly one signing refresh and retries the same sparse range", async () => {
  const dataset = await open("auth.fgb");
  assert.ok(dataset.bands().length > 0);
  assert.equal(signingRequests.length, 2);
  assert.deepEqual(requests.slice(0, 2).map((request) => request.range), ["bytes=0-0", "bytes=0-0"]);
  assert.deepEqual(requests.slice(0, 2).map((request) => request.token), ["auth-1", "auth-2"]);
});

test("a generation change discards the open dataset and all cached state", async () => {
  const dataset = await open();
  await dataset.queryA5({ bbox: WORLD });
  generation = "g2";
  rejectNextAuthorizedRead = true;
  const before = requests.length;
  await assert.rejects(dataset.queryFeatures({ bbox: [0, 0, 1, 1], limit: 1 }), (error) => {
    assert.equal(error.name, "GenerationChanged");
    assert.doesNotMatch(error.message, /token=|127\.0\.0\.1/);
    return true;
  });
  assert.throws(() => dataset.bands(), (error) => error.name === "DatasetClosed");
  const after = requests.length;
  await assert.rejects(dataset.queryA5({ bbox: WORLD }), (error) => error.name === "DatasetClosed");
  assert.equal(requests.length, after);
  assert.ok(after > before);
});

test("signing refreshes proactively inside the TTL skew and requests stay capped", async () => {
  let signs = 0;
  const store = createSignedObjectStore({
    key: "cities_12.a.fgb",
    generation: "g1",
    signEndpoint: async () => {
      signs += 1;
      return {
        url: `${objectOrigin}/cities_12.a.fgb?token=short-${signs}`,
        expiresAt: Date.now() + 40,
        generation: "g1",
      };
    },
  });
  store.beginOperation(2);
  const probe = await store.get({ key: "cities_12.a.fgb", rangeHeader: "bytes=0-0" });
  assert.equal(probe.status, 206);
  await delay(60);
  const second = await store.get({ key: "cities_12.a.fgb", rangeHeader: "bytes=1-1" });
  assert.equal(second.status, 206);
  assert.equal(signs, 2);
  await assert.rejects(
    store.get({ key: "cities_12.a.fgb", rangeHeader: "bytes=2-2" }),
    (error) => error.name === "RequestLimit",
  );
  assert.equal(requests.length, 2);
});

test("invalid responses are cancelled before unbounded body consumption", async () => {
  let canceled = false;
  const fullResponse = new Response(new ReadableStream({
    pull(controller) {
      controller.enqueue(fixture);
    },
    cancel() {
      canceled = true;
    },
  }), { status: 200 });
  const fullStore = createSignedObjectStore({
    key: "cities_12.a.fgb",
    generation: "g1",
    signEndpoint: async () => ({
      url: "https://object.invalid/cities_12.a.fgb",
      expiresAt: Date.now() + 10_000,
      generation: "g1",
    }),
    fetchImpl: async () => fullResponse,
  });
  fullStore.beginOperation(1);
  await assert.rejects(
    fullStore.get({ key: "cities_12.a.fgb", rangeHeader: "bytes=0-0" }),
    (error) => error.name === "RangeRequired",
  );
  assert.equal(canceled, true);

  const longResponse = new Response(new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array([1, 2]));
      controller.close();
    },
  }), {
    status: 206,
    headers: { "Content-Range": `bytes 0-0/${fixture.length}` },
  });
  const longStore = createSignedObjectStore({
    key: "cities_12.a.fgb",
    generation: "g1",
    signEndpoint: async () => ({
      url: "https://object.invalid/cities_12.a.fgb",
      expiresAt: Date.now() + 10_000,
      generation: "g1",
    }),
    fetchImpl: async () => longResponse,
  });
  longStore.beginOperation(1);
  await assert.rejects(
    longStore.get({ key: "cities_12.a.fgb", rangeHeader: "bytes=0-0" }),
    (error) => error.name === "ShortRead",
  );
});
