import assert from "node:assert/strict";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { after, afterEach, before, beforeEach, test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { FgbaDataset, initializeFgbaWasm } from "../js/client.js";
import { createSignedObjectStore } from "../js/object-store.js";

const WORLD = [-180, -85.0511, 180, 85.0511];
const fixture = await readFile(new URL("../../../testdata/cities_12.a.fgb", import.meta.url));

function rtreeLayout(count, nodeSize) {
  const sizes = [count];
  let total = count;
  let n = count;
  while (n > 1) {
    n = Math.ceil(n / nodeSize);
    sizes.push(n);
    total += n;
  }
  const starts = [];
  n = total;
  for (const size of sizes) {
    starts.push(n - size);
    n -= size;
  }
  return { sizes, starts, total, leafStart: starts[0] };
}

function readNode(bytes, offset) {
  return {
    minX: bytes.readDoubleLE(offset),
    minY: bytes.readDoubleLE(offset + 8),
    maxX: bytes.readDoubleLE(offset + 16),
    maxY: bytes.readDoubleLE(offset + 24),
    offset: bytes.readBigUInt64LE(offset + 32),
  };
}

function writeNode(bytes, offset, node) {
  bytes.writeDoubleLE(node.minX, offset);
  bytes.writeDoubleLE(node.minY, offset + 8);
  bytes.writeDoubleLE(node.maxX, offset + 16);
  bytes.writeDoubleLE(node.maxY, offset + 24);
  bytes.writeBigUInt64LE(node.offset, offset + 32);
}

function parentNode(children, offset) {
  return {
    minX: Math.min(...children.map((node) => node.minX)),
    minY: Math.min(...children.map((node) => node.minY)),
    maxX: Math.max(...children.map((node) => node.maxX)),
    maxY: Math.max(...children.map((node) => node.maxY)),
    offset: BigInt(offset),
  };
}

function crc32(bytes) {
  let crc = ~0;
  for (const byte of bytes) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit += 1) crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
  }
  return ~crc >>> 0;
}

function makeNodeSizeFixture(nodeSize, featureGap = 0) {
  assert.notEqual(nodeSize, 16);
  const source = Buffer.from(fixture);
  const headerSize = source.readUInt32LE(8);
  const indexStart = 12 + headerSize;
  const root = 12 + source.readUInt32LE(12);
  const vtable = root - source.readInt32LE(root);
  const countOffset = source.readUInt16LE(vtable + 20);
  const featureCount = Number(source.readBigUInt64LE(root + countOffset));
  const oldLayout = rtreeLayout(featureCount, 16);
  const oldIndexSize = oldLayout.total * 40;
  const dataStart = indexStart + oldIndexSize;
  const footerStart = source.length - 32;
  const oldDirectoryOffset = Number(source.readBigUInt64LE(footerStart + 8));
  const oldBandOffset = Number(source.readBigUInt64LE(oldDirectoryOffset + 6));
  const bodyEnd = oldBandOffset - 4;
  const features = [];
  let featurePosition = dataStart;
  for (let index = 0; index < featureCount; index += 1) {
    const size = source.readUInt32LE(featurePosition) + 4;
    features.push(Buffer.from(source.subarray(featurePosition, featurePosition + size)));
    featurePosition += size;
  }
  assert.equal(featurePosition, bodyEnd);
  const featureData = Buffer.alloc(bodyEnd - dataStart + featureGap * (featureCount - 1));
  let featureOffset = 0;
  const featureOffsets = features.map((feature) => {
    const offset = featureOffset;
    feature.copy(featureData, featureOffset);
    featureOffset += feature.length + featureGap;
    return offset;
  });
  const leaves = Array.from({ length: featureCount }, (_, index) => {
    const leaf = readNode(source, indexStart + (oldLayout.leafStart + index) * 40);
    leaf.offset = BigInt(featureOffsets[index]);
    return leaf;
  });
  const layout = rtreeLayout(featureCount, nodeSize);
  const nodes = new Array(layout.total);
  for (let index = 0; index < featureCount; index += 1) nodes[layout.leafStart + index] = leaves[index];
  for (let level = 0; level < layout.sizes.length - 1; level += 1) {
    const childrenStart = layout.starts[level];
    const parentStart = layout.starts[level + 1];
    for (let parent = 0; parent < layout.sizes[level + 1]; parent += 1) {
      const first = parent * nodeSize;
      const end = Math.min(first + nodeSize, layout.sizes[level]);
      const children = nodes.slice(childrenStart + first, childrenStart + end);
      nodes[parentStart + parent] = parentNode(children, childrenStart + first);
    }
  }
  const index = Buffer.alloc(layout.total * 40);
  nodes.forEach((node, position) => writeNode(index, position * 40, node));

  assert.equal(source.readUInt16LE(vtable + 22), 0, "node-size field must be absent in the default fixture");
  assert.deepEqual(source.subarray(root + 4, root + 6), Buffer.alloc(2), "header padding must be free");
  source.writeUInt16LE(4, vtable + 22);
  source.writeUInt16LE(nodeSize, root + 4);

  const directory = Buffer.from(source.subarray(oldDirectoryOffset, footerStart));
  const footer = Buffer.from(source.subarray(footerStart));
  const delta = index.length - oldIndexSize + featureData.length - (bodyEnd - dataStart);
  assert.equal(directory[0], 1);
  assert.equal(directory[1], 1);
  directory.writeBigUInt64LE(directory.readBigUInt64LE(6) + BigInt(delta), 6);
  footer.writeBigUInt64LE(BigInt(oldDirectoryOffset + delta), 8);
  footer.writeUInt32LE(crc32(directory), 24);
  return Buffer.concat([
    source.subarray(0, indexStart),
    index,
    featureData,
    source.subarray(bodyEnd, oldDirectoryOffset),
    directory,
    footer,
  ]);
}

const node4Fixture = makeNodeSizeFixture(4, 5000);
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

function sendPartial(response, start, end, mode, source = fixture) {
  let body = source.subarray(start, end + 1);
  let contentStart = start;
  let contentEnd = end;
  if (mode === "short") body = body.subarray(0, -1);
  if (mode === "wrong") {
    contentStart += 1;
    contentEnd += 1;
  }
  response.statusCode = 206;
  response.setHeader("Content-Range", `bytes ${contentStart}-${contentEnd}/${source.length}`);
  response.setHeader("Content-Length", String(body.length));
  response.end(body);
}

before(async () => {
  objectServer = createServer((request, response) => {
    cors(response);
    const url = new URL(request.url, "http://object.invalid");
    const range = request.headers.range || null;
    const mode = url.pathname.slice(1).replace(/\.fgb$/, "");
    const source = mode === "cities-node4.a" ? node4Fixture : fixture;
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
      response.setHeader("Content-Length", String(source.length));
      response.end(source);
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
    if (end >= source.length) {
      response.statusCode = 416;
      response.setHeader("Content-Range", `bytes */${source.length}`);
      response.end();
      return;
    }
    sendPartial(response, start, end, mode, source);
  });
  objectOrigin = `http://127.0.0.1:${await listen(objectServer)}`;

  signServer = createServer(async (request, response) => {
    cors(response);
    const route = /^\/api\/layers\/([^/]+)\/sign$/.exec(request.url || "");
    if (request.method !== "POST" || !route) {
      response.statusCode = 404;
      response.end();
      return;
    }
    let body = "";
    for await (const chunk of request) body += chunk;
    const { paths } = JSON.parse(body);
    if (!Array.isArray(paths) || paths.length !== 1 || typeof paths[0] !== "string") {
      response.statusCode = 400;
      response.end("expected one relative path");
      return;
    }
    const [key] = paths;
    token += 1;
    const issuedToken = key === "auth.fgb" ? `auth-${++authToken}` : `token-${token}`;
    signingRequests.push({ key, paths, layerKey: decodeURIComponent(route[1]) });
    response.statusCode = 200;
    response.setHeader("Content-Type", "application/json");
    response.end(JSON.stringify({
      generation,
      expires_at: Math.floor(Date.now() / 1000) + 300,
      objects: [{ path: key, url: `${objectOrigin}/${key}?token=${issuedToken}` }],
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
    signEndpoint: `${signOrigin}/api/layers/${encodeURIComponent(key)}/sign`,
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
  assert.ok(signingRequests.every((request) => request.paths.length === 1 && request.paths[0] === "cities_12.a.fgb"));
  assert.ok(signingRequests.every((request) => request.layerKey === "cities_12.a.fgb"));
});

test("a non-default FlatGeobuf index node size returns every exact city", async () => {
  const expectedDataset = await open();
  const expected = await expectedDataset.queryFeatures({ bbox: WORLD, limit: 50_000 });
  const dataset = await open("cities-node4.a.fgb");
  const cities = await dataset.queryFeatures({ bbox: WORLD, limit: 50_000 });
  assert.equal(cities.features.length, 12);
  assert.deepEqual(cities, expected);
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
        generation: "g1",
        expires_at: Math.floor(Date.now() / 1000) + 1,
        objects: [{
          path: "cities_12.a.fgb",
          url: `${objectOrigin}/cities_12.a.fgb?token=short-${signs}`,
        }],
      };
    },
  });
  store.beginOperation(2);
  const probe = await store.get({ key: "cities_12.a.fgb", rangeHeader: "bytes=0-0" });
  assert.equal(probe.status, 206);
  await delay(1100);
  const second = await store.get({ key: "cities_12.a.fgb", rangeHeader: "bytes=1-1" });
  assert.equal(second.status, 206);
  assert.equal(signs, 2);
  await assert.rejects(
    store.get({ key: "cities_12.a.fgb", rangeHeader: "bytes=2-2" }),
    (error) => error.name === "RequestLimit",
  );
  assert.equal(requests.length, 2);
});

test("numeric signing expires_at uses Unix seconds and requires a safe integer", async () => {
  const expiresAt = Math.floor(Date.now() / 1000) + 30;
  const store = createSignedObjectStore({
    key: "cities_12.a.fgb",
    generation: "g1",
    signEndpoint: async () => ({
      generation: "g1",
      expires_at: expiresAt,
      objects: [{ path: "cities_12.a.fgb", url: `${objectOrigin}/cities_12.a.fgb?token=unix-seconds` }],
    }),
  });
  store.beginOperation(1);
  const response = await store.get({ key: "cities_12.a.fgb", rangeHeader: "bytes=0-0" });
  assert.equal(response.status, 206);

  const fractional = createSignedObjectStore({
    key: "cities_12.a.fgb",
    generation: "g1",
    signEndpoint: async () => ({
      generation: "g1",
      expires_at: expiresAt + 0.5,
      objects: [{ path: "cities_12.a.fgb", url: `${objectOrigin}/cities_12.a.fgb?token=fractional` }],
    }),
  });
  fractional.beginOperation(1);
  await assert.rejects(
    fractional.get({ key: "cities_12.a.fgb", rangeHeader: "bytes=0-0" }),
    (error) => error.name === "SigningFailed",
  );
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
      generation: "g1",
      expires_at: Math.floor(Date.now() / 1000) + 10,
      objects: [{ path: "cities_12.a.fgb", url: "https://object.invalid/cities_12.a.fgb" }],
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
      generation: "g1",
      expires_at: Math.floor(Date.now() / 1000) + 10,
      objects: [{ path: "cities_12.a.fgb", url: "https://object.invalid/cities_12.a.fgb" }],
    }),
    fetchImpl: async () => longResponse,
  });
  longStore.beginOperation(1);
  await assert.rejects(
    longStore.get({ key: "cities_12.a.fgb", rangeHeader: "bytes=0-0" }),
    (error) => error.name === "ShortRead",
  );
});
