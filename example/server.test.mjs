import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { after, before, test } from "node:test";
import { register } from "node:module";
import { createExampleServer } from "./server.mjs";

register(new URL("./a5-test-loader.mjs", import.meta.url));
const { queryBand, WORLD } = await import("../crates/fgba/examples/wasm/main.js");

let server;
let origin;
let uploadRoot;

before(async () => {
  uploadRoot = await mkdtemp(path.join(tmpdir(), "fgba-range-lab-test-"));
  server = createExampleServer({ uploadRoot });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  origin = `http://127.0.0.1:${server.address().port}`;
});

after(async () => {
  try {
    await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  } finally {
    await rm(uploadRoot, { recursive: true, force: true });
  }
});

test("bucket listing resolves to a real object listing", async () => {
  const bucketResponse = await fetch(`${origin}/api/buckets`);
  assert.equal(bucketResponse.status, 200);
  const { buckets } = await bucketResponse.json();
  assert.equal(buckets[0].name, "vector-datasets");

  const objectResponse = await fetch(`${origin}/api/buckets/${buckets[0].name}/objects`);
  assert.equal(objectResponse.status, 200);
  const { objects } = await objectResponse.json();
  const cities = objects.find((object) => object.key === "cities_12.a.fgb");
  assert.ok(cities);
  assert.equal(cities.size, 1777);
  assert.equal(cities.url, "/s3/vector-datasets/cities_12.a.fgb");
});

test("India building object is available when converted locally", async (context) => {
  if (!existsSync(new URL("./data/india-buildings.a.fgb", import.meta.url))) {
    context.skip("Run the optional India conversion first");
    return;
  }
  const response = await fetch(`${origin}/api/buckets/india-buildings/objects`);
  assert.equal(response.status, 200);
  const { objects } = await response.json();
  const buildings = objects.find((object) => object.key === "india-buildings.a.fgb");
  assert.ok(buildings);
  assert.ok(buildings.size > 3_000_000_000);
  const range = await fetch(`${origin}${buildings.url}`, { headers: { Range: "bytes=-32" } });
  assert.equal(range.status, 206);
  assert.equal((await range.arrayBuffer()).byteLength, 32);
});

test("suffix and bounded ranges return the exact object bytes", async () => {
  const source = await readFile(new URL("../testdata/cities_12.a.fgb", import.meta.url));
  for (const [range, start] of [["bytes=-32", source.length - 32], ["bytes=0-15", 0]]) {
    const response = await fetch(`${origin}/s3/vector-datasets/cities_12.a.fgb`, { headers: { Range: range } });
    assert.equal(response.status, 206);
    assert.equal(response.headers.get("accept-ranges"), "bytes");
    const body = Buffer.from(await response.arrayBuffer());
    assert.deepEqual(body, source.subarray(start, start + (range === "bytes=-32" ? 32 : 16)));
  }
});

test("full reads, HEAD, and unsatisfiable ranges have S3-like responses", async () => {
  const url = `${origin}/s3/vector-datasets/cities_12.a.fgb`;
  const full = await fetch(url);
  assert.equal(full.status, 200);
  assert.equal((await full.arrayBuffer()).byteLength, 1777);

  const head = await fetch(url, { method: "HEAD" });
  assert.equal(head.status, 200);
  assert.equal(head.headers.get("content-length"), "1777");
  assert.equal((await head.arrayBuffer()).byteLength, 0);

  const invalid = await fetch(url, { headers: { Range: "bytes=999999-" } });
  assert.equal(invalid.status, 416);
  assert.equal(invalid.headers.get("content-range"), "bytes */1777");
});

test("unknown objects and traversal attempts are rejected", async () => {
  const missing = await fetch(`${origin}/s3/vector-datasets/missing.fgb`);
  assert.equal(missing.status, 404);
  const traversal = await fetch(`${origin}/s3/vector-datasets/..%2Fserver.mjs`);
  assert.equal(traversal.status, 403);
});

test("the website and local browser modules are served", async () => {
  for (const path of ["/", "/app.js", "/fgba-browser.js", "/a5.js", "/deck.gl.min.js", "/flatgeobuf-geojson.min.js"]) {
    const response = await fetch(`${origin}${path}`);
    assert.equal(response.status, 200, path);
    assert.ok((await response.arrayBuffer()).byteLength > 100, path);
  }
});

const POINTS = {
  type: "FeatureCollection",
  features: [
    {
      type: "Feature",
      properties: { category: "a", value: 1 },
      geometry: { type: "Point", coordinates: [10, 20] },
    },
    {
      type: "Feature",
      properties: { category: "b", value: 2 },
      geometry: { type: "Point", coordinates: [11, 21] },
    },
  ],
};

function geoJsonForm({ name = "points.geojson", contents = JSON.stringify(POINTS), analytics = "true", chunkRows = "2", outputName } = {}) {
  const form = new FormData();
  form.append("files", new Blob([contents], { type: "application/geo+json" }), name);
  form.append("analytics", analytics);
  form.append("chunkRows", chunkRows);
  if (outputName !== undefined) form.append("outputName", outputName);
  return form;
}

function postUpload(form) {
  return fetch(`${origin}/api/uploads`, { method: "POST", body: form });
}

function postQuery(payload, base = origin) {
  return fetch(`${base}/api/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

async function expectError(response, status) {
  const body = await response.json();
  assert.equal(response.status, status, JSON.stringify(body));
  assert.equal(typeof body.error, "string");
  assert.ok(body.error.length > 0);
  return body.error;
}

test("GeoJSON upload converts to a queryable uploads object with real CLI SQL", async () => {
  const response = await postUpload(geoJsonForm());
  const uploaded = await response.json();
  assert.equal(response.status, 201, JSON.stringify(uploaded));
  assert.equal(uploaded.bucket, "uploads");
  assert.equal(uploaded.object.key, "points.a.fgb");
  assert.ok(uploaded.object.size > 0);
  assert.equal(uploaded.object.url, "/s3/uploads/points.a.fgb");
  assert.equal(uploaded.features, 2);
  assert.equal(uploaded.skipped, 0);
  assert.equal(uploaded.attributeColumns, 2);
  assert.equal(uploaded.analytics, true);
  assert.equal(uploaded.chunkRows, 2);

  const listingResponse = await fetch(`${origin}/api/buckets/uploads/objects`);
  assert.equal(listingResponse.status, 200);
  const { bucket, objects } = await listingResponse.json();
  assert.equal(bucket, "uploads");
  const listed = objects.find((object) => object.key === uploaded.object.key);
  assert.ok(listed);
  assert.equal(listed.size, uploaded.object.size);
  assert.equal(listed.url, uploaded.object.url);

  const grouped = await postQuery({
    bucket: "uploads",
    key: uploaded.object.key,
    sql: "SELECT category, SUM(value) AS total FROM features GROUP BY category ORDER BY category",
  });
  const groupedBody = await grouped.json();
  assert.equal(grouped.status, 200, JSON.stringify(groupedBody));
  assert.deepEqual(groupedBody.csv.trim().split(/\r?\n/), ["category,total", "a,1", "b,2"]);

  const count = await postQuery({
    bucket: "uploads",
    key: uploaded.object.key,
    sql: "SELECT COUNT(*) AS count FROM features",
    bbox: null,
    res: null,
  });
  const countBody = await count.json();
  assert.equal(count.status, 200, JSON.stringify(countBody));
  assert.deepEqual(countBody.csv.trim().split(/\r?\n/), ["count", "2"]);

  const viewport = await postQuery({
    bucket: "uploads",
    key: uploaded.object.key,
    sql: "SELECT COUNT(*) AS cells FROM a5_viewport_approx",
    bbox: "9,19,12,22",
    res: 2,
  });
  const viewportBody = await viewport.json();
  assert.equal(viewport.status, 200, JSON.stringify(viewportBody));
  assert.match(viewportBody.csv, /^cells\r?\n\d+\r?\n?$/);
});

test("upload validation rejects unsafe names, bad options, and incomplete inputs", async () => {
  await expectError(await postUpload(geoJsonForm({ name: "invalid-chunk.geojson", chunkRows: "0" })), 400);
  await expectError(await postUpload(geoJsonForm({ name: "large-chunk.geojson", chunkRows: "1000001" })), 400);
  await expectError(await postUpload(geoJsonForm({ name: "escape.geojson", outputName: "../escape.a.fgb" })), 400);
  await expectError(await postUpload(geoJsonForm({ name: "duplicate-output.geojson", outputName: "points.a.fgb" })), 409);
  await expectError(await postUpload(geoJsonForm({ name: "../traversal.geojson" })), 400);

  const duplicate = new FormData();
  duplicate.append("files", new Blob([JSON.stringify(POINTS)]), "duplicate.geojson");
  duplicate.append("files", new Blob([JSON.stringify(POINTS)]), "duplicate.geojson");
  duplicate.append("analytics", "false");
  duplicate.append("chunkRows", "2");
  const duplicateError = await expectError(await postUpload(duplicate), 400);
  assert.match(duplicateError, /Duplicate filename/);

  const multiple = new FormData();
  multiple.append("files", new Blob([JSON.stringify(POINTS)]), "one.geojson");
  multiple.append("files", new Blob([JSON.stringify(POINTS)]), "two.json");
  multiple.append("analytics", "false");
  multiple.append("chunkRows", "2");
  const multipleError = await expectError(await postUpload(multiple), 400);
  assert.match(multipleError, /Exactly one primary/);

  const shape = new FormData();
  shape.append("files", new Blob([Buffer.alloc(100)]), "points.shp");
  shape.append("analytics", "false");
  shape.append("chunkRows", "2");
  const shapeError = await expectError(await postUpload(shape), 400);
  assert.match(shapeError, /\.shx/);

  const straySidecar = geoJsonForm({ name: "sidecar.geojson" });
  straySidecar.append("files", new Blob([Buffer.alloc(1)]), "sidecar.dbf");
  const sidecarError = await expectError(await postUpload(straySidecar), 400);
  assert.match(sidecarError, /require a \.shp/);

  const listing = await (await fetch(`${origin}/api/buckets/uploads/objects`)).json();
  assert.deepEqual(listing.objects.map((object) => object.key), ["points.a.fgb"]);
});

test("failed real conversion returns stderr and leaves no output object", async () => {
  const response = await postUpload(geoJsonForm({ name: "broken.geojson", contents: "not GeoJSON" }));
  const error = await expectError(response, 400);
  assert.match(error, /GeoJSON|error/i);
  const listing = await (await fetch(`${origin}/api/buckets/uploads/objects`)).json();
  assert.ok(!listing.objects.some((object) => object.key === "broken.a.fgb"));
});

test("an .a.fgb primary retains its default output name", async () => {
  const bytes = await readFile(new URL("../testdata/cities_12.a.fgb", import.meta.url));
  const form = new FormData();
  form.append("files", new Blob([bytes]), "already.a.fgb");
  form.append("analytics", "false");
  form.append("chunkRows", "12");
  const response = await postUpload(form);
  const uploaded = await response.json();
  assert.equal(response.status, 201, JSON.stringify(uploaded));
  assert.equal(uploaded.object.key, "already.a.fgb");
  assert.equal(uploaded.features, 12);
  assert.equal(uploaded.attributeColumns, 0);
});

test("query validation, traversal, unknown objects, and writes return JSON errors", async () => {
  const base = { bucket: "vector-datasets", key: "cities_12.a.fgb" };
  await expectError(await postQuery({ ...base }), 400);
  await expectError(await postQuery({ ...base, sql: "   " }), 400);
  await expectError(await postQuery({ ...base, sql: "SELECT 1", bbox: "1,2,0,3" }), 400);
  await expectError(await postQuery({ ...base, sql: "SELECT 1", res: 2 }), 400);
  await expectError(await postQuery({ ...base, sql: "SELECT 1", bbox: "0,0,1,1", res: 31 }), 400);
  await expectError(await postQuery({ ...base, key: "../server.mjs", sql: "SELECT 1" }), 403);
  await expectError(await postQuery({ ...base, key: "missing.a.fgb", sql: "SELECT 1" }), 404);

  const writeError = await expectError(await postQuery({ ...base, sql: "CREATE TABLE x (a BIGINT)" }), 400);
  assert.match(writeError, /CREATE|read.only|supported|statement/i);
  const noBboxError = await expectError(await postQuery({ ...base, sql: "SELECT * FROM a5_viewport_approx" }), 400);
  assert.match(noBboxError, /a5_viewport_approx|bbox/i);
});

test("POST CORS preflight and method Allow advertise the implemented methods", async () => {
  const preflight = await fetch(`${origin}/api/uploads`, { method: "OPTIONS" });
  assert.equal(preflight.status, 204);
  assert.match(preflight.headers.get("access-control-allow-methods"), /POST/);
  assert.match(preflight.headers.get("access-control-allow-headers"), /Content-Type/);

  const rejected = await fetch(`${origin}/`, { method: "POST" });
  assert.equal(rejected.status, 405);
  assert.equal(rejected.headers.get("allow"), "GET, HEAD, POST, OPTIONS");
  await rejected.arrayBuffer();
});

test("cross-site POSTs are rejected while same-origin and Origin-free requests work", async () => {
  const sql = "SELECT COUNT(*) FROM features";
  const mismatched = await fetch(`${origin}/api/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Origin: "https://evil.example" },
    body: JSON.stringify({ bucket: "vector-datasets", key: "cities_12.a.fgb", sql }),
  });
  await expectError(mismatched, 403);

  const crossSite = await fetch(`${origin}/api/uploads`, {
    method: "POST",
    headers: { "Sec-Fetch-Site": "cross-site" },
    body: geoJsonForm({ name: "cross-site.geojson" }),
  });
  await expectError(crossSite, 403);
  const listing = await (await fetch(`${origin}/api/buckets/uploads/objects`)).json();
  assert.ok(!listing.objects.some((object) => object.key === "cross-site.a.fgb"));

  const sameOrigin = await fetch(`${origin}/api/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Origin: origin },
    body: JSON.stringify({ bucket: "vector-datasets", key: "cities_12.a.fgb", sql }),
  });
  assert.equal(sameOrigin.status, 200);
  assert.equal((await sameOrigin.json()).csv.trim().split(/\r?\n/).at(-1), "12");
});

async function withExampleServer(options, callback) {
  const isolated = createExampleServer({ uploadRoot, ...options });
  await new Promise((resolve) => isolated.listen(0, "127.0.0.1", resolve));
  const isolatedOrigin = `http://127.0.0.1:${isolated.address().port}`;
  try {
    await callback(isolatedOrigin);
  } finally {
    await new Promise((resolve, reject) => isolated.close((error) => error ? reject(error) : resolve()));
  }
}

test("CLI and request limits produce readable 413, 503, and 504 errors", async () => {
  const query = { bucket: "vector-datasets", key: "cities_12.a.fgb", sql: "SELECT COUNT(*) FROM features" };
  await withExampleServer({ cliPath: path.join(uploadRoot, "missing-fgba") }, async (isolatedOrigin) => {
    const error = await expectError(await postQuery(query, isolatedOrigin), 503);
    assert.match(error, /cargo build -p fgba-cli/);
  });
  await withExampleServer({ maxCliOutputBytes: 1 }, async (isolatedOrigin) => {
    await expectError(await postQuery(query, isolatedOrigin), 413);
  });
  await withExampleServer({ commandTimeoutMs: 1 }, async (isolatedOrigin) => {
    await expectError(await postQuery(query, isolatedOrigin), 504);
  });
  await withExampleServer({ maxUploadBytes: 128, maxQueryBytes: 16 }, async (isolatedOrigin) => {
    await expectError(await fetch(`${isolatedOrigin}/api/uploads`, { method: "POST", body: geoJsonForm() }), 413);
    await expectError(await postQuery(query, isolatedOrigin), 413);
  });
});

test("wide browser band queries retain every occupied cell", () => {
  const newYork = 2774217370460225536n;
  const europe = 7169730606773829632n;
  const band = { res: 2, ids: [newYork, europe], counts: [1, 3], total: 4 };
  const total = (hits) => hits.reduce((sum, hit) => sum + hit.count, 0);
  assert.equal(total(queryBand(band, WORLD)), 4);
  const fitted = queryBand(band, [-105.8272198716823, 25.612994866370542, 42.03886105956777, 64.46769417627937]);
  assert.deepEqual(fitted.map((hit) => hit.cell.toString()).sort(), [newYork, europe].map(String).sort());
  assert.equal(total(fitted), 4);
  assert.equal(total(queryBand(band, [-80, 35, -65, 50])), 1);
  assert.equal(total(queryBand(band, [-10, 40, 10, 60])), 3);
});
