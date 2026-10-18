// FGBA two-phase demo (SPEC §9.3): parse the footer, directory and one
// aggregate band with pure byte reads over HTTP range requests, then draw the
// occupied A5 pentagons on a 2D canvas. The FGBA parsing is dependency-free;
// only the A5 grid math (viewport covering, pentagon outlines) comes from the
// official a5-js bundle (import map in index.html) — the same grid library
// family as the Rust `a5` crate. Honest scope: a directory/band parser demo,
// not a port of the `fgba` crate.

import { polygonToCells, uncompact, cellToBoundary } from "a5-js";

export const WORLD = [-180, -85.0511, 180, 85.0511]; // SPEC §9.4: clamp to the Web Mercator range
const BAND_CAP = 8 << 20; // SPEC §9.2: recommended whole-band fetch cap
const MAGIC = [0x46, 0x47, 0x42, 0x41, 0x47, 0x47, 0x30, 0x31]; // "FGBAGG01"

let requests = 0;
let bytesRead = 0;
export function counters() {
  return { requests, bytesRead };
}

// One range request; `span` is a Range value like "bytes=10-19" or "bytes=-32".
async function range(url, span) {
  const res = await fetch(url, { headers: { Range: span } });
  if (res.status !== 206) {
    throw new Error(`server answered ${res.status} to "Range: ${span}"; FGBA needs a host with range support (206, SPEC §9.2)`);
  }
  const buf = await res.arrayBuffer();
  requests += 1;
  bytesRead += buf.byteLength;
  return { buf, contentRange: res.headers.get("Content-Range") || "" };
}

// CRC-32 (IEEE 802.3, as in PNG/zip) of the directory bytes (SPEC §7).
function crc32(bytes) {
  let crc = ~0;
  for (const b of bytes) {
    crc ^= b;
    for (let k = 0; k < 8; k += 1) crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
  }
  return ~crc >>> 0;
}

// Phase 1: fetch the file's last 32 bytes (the footer — this doubles as the
// range-support probe), then the directory, CRC-verified. Returns the band
// table [{res, offset, size}] of every kind-1 section.
export async function openDirectory(url) {
  const { buf, contentRange } = await range(url, "bytes=-32");
  const total = Number((/\/(\d+)$/.exec(contentRange) || [])[1]);
  if (!Number.isFinite(total) || total < 32) throw new Error(`unparseable Content-Range ${JSON.stringify(contentRange)}`);
  const f = new DataView(buf);
  if (MAGIC.some((b, i) => f.getUint8(i) !== b)) throw new Error("no FGBAGG01 footer magic — plain FlatGeobuf file");
  const dirOffset = f.getBigUint64(8, true);
  const dirSize = f.getBigUint64(16, true);
  const crc = f.getUint32(24, true);
  if (dirSize < 1n || dirSize > (1n << 20n)) throw new Error(`directory size ${dirSize} out of bounds (SPEC §12)`);
  if (dirOffset + dirSize !== BigInt(total - 32)) throw new Error("directory does not end at the footer");
  const { buf: dirBuf } = await range(url, `bytes=${dirOffset}-${dirOffset + dirSize - 1n}`);
  if (crc32(new Uint8Array(dirBuf)) !== crc) throw new Error("directory CRC-32 mismatch");
  const dv = new DataView(dirBuf);
  if (dv.getUint8(0) !== 1) throw new Error(`directory version ${dv.getUint8(0)} (this demo reads v1)`);
  const bands = [];
  let p = 2;
  for (let i = 0, n = dv.getUint8(1); i < n; i += 1) {
    const kind = dv.getUint16(p, true);
    const offset = dv.getBigUint64(p + 4, true);
    const size = dv.getBigUint64(p + 12, true);
    const metaLen = dv.getUint16(p + 20, true);
    if (kind === 1 && metaLen >= 1) bands.push({ res: dv.getUint8(p + 22), offset: Number(offset), size: Number(size) });
    p += 22 + metaLen; // unknown kinds are skipped, never failed on (SPEC §5)
  }
  return bands;
}

// Phase 2: fetch the deepest band within the cap, whole, and parse records.
export async function loadBand(url, bands) {
  const band = bands.filter((b) => b.size <= BAND_CAP).sort((a, b) => b.res - a.res)[0];
  if (!band) throw new Error(`every band exceeds the ${BAND_CAP >> 20} MiB cap (SPEC §9.2)`);
  const { buf } = await range(url, `bytes=${band.offset}-${band.offset + band.size - 1}`);
  const dv = new DataView(buf);
  if (dv.getUint8(0) !== 1 || dv.getUint8(1) !== band.res) throw new Error("band header disagrees with the directory (SPEC §6)");
  const total = dv.getBigUint64(4, true);
  const num = Number(dv.getBigUint64(12, true));
  if (20 + num * 12 !== buf.byteLength) throw new Error("band section size mismatch (SPEC §6)");
  const ids = new BigUint64Array(num);
  const counts = new Uint32Array(num);
  for (let i = 0, q = 20; i < num; i += 1, q += 12) {
    ids[i] = dv.getBigUint64(q, true);
    counts[i] = dv.getUint32(q + 8, true);
  }
  return { ...band, total, ids, counts };
}

// Binary search of one covering cell in the curve-sorted records (SPEC §9.1).
export function findCell(ids, cell) {
  let lo = 0;
  let hi = ids.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (ids[mid] < cell) lo = mid + 1;
    else if (ids[mid] > cell) hi = mid - 1;
    else return mid;
  }
  return -1;
}

// The occupied band records whose cells cover the viewport [w, s, e, n]
// (overlapping containment, SPEC §9.4).
export function queryBand(band, view) {
  const [w, s, e, n] = view;
  const ring = [
    [w, s],
    [e, s],
    [e, n],
    [w, n],
  ];
  const covering = uncompact(polygonToCells(ring, band.res, { containment: "overlapping" }));
  const hits = [];
  for (const cell of covering) {
    const i = findCell(band.ids, cell);
    if (i >= 0) hits.push({ cell: band.ids[i], count: band.counts[i] });
  }
  return hits;
}

// ---------- browser UI (absent under node — the headless test drives the above) ----------

function draw(hits) {
  const cv = document.getElementById("map");
  const ctx = cv.getContext("2d");
  ctx.clearRect(0, 0, cv.width, cv.height);
  // Equirectangular projection; a pentagon crossing the antimeridian streaks
  // across the canvas — acceptable for a demo, a real renderer would clip.
  const project = ([lon, lat]) => [((lon + 180) / 360) * cv.width, ((85.0511 - lat) / 170.1022) * cv.height];
  const max = hits.reduce((m, h) => Math.max(m, h.count), 1);
  for (const { cell, count } of hits) {
    ctx.beginPath();
    cellToBoundary(cell)
      .map(project)
      .forEach(([x, y], i) => (i ? ctx.lineTo(x, y) : ctx.moveTo(x, y)));
    ctx.fillStyle = `hsl(212 75% ${82 - Math.round((55 * count) / max)}%)`;
    ctx.fill();
    ctx.strokeStyle = "#234";
    ctx.stroke();
  }
}

function say(text, isErr) {
  const el = document.getElementById("status");
  el.textContent = text;
  el.className = isErr ? "err" : "";
}

async function run(url) {
  say("loading…");
  const bands = await openDirectory(url);
  const band = await loadBand(url, bands);
  const hits = queryBand(band, WORLD);
  draw(hits);
  const found = hits.reduce((a, h) => a + h.count, 0);
  const { requests: r, bytesRead: b } = counters();
  say(
    `${url}\nbands: ${bands.map((x) => `res ${x.res} (${x.size} B)`).join(", ")}\n` +
      `drew res ${band.res}: ${hits.length} of ${band.ids.length} occupied cells cover the world view, ` +
      `~${found} of ${band.total} features\n${r} range requests, ${b} bytes transferred`
  );
}

if (typeof document !== "undefined") {
  const input = document.getElementById("url");
  input.value = new URLSearchParams(location.search).get("url") || "../../../../testdata/cities_12.a.fgb";
  document.getElementById("load").addEventListener("click", () => run(input.value).catch((e) => say(String(e), true)));
  run(input.value).catch((e) => say(String(e), true));
}
