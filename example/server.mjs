import http from "node:http";
import { constants, createReadStream } from "node:fs";
import { access, copyFile, lstat, mkdir, mkdtemp, readFile, readdir, realpath, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";

const execFileAsync = promisify(execFile);
const EXAMPLE_DIR = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(EXAMPLE_DIR, "..");
const DEFAULT_UPLOAD_ROOT = path.join(EXAMPLE_DIR, "data", "uploads");
const BUCKETS = new Map([
  ["vector-datasets", path.join(REPO_ROOT, "testdata")],
  ["india-buildings", path.join(EXAMPLE_DIR, "data")],
  ["uploads", DEFAULT_UPLOAD_ROOT],
]);
const ALLOWED_METHODS = "GET, HEAD, POST, OPTIONS";
const PRIMARY_EXTENSIONS = [".geojson", ".json", ".csv", ".tsv", ".shp", ".fgb", ".a.fgb"];
const SIDECAR_EXTENSIONS = new Set([".dbf", ".shx", ".prj"]);
const STATIC_FILES = new Map([
  ["/", [path.join(EXAMPLE_DIR, "index.html"), "text/html; charset=utf-8"]],
  ["/index.html", [path.join(EXAMPLE_DIR, "index.html"), "text/html; charset=utf-8"]],
  ["/styles.css", [path.join(EXAMPLE_DIR, "styles.css"), "text/css; charset=utf-8"]],
  ["/app.js", [path.join(EXAMPLE_DIR, "app.js"), "text/javascript; charset=utf-8"]],
  ["/favicon.svg", [path.join(EXAMPLE_DIR, "favicon.svg"), "image/svg+xml"]],
  ["/fgba-browser.js", [path.join(REPO_ROOT, "crates/fgba/examples/wasm/main.js"), "text/javascript; charset=utf-8"]],
  ["/a5.js", [path.join(EXAMPLE_DIR, "vendor/a5.js"), "text/javascript; charset=utf-8"]],
  ["/deck.gl.min.js", [path.join(EXAMPLE_DIR, "vendor/deck.gl-9.4.0.min.js"), "text/javascript; charset=utf-8"]],
  ["/flatgeobuf-geojson.min.js", [path.join(EXAMPLE_DIR, "vendor/flatgeobuf/flatgeobuf-geojson-4.6.1.min.js"), "text/javascript; charset=utf-8"]],
]);

class HttpError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

function commonHeaders(extra = {}) {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Range, Content-Type",
    "Access-Control-Allow-Methods": ALLOWED_METHODS,
    "Access-Control-Expose-Headers": "Accept-Ranges, Content-Length, Content-Range",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    ...extra,
  };
}

function sendJson(req, res, status, value, extraHeaders = {}) {
  const body = Buffer.from(JSON.stringify(value));
  res.writeHead(status, commonHeaders({
    "Cache-Control": "no-store",
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": body.length,
    ...extraHeaders,
  }));
  res.end(req.method === "HEAD" ? undefined : body);
}

async function listObjects(bucketName, buckets) {
  const root = buckets.get(bucketName);
  if (!root) throw new HttpError(404, "Bucket not found");
  let entries;
  try {
    entries = await readdir(root, { withFileTypes: true });
  } catch (error) {
    if (error.code === "ENOENT" && bucketName === "uploads") return [];
    throw error;
  }
  const files = entries
    .filter((entry) => entry.isFile() && (bucketName !== "uploads" || entry.name.endsWith(".a.fgb")))
    .map((entry) => entry.name)
    .sort();
  return Promise.all(files.map(async (key) => {
    const info = await stat(path.join(root, key));
    return {
      key,
      size: info.size,
      lastModified: info.mtime.toISOString(),
      url: `/s3/${encodeURIComponent(bucketName)}/${encodeURIComponent(key)}`,
    };
  }));
}

function decodePart(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    throw new HttpError(400, "Malformed URL encoding");
  }
}

async function resolveObject(bucketName, key, buckets) {
  const root = buckets.get(bucketName);
  if (!root) throw new HttpError(404, "Bucket not found");
  if (!key || key.includes("\0") || key.includes("\\") || key.split("/").some((part) => part === "." || part === "..")) {
    throw new HttpError(403, "Invalid object key");
  }

  let canonicalRoot;
  try {
    canonicalRoot = await realpath(root);
  } catch (error) {
    if (error.code === "ENOENT") throw new HttpError(404, "Object not found");
    throw error;
  }
  const candidate = path.resolve(canonicalRoot, key);
  if (candidate !== canonicalRoot && !candidate.startsWith(`${canonicalRoot}${path.sep}`)) {
    throw new HttpError(403, "Object key escapes the bucket");
  }

  let canonicalFile;
  try {
    canonicalFile = await realpath(candidate);
  } catch (error) {
    if (error.code === "ENOENT") throw new HttpError(404, "Object not found");
    throw error;
  }
  if (!canonicalFile.startsWith(`${canonicalRoot}${path.sep}`)) {
    throw new HttpError(403, "Object key escapes the bucket");
  }
  const info = await stat(canonicalFile);
  if (!info.isFile()) throw new HttpError(404, "Object not found");
  return { file: canonicalFile, size: info.size };
}

// One RFC 9110 byte range: a-b, a-, or a suffix (-b). Invalid ranges are
// ignored; well-formed but unsatisfiable ranges return 416.
function parseRange(header, size) {
  const full = { start: 0, end: Math.max(0, size - 1), partial: false };
  if (!header) return full;
  const match = /^bytes=(\d*)-(\d*)$/i.exec(header.trim());
  if (!match || (!match[1] && !match[2])) return full;
  if (size === 0) return { unsatisfiable: true };

  let start;
  let end;
  if (!match[1]) {
    const suffix = Number(match[2]);
    if (!Number.isSafeInteger(suffix) || suffix === 0) return { unsatisfiable: true };
    start = Math.max(0, size - suffix);
    end = size - 1;
  } else {
    start = Number(match[1]);
    end = match[2] ? Number(match[2]) : size - 1;
    if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end)) return { start: 0, end: size - 1, partial: false };
  }
  if (start >= size || start > end) return { unsatisfiable: true };
  return { start, end: Math.min(end, size - 1), partial: true };
}

async function sendObject(req, res, bucketName, key, buckets) {
  const { file, size } = await resolveObject(bucketName, key, buckets);
  const range = parseRange(req.headers.range, size);
  if (range.unsatisfiable) {
    res.writeHead(416, commonHeaders({
      "Accept-Ranges": "bytes",
      "Content-Range": `bytes */${size}`,
      "Content-Length": 0,
    }));
    res.end();
    return;
  }

  const length = size === 0 ? 0 : range.end - range.start + 1;
  const headers = commonHeaders({
    "Accept-Ranges": "bytes",
    "Content-Type": "application/octet-stream",
    "Content-Length": length,
  });
  if (range.partial) headers["Content-Range"] = `bytes ${range.start}-${range.end}/${size}`;
  res.writeHead(range.partial ? 206 : 200, headers);
  if (req.method === "HEAD" || length === 0) {
    res.end();
    return;
  }
  const stream = createReadStream(file, { start: range.start, end: range.end });
  stream.on("error", () => res.destroy());
  stream.pipe(res);
}

function readBody(req, maxBytes) {
  const declared = req.headers["content-length"];
  if (declared !== undefined && /^\d+$/.test(declared) && Number(declared) > maxBytes) {
    req.resume();
    throw new HttpError(413, `Request body exceeds the ${maxBytes}-byte limit`);
  }

  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let settled = false;
    const cleanup = () => {
      req.off("data", onData);
      req.off("end", onEnd);
      req.off("error", onError);
      req.off("aborted", onAborted);
    };
    const fail = (error, drain = false) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (drain) {
        req.on("error", () => {});
        req.resume();
      }
      reject(error);
    };
    const onData = (chunk) => {
      size += chunk.length;
      if (size > maxBytes) {
        fail(new HttpError(413, `Request body exceeds the ${maxBytes}-byte limit`), true);
        return;
      }
      chunks.push(chunk);
    };
    const onEnd = () => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(Buffer.concat(chunks, size));
    };
    const onError = (error) => fail(error);
    const onAborted = () => fail(new HttpError(400, "Request body was incomplete"));
    req.on("data", onData);
    req.on("end", onEnd);
    req.on("error", onError);
    req.on("aborted", onAborted);
  });
}

async function resolveCli(configuredPath) {
  if (configuredPath) return configuredPath;
  if (process.env.FGBA_CLI) return process.env.FGBA_CLI;
  for (const candidate of [path.join(REPO_ROOT, "target/debug/fgba"), path.join(REPO_ROOT, "target/release/fgba")]) {
    try {
      await access(candidate, constants.X_OK);
      return candidate;
    } catch {
      // Try the next build profile, then PATH.
    }
  }
  return "fgba";
}

async function runCli(options, args) {
  const cli = await resolveCli(options.cliPath);
  try {
    const result = await execFileAsync(cli, args, {
      encoding: "buffer",
      maxBuffer: options.maxCliOutputBytes,
      shell: false,
      timeout: options.commandTimeoutMs,
      windowsHide: true,
    });
    return { stdout: result.stdout.toString("utf8"), stderr: result.stderr.toString("utf8") };
  } catch (error) {
    const stderr = Buffer.isBuffer(error.stderr) ? error.stderr.toString("utf8").trim() : String(error.stderr || "").trim();
    const stdout = Buffer.isBuffer(error.stdout) ? error.stdout.toString("utf8").trim() : String(error.stdout || "").trim();
    if (error.code === "ENOENT") {
      throw new HttpError(503, "FGBA CLI not found; run `cargo build -p fgba-cli` or set FGBA_CLI");
    }
    if (error.code === "ERR_CHILD_PROCESS_STDIO_MAXBUFFER" || /maxBuffer length exceeded/.test(error.message)) {
      throw new HttpError(413, `FGBA CLI output exceeds the ${options.maxCliOutputBytes}-byte limit`);
    }
    if (error.killed) {
      throw new HttpError(504, `FGBA CLI timed out after ${options.commandTimeoutMs} ms`);
    }
    if (Number.isInteger(error.code)) {
      throw new HttpError(400, stderr || stdout || `FGBA CLI exited with status ${error.code}`);
    }
    throw new HttpError(500, `Could not run FGBA CLI: ${error.message}`);
  }
}

function safeFileName(name, label) {
  if (
    typeof name !== "string" || !name || name === "." || name === ".." ||
    Buffer.byteLength(name, "utf8") > 255 || /[/\\\0-\x1f\x7f]/.test(name)
  ) {
    throw new HttpError(400, `${label} must be a safe basename of at most 255 bytes`);
  }
  return name;
}

function extensionOf(name) {
  const lower = name.toLowerCase();
  return lower.endsWith(".a.fgb") ? ".a.fgb" : path.extname(lower);
}

function singleFormField(form, name, required = true) {
  const values = form.getAll(name);
  if (!required && values.length === 0) return undefined;
  if (values.length !== 1 || typeof values[0] !== "string") {
    throw new HttpError(400, `${name} must be supplied exactly once as text`);
  }
  return values[0];
}

function validateUpload(form) {
  const analyticsText = singleFormField(form, "analytics");
  if (analyticsText !== "true" && analyticsText !== "false") {
    throw new HttpError(400, "analytics must be exactly true or false");
  }
  const chunkRowsText = singleFormField(form, "chunkRows");
  if (!/^\d+$/.test(chunkRowsText)) throw new HttpError(400, "chunkRows must be a positive integer");
  const chunkRows = Number(chunkRowsText);
  if (!Number.isSafeInteger(chunkRows) || chunkRows < 1 || chunkRows > 1_000_000) {
    throw new HttpError(400, "chunkRows must be a positive integer no greater than 1000000");
  }

  const incoming = form.getAll("files");
  if (incoming.length === 0) throw new HttpError(400, "At least one file is required");
  if (incoming.length > 8) throw new HttpError(413, "At most 8 files are allowed");
  const names = new Set();
  const files = incoming.map((file) => {
    if (typeof file === "string" || typeof file?.name !== "string" || typeof file.arrayBuffer !== "function") {
      throw new HttpError(400, "Every files field must contain a file");
    }
    const name = safeFileName(file.name, "Filename");
    const duplicateKey = name.toLowerCase();
    if (names.has(duplicateKey)) throw new HttpError(400, `Duplicate filename: ${name}`);
    names.add(duplicateKey);
    const extension = extensionOf(name);
    if (!PRIMARY_EXTENSIONS.includes(extension) && !SIDECAR_EXTENSIONS.has(extension)) {
      throw new HttpError(400, `Unsupported file extension: ${name}`);
    }
    return { file, name, extension };
  });

  const primaries = files.filter((file) => PRIMARY_EXTENSIONS.includes(file.extension));
  if (primaries.length !== 1) throw new HttpError(400, "Exactly one primary input file is required");
  const primary = primaries[0];
  const sidecars = files.filter((file) => SIDECAR_EXTENSIONS.has(file.extension));
  if (sidecars.length > 0 && primary.extension !== ".shp") {
    throw new HttpError(400, "Shapefile sidecars require a .shp primary input");
  }
  if (primary.extension === ".shp") {
    const basename = primary.name.slice(0, -primary.extension.length);
    for (const sidecar of sidecars) {
      if (sidecar.name.slice(0, -sidecar.extension.length) !== basename) {
        throw new HttpError(400, `Shapefile sidecar basename does not match: ${sidecar.name}`);
      }
    }
    if (!sidecars.some((file) => file.extension === ".shx")) {
      throw new HttpError(400, "Shapefile uploads require the matching .shx sidecar");
    }
  }

  const requestedName = singleFormField(form, "outputName", false);
  const defaultName = `${primary.name.slice(0, -primary.extension.length)}.a.fgb`;
  const outputName = safeFileName(requestedName ?? defaultName, "outputName");
  if (outputName.length <= ".a.fgb".length || !outputName.endsWith(".a.fgb")) {
    throw new HttpError(400, "outputName must be a nonempty basename ending in .a.fgb");
  }
  return { files, primary, outputName, analytics: analyticsText === "true", chunkRows };
}

function parseConversionReport(stdout) {
  const featureMatch = /(?:^|\r?\n)\s*features:\s*(\d+)\s*\((\d+) skipped\)/.exec(stdout)
    || /\((\d+) features?,\s*(\d+) skipped\)/.exec(stdout);
  if (!featureMatch) throw new HttpError(502, "FGBA CLI returned an unreadable conversion report");
  const attributes = /(?:^|\r?\n)\s*attribute columns:\s*(\d+)/.exec(stdout);
  return {
    features: Number(featureMatch[1]),
    skipped: Number(featureMatch[2]),
    attributeColumns: attributes ? Number(attributes[1]) : 0,
  };
}

async function handleUpload(req, res, options) {
  const body = await readBody(req, options.maxUploadBytes);
  let form;
  try {
    form = await new Request("http://localhost/api/uploads", {
      method: "POST",
      headers: req.headers,
      body,
    }).formData();
  } catch {
    throw new HttpError(400, "Malformed multipart/form-data request");
  }
  const { files, primary, outputName, analytics, chunkRows } = validateUpload(form);
  await mkdir(options.uploadRoot, { recursive: true });
  const outputPath = path.join(options.uploadRoot, outputName);
  const exists = await lstat(outputPath).then(() => true, (error) => {
    if (error.code === "ENOENT") return false;
    throw error;
  });
  if (exists) throw new HttpError(409, `Object already exists: ${outputName}`);

  const tempDir = await mkdtemp(path.join(tmpdir(), "fgba-upload-"));
  try {
    const inputDir = path.join(tempDir, "input");
    await mkdir(inputDir);
    for (const { file, name } of files) {
      const bytes = Buffer.from(await file.arrayBuffer());
      await writeFile(path.join(inputDir, name), bytes, { flag: "wx" });
    }
    const tempOutput = path.join(tempDir, "output.a.fgb");
    const args = [
      "convert", path.join(inputDir, primary.name),
      "-o", tempOutput,
      "--chunk-rows", String(chunkRows),
    ];
    if (analytics) args.push("--analytics");
    const { stdout } = await runCli(options, args);
    const report = parseConversionReport(stdout);
    const outputInfo = await stat(tempOutput).catch((error) => {
      if (error.code === "ENOENT") throw new HttpError(502, "FGBA CLI did not create an output file");
      throw error;
    });
    if (!outputInfo.isFile()) throw new HttpError(502, "FGBA CLI did not create a regular output file");
    try {
      await copyFile(tempOutput, outputPath, constants.COPYFILE_EXCL);
    } catch (error) {
      if (error.code === "EEXIST") throw new HttpError(409, `Object already exists: ${outputName}`);
      throw error;
    }
    sendJson(req, res, 201, {
      bucket: "uploads",
      object: {
        key: outputName,
        size: outputInfo.size,
        url: `/s3/uploads/${encodeURIComponent(outputName)}`,
      },
      ...report,
      analytics,
      chunkRows,
    });
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

function validateBbox(value) {
  if (typeof value !== "string") throw new HttpError(400, "bbox must be minx,miny,maxx,maxy");
  const parts = value.split(",");
  const numberPattern = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/i;
  if (parts.length !== 4 || parts.some((part) => !numberPattern.test(part.trim()))) {
    throw new HttpError(400, "bbox must contain four comma-separated numbers");
  }
  const coordinates = parts.map((part) => Number(part.trim()));
  if (coordinates.some((coordinate) => !Number.isFinite(coordinate))) {
    throw new HttpError(400, "bbox coordinates must be finite");
  }
  if (coordinates[0] > coordinates[2] || coordinates[1] > coordinates[3]) {
    throw new HttpError(400, "bbox minimums must not exceed maximums");
  }
  return value;
}

async function handleQuery(req, res, options) {
  const body = await readBody(req, options.maxQueryBytes);
  let input;
  try {
    input = JSON.parse(body.toString("utf8"));
  } catch {
    throw new HttpError(400, "Invalid JSON request body");
  }
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    throw new HttpError(400, "Query body must be a JSON object");
  }
  if (typeof input.bucket !== "string" || typeof input.key !== "string" || typeof input.sql !== "string") {
    throw new HttpError(400, "bucket, key, and sql must be strings");
  }
  if (!input.sql.trim()) throw new HttpError(400, "sql must not be empty");
  const bbox = input.bbox == null ? undefined : validateBbox(input.bbox);
  const hasRes = input.res !== undefined && input.res !== null;
  if (hasRes && (!Number.isInteger(input.res) || input.res < 0 || input.res > 30)) {
    throw new HttpError(400, "res must be an integer from 0 to 30");
  }
  if (hasRes && bbox === undefined) throw new HttpError(400, "res requires bbox");

  const { file } = await resolveObject(input.bucket, input.key, options.buckets);
  if (!input.key.endsWith(".a.fgb")) throw new HttpError(400, "Only .a.fgb objects can be queried");
  const args = ["query", file, input.sql];
  if (bbox !== undefined) args.push("--bbox", bbox);
  if (hasRes) args.push("--res", String(input.res));
  const { stdout } = await runCli(options, args);
  sendJson(req, res, 200, { csv: stdout });
}

function rejectCrossSitePost(req) {
  if (req.headers["sec-fetch-site"]?.trim().toLowerCase() === "cross-site") {
    throw new HttpError(403, "Cross-site POST requests are not allowed");
  }
  const origin = req.headers.origin;
  if (origin === undefined) return;
  let parsed;
  try {
    parsed = new URL(origin);
  } catch {
    throw new HttpError(403, "Cross-site POST requests are not allowed");
  }
  if (
    !["http:", "https:"].includes(parsed.protocol) || !req.headers.host ||
    parsed.host.toLowerCase() !== req.headers.host.toLowerCase()
  ) {
    throw new HttpError(403, "Cross-site POST requests are not allowed");
  }
}

async function handle(req, res, options) {
  if (req.method === "OPTIONS") {
    res.writeHead(204, commonHeaders({ "Content-Length": 0 }));
    res.end();
    return;
  }
  const { pathname } = new URL(req.url, "http://localhost");
  if (pathname === "/api/uploads" || pathname === "/api/query") {
    if (req.method !== "POST") throw new HttpError(405, `${pathname} only supports POST`);
    rejectCrossSitePost(req);
    if (pathname === "/api/uploads") await handleUpload(req, res, options);
    else await handleQuery(req, res, options);
    return;
  }
  if (!["GET", "HEAD"].includes(req.method)) {
    throw new HttpError(405, "Only GET, HEAD, POST, and OPTIONS are supported");
  }

  if (pathname === "/api/buckets") {
    const buckets = await Promise.all([...options.buckets.keys()].map(async (name) => {
      const objects = await listObjects(name, options.buckets);
      return { name, objectCount: objects.length, bytes: objects.reduce((sum, object) => sum + object.size, 0) };
    }));
    sendJson(req, res, 200, { buckets });
    return;
  }

  const objectListMatch = /^\/api\/buckets\/([^/]+)\/objects\/?$/.exec(pathname);
  if (objectListMatch) {
    const bucket = decodePart(objectListMatch[1]);
    sendJson(req, res, 200, { bucket, objects: await listObjects(bucket, options.buckets) });
    return;
  }

  if (pathname.startsWith("/s3/")) {
    const raw = pathname.slice(4);
    const separator = raw.indexOf("/");
    const bucket = decodePart(separator < 0 ? raw : raw.slice(0, separator));
    const key = decodePart(separator < 0 ? "" : raw.slice(separator + 1));
    await sendObject(req, res, bucket, key, options.buckets);
    return;
  }

  const staticFile = STATIC_FILES.get(pathname);
  if (!staticFile) throw new HttpError(404, "Not found");
  try {
    const body = await readFile(staticFile[0]);
    res.writeHead(200, commonHeaders({
      "Cache-Control": "no-cache",
      "Content-Type": staticFile[1],
      "Content-Length": body.length,
    }));
    res.end(req.method === "HEAD" ? undefined : body);
  } catch (error) {
    if (error.code === "ENOENT") throw new HttpError(404, "Not found");
    throw error;
  }
}

export function createExampleServer(options = {}) {
  const uploadRoot = path.resolve(options.uploadRoot ?? DEFAULT_UPLOAD_ROOT);
  const buckets = new Map(BUCKETS);
  buckets.set("uploads", uploadRoot);
  const configuration = {
    buckets,
    uploadRoot,
    cliPath: options.cliPath,
    maxUploadBytes: options.maxUploadBytes ?? 512 * 1024 * 1024,
    maxQueryBytes: options.maxQueryBytes ?? 64 * 1024,
    maxCliOutputBytes: options.maxCliOutputBytes ?? 8 * 1024 * 1024,
    commandTimeoutMs: options.commandTimeoutMs ?? 120_000,
  };
  return http.createServer((req, res) => {
    handle(req, res, configuration).catch((error) => {
      if (res.headersSent) {
        res.destroy();
        return;
      }
      const status = error instanceof HttpError ? error.status : 500;
      if (status === 500) console.error(error);
      sendJson(req, res, status, { error: status === 500 ? "Internal server error" : error.message }, { Allow: ALLOWED_METHODS });
    });
  });
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const port = Number(process.env.PORT || 8090);
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    console.error("PORT must be an integer from 0 to 65535");
    process.exit(1);
  }
  const server = createExampleServer();
  server.on("error", (error) => {
    console.error(`Could not start the example server: ${error.message}`);
    process.exit(1);
  });
  server.listen(port, "127.0.0.1", () => {
    const { port: actualPort } = server.address();
    console.log(`FGBA Range Lab → http://127.0.0.1:${actualPort}`);
    console.log("Local uploads, CLI conversion, and S3-style range storage; Ctrl+C to stop.");
  });
}
