# Tilify integration: native FGBA and standalone browser WASM

This note describes the two distinct FGBA artifacts used by Tilify Everything.
For the end-to-end service contracts, see Tilify's `docs/architecture.md`.
The binary format remains defined by [SPEC.md](../SPEC.md).

## Roles: native processor versus browser artifact

| Artifact | Role in Tilify | What it is not |
|---|---|---|
| `fgba-cli` / `fgba` binary | Native upload ETL: builds and verifies the immutable FGBA object; native `fgba query` remains available for local SQL | Not a browser bundle and not an application-server query/tile API |
| `fgba-sql` | Native streaming DataFusion SQL plus the pure-Rust one-partition `EagerSession` reused by browser WASM | Not DuckDB, SQL.js, or a remote SQL service |
| `fgba-wasm` | Standalone Rust/WASM + JavaScript worker/client for sparse signed reads, A5/GeoJSON visualization, and bounded local SQL | Not a whole-object downloader and not an unbounded lazy SQL engine |
| `crates/fgba/examples/wasm` | Manual, pure-JavaScript footer/directory/band parser and canvas demo | Not the standalone WASM artifact, not a Rust port, and not SQL |
| `example/` Range Lab | Local native-CLI/server demonstration with its own explicit demo routes | Not Tilify's direct-browser architecture |

The workspace MSRV is **Rust 1.91**. The registry release order is
`fgba` → `fgba-sql` → `fgba-cli`; the browser package is built from this source
tree with wasm-pack rather than treated as an additional registry release step.

## Native Tilify ETL

Tilify normalizes an uploaded vector to FlatGeobuf, then runs:

```bash
fgba build normalized.fgb --out analysis.a.fgb --analytics --geometry-metrics
fgba verify analysis.a.fgb
```

The resulting bytes are stored as
`vectors/<layer-key>/<generation>.a.fgb`. The control plane exposes the logical
path `analysis.a.fgb`; the registry maps that path to the immutable generation.
A ready vector has `format=fgba` and `queryable=true`.

Native `fgba query` still executes streaming read-only SQL over local FGB/FGBA
files. It supports `features`, `a5_band_<res>`, `a5_viewport_approx`, and
`attribute_stats`, including planar `st_length`, `st_area`, and
`st_npoints(Binary)` UDFs and stored `geom_length`, `geom_area`, and
`geom_npoints` metric columns. These are planar degree-unit operations over
WGS84 coordinates, not CRS-transforming geodesic functions.

`fgba serve PATH` is only a minimal sequential loopback **static file server**
with GET/HEAD and single-range support. It does not serve a viewer, execute SQL,
or provide map/tile/query endpoints. The Range Lab has a separate explicit demo
server and must not be confused with `fgba serve`.

Native `Reader::query`/`stats` cache only the most recently used band;
changing resolution replaces it, and `read_band` performs a fresh decode. Other
clients may implement their own bounded caches. Cache size/policy is not a
binary-format guarantee.

## Build the actual standalone browser package

From the FGBA repository root:

```bash
rustup target add wasm32-unknown-unknown
npx --yes wasm-pack@0.15.1 build crates/fgba-wasm \
  --target web --release -- --locked
```

The default output is generated `crates/fgba-wasm/pkg/`, which is build output,
not source to commit. Tilify's `scripts/build-web.mjs` instead supplies a
temporary `--out-dir`, fingerprints the generated `pkg/` plus `js/` files, and
copies them into its ignored, versioned `web/assets/runtimes/` layout.

The browser-facing modules are:

- `js/client.js`: `FgbaDataset.open`, `bands`, `queryA5`, `queryFeatures`,
  `querySql`, `close`, and the 30-second `FgbaWorkerClient` SQL watchdog;
- `js/worker.js`: serialized mutable WASM operations;
- `js/object-store.js`: signed URL refresh, strict range validation, and the
  128-request-per-core-operation ceiling;
- Rust `range`/`reader`: sparse `Read + Seek`, typed cache misses, A5 and exact
  FlatGeobuf reads;
- Rust `arrow`/`sql` plus `fgba-sql`: bounded source batches and direct async
  DataFusion execution.

Tilify opens the logical object with the same registry generation throughout:

```text
FgbaDataset.open({
  key: "analysis.a.fgb",
  signEndpoint,
  generation,
  maxRangeBytes: 8 MiB,
  maxCacheBytes: 64 MiB
})
```

A5 results are GeoJSON with estimated counts preserved safely; exact feature
results are capped at 50,000 per draw request. Large integer/count/offset values
cross JavaScript as decimal strings/BigInt where needed.

## Signed URL lifecycle and CORS

Tilify's signer accepts only `analysis.a.fgb` for a vector and returns:

```json
{
  "generation": "immutable-generation",
  "expires_at": 1790000300,
  "objects": [{"path":"analysis.a.fgb","url":"https://object-store/...X-Amz-Signature=..."}]
}
```

The response is `Cache-Control: no-store` and
`Referrer-Policy: no-referrer`. Only GET is signed. The caller cannot select a
bucket, host, generation, arbitrary prefix, or another extension.

The browser transport:

1. signs through the app, then fetches only from the returned object URL;
2. probes `Range: bytes=0-0`, parses the total from `Content-Range`, and uses an
   explicit `<size-32>-<size-1>` footer range instead of relying on suffix
   range Fetch behavior;
3. requires `206` and exact range/body metadata for every partial read — a
   `200` response is an error and never triggers a whole-object fallback;
4. refreshes at `expires_at - min(60 seconds, 20% of TTL)` and retries one
   `401`/`403` after refreshing;
5. rejects a generation change and discards the dataset/cache rather than
   mixing generations;
6. uses `credentials: "omit"`, `redirect: "error"`, and `cache: "no-store"`;
   production object URLs require HTTPS, with HTTP only for loopback tests.

Signed URLs remain out of logs, DOM/location state, localStorage, analytics,
QGIS state, and persistent cache manifests.

Bucket CORS must allow the exact Tilify web origins, GET, and `Range`, and expose
`Accept-Ranges`, `Content-Length`, and `Content-Range`. The provider's unsigned
preflight `OPTIONS` must permit Range; it is not an object method that needs a
signature. Presigning does not bypass CORS. Local MinIO verification does not
prove Alibaba OSS compatibility.

## Browser SQL support and hard limits

A browser query accepts exactly one `SELECT` or plain read-only `EXPLAIN`.
The parser/source allowlist and DataFusion `SQLOptions` reject DDL/DML,
multiple statements, mutation hidden in CTE/EXPLAIN, `SELECT INTO`, external
table functions, unknown sources, and `EXPLAIN ANALYZE`. The physical-plan gate
also rejects analyze/repartition/coalesce plans and requires one final output
partition.

Only these sources are exposed:

| Source | Browser materialization |
|---|---|
| `features` | Referenced columns from kind-2/3 Arrow chunks when sufficient; bounded exact-body conversion for wildcard/WKB/missing metrics/malformed Arrow fallback |
| `a5_band_<res>` | The selected existing aggregate band |
| `a5_viewport_approx` | A5 covering for the required viewport, optionally with an available resolution |
| `attribute_stats` | Public kind-2 global metadata; requires valid BODY_ANALYTICS |

The existing planar geometry UDFs and stored geometry metric columns are
available within the supported DataFusion corpus. DataFusion runs over isolated
one-partition `MemTable` batches through direct async streams. There is no
`block_on`, `spawn_blocking`, Tokio runtime, JavaScript SQL engine, or DuckDB.

This is **bounded eager materialization**. Browser v1 does not push down filters,
`LIMIT`, joins, or zone maps and has no disk spill. Input caps are not a bound on
all query working memory.

| Limit | Hard cap |
|---|---:|
| One sparse range | 8 MiB |
| Sparse cache | 64 MiB |
| Requests driven by one core operation | 128 |
| Exact features per draw request | 50,000 |
| Encoded SQL source bytes | 256 MiB |
| Decoded SQL source arrays | 512 MiB |
| Source rows across referenced tables | 1,000,000 |
| Result rows | 1,000 |
| Result JSON | 8 MiB |
| SQL worker watchdog | 30 seconds |

Result JSON uses decimal strings for signed/unsigned 64-bit integers. Callers
may set lower limits, never values above the hard caps.

## QGIS and deletion in Tilify

QGIS lists the measured FGBA object, enforces its default **1 GiB** download cap
before signing/fetching, signs the logical `analysis.a.fgb` path, and downloads
directly into a managed `<layer-key>/<generation>` cache. Exact path/size checks
and a metadata-only manifest gate cache reuse; partial generations are removed.
Saved projects open the local FGBA through OGR, never an expiring URL. Vector
retains **QGIS 3.22+**; signed URLs never enter project files, QSettings,
history, logs, or cache manifests.

Tilify owner deletion marks the registry generation `deleting`, removes the raw
source prefix and `vectors/<key>/<generation>.a.fgb`, then erases the registry
row and returns 204. Storage failure leaves a retryable deleting record and
returns 502. Anonymous TTL and failed/stale-upload cleanup use the same
registered-prefix lifecycle. FGBA itself does not implement this multi-tenant
deletion service.

## Local verification

Run native, actual-WASM, and direct signed-storage checks separately; a source
inspection or pure-JS demo is not a WASM build proof:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
cargo check --locked --target wasm32-unknown-unknown -p fgba
cargo check --locked --target wasm32-unknown-unknown -p fgba --features analytics
cargo test -p fgba-wasm --locked
cargo clippy -p fgba-wasm --target wasm32-unknown-unknown --locked -- -D warnings
npx --yes wasm-pack@0.15.1 build crates/fgba-wasm --target web --release -- --locked
npx --yes wasm-pack@0.15.1 test --headless --chrome crates/fgba-wasm -- --locked
node --test crates/fgba-wasm/tests/browser.test.mjs
npm --prefix example test
cargo +1.91.0 test --workspace --locked
```

The Node browser tests use a separate-origin mock object store and real FGBA
bytes to cover sparse ranges, CORS/error behavior, A5/exact rendering, SQL and
UDFs, caps, generation changes, and expiry refresh. Tilify's disposable storage
E2E additionally proves the complete upload → sign → direct object → map/SQL →
delete journey. The Range Lab checklist and canvas demo remain manual/native
checks, not substitutes for either suite.
