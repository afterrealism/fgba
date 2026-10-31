# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

### Added

- **`fgba` library crate**: reader and writer for FGBA (FlatGeobuf Aggregates) —
  valid FlatGeobuf files with a curve-sorted A5 aggregate trailer.
  - `build` / `trailer_for`: append the aggregate trailer to any FlatGeobuf body.
  - `Reader`: footer/directory discovery at EOF, band metadata, `read_band`,
    viewport `query` and `stats` answered from trailer bytes alone.
  - Warm-band caching: one most-recently-used band avoids repeated band reads;
    changing resolution evicts it, while `read_band` remains a fresh decode.
  - Optional kind-2 `BODY_ANALYTICS`: exact global/chunk property statistics,
    numeric/null zone maps, verified body offsets, and independent Arrow IPC
    property chunks with validity.
  - Optional kind-3 `GEOMETRY_METRICS`: seven nullable length, unsigned area,
    vertex-count, and bbox columns on the kind-2 chunk grid, with deterministic
    stored statistics and independent Arrow IPC chunks.
  - `SliceReader` for in-memory slices; `HttpRangeReader` (optional `http`
    feature) for range-request reads off plain static hosting (S3/R2).
  - Normative test vector (`tests/data/cities_12.a.fgb`) covering SPEC Appendix A.
- **`fgba-cli` crate** (`fgba` binary): `convert` (GeoJSON/CSV/TSV/Shapefile/
  FlatGeobuf → `.a.fgb`), `build`, `query`, `info`, `stats`, `verify`,
  `serve` (loopback static GET/HEAD with byte ranges), `bench`.
  - Conversion preserves properties with deterministic schema/order,
    nullability, and lossless numeric widening.
  - `build`/`convert --analytics` emits kind-2 analytics and, by default,
    kind-3 geometry metrics when property columns exist. `--geometry-metrics`
    also enables analytics, `--no-geometry-metrics` opts out of kind 3, and
    `--chunk-rows auto` adaptively sizes chunks unless an explicit positive
    size is supplied.
  - File-oriented `build` streams the body and spooled sections into atomic
    output; anonymous temporary files hold Arrow IPC chunks instead of complete
    analytical sections or the trailer in memory.
- **`fgba-sql` crate**: native, read-only, streaming DataFusion SQL over local
  FlatGeobuf/FGBA bodies, A5 bands, viewport coverings, and attribute metadata;
  exposed by `fgba query` without an external SQL engine.
  - Full and zone-pruned pure property/metric/bbox projections scan independent
    analytical chunks in parallel under DataFusion target partitions
    (`--scan-threads`); mixed WKB/metric projections and `SELECT *` use exact
    body execution.
  - Planar `st_length`, `st_area`, and `st_npoints(Binary)` functions over
    supported standard WKB, without CRS transformation.
  - Minimal `EagerSession` execution over one-partition in-memory batches,
    with read-only SQL options and direct async DataFusion streams. The crate
    compiles for `wasm32-unknown-unknown` without DuckDB or another SQL engine.
- **`fgba-wasm` standalone browser artifact**: sparse signed-range FGBA reads,
  A5 and exact GeoJSON visualization, proactive same-generation URL refresh,
  and bounded local DataFusion SQL over Arrow RecordBatches. Includes strict
  200/206/CORS handling, 8 MiB ranges, a 64 MiB sparse cache, source/result
  budgets, and a 30-second SQL worker watchdog. The older pure-JavaScript
  canvas demo remains a separate manual example.
- **Range Lab** (`example/`): local vector uploads with analytics/chunk controls,
  automatic FGBA conversion into an uploads bucket, map preview, and read-only
  SQL results. Wide browser band queries split and deduplicate covering panels
  so fitted viewports retain occupied cells.
- **SPEC.md**: binary specification, Draft v1 (directory version 1), with
  normative Appendix A vectors and the additive kind-2/kind-3 analytical
  layouts.
- **Website** (`docs/`, GitHub Pages): landing page, rendered spec, guide, and
  browser examples, deployed via GitHub Actions.
- `xtask` test-vector tooling: `cargo xtask gen-testdata [--write]` and
  `cargo xtask verify-vectors` (also keeps the per-crate vector copies in sync).

### Changed

- Raised the workspace MSRV from Rust 1.89 to 1.91 and aligned the dedicated
  CI MSRV job with Rust 1.91.0; package versions remain 0.1.0.

### Fixed

- Missing, unsupported, or malformed optional analytical metadata disables only
  the affected acceleration, with exact-body execution where required.
  Malformed Arrow payloads fall back per chunk to the exact body range without
  invalidating independent valid sections or kind-1 bands.
- Mixed WKB/metric projections and `SELECT *` use exact body execution, keeping
  metric, bbox, and empty-geometry null results consistent with kind-3 scans.
- Viewports whose spherical area is at least half the sphere use conservative
  per-cell bbox filtering instead of ambiguous spherical ring coverings;
  covering sums remain regional estimates rather than exact geometry counts.

### Known provisional area (Draft v1, per SPEC §11)

- **Band-cap guidance (§9.2)**: the RECOMMENDED 8 MiB band cap is advisory;
  treat it as guidance until real-world sizing data settles it.
