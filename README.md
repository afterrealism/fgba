# FGBA — FlatGeobuf Aggregates

[![crates.io](https://img.shields.io/crates/v/fgba.svg)](https://crates.io/crates/fgba)
[![docs.rs](https://img.shields.io/docsrs/fgba.svg)](https://docs.rs/fgba)
[![CI](https://github.com/afterrealism/fgba/actions/workflows/ci.yml/badge.svg)](https://github.com/afterrealism/fgba/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue.svg)](#license)

**A cloud-optimized vector format: full-fidelity FlatGeobuf features plus precomputed [A5](https://a5geo.org) aggregate cells, streamable over plain HTTP range requests.**

An FGBA file *is* a [FlatGeobuf](https://flatgeobuf.org) file — every existing FGB reader (GDAL, QGIS, the JS and Rust libraries) reads all features from it unchanged. After the feature data, a small trailer carries counted, equal-area A5 pentagon cells at several resolutions. Readers that know the trailer get instant low-zoom overviews and count analytics, locally or straight off S3/R2/static hosting; readers that don't never notice it.

```
┌─────────────────────────────────────────────┐
│ plain FlatGeobuf (full resolution features) │ ← readable by any fgb reader
├─────────────────────────────────────────────┤
│ aggregate bands (cell, count) by resolution │ ← FGBA-aware readers get
│ directory · footer (32B discovery at EOF)   │    fast at every zoom
└─────────────────────────────────────────────┘
```

## Quickstart

```sh
cargo install fgba-cli
fgba convert buildings.geojson -o buildings.a.fgb    # or .csv / .shp / .fgb
fgba info buildings.a.fgb
fgba query buildings.a.fgb "SELECT COUNT(*) FROM features"
fgba serve buildings.a.fgb                           # range-capable local server + viewer
```

```rust
// Cargo.toml: fgba = { version = "0.1", features = ["http"] }
let mut r = fgba::Reader::open_url("https://example.com/buildings.a.fgb")?;
let cells = r.query([-0.5, 51.3, 0.3, 51.7], 7)?;   // (cell, count) — a few hundred bytes
let total = r.stats([-180.0, -85.0, 180.0, 85.0], 2)?.total;
# Ok::<(), fgba::Error>(())
```

`fgba query` uses the native `fgba-sql` crate for read-only SQL over local
FlatGeobuf/FGBA files; no external query engine or format conversion is needed.
Pure property, geometry-metric, and bbox projections can use parallel Arrow
column scans, while mixed WKB/metric projections and `SELECT *` execute against
the exact body. Planar `st_length`, `st_area`, and `st_npoints` WKB functions are
also available.

`fgba build --analytics` and `fgba convert --analytics` emit kind-2
`BODY_ANALYTICS` and, by default, kind-3 `GEOMETRY_METRICS` when property
columns exist.
`--geometry-metrics` also enables analytics; `--no-geometry-metrics` retains
only kind 2. Kind 3 stores `length`, `area`, `npoints`, and the four bbox
columns on the kind-2 chunk grid; SQL exposes the first three as `geom_length`,
`geom_area`, and `geom_npoints`. `--chunk-rows auto` is the default;
an explicit positive size overrides it. See the
[native analytics guide](docs/guide/analytics.html).

**Round-2 highways headline:** 255,381,003 features; `COUNT(*)` **105.9 ms**,
`SUM(geom_length + geom_area)` **467.4 ms**, and near-world zoom **1.0 ms**.
The Round-1 GeoParquet geometry-aggregate comparator was **13.6 s**
(**historical baseline, not rerun in Round 2**). See the
[Round-2 report](data/formats/round2/REPORT.md) and
[full Round-2 results](data/formats/RESULTS.md#round-2).

Measured on the reference machine (1M features): conversion ≈ 4 s, trailer ≈ 2% of the file, an aggregate viewport query is microseconds; over HTTP, open + the first band costs 4 requests, repeated same-resolution viewports cost **0 band requests**, and changing resolution evicts the one cached band.

## Feature flags and native SQL

- `http` — blocking `Read + Seek` over HTTP range requests (`ureq`); native targets.
- `analytics` — optional kind-2 property statistics and kind-3 geometry metrics with independent Arrow IPC chunks.
- `fgba-sql` — native read-only DataFusion SQL, used by `fgba query`; local files only.

## WebAssembly

The core crate builds for `wasm32-unknown-unknown`. In the browser, fetch byte ranges asynchronously (`fetch` + `Range` header), feed them to `fgba::SliceReader`, decode — two phases, no blocking. See [`crates/fgba/examples/wasm/`](crates/fgba/examples/wasm/) for a dependency-free two-phase browser demo.

## Versioning

The format versions by the footer magic (`FGBAGG01`) and the directory version byte; A5 cell IDs are frozen upstream, so files written today refer to the same cells forever. The crates follow SemVer. The specification is [SPEC.md](SPEC.md) (**Draft v1**); optional kind-2 and kind-3 analytical columns/statistics are additive and do not change either version.

## Safety

`#![forbid(unsafe_code)]` crate-wide. Trailer bytes are hostile input: every count and length is bounded before allocation, malformed trailers fall back to the plain-FGB body, and nothing panics.

## Status

Draft v1 spec, reference implementation in Rust. Native SQL, property-preserving conversion, and optional kind-2/kind-3 analytics are implemented. Roadmap: composition with [FGBO](https://github.com/reearth/flatgeobuf-overviews) overview sections and a second-language reader.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this project shall be dual-licensed as above, without any additional terms or conditions.
