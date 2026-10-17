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
fgba convert buildings.geojson -o buildings.a.fgb    # or .shp / .gpkg / .parquet / .fgb
fgba info buildings.a.fgb
fgba serve buildings.a.fgb                           # range-capable local server + viewer
```

```rust
// Cargo.toml: fgba = { version = "0.1", features = ["http"] }
let mut r = fgba::Reader::open_url("https://example.com/buildings.a.fgb")?;
let cells = r.query([-0.5, 51.3, 0.3, 51.7], 7)?;   // (cell, count) — a few hundred bytes
let total = r.stats([-180.0, -85.0, 180.0, 85.0], 2)?.total;
# Ok::<(), fgba::Error>(())
```

Measured on the reference machine (1M features): conversion ≈ 4 s, trailer ≈ 2% of the file, an aggregate viewport query is microseconds; over HTTP it costs 3 requests to open the file, 1 per new zoom band, and **0 per viewport** once the band is cached.

## Feature flags

- `http` — blocking `Read + Seek` over HTTP range requests (`ureq`); native targets.
- `convert` (CLI) — GeoJSON/CSV/Shapefile readers; `parquet`, `gpkg` (native only) for GeoParquet/GeoPackage.

## WebAssembly

The core crate builds for `wasm32-unknown-unknown`. In the browser, fetch byte ranges asynchronously (`fetch` + `Range` header), feed them to `fgba::SliceReader`, decode — two phases, no blocking. See `examples/wasm/`.

## Versioning

The format versions by the footer magic (`FGBAGG01`) and the directory version byte; A5 cell IDs are frozen upstream, so files written today refer to the same cells forever. The crates follow SemVer. The specification is [SPEC.md](SPEC.md) (**Draft v1** — the attribute-aggregate record layout is provisional).

## Safety

`#![forbid(unsafe_code)]` crate-wide. Trailer bytes are hostile input: every count and length is bounded before allocation, malformed trailers fall back to the plain-FGB body, and nothing panics.

## Status

Draft v1 spec, reference implementation in Rust. Roadmap: attribute aggregates (sum/min/max), composition with [FGBO](https://github.com/reearth/flatgeobuf-overviews) overview sections, browser demo, a second-language reader.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this project shall be dual-licensed as above, without any additional terms or conditions.
