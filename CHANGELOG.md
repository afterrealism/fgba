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
  - `SliceReader` for in-memory slices; `HttpRangeReader` (optional `http`
    feature) for range-request reads off plain static hosting (S3/R2).
  - Normative test vector (`tests/data/cities_12.a.fgb`) covering SPEC Appendix A.
- **`fgba-cli` crate** (`fgba` binary): `convert` (GeoJSON/Shapefile/GeoPackage/
  GeoParquet/FlatGeobuf → `.a.fgb`), `build`, `info`, `stats`, `verify`,
  `serve` (range-capable local server + viewer), `bench`.
- **SPEC.md**: binary specification, Draft v1 (directory version 1), with
  normative Appendix A vectors.
- **Website** (`docs/`, GitHub Pages): landing page, rendered spec, guide, and
  browser examples, deployed via GitHub Actions.
- `xtask` test-vector tooling: `cargo xtask gen-testdata [--write]` and
  `cargo xtask verify-vectors` (also keeps the per-crate vector copies in sync).

### Known provisional areas (Draft v1, per SPEC §11)

- **Attribute-aggregate record layout (§6)**: the per-attribute `f64 sum/min/max`
  encoding is provisional and may change before the spec leaves Draft.
- **Band-cap guidance (§9.2)**: the RECOMMENDED 8 MiB band cap is advisory;
  treat it as guidance until real-world sizing data settles it.
