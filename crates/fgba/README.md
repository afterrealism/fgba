# fgba

Reader and writer for **FGBA (FlatGeobuf Aggregates)**: valid [FlatGeobuf](https://flatgeobuf.org)
files with a curve-sorted [A5](https://a5geo.org) aggregate trailer, readable locally or over
HTTP range requests (optional `http` feature).

An FGBA file *is* a FlatGeobuf file — every existing reader (GDAL, QGIS, the JS and Rust
libraries) reads all features unchanged. A small trailer after the feature data carries counted,
equal-area A5 cells at several resolutions, so FGBA-aware readers get instant low-zoom overviews
and count analytics straight off S3/R2/static hosting.

## Usage

```rust
// Open any .a.fgb (a plain .fgb simply has no trailer).
let mut reader = fgba::Reader::open_path("buildings.a.fgb".as_ref())?;
for band in reader.bands() {
    println!("A5 res {}: {} bytes at offset {}", band.res, band.size, band.offset);
}
// Count analytics over a bounding box, answered from trailer bytes alone.
let stats = reader.stats([-11.0, 35.0, 31.0, 71.0], 5)?;
println!("{} occupied cells, ~{} features in view", stats.cells, stats.total);
```

CLI: [`fgba-cli`](https://crates.io/crates/fgba-cli) (`cargo install fgba-cli`).
Its `fgba query` command uses [`fgba-sql`](https://crates.io/crates/fgba-sql)
for native, read-only SQL over local files.

Enable the `analytics` feature to read optional kind-2 `BODY_ANALYTICS`
sections. CLI `build`/`convert --analytics` writes exact global statistics,
per-chunk zone maps and body offsets, and independent Arrow IPC property chunks
when properties exist. Missing or malformed analytics disables only that
acceleration; the FlatGeobuf body and kind-1 bands remain available.

`Reader::query`/`stats` cache only the most recently used band; changing
resolution evicts it. `read_band(res)` always performs a fresh decode.

Format spec, guide, and examples: <https://github.com/afterrealism/fgba>

License: Apache-2.0 OR MIT
