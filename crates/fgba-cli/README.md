# fgba-cli

Command line tool for **FGBA (FlatGeobuf Aggregates)** files.

```sh
cargo install fgba-cli

fgba convert buildings.geojson -o buildings.a.fgb   # or .shp / .gpkg / .parquet / .fgb
fgba info buildings.a.fgb                           # header, bands, trailer summary
fgba stats buildings.a.fgb                          # per-resolution cell counts
fgba verify buildings.a.fgb                         # trailer integrity checks
fgba serve buildings.a.fgb                          # range-capable local server + viewer
fgba bench buildings.a.fgb                          # local vs range-request timings
```

Library crate: [`fgba`](https://crates.io/crates/fgba).
Format spec, guide, and examples: <https://github.com/afterrealism/fgba>

License: Apache-2.0 OR MIT
