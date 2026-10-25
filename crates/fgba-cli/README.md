# fgba-cli

Command line tool for **FGBA (FlatGeobuf Aggregates)** files.

```sh
cargo install fgba-cli

fgba convert buildings.geojson -o buildings.a.fgb   # or .csv / .shp / .fgb; properties preserved
fgba convert buildings.geojson --analytics -o buildings.a.fgb
fgba build existing.fgb --analytics --chunk-rows 4096 -o existing.a.fgb
fgba query buildings.a.fgb "SELECT COUNT(*) FROM features"
fgba info buildings.a.fgb                           # header, bands, trailer summary
fgba stats buildings.a.fgb --bbox -11,35,31,71     # viewport covering estimate
fgba verify buildings.a.fgb                         # trailer integrity checks
fgba serve buildings.a.fgb                          # range-capable local server + viewer
fgba bench buildings.a.fgb                          # local vs range-request timings
```

`fgba query` uses the native [`fgba-sql`](https://crates.io/crates/fgba-sql)
crate for read-only SQL over local FlatGeobuf/FGBA files and streams CSV to
stdout. `--analytics` adds the optional kind-2 global/chunk statistics and
independent Arrow IPC property columns when the body has properties; ordinary
builds remain unchanged. No external SQL engine or intermediate analytical
file is required.

Library crate: [`fgba`](https://crates.io/crates/fgba).
Format spec, guide, and examples: <https://github.com/afterrealism/fgba>

License: Apache-2.0 OR MIT
