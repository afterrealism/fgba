# Format Benchmark Results — FGBA / FGB / Shapefile / GeoJSON / DuckDB / GeoParquet

**Hardware:** 24-core, 125 GB RAM, ZFS pool (NVMe-backed), Linux.
**Data:** the 10 largest geospatial vector files on this machine (1.8 GB – 84 GB per file, 103 GB total source), converted at **full size** into 6 formats each: `fgb` (FlatGeobuf), `afgba` (FlatGeobuf + A5 aggregate trailer), `shp` (Shapefile), `geojson`, `duckdb` (native table in a .duckdb database), `parquet` (GeoParquet, zstd).
**Harness:** Rust (`bench/format-bench`), one isolated process per dataset×format×workload, wall time + peak RSS via `/usr/bin/time`. SQL formats measured through the DuckDB CLI (includes ~40 ms process startup).

## Workloads

| workload | type | what it does |
|---|---|---|
| full_count | analytical | full-scan feature count |
| geom_agg | analytical | full scan, SUM(length)+SUM(area) over every geometry |
| attr_group | analytical | GROUP BY first string column, top-10 group counts |
| bbox_count | analytical | count features intersecting a 10%-of-extent hot window |
| viewport_decode | visualization | 1%-of-extent window, decode all intersecting geometries, count vertices |
| zoom_agg | visualization | whole-extent aggregate count from precomputed band (afgba only) |

Full tables: [BENCHMARK_REPORT.md](BENCHMARK_REPORT.md) · raw rows: [results.jsonl](results.jsonl)

## Headline numbers

### Size (median across datasets, relative to fgb)
| format | relative size |
|---|---|
| parquet (zstd) | **0.55×** — smallest every time |
| duckdb | 0.9× |
| fgb | 1.0× (baseline) |
| afgba | 1.002× — trailer overhead ≈ 0.1–0.7% |
| shp | ≈1.0× (where it fits) |
| geojson | 1.6–2.1× |

### Analytical throughput (highways_lines, 255 M features, 78 GB fgb)
| | fgb | afgba | geojson | duckdb | parquet |
|---|---|---|---|---|---|
| full_count | 58 s | 48 s | **801 s** | 0.12 s* | 0.06 s* |
| geom_agg (full decode) | 96 s | 96 s | 840 s | 26 s | 14 s |
| attr_group | 60 s | 60 s | 811 s | 0.4 s | 0.5 s |
| bbox_count (10% window) | **0.6 s** | **0.5 s** | 868 s | 26 s | 16 s |

\* DuckDB COUNT(*) over an uncropped table/column is satisfied from metadata — it is not a decode cost.

### Visualization workloads
- **Spatially indexed viewport reads (fgb/afgba) are 3–4 orders of magnitude faster than any scan-based format**: 1%-window decode took 0–610 ms via the FlatGeobuf R-tree vs 16 s–**15 min** for GeoJSON full-scan-decode and 1.2–25 s for DuckDB/Parquet envelope filters (no persisted spatial index).
- **afgba zoom aggregates answered in ≤ 9 ms** from the trailer band vs a full scan for every other format (48 s–801 s on highways). That is the format's reason to exist and it delivers.
- GeoJSON/Shapefile must read the entire file for any spatial query — unusable for interactive visualization at this scale.

## Failures & caveats (all reproducible)

1. **Shapefile 2 GB limit**: 7 of 10 datasets could not be written as Shapefile at all (buildings_gpkg, Flood_Hazard_Areas, footways, tracks, trails, US_Census_Blocks, highways). Only Land, Lakes and buildings_geojson converted (buildings_geojson's 3.5 GB .shp likely exceeds interoperable limits).
2. ~~**`fgba build` OOM at 255 M features**~~ **FIXED** (sort-based bands plus streaming/spooled build, see [Round 2](#round-2)): the pre-Round-2 highways build was OOM-killed at 112 GiB anonymous RSS. The final spooled build completed in **884.80 s** at **7,723,444 KiB (~7.37 GiB)** peak RSS, and the promoted highways_lines.a.fgb verifies at 255,381,003 features.
3. ~~**afgba world-viewport covering undercounts**~~ **FIXED** (near-world bbox-overlap selection, see [Round 2](#round-2)): the old complement-covering path could read a large spherical ring as its tiny complement. Fresh Round-2 totals equal exact body counts for nine datasets; Land alone is **-7** because of the measured ±85° polar clamp, with no gross undercount.
4. **Column-name divergence**: shapefile truncates field names to 10 chars and GeoJSON/driver naming differs (`felt:created_by` vs `felt_cre_1`), so attr_group keys are format-specific; counts were cross-verified instead (identical feature counts across all formats for every dataset).
5. DuckDB/Parquet numbers include CLI process startup (~40 ms). In the historical Round-1 comparison, their parallel scan workloads won geom_agg by 4–7× over FGB/FGBA body scans; see Round 2 for fresh native-SQL measurements.

## Round-1 bottom line (historical)

- **Analytics**: GeoParquet (and DuckDB tables) — parallel columnar scans won the Round-1 full-scan comparisons against FGB/FGBA body scans, plus the smallest files.
- **Visualization / viewport serving**: FGBA/FGB — indexed random access is unbeaten by scan formats at any size, and the a.fgb trailer makes whole-extent zoom counts effectively free (≤ 9 ms).
- **Avoid at this scale**: GeoJSON (1.6–2× larger, 10–100× slower everywhere, no index) and Shapefile (doesn't even fit past 2 GB).
- The two formats are complementary: keep GeoParquet for batch analytics, serve FGBA for maps.

## Repro

```sh
scripts/convert-one.sh <source> data/formats [name]   # one file -> all 6 formats
bench/format-bench/target/release/format-bench orchestrate data/formats data/formats/results.jsonl
bench/format-bench/target/release/format-bench report data/formats/results.jsonl
```

## Round 2

Fresh Round-2 evidence covers native `afgba-sql` analytics and `afgba` zoom
queries on the promoted artifacts. The final 40-row SQL matrix is authoritative
for the gate values below; earlier manual `*.correct` and `*.final` logs are
diagnostic history. Round-1 DuckDB/GeoParquet timings appear only as an
**explicitly labeled historical baseline**, not as a fresh controlled rerun.

Raw evidence: [Round-2 report](round2/REPORT.md) ·
[40-row SQL results](round2/results.jsonl) ·
[10-row zoom results](round2/zoom.jsonl) ·
[combined 50-row results](round2/all-results.jsonl) ·
[build/verify/time logs](round2/) ·
[pre-Round-2 backups](round2/pre-round2/).

### All-ten build resources

| dataset | wall (s) | maxRSS (KiB) | output (MiB) |
|---|---:|---:|---:|
| Flood_Hazard_Areas | 84.43 | 54,588 | 9691.2 |
| Lakes | 7.75 | 66,184 | 1873.8 |
| Land | 4.97 | 34,548 | 1553.3 |
| US_Census_Blocks | 64.11 | 232,508 | 13562.6 |
| buildings_geojson | 91.89 | 580,952 | 7227.6 |
| buildings_gpkg | 42.62 | 388,540 | 10168.1 |
| footways_lines | 75.01 | 741,292 | 11246.0 |
| highways_lines | **884.80** | **7,723,444** | **100751.3** |
| tracks_lines | 162.81 | 885,760 | 15937.7 |
| trails_lines | 59.78 | 471,128 | 8337.4 |

Wall time is the CLI value in each `*.build.log`; maxRSS is from the matching
`/usr/bin/time -v` log. Highways used the final spooled build: **884.80 s** and
**7,723,444 KiB (~7.37 GiB)**. Every dataset is below the **16 GiB** gate, every
build reported zero skipped features, and every final artifact passed `fgba
verify`.

### Highways parity gates

Round-2 highways has **255,381,003 features**. Times are milliseconds. `count`
is the exact body-row count except for `bbox_count`, where it is the number of
matching rows. The metric is the top-10 group-row sum for `attr_group`,
`SUM(geom_length + geom_area)` for `geom_agg`, and selected cells for
`zoom_agg`.

| workload | gate | Round-2 FGBA ms | count | metric / detail | Round-1 DuckDB ms (**historical**) | Round-1 GeoParquet ms (**historical**) |
|---|---:|---:|---:|---|---:|---:|
| full_count | ≤ 500 | **105.9** | 255,381,003 | metadata count | 116.1 | 64.4 |
| attr_group | ≤ 3,000 | **2623.1** | 255,381,003 | 243,912,469 top-10 rows; `highway` | 420.6 | 518.5 |
| geom_agg | ≤ 5,000 | **467.4** | 255,381,003 | 927,413.7407 | 25,829.0 | 13,555.5 |
| bbox_count | ≤ 1,000 | **723.6** | 1,772,568 | 10%-extent hot bbox | 26,454.5 | 16,041.5 |
| zoom_agg | ≤ 10 | **1.0** | 255,381,003 | 1,667 cells at res 4 | — | — |

All five measured gates pass. This is not a claim that all datasets, engines,
or workloads are equivalent. In particular, `attr_group` passes its 3 s gate
but retains a substantial DataFusion-versus-DuckDB engine gap. The historical
SQL harness also did not use the same top-10 metric as the final FGBA harness,
so no cross-round `attr_group` metric-parity claim is made.

### Fresh near-world zoom results

The fresh zoom rows span **0.6–1.6 ms**. “Exact features” is the body count
from the final SQL matrix; the zoom total is a midpoint-attributed band sum,
not an exact geometry-intersection count.

| dataset | exact features | zoom total | delta | band res | cells | ms |
|---|---:|---:|---:|---:|---:|---:|
| Flood_Hazard_Areas | 361,979 | 361,979 | 0 | 5 | 271 | 0.7 |
| Lakes | 1,427,688 | 1,427,688 | 0 | 4 | 1,437 | 0.9 |
| Land | 847,984 | 847,977 | **-7** | 5 | 3,452 | 1.4 |
| US_Census_Blocks | 8,180,866 | 8,180,866 | 0 | 5 | 433 | 0.7 |
| buildings_geojson | 24,083,188 | 24,083,188 | 0 | 4 | 9 | 0.6 |
| buildings_gpkg | 15,705,867 | 15,705,867 | 0 | 5 | 141 | 0.8 |
| footways_lines | 29,139,462 | 29,139,462 | 0 | 4 | 1,474 | 0.9 |
| highways_lines | 255,381,003 | 255,381,003 | 0 | 4 | 1,667 | 1.0 |
| tracks_lines | 28,949,836 | 28,949,836 | 0 | 4 | 1,595 | 1.6 |
| trails_lines | 15,791,189 | 15,791,189 | 0 | 4 | 1,570 | 1.1 |

Nine datasets equal their exact body totals. Land is the sole exception at
exactly **-7** because the measured harness clamps its `-90°` south edge to
`-85°`; its unclamped res-5 band verifies at 847,984 features. This is the
expected polar-clamp effect, **not a gross near-world undercount**. Regional
viewport sums remain band-resolution-dependent estimates with no general error
bound.

### B1/B2/B3 fixes and remaining limits

- **B1 — band-build memory:** the pre-Round-2 highways band builder was
  OOM-killed at **112 GiB anonymous RSS**. Sort-based, reusable-buffer band
  aggregation removes that failure mode.
- **B2 — streaming/spooled analytics:** the first completed Round-2 highways
  build measured **79,021,400 KiB = 75.36 GiB** maxRSS. The final spooled build
  reduced this to **7,723,444 KiB (~7.37 GiB)** while producing the same
  100751.3 MiB output size and a verified 255,381,003-feature artifact. See the
  [initial timing log](round2/highways_lines.round2.build.time.log),
  [final build log](round2/highways_lines.round2-spool.build.log),
  [final timing log](round2/highways_lines.round2-spool.build.time.log), and
  [verification log](round2/highways_lines.round2-spool.verify.log).
- **B3 — near-world covering:** bbox overlap for the band's own cells replaces
  the ambiguous hemisphere/complement covering path. The fresh table above has
  exact body totals for nine datasets and only Land's documented **-7** polar
  clamp delta; the former gross undercount is absent.
- **Expected workload limitation:** `buildings_geojson` is the sole
  `attr_group` error in the 40-row matrix because it has **no string column**.
  This is an expected schema limitation, not a failed timing gate.
- **Planar polygon semantics:** DuckDB `ST_Length` returns `0` for polygons,
  whereas FGBA `geom_length` includes exterior and interior rings. Therefore
  `ST_Length + ST_Area` is not a polygon-equivalent comparator. In the
  single-partition Land check, FGBA ring length matched DuckDB `ST_Perimeter`
  within **1.30e-15 relative**, and FGBA area matched DuckDB `ST_Area` within
  **1.15e-15 relative**. These are planar degree-unit metrics, not geodesic
  measurements; multipartition aggregates can reassociate low-order bits.

### Census scan scaling

A separate filtered `US_Census_Blocks` count returned 8,180,866 at both
settings: `--scan-threads 1` took **0.35 s** and `--scan-threads 8` took
**0.04 s**, an arithmetic **8.75×** ratio on the displayed values. These are
single `/usr/bin/time` runs with **centisecond resolution**, not a controlled
cache-isolated scaling study; do not treat 8.75× as a stable general speedup.
Raw logs: [1 thread](round2/US_Census_Blocks.scan_threads_1.time.log) ·
[8 threads](round2/US_Census_Blocks.scan_threads_8.time.log).

Task 6 HTTP prefetch remains **deferred**: Round-2 SQL is local-only, and these
measurements do not implement or exercise HTTP SQL prefetch.

### Round-2 repro with promoted artifacts

The historical logs retain their pre-promotion staging paths. New runs should
use the canonical `data/formats` tree. The result filenames below are new so
the raw Round-2 evidence is not overwritten; choose a different build output
name as well if the promoted artifact must be preserved.

```sh
/usr/bin/time -v target/release/fgba build \
  data/formats/fgb/highways_lines.fgb \
  --out data/formats/a.fgb/highways_lines.a.fgb \
  --analytics --chunk-rows auto
target/release/fgba verify data/formats/a.fgb/highways_lines.a.fgb

bench/format-bench/target/release/format-bench orchestrate \
  data/formats data/formats/round2/repro-results.jsonl --formats afgba-sql
bench/format-bench/target/release/format-bench report \
  data/formats/round2/repro-results.jsonl > data/formats/round2/repro-REPORT.md
```
