# FGBA Analytics Throughput Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven development (recommended, one fresh subagent per task) or the executing-plans skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make FGBA match or beat GeoParquet+DuckDB on analytical workloads (full-scan count, attribute aggregation, geometry aggregation) at 250M+ feature scale, while keeping its existing visualization advantages (R-tree viewport reads, sub-10ms zoom aggregates).

**Architecture:** Three levers, in order of measured impact: (1) fix the two scale blockers found by the benchmark (band-build OOM, near-world covering undercount) so every dataset can *have* an analytics trailer; (2) parallelize the already-columnar chunk scan (kind-2 Arrow IPC chunks are independent by spec — one DataFusion partition per chunk range instead of one global partition); (3) add a new kind-3 GEOMETRY_METRICS section storing per-feature `length`/`area`/`npoints`/`bbox` as chunked f64/u32 Arrow columns with zone maps, turning geometry aggregation from row-oriented flatbuffer decode (96 s on 255M features) into a pure columnar SUM (~1–3 s), with a vectorized WKB UDF path as the general fallback. Disk size is explicitly not a constraint (owner decision 2026-10-25).

**Tech Stack:** Rust 1.89 (MSRV), existing pinned deps: `a5 =1.0.0`, `flatgeobuf 6.0.1`, `arrow-* =55.2.0` (analytics feature), DataFusion 48.0.1 (fgba-sql), `geozero 0.15.1`. Benchmark harness: `bench/format-bench` (standalone crate).

**Spec:** `SPEC.md` (Draft v1) — this plan extends it additively (§6A kind-2 exists; Tasks 7–8 add kind-3 and amend §6A/§8 text). Per SPEC §5 and `format.rs:95-109`, unknown section kinds are preserved verbatim and skipped by readers (verified: `directory_round_trip_with_unknown_kind` test at `format.rs:274`), so kind-3 is backward-compatible by construction.

## Global Constraints

- `#![forbid(unsafe_code)]` crate-wide; workspace clippy lints deny `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `unreachable` (Cargo.toml `[workspace.lints.clippy]`) — all new code in `crates/` must use `?`/`ok_or` and total fallible arithmetic (`checked_*`).
- MSRV 1.89, edition 2021. `a5` pinned `=1.0.0`; `arrow-array/arrow-ipc/arrow-schema` pinned `=55.2.0` under the `analytics` feature; kind-3 MUST reuse these exact arrow crates (no second arrow version in the graph).
- Format changes are **additive only**: no footer magic change, no directory version change, old readers must read new files (SPEC §10.2 fallback rule).
- Determinism (SPEC §6A.3): sequential binary64 float accumulation in body order for sums; no parallel/reassociated float reduction in *stored statistics*. (Parallel scan *execution* is fine — query-time aggregation order is not a stored statistic; document per-query float non-determinism in the SQL guide, same caveat as DataFusion generally.)
- Every kind-3 metric column MUST carry exact per-chunk zone maps with the same semantics as §6A.2 `ChunkStat`.
- Commit style: existing history uses conventional commits (`feat:`, `fix:`); run `cargo fmt --all` and `cargo clippy --workspace --all-features` before each commit (CI workflow `.github/workflows/ci.yml`).
- Tests run with `cargo test -p fgba`, `cargo test -p fgba-sql`, `cargo test -p fgba-cli` from repo root.

---

## Why Parquet wins today — measured evidence (session 2026-10-25)

Full benchmark: `data/formats/RESULTS.md`, raw rows `data/formats/results.jsonl` (310 measurements, 10 datasets up to 255M features / 78 GB fgb, harness `bench/format-bench`).

Highways dataset (255,381,003 features), median-of-one wall times:

| workload | fgb/afgba (row scan) | geojson | duckdb | parquet |
|---|---|---|---|---|
| full_count | 48–58 s | 801 s | 0.12 s (metadata) | 0.06 s (metadata) |
| attr_group | 60 s | 811 s | 0.42 s | 0.52 s |
| geom_agg (SUM length+area, full decode) | 96 s | 840 s | 26 s | **14 s** |
| bbox_count (10% window) | **0.5–0.6 s (R-tree)** | 868 s | 26 s | 16 s |

What Parquet+DuckDB has that the measured FGBA path lacked:

1. **Columnar storage with per-column pruning** → already designed in FGBA (kind-2 Arrow IPC chunks + zone maps) but the benchmarked files were built *without* `--analytics`. Empirical proof it works: Land (848K rows) built with `--analytics` answers `COUNT(*)` in 266 ms wall (~0 ms user — metadata path via `metadata_count_plan`, `session.rs:88`), group-by in 157 ms (vs duckdb 123 ms), zone-map-pruned filter in 20 ms. **Property analytics are already at parity; they just weren't exercised.**
2. **Morsel-driven parallelism** → FGBA scans are single-threaded: `FgbaTable::scan` creates exactly one `FgbPartition` (`table.rs`, `vec![Arc::new(partition)]`), executed by one `spawn_blocking` task (`stream.rs:712-725`). DuckDB used 24 cores. The chunk layout is embarrassingly parallel (independent body ranges + Arrow ranges by SPEC §6A.1) — this is the biggest untapped lever.
3. **Vectorized geometry kernels over flat arrays** → DuckDB GEOMETRY is a struct of flat buffers; `ST_Length`/`ST_Area` run batch-wise. FGBA geometry is row-oriented flatbuffers; every `geom_agg` pays per-feature decode + `geo_types` object construction (96 s). Fix: store derived metric columns (kind-3) and/or flat columnar geometry.
4. **Metadata-only COUNT(*)** → exists (`plan.rs::metadata_count_plan`) once `--analytics` is on.

Two scale blockers prevent applying any of this to the biggest datasets:

- **B1 — band-build OOM**: `fgba build` on highways (255M features, *no* analytics) was OOM-killed at 112 GB anon-rss (kernel log 2026-10-25). Root cause: per-band `BTreeMap<u64,u32>` cell aggregation (`write.rs:151` in `trailer_at_with_analytics`) — hundreds of millions of map nodes per band. Highways therefore has **no a.fgb at all** today.
- **B2 — analytics requires whole body in RAM**: CLI `cmd_build` (`main.rs:212`): *"Analytics needs the whole body; keep the in-memory path"* — `--analytics` on a 78 GB body is impossible. Must become streaming.
- **B3 — near-world aggregate undercount**: `stats()`/`query()` over whole-extent viewports on global datasets returns grossly wrong totals (trails_lines res-4 band: 8,056 vs true 15,791,189; regional viewports are correct — Europe window: 4.6M plausible). Root cause: `query()`'s complement path (`read.rs:219-238`) builds exclusion rings via `rect_covering_cells`, which splits >180° rects into two **~180°-wide** rings (`read.rs:412-418`); hemisphere-scale rings are ambiguous to `a5::polygon_to_cells` and can be read as their complement, excluding nearly the whole band. Land/Census (extents touching ±180/−90 exactly) escape because their zero-width complement pieces are skipped.

## Architecture after this plan

```
FGBA file layout (additive; old readers see a valid FGB + skippable trailer)
┌──────────────────────────────────────────────────────────────┐
│ FlatGeobuf body (unchanged, row-oriented, Hilbert R-tree)    │
├──────────────────────────────────────────────────────────────┤
│ kind-2 BODY_ANALYTICS (exists):                              │
│   per-column Arrow IPC chunks + global/chunk stats           │
│   chunk_offsets[C+1] → body byte range per chunk             │
├──────────────────────────────────────────────────────────────┤
│ kind-3 GEOMETRY_METRICS (NEW, Task 7):                       │
│   same chunk grid; per-feature Arrow columns:                │
│     length f64 · area f64 · npoints u32                      │
│     bbox_xmin/ymin/xmax/ymax f64 (zone-mapped)               │
├──────────────────────────────────────────────────────────────┤
│ kind-1 aggregate bands · directory · footer (unchanged)      │
└──────────────────────────────────────────────────────────────┘

Query engine (fgba-sql, DataFusion 48):
  COUNT(*)            → metadata (exists)                    ~0 ms
  property scans      → SourceMode::Columns over kind-2      exists
  SUM(geom_length)    → kind-3 f64 column scan               NEW
  bbox_xmin predicates→ kind-3 zone maps + column filter     NEW
  N-partition scan    → one FgbPartition per chunk range     NEW (Task 5)
  viewport queries    → Hilbert R-tree (unchanged, wins)
  zoom aggregates     → kind-1 bands (fixed covering, Task 1)
```

## File Structure

| File | Responsibility | Tasks |
|---|---|---|
| `crates/fgba/src/read.rs` | covering/complement fix; `band_cells_in_view` | 1 |
| `crates/fgba/src/band.rs` | expose band cell iteration for Task 1 filter | 1 |
| `crates/fgba/src/write.rs` | sort-based band aggregation; adaptive chunk_rows; kind-3 writer hook | 2, 4, 7 |
| `crates/fgba/src/analytics.rs` | kind-3 record layout, validation, reader (`GeometryMetrics`) | 7, 8 |
| `crates/fgba/src/format.rs` | `KIND_GEOMETRY_METRICS = 3` constant | 7 |
| `crates/fgba/src/geom_metrics.rs` (new) | planar length/area/npoints/bbox kernels over geozero geometry (build side) | 7 |
| `crates/fgba-cli/src/main.rs` | streaming analytics build path (drop whole-body RAM) | 3 |
| `crates/fgba-sql/src/table.rs` | multi-partition `scan()` | 5 |
| `crates/fgba-sql/src/stream.rs` | `Scan` chunk-range field; per-partition `run`; kind-3 column source; WKB UDF eval path | 5, 8, 9 |
| `crates/fgba-sql/src/schema.rs` | virtual columns `geom_length`, `geom_area`, `geom_npoints` (kind-3 backed) | 8 |
| `crates/fgba-sql/src/session.rs` | register `st_length`/`st_area`/`st_npoints` UDFs | 9 |
| `crates/fgba-sql/src/udf_geom.rs` (new) | vectorized WKB metric kernels (no geo_types objects) | 9 |
| `SPEC.md` | §6B kind-3 section; §9.4 covering semantics note | 1, 7 |
| `bench/format-bench/src/main.rs` | `afgba-sql` format via `fgba query` subprocess | 10 |
| `docs/guide/analytics.html` | document kind-3 + parallel scan + parity numbers | 11 |

Task order is the phase order: A (1–4) unblocks scale, B (5–6) delivers the parallel lever, C (7–9) delivers geometry parity, D (10–11) proves it. Tasks 1–4 are independent of each other and can be parallelized across subagents; 5 depends on none but benefits from 3; 7 depends on 3 (streaming writer); 8 depends on 7; 9 independent of 7/8; 10 depends on all.

---

### Task 1: Fix near-world viewport aggregate covering (B3)

**Files:**
- Modify: `crates/fgba/src/read.rs` (`query`, replace complement path; delete `complement_covering` + `COMPLEMENT_POLE` after migration)
- Modify: `crates/fgba/src/band.rs` (expose cell iteration if not already public — `AggBand.cells` is `Vec<(u64,u32)>`; check `pub(crate)` visibility and add a `pub fn cells(&self) -> &[(u64, u32)]` accessor on the *reader-side* band type used by `read_band`)
- Test: `crates/fgba/src/read.rs` (`#[cfg(test)]` module at bottom)

**Interfaces:**
- Consumes: `Reader::read_band(res) -> Result<Band>` (existing, `read.rs`), band cell list ascending by cell id.
- Produces: `fn band_cells_in_view(cells: &[(u64, u32)], view: &[f64; 4], res: u8) -> Result<Vec<(u64, u32)>>` — used by `query()` for `viewport_area_fraction > 0.5`; keeps exact `(cell, count)` pairs, same ordering.

**Design:** For viewports covering >half the sphere, stop building complement ring coverings (ambiguous at hemisphere scale). Instead filter the band's own cells: keep cell iff its conservative lon/lat bounding box overlaps the viewport rect. Cell bbox from `a5::cell_to_boundary(cell, None) -> Result<Vec<LonLat>, String>` (verified in a5-1.0.0/src/core/cell.rs:448; `LonLat` has `.lon`/`.lat` f64 fields — verify field names in a5-1.0.0/src/coordinate_systems before coding). Bands hold ≤ a few million cells even at fine res (trails res-10: 555,806 cells → boundary calls are ~µs each → worst case a few seconds at the finest band, typically <10 ms at overview resolutions; cache nothing in v1). Overlap test on bboxes is conservative (may keep boundary cells whose polygon misses the rect) — identical semantics to the spec's *overlapping* containment, and counts stay upper-bound-consistent with the existing `query` contract.

- [ ] **Step 1: Write the failing test** (in `read.rs` tests; mirrors the benchmark regression: a synthetic band whose cells tile the whole world at res 2, queried with a trails-like extent, must return ~all counts):

```rust
#[test]
fn near_world_viewport_returns_full_band_total() {
    // Build a temp fgb-free Reader is heavy; test the pure filter instead.
    use crate::read::band_cells_in_view;
    // res-2 world tiling: every base cell of A5 (122 of them) with count 1.
    let cells: Vec<(u64, u32)> = (0..122u64)
        .map(|i| (a5::base_cell(i).unwrap(), 1))
        .collect();
    let view = [-179.999, -79.776, 179.997, 82.499]; // trails_lines extent
    let kept = band_cells_in_view(&cells, &view, 2).unwrap();
    let total: u64 = kept.iter().map(|&(_, c)| u64::from(c)).sum();
    // A5 base cells are hexagons/pentagons; with conservative bboxes nearly
    // all overlap the near-world rect. Assert we keep the overwhelming majority
    // (old code kept ~0.05%).
    assert!(total >= 110, "kept only {total}/122 base cells");
}
```

(Note for the implementer: `a5::base_cell` may be named differently in a5 1.0.0 — check `pub use` list in `a5-1.0.0/src/lib.rs`; any function enumerating the 122 base cells works. If none is exported, construct cells via `a5::lonlat_to_cell` over a 12×24 lon/lat grid at res 2 and dedup.)

- [ ] **Step 2: Run test to verify it fails** — `cargo test -p fgba near_world` → FAIL (`band_cells_in_view` not defined).
- [ ] **Step 3: Implement** `band_cells_in_view` in `read.rs`:

```rust
/// Conservative per-cell rect filter for viewports larger than a hemisphere,
/// where spherical ring coverings are ambiguous (SPEC §9.4 note).
pub(crate) fn band_cells_in_view(
    cells: &[(u64, u32)],
    view: &[f64; 4],
    _res: u8,
) -> Result<Vec<(u64, u32)>> {
    let [min_x, min_y, max_x, max_y] = *view;
    let mut out = Vec::new();
    for &(cell, count) in cells {
        let ring = a5::cell_to_boundary(cell, None).map_err(Error::A5)?;
        let (mut x0, mut y0, mut x1, mut y1) = (f64::MAX, f64::MAX, f64::MIN, f64::MIN);
        for p in &ring {
            x0 = x0.min(p.lon); x1 = x1.max(p.lon);
            y0 = y0.min(p.lat); y1 = y1.max(p.lat);
        }
        if x0 <= max_x && x1 >= min_x && y0 <= max_y && y1 >= min_y {
            out.push((cell, count));
        }
    }
    Ok(out)
}
```

Then in `query()`, replace the `complement_covering`/`query_excluding` branch: read the band first, then `return Ok(band_cells_in_view(&band.cells, &view, res)?)` (adjust to the reader-side band struct's field/accessor; keep the `cached_band` behavior). Delete `complement_covering`, `COMPLEMENT_POLE`, and `Band::query_excluding` if now unused (grep first — `band.rs:284` test `query_binary_searches_the_covering` may reference it).

- [ ] **Step 4: Run tests** — `cargo test -p fgba` all green, including existing covering tests.
- [ ] **Step 5: Integration check against the benchmark regression** (requires `data/formats` fixtures from the 2026-10-25 benchmark run; skip in CI, run locally):

```sh
cargo run --release -p fgba-cli -- query data/formats/a.fgb/trails_lines.a.fgb \
  "SELECT SUM(count) FROM cells WHERE TRUE" # if such SQL exists; otherwise use the bench harness:
bench/format-bench/target/release/format-bench run-one data/formats trails_lines afgba zoom_agg \
  -179.9990559 -79.7764677 179.9971833 82.4985836
# expect count ≈ 15,791,189 (true feature count), not 8,056
```

- [ ] **Step 6: Amend SPEC §9.4** with one paragraph: viewports larger than a hemisphere are resolved by per-cell bbox filtering of the band, not ring coverings. Commit: `fix: correct aggregate covering for near-world viewports`.

---

### Task 2: Sort-based band aggregation — fix build OOM (B1)

**Files:**
- Modify: `crates/fgba/src/write.rs` (`trailer_at_with_analytics`, the per-band `BTreeMap<u64,u32>` loop at ~line 150)
- Test: `crates/fgba/src/write.rs` tests

**Interfaces:**
- Consumes: `points: Vec<(f64,f64)>` (already collected), `band_ladder(count) -> Vec<u8>` (write.rs:87).
- Produces: same `AggBand { res, total, cells: Vec<(u64,u32)> }` output, cells ascending — byte-identical trailers to the BTreeMap version for any input (determinism is a spec requirement; verify with a golden test).

**Design:** Replace the map with: reusable `Vec<u64>` cell-id buffer → fill per band → `sort_unstable` → run-length encode into `Vec<(u64,u32)>`. Memory: 8 B/point reused across bands + 12 B/output cell, vs ~100+ B/entry for BTreeMap nodes. At 255M features: ~2 GB transient per band instead of the observed 112 GB. Counts are `u32` per spec — a single cell exceeding `u32::MAX` features is impossible at res ≥ 2 for realistic data, but use `checked_add` and error on overflow (clippy denies wrapping).

- [ ] **Step 1: Write the golden-equivalence test** (captures today's exact bytes before the change):

```rust
#[test]
fn sort_based_bands_match_btreemap_bytes() {
    // deterministic pseudo-random points
    let mut state = 0x2545F4914F6CDD1Du64;
    let mut rng = move || { state ^= state << 13; state ^= state >> 7; state ^= state << 17; state };
    let pts: Vec<(f64, f64)> = (0..50_000)
        .map(|_| ((rng() % 360_000) as f64 / 1000.0 - 180.0, (rng() % 180_000) as f64 / 1000.0 - 90.0))
        .collect();
    let before = include_bytes!("../tests/fixtures/trailer_50k_golden.bin"); // Step 1a: generate with current code
    let after = trailer_for(pts.into_iter(), Some(50_000)).unwrap();
    assert_eq!(after, before.to_vec());
}
```

Step 1a: before changing anything, generate `crates/fgba/tests/fixtures/trailer_50k_golden.bin` by running the same point generator through the *current* `trailer_for` in a scratch test and writing the bytes (`std::fs::write`). Commit the fixture first.

- [ ] **Step 2: Run test — PASS** (fixture equals current output; this is the regression pin).
- [ ] **Step 3: Replace the BTreeMap loop** in `trailer_at_with_analytics`:

```rust
let mut cell_buf: Vec<u64> = Vec::new(); // reused across bands
for (i, &res) in ladder.iter().enumerate() {
    cell_buf.clear();
    cell_buf.reserve(points.len());
    for &(lon, lat) in &points {
        if let Ok(cell) = a5::lonlat_to_cell(a5::LonLat::new(lon, lat), i32::from(res)) {
            cell_buf.push(cell);
        }
    }
    cell_buf.sort_unstable();
    let mut cells: Vec<(u64, u32)> = Vec::new();
    let mut total = 0u64;
    let mut runs = cell_buf.iter();
    if let Some(&first) = runs.next() {
        let (mut cur, mut n) = (first, 1u32);
        for &c in runs {
            if c == cur {
                n = n.checked_add(1).ok_or_else(|| Error::Format("cell count overflows u32".into()))?;
            } else {
                total += u64::from(n);
                cells.push((cur, n));
                cur = c; n = 1;
            }
        }
        total += u64::from(n);
        cells.push((cur, n));
    }
    // ...unchanged AggBand/SectionEntry assembly using `cells` and `total`
}
```

- [ ] **Step 4: Run tests** — golden test PASS (byte-identical), full `cargo test -p fgba` green.
- [ ] **Step 5: Scale smoke test (local, not CI):** `cargo run --release -p fgba-cli -- build data/formats/fgb/highways_lines.fgb -o /tmp/highways.a.fgb` with `/usr/bin/time -v`; expect completion with max RSS < 16 GB (previously OOM-killed at 112 GB). Record the number for Task 10's report.
- [ ] **Step 6: Commit** `fix: sort-based band aggregation bounds build memory to O(features)`.

---

### Task 3: Streaming analytics writer (B2)

**Files:**
- Modify: `crates/fgba/src/write.rs` (new `build_trailer_streaming_with_analytics<R: Read + Seek>`; refactor the in-memory analytics encoder into a chunk-at-a-time writer)
- Modify: `crates/fgba-cli/src/main.rs:200-220` (`cmd_build`: use the streaming path for `--analytics` too; delete the "Analytics needs the whole body" branch)
- Test: `crates/fgba/src/write.rs` tests + `crates/fgba-cli` integration

**Interfaces:**
- Consumes: `build_trailer_streaming<R: Read + Seek>(reader, options) -> Result<(Vec<u8>, BuildReport)>` (write.rs:306) — extend or sibling it: `pub fn build_trailer_streaming_with_analytics<R: Read + Seek>(reader: R, options: &BuildOptions) -> Result<(Vec<u8>, BuildReport)>`.
- Produces: byte-identical trailer to the existing in-memory `build_with_options` for the same input+options (golden test), with peak memory O(chunk_rows × columns), not O(body).

**Design:** The kind-2 layout is already streaming-friendly: absolute offsets, independent per-chunk Arrow IPC files, chunk grid fixed by `chunk_rows` and `row_count` (known from the FGB header before scanning). Two passes over the body via `Read + Seek` (no full buffering): pass 1 streams features → midpoints (existing) AND property values → per-column row buffers; every `chunk_rows` rows, encode each column's buffer as one Arrow IPC file, append to a trailer scratch (`Vec<u8>` or temp file), record `arrow_offsets`, compute `ChunkStat`/`GlobalStat` incrementally (sequential float accumulation per SPEC §6A.3 — single-threaded pass keeps determinism). Because `SectionEntry.offset` is `base_offset + out.len()` computed after the body copy, the streaming CLI path writes: body copy → analytics section bytes → bands → directory → footer (same assembly order as `trailer_at_with_analytics`; keep that function as the single assembler, feeding it the pre-encoded analytics bytes exactly as today — the change is *how* the analytics bytes are produced, from a streaming reader instead of `&[u8]`).

- [ ] **Step 1: Golden test** — take the existing small analytics fixture (find one via `grep -rn "analytics" crates/fgba/tests/ | head`; if none exists, build one: 10K-feature fgb from `testdata/cities_12.a.fgb`'s body or generate with `flatgeobuf::FgbWriter`), run in-memory `build_with_options(body, opts{analytics:true})` and save bytes as fixture; then assert streaming build of the same file yields identical bytes.
- [ ] **Step 2: Run — FAIL** (streaming fn missing).
- [ ] **Step 3: Implement** per Design. Key reuse: the existing in-memory analytics encoder (find it: `grep -n "KIND_BODY_ANALYTICS\|encode" crates/fgba/src/analytics.rs crates/fgba/src/write.rs`) — split it into `AnalyticsColumnWriter` (per-column row buffer + flush-to-Arrow-IPC) so both paths share encoding code (one implementation, two drivers — DRY).
- [ ] **Step 4: Tests green**; `cargo test -p fgba --features analytics`.
- [ ] **Step 5: CLI wiring** — `cmd_build` routes `--analytics` through `build_streaming_to_file` (main.rs:243+) extended with the analytics variant; remove the whole-body read. Integration: `fgba build data/formats/fgb/Land.fgb -o /tmp/land2.a.fgb --analytics` → `fgba verify /tmp/land2.a.fgb` (if a verify subcommand exists — check `Cmd` enum in main.rs) and `fgba query COUNT(*)` returns 847,984.
- [ ] **Step 6: Scale smoke** — `fgba build data/formats/fgb/highways_lines.fgb -o /tmp/hw.a.fgb --analytics` under `/usr/bin/time -v`: completes, RSS < 16 GB. (This also produces the highways a.fgb the benchmark lacked.)
- [ ] **Step 7: Commit** `feat: streaming analytics trailer build (memory O(chunk), not O(body))`.

---

### Task 4: Adaptive `chunk_rows`

**Files:**
- Modify: `crates/fgba-cli/src/main.rs` (`Build.chunk_rows` default handling), `crates/fgba/src/write.rs` (`BuildOptions`)
- Test: `crates/fgba/src/write.rs` tests

**Interfaces:**
- Produces: `pub fn effective_chunk_rows(row_count: u64, requested: u32) -> u32` — `requested` if the user passed `--chunk-rows` explicitly; else `clamp(next_power_of_two(ceil(row_count / 8192)), 4096, 1 << 20)`. Rationale: targets ~8K chunks max (8192 chunks × per-column 32 B stats+offsets stays well under the 16 MiB metadata cap for ≤ 60 columns) while never going below the spec's 4096 reference default; bigger chunks → bigger Arrow batches → fewer IPC footers, better scan throughput. SPEC §6A.1 explicitly allows this ("`K = 4096` is the reference writer default, not a format constant").

- [ ] **Step 1: Failing test:**

```rust
#[test]
fn adaptive_chunk_rows_targets_8k_chunks() {
    assert_eq!(effective_chunk_rows(847_984, 0), 4096);            // small: floor
    assert_eq!(effective_chunk_rows(255_381_003, 0), 1 << 15);     // 255M/8192≈31K → next pow2 32768
    assert_eq!(effective_chunk_rows(u64::MAX / 2, 0), 1 << 20);    // cap
    assert_eq!(effective_chunk_rows(255_381_003, 4096), 4096);     // explicit request wins
}
```

(`0` as the "not requested" sentinel; or change the CLI option to `Option<u32>` — preferred: make `BuildOptions.chunk_rows` an `Option<u32>` and keep the struct field name.)

- [ ] **Step 2: Run — FAIL.** Step 3: implement + wire into both build paths (in-memory and streaming) so `row_count` (FGB header `features_count`) drives it. Step 4: `cargo test -p fgba` green; rebuild Land fixture and confirm metadata size drops vs the 144 MiB trailer observed with fixed 4096 rows (expect ~10× smaller metadata, same Arrow payload). Step 5: commit `feat: adaptive analytics chunk sizing`.

---

### Task 5: Multi-partition parallel chunk scan (the parallelism lever)

**Files:**
- Modify: `crates/fgba-sql/src/stream.rs` (`Scan` gains `chunks: std::ops::Range<u64>`; `run_columns` and the `BodySource::Range` body path iterate only that subrange; `Selection::Prune` chunk list is partitioned, not global)
- Modify: `crates/fgba-sql/src/table.rs` (`scan()` builds `P` partitions)
- Test: `crates/fgba-sql/tests/` (find existing SQL integration tests: `ls crates/fgba-sql/tests/ 2>/dev/null || grep -rn "#\[test\]\|#\[tokio::test\]" crates/fgba-sql/src | head`)

**Interfaces:**
- Consumes: `BodyAnalytics { chunk_count, chunk_offsets, arrow_range(col, chunk), body_range(chunk) }` (analytics.rs:224-301, all public), `ChunkPruner::surviving_chunks(&[Expr]) -> Vec<u64>` (prune.rs:44), `Scan::run(&mut self, emit)` (stream.rs, `pub(crate)`).
- Produces: `FgbaTable::scan` returns `StreamingTableExec` with `P = min(chunk_count.surviving, available_parallelism())` partitions; each `FgbPartition` carries `Scan { chunks: lo..hi }`; `Scan::source_mode == Columns` partitions read only their Arrow ranges, `Body` partitions only their body ranges. Row order across partitions is unspecified (SQL semantics allow it; existing single-partition tests that relied on order must add `ORDER BY` — check and fix).

**Design notes:**
- Partitioning is trivially safe: SPEC §6A.1 guarantees chunk independence (each Arrow IPC file is self-contained; body ranges are `[chunk_offsets[i], chunk_offsets[i+1])`).
- Each partition opens its own `File` (no shared cursor) — matches the existing "fresh blocking source per execution" comment at stream.rs:676.
- `Selection::BBox` (R-tree path) stays single-partition in v1 (R-tree search is already sub-second; partitioning it needs per-chunk bbox zone maps — that lands free with Task 7's bbox columns, note as follow-up).
- DataFusion aggregates (hash group-by, SUM) automatically run two-phase over partitions → parallel final aggregation. `metadata_count_plan` short-circuits before partitions anyway.
- Cap: `available_parallelism()` from `std::thread` (no new dep); expose `--scan-threads` CLI flag on `fgba query` mapping to `SessionConfig::with_target_partitions` (DataFusion 48 API — verify exact method name against datafusion 48.0.1 in Cargo.lock before coding).

- [ ] **Step 1: Failing test** (deterministic equivalence + partition count):

```rust
// crates/fgba-sql/tests/parallel_scan.rs (or the established test location)
#[tokio::test]
async fn partitioned_scan_matches_single_partition() {
    // fixture: analytics-enabled a.fgb with >= 8 chunks (reuse/extend existing fixture builder)
    let multi = query_to_rows(&fixture, "SELECT id, name FROM features", /*threads*/ 8).await;
    let single = query_to_rows(&fixture, "SELECT id, name FROM features", 1).await;
    let mut m = multi.clone(); m.sort();
    let mut s = single.clone(); s.sort();
    assert_eq!(m, s);
    assert_eq!(multi.len(), single.len());
}
```

Plus an assertion that the physical plan has >1 partition (`EXPLAIN` contains `partitions=8` or plan-node introspection — follow whatever the existing tests use to inspect plans, if any).

- [ ] **Step 2: Run — FAIL** (single partition today).
- [ ] **Step 3: Implement** per Interfaces. In `table.rs::scan`, replace the single-partition vec with:

```rust
let n_chunks = self.analytics.as_ref().map(|a| a.chunk_count).unwrap_or(1);
let pruned: Vec<u64> = match &selection {
    Selection::Prune { filters, .. } => pruner.surviving_chunks(filters),
    _ => (0..n_chunks).collect(),
};
let p = pruned.len().min(available_parallelism().map(|n| n.get()).unwrap_or(1)).max(1);
let partitions: Vec<Arc<FgbPartition>> = pruned
    .chunks(pruned.len().div_ceil(p))
    .map(|group| Arc::new(FgbPartition::new(scan.with_chunks(group.to_vec()), Arc::clone(&projected))))
    .collect();
```

(`Scan` gains `chunks: Arc<[u64]>` or a `Range` per partition when contiguous — prefer explicit `Vec<u64>`/`Arc<[u64]>` since pruning makes groups non-contiguous.)
Without analytics (`self.analytics.is_none()`), keep exactly one partition (body-only scan; chunk grid unknown).

- [ ] **Step 4: Tests green**, including all pre-existing fgba-sql tests (fix any order-dependent ones with `ORDER BY`).
- [ ] **Step 5: Throughput smoke (local):** on `US_Census_Blocks.a.fgb` rebuilt with `--analytics` (8.2M rows): time `fgba query "SELECT COUNT(*) FROM features WHERE <string col> = '<val>'"` (forces Columns-mode scan) single vs 8 threads; expect ≥4× speedup at 8 threads on this machine. Record numbers for Task 10.
- [ ] **Step 6: Commit** `feat: parallel chunk-partitioned analytics scans`.

---

### Task 6: Parallel HTTP chunk fetch (stretch, do after Task 10 if time-boxed)

**Files:** Modify `crates/fgba/src/http.rs` (range reader), `crates/fgba-sql/src/stream.rs` (prefetch).
**Design:** Chunk independence means partition *p* can issue its Arrow-range reads concurrently (bounded window, e.g. 4 in-flight ranges per partition) using the existing `ureq`-based `Read+Seek` (blocking) — prefetch next chunk's `[arrow_range]` bytes into a memory buffer while decoding the current one. For v1 keep it simple: sequential-with-one-slot-prefetch per partition; measure; only add a thread pool if the smoke test shows latency-bound stalls. Test: `fgba serve` + `fgba query http://127.0.0.1:PORT/x.a.fgb ...` timing vs Task 5 local numbers; assert request count ≤ 2× chunk count (existing HTTP counters in `http.rs`). Commit `feat: prefetch analytics chunks over HTTP`.

---

### Task 7: kind-3 GEOMETRY_METRICS section (spec + writer)

**Files:**
- Modify: `SPEC.md` — new §6B (layout below) + §6A.1 note that kind-3 shares the chunk grid
- Modify: `crates/fgba/src/format.rs` — `pub const KIND_GEOMETRY_METRICS: u16 = 3;`
- Create: `crates/fgba/src/geom_metrics.rs` — build-side kernels
- Modify: `crates/fgba/src/write.rs` — emit kind-3 when `BuildOptions.geometry_metrics` (new field, default false; CLI flag `--geometry-metrics`, implied by `--analytics` unless `--no-geometry-metrics`)
- Modify: `crates/fgba/src/analytics.rs` — reader structs + validation (or new `crates/fgba/src/geom_analytics.rs` if analytics.rs grows past ~2200 lines)
- Test: `crates/fgba/tests/` round-trip fixture tests

**Spec §6B layout (write this text into SPEC.md):**

```text
SectionHeader (32 bytes) — same shape as §6A.1:
  encoding_version=1, flags=0, chunk_rows (MUST equal the kind-2 section's),
  row_count (MUST equal features_count), chunk_count (MUST equal kind-2's)
MetricDesc[5] (24 bytes each): metric_id u16 (0=length,1=area,2=npoints,3=bbox_xmin..: see table),
  arrow_type u8, flags u32=1 (HAS_ARROW_IPC), stream_offset u64, stream_size u64
  — one descriptor per metric column: length f64, area f64, npoints u32,
    bbox_xmin f64, bbox_ymin f64, bbox_xmax f64, bbox_ymax f64 (7 descriptors; metric_id 0..6)
Per stream: GlobalStat(56B) + ChunkStat[C](24B) + arrow_offsets[C+1] + ArrowIpcChunk[C]
  — identical record formats and semantics to §6A.2 (reuse the words verbatim).
Geometry semantics: planar (Euclidean, degree-unit) length/area over WGS84 lon/lat,
  matching geo::EuclideanLength / geo::unsigned_area; npoints counts all vertices
  (interior rings included); bbox is the geometry's exact lon/lat envelope;
  empty/null geometries store NaN length/area, 0 npoints, NaN bbox with validity=null.
A file MAY contain kind-3 only together with kind-2 (same chunk grid).
```

**Interfaces:**
- Produces (writer): `geom_metrics::feature_metrics(g: &flatgeobuf::FgbFeature) -> FeatureMetrics { length: f64, area: f64, npoints: u32, bbox: [f64;4] }` computed via a single `geozero::GeomProcessor` pass (no geo_types allocation — mirror `count_verts`/length/area math from `bench/format-bench/src/main.rs:metrics()` but streaming: processor accumulates segment lengths, shoelace area, vertex count, min/max x/y).
- Produces (reader): `pub struct GeometryMetrics { pub chunk_rows: u32, pub row_count: u64, pub chunk_count: u64, pub columns: [MetricColumn; 7] }` with `MetricColumn { global: GlobalStat, chunks: Vec<ChunkStat>, arrow_offset(col,chunk) -> (u64,u64) }` and `Reader::geometry_metrics() -> Result<Option<&GeometryMetrics>>` mirroring `analytics_metadata()` (read.rs:249+).

- [ ] **Step 1: Kernel unit tests first** (`geom_metrics.rs`): known geometries — 2-point line at equator (length = Δlon, area = 0), unit square polygon (area = 1.0, length = 4.0, npoints = 5), multipolygon with hole (hand-computed shoelace), empty geometry (NaN/0/NaN). Compare against `geo` crate results on the same input within 1e-9.
- [ ] **Step 2: Implement kernel** (GeomProcessor: `xy` updates bbox+npoints+segment length; ring/polygon callbacks accumulate shoelace terms; multi* handled by processor nesting). Run — PASS.
- [ ] **Step 3: Writer** — extend the streaming analytics pass (Task 3) to also feed `feature_metrics` per feature into 7 column writers sharing the kind-2 chunk flush points; assemble kind-3 section bytes; `trailer_at_with_analytics` gains `geometry: Option<Vec<u8>>` and emits the SectionEntry *before* kind-2 (directory order: kind 3, kind 2, kind 1 bands — or after kind-2; pick one and write it into the spec text; readers must not depend on order).
- [ ] **Step 4: Round-trip test:** build small fixture with `--analytics` (implies metrics) → reader returns `GeometryMetrics` with `row_count == features_count`, per-chunk zone maps bracket the global stats, and `SUM(length)` over Arrow chunks equals sequential kernel sum over body decode (same float order → exact equality).
- [ ] **Step 5: Old-reader compatibility test:** open the kind-3 file with the kind-2-only code path (e.g., construct `Reader` and call `stats()`/`query()`/`analytics_metadata()`) — all succeed, kind-3 ignored (relies on the verified unknown-kind skip, format.rs:95-109; add an explicit test that a directory containing kind 3 parses and band queries work).
- [ ] **Step 6: Validation hardening** (SPEC §6A.4-style bounds for kind-3: cap metadata by directory section size, chunk length 1..=64 MiB, exact stream formulas, row_count equals kind-2's and features_count) + malformed-input tests (truncated section, wrong row_count, overlapping ranges → clean `Error::Format`, body still readable).
- [ ] **Step 7: Commit** `feat: kind-3 geometry metrics section (spec §6B, writer, reader)`.

---

### Task 8: SQL virtual columns over kind-3

**Files:**
- Modify: `crates/fgba-sql/src/schema.rs` (add `geom_length`, `geom_area`, `geom_npoints` to the reserved geometry-output names next to `geom_wkb`/`bbox_*` at schema.rs:12-14; map them to kind-3 metric ids when present, else mark unsupported → planner error "requires --geometry-metrics")
- Modify: `crates/fgba-sql/src/stream.rs` (`SourceMode::Columns` extended: a scan selecting only virtual metric columns + properties reads kind-2 + kind-3 Arrow ranges, never the body; `bbox_xmin..ymax` columns switch from body-decoded to kind-3 stored when available — keep decode fallback)
- Modify: `crates/fgba-sql/src/prune.rs` (zone-map pruning for metric columns comes free via the same `ChunkStat` machinery — extend `PropertySchema` to address kind-3 columns)
- Test: fgba-sql integration tests

**Interfaces:**
- Produces: SQL surface `SELECT SUM(geom_length + geom_area) FROM features`, `SELECT COUNT(*) FROM features WHERE geom_area > 1e-4`, `... WHERE bbox_xmax >= :minx AND bbox_xmin <= :maxx ...` all answered without body decode when kind-3 exists.

- [ ] **Step 1: Failing tests:** on the Task 7 fixture — (a) `SELECT SUM(geom_length) FROM features` equals the brute-force body-decode sum (exact f64 equality, single partition to fix summation order); (b) `SELECT COUNT(*) FROM features WHERE geom_npoints > 100` matches body-decode count; (c) metrics counters (stream.rs `ScanMetrics`/`MetricsInner`, stream.rs:39-46) show `body_bytes_read == 0` for both queries; (d) with 8 partitions the SUM matches within documented float-reassociation tolerance (relative 1e-9).
- [ ] **Step 2: Run — FAIL** (unknown column).
- [ ] **Step 3: Implement** per Interfaces (reuse `decode_arrow_chunk`, stream.rs:609+, for kind-3 streams — same Arrow IPC format).
- [ ] **Step 4: Tests green.** Step 5: commit `feat: SQL geometry-metric virtual columns backed by kind-3`.

---

### Task 9: Vectorized WKB geometry UDFs (general path, no kind-3 required)

**Files:**
- Create: `crates/fgba-sql/src/udf_geom.rs`
- Modify: `crates/fgba-sql/src/session.rs` (register UDFs)
- Test: `crates/fgba-sql/src/udf_geom.rs` unit tests

**Interfaces:**
- Produces: DataFusion `ScalarUDF`s `st_length(Binary) -> Float64`, `st_area(Binary) -> Float64`, `st_npoints(Binary) -> UInt32` operating on the `geom_wkb` column. Kernels parse WKB directly (little-endian flat f64 sequences — WKB line/polygon payloads are contiguous coordinate arrays) computing length/area/npoints in one pass without geo_types objects; reuse the shoelace/segment math from `geom_metrics.rs` via a shared `Coords` trait over (a) geozero processor events and (b) WKB byte cursors (one implementation of the math, two adapters).
- Semantics documented as planar-degree (same as Task 7); this differs from DuckDB's `ST_Length` only in that DuckDB 1.5.4 spatial also computes planar degrees for EPSG:4326 by default — the benchmark comparison stays apples-to-apples (verify by comparing `SELECT SUM(st_length(geom_wkb))` vs duckdb `SUM(ST_Length(geom))` on Land: expect relative diff < 1e-9; if DuckDB applies spheroidal math, note it in RESULTS instead).

- [ ] **Step 1: Kernel unit tests:** hand-built WKB for LineString [(0,0),(1,0),(1,1)] → length 2.0, area 0, npoints 3; Polygon unit square → area 1.0; MultiPolygon; empty GeometryCollection → 0/0/0; truncated WKB → `Err` not panic (hostile input rule).
- [ ] **Step 2: Run — FAIL.** Step 3: implement kernels + `ScalarUDFImpl` (DataFusion 48: implement `ScalarUDFImpl` with `invoke_batch`; copy the registration pattern from any existing UDF in the workspace — `grep -rn "ScalarUDF" crates/ | head`; if none, follow datafusion 48 docs example). Step 4: SQL-level test: `SELECT SUM(st_length(geom_wkb)) FROM features` on a no-kind-3 fixture equals body-decode sum; confirm it runs through `SourceMode::Body` (metrics: body bytes > 0). Step 5: commit `feat: st_length/st_area/st_npoints WKB UDFs`.

---

### Task 10: Rebuild + re-benchmark + parity gates

**Files:**
- Modify: `bench/format-bench/src/main.rs` (new format `afgba-sql`: workloads map to `fgba query` subprocess calls — `full_count`→`SELECT COUNT(*)`, `attr_group`→`GROUP BY` (quote column names — benchmark lesson: names contain `:`), `geom_agg`→`SELECT SUM(geom_length+geom_area)`, `bbox_count`→`WHERE bbox_xmax>=.. AND bbox_xmin<=.. AND bbox_ymax>=.. AND bbox_ymin<=..` over kind-3 columns; parse CSV stdout like the duckdb path does; reuse `run_duckdb`'s `/usr/bin/time`-wrapped subprocess pattern for RSS)
- Modify: `scripts/convert-one.sh` (fgba build step gains `--analytics`; note: this also fixes the `fgba build` OOM for highways via Tasks 2+3)
- Modify: `data/formats/RESULTS.md` (append "Round 2" section)

**Steps:**
- [ ] Rebuild all 10 a.fgb with `--analytics --chunk-rows auto` (Task 4) — includes highways for the first time. Record build wall time + peak RSS per dataset.
- [ ] Re-run `format-bench orchestrate` for the `afgba-sql` format only (existing formats' numbers stay valid; add a `--formats` filter flag to orchestrate).
- [ ] **Parity gates (highways, 255M features):** `full_count` ≤ 0.5 s (metadata; parquet 0.06 s incl. duckdb startup — parity class "metadata", not byte-for-byte); `attr_group` ≤ 3 s (parquet 0.52 s, duckdb 0.42 s — target within ~5×, documented as DataFusion-vs-DuckDB engine gap); `geom_agg` ≤ 5 s via kind-3 columns (**beats** parquet 13.6 s); `bbox_count` ≤ 1 s (R-tree, unchanged — beats parquet 16 s); `zoom_agg` correct totals now (Task 1: trails/tracks/footways within band-estimate semantics) and ≤ 10 ms.
- [ ] Update `data/formats/RESULTS.md` with a Round-2 table + the two bug-fix notes (B1/B3) + build-resource numbers.
- [ ] Commit `bench: round-2 analytics parity results`.

---

### Task 11: Docs + spec polish

- [ ] `docs/guide/analytics.html`: kind-3 columns, `--geometry-metrics`, parallel scan flag, the three measured parity tables from Task 10, and the honest limitations (planar-degree semantics; attr_group engine gap vs DuckDB; zoom_agg estimates are band-resolution-dependent).
- [ ] SPEC.md: finalize §6B wording (Task 7 draft), §9.4 covering note (Task 1), changelog entry in `CHANGELOG.md` (unreleased section; follow existing entry style — read it first).
- [ ] README: extend the "native analytics guide" paragraph with one kind-3 sentence + the highways parity headline (COUNT 255M features < 0.5 s; SUM(geometry metrics) faster than GeoParquet).
- [ ] Commit `docs: kind-3 geometry metrics + parallel analytics scans`.

---

## Reference Implementations

Key algorithms are embedded in their tasks above (band_cells_in_view — Task 1 Step 3; sort-based aggregation — Task 2 Step 3; partition assignment — Task 5 Step 3). The geozero metrics kernel (Task 7) reuses the exact math already validated in `bench/format-bench/src/main.rs` (`metrics()`, `count_verts()`, `geom_length()` — verified against `geo` crate results on Land during the benchmark session).

## Session Research (verified API facts, 2026-10-25)

- `fgba::Reader`: `open_path(&Path) -> Result<Reader<BufReader<File>>>` (read.rs:346), `query(&mut self, [f64;4], u8) -> Result<Vec<(u64,u32)>>` (read.rs:216), `stats(...) -> Result<BandStats{cells,total}>` (read.rs:229/36), `bands() -> Vec<BandMeta{res,offset,size}>` (read.rs:156/24).
- Covering: `covering()` (read.rs:373), `rect_covering_cells()` splits >180° into two halves (read.rs:412-418) — the B3 root cause; `COMPLEMENT_AREA_FRACTION=0.5`, `COMPLEMENT_POLE=89.9`; `query()` complement branch at read.rs:219-238; `Band::query_excluding` exists (band.rs, used only by that branch).
- a5 1.0.0 (pinned): `cell_to_boundary(cell_id: u64, Option<CellToBoundaryOptions>) -> Result<Vec<LonLat>, String>` (core/cell.rs:448), `cell_to_lonlat(cell) -> Result<LonLat, String>` (cell.rs:421), `lonlat_to_cell`, `polygon_to_cells` with `Containment::Overlapping`, `get_num_cells(res)`, `uncompact`.
- Writer: `build(body: &[u8])` (write.rs:199), `build_with_options` (write.rs:204), `build_trailer_streaming<R: Read+Seek>` (write.rs:306), `trailer_at_with_analytics(midpoints, total_hint, base_offset, Option<Vec<u8>>)` (write.rs:117) with the per-band `BTreeMap<u64,u32>` at write.rs:151 (B1 root cause), `band_ladder(count)` (write.rs:87, target = 4×count cells), `BuildOptions{..., chunk_rows: u32 = 4096}`.
- CLI: `cmd_build` (main.rs:200); whole-body in-memory requirement for `--analytics` at main.rs:212-218 (B2); streaming path `build_streaming_to_file` (main.rs:243).
- Analytics reader: `BodyAnalytics{section_start, section_size, chunk_rows, column_count, row_count, chunk_count, chunk_offsets: Vec<u64>, columns: Vec<ColumnAnalytics>, metadata_bytes_read}` (analytics.rs:224); `column(idx)` (247), `chunk_row_count(i)` (254), `body_range(i) -> BodyRange` (264), `arrow_range(col,chunk) -> (u64,u64)` (301); `ArrowChunk` (338).
- fgba-sql: `FgbaTable::scan` builds ONE `FgbPartition` in a `StreamingTableExec` (table.rs); `Scan{path, output_schema, projected_schema, projection, analytics, selection, config, metrics}` is `Clone` with "no cursor shared" (stream.rs:139); `SourceMode::Columns` chosen iff analytics present AND no geometry column selected (stream.rs `source_mode`); `Selection::{All, BBox([f64;4]), Prune{bbox, filters}}`; `FgbPartition::execute` → one `spawn_blocking` (stream.rs:712-725); `metadata_count_plan` short-circuit (session.rs:88, plan.rs); `ChunkPruner::surviving_chunks` (prune.rs:44); geometry output columns `geom_wkb`, `bbox_xmin/ymin/xmax/ymax` reserved in schema.rs:12-14; `ScanMetrics` tracks decoded features + analytics bytes + ranges (stream.rs:39-46); DataFusion **48.0.1** (Cargo.lock:630).
- Directory: unknown section kinds preserved verbatim (`format.rs:95-109`), round-trip test with kind `0x8001` at format.rs:274 → kind-3 is safe for old readers.
- Empirical (this session, machine: 24 cores / 125 GB / ZFS): Land `--analytics` COUNT(*) 266 ms wall ≈ 0 ms user; GROUP BY 157 ms; zone-pruned filter 20 ms; fgb body scans 848/433 ms; duckdb 43/123 ms; Land trailer with chunk_rows=4096: 144 MiB. Highways non-analytics build OOM-killed at 112 GB anon-rss (255,381,003 features). Band totals wrong for near-world viewports on trails/tracks/footways (8,056 vs 15,791,189) but exact for Land/Census/buildings/Flood; regional viewport (Europe) correct → confirms complement-ring ambiguity, not band data corruption.
- Benchmark harness facts (for Task 10): `format-bench` lives at `bench/format-bench` (standalone, `[workspace]` excluded); formats are per-dataset files under `data/formats/{fgb,a.fgb,geojson,shp,duckdb,parquet}/`; SQL identifiers with `:` need double-quoting; duckdb 1.5.4 has NO `ST_EnvIntersects` (use `ST_Intersects(ST_MakeEnvelope(...), geom)`); geoparquet geometry reads back as native `GEOMETRY` (no `ST_GeomFromWKB` needed in duckdb).
- External prior art: GeoArrow flat geometry encoding + vectorized kernels ([geoarrow-rs compute](http://geoarrow.org/geoarrow-rs/python/v0.3.0/api/compute/enums/), [area/length method enums](http://geoarrow.org/geoarrow-rs/python/v0.3.0/api/compute/types/#geoarrow.rust.compute.types.AreaMethodT)) — Task 7's kind-3 is the same idea (flat columnar geometry data) specialized to precomputed metrics; GeoParquet `covering`/bbox-column pruning ([geoparquet#297](https://github.com/opengeospatial/geoparquet/issues/297), [Sedona SEDONA-627](https://issues.apache.org/jira/browse/SEDONA-627), [ClickHouse geoparquet rowgroup pruning](https://github.com/ClickHouse/pull/104435)) — validates the stored-bbox-zone-map approach in Tasks 7–8. Optional future: depend on `geoarrow` crates instead of hand-rolled flat encoding if full columnar geometry (not just metrics) is later wanted; not required for this plan's parity gates.

## Risks & Mitigations

| risk | mitigation |
|---|---|
| A5 `cell_to_boundary` cost at fine bands (millions of cells × ring computation) in Task 1 | Only the >half-sphere branch uses it; overview bands (res ≤ 6) have ≤ ~10K cells. Finest-band world queries are rare; measure in Task 1 Step 5, add a coarse-res fast path (`get_num_cells(res)` small → direct ring covering is unambiguous) if needed. |
| Byte-identical trailer requirement (Task 2) fails due to u32 count ordering | Golden fixture test pins bytes before the change; run-length encoding over sorted ids yields ascending `(cell,count)` exactly like `BTreeMap::into_iter`. |
| Streaming analytics writer divergence from in-memory encoder | Shared `AnalyticsColumnWriter` (one encoder, two drivers) + golden byte-equality test (Task 3 Step 1). |
| DataFusion 48 multi-partition plan changes row order, breaking existing tests | Audit fgba-sql tests for order assumptions first; add `ORDER BY` where semantics require it; document unordered scans. |
| kind-3 float sums vs body-decode sums mismatch | Both use sequential body-order accumulation (spec-mandated); equality test is exact for single-partition; multi-partition tolerance documented (Task 8 Step 1d). |
| `--geometry-metrics` size blowup (~52 B/feature: 7 columns) | Owner accepted size for speed; flag is opt-out-able (`--no-geometry-metrics`); document ~5% of fgb body size at 4096-row chunks (measured on Land in Task 7 Step 4 — record actual). |
| Planar-degree vs spheroidal semantics mismatch vs DuckDB in comparisons | Task 9 Step 5 empirical cross-check on Land; document whichever way it lands. |
| highways (255M) rebuild during Task 10 takes ~1 h+ | Run detached with nohup + log (session lesson: long foreground runs get SIGTERM'd); verify with `fgba info` after. |

## Acceptance Criteria (definition of done)

1. `fgba build --analytics` completes on all 10 benchmark sources including highways (255M features) with peak RSS < 16 GB.
2. `zoom_agg` totals are correct (±band-estimate semantics) for near-world extents on all datasets.
3. On highways: `SELECT COUNT(*)` ≤ 0.5 s; `SUM(geom_length+geom_area)` ≤ 5 s; string-column `GROUP BY` ≤ 3 s; 10%-window bbox count ≤ 1 s — all via `fgba query` on the a.fgb, measured by `format-bench` with RSS recorded.
4. All existing tests green; new tests per task; `cargo clippy --workspace --all-features` clean under the deny lints; SPEC.md, guide, README, CHANGELOG updated; RESULTS.md Round-2 section written.
