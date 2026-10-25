# FGBA Native SQL and Analytical Columns Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven development (recommended, one fresh subagent per task) or the executing-plans skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make native FGBA SQL fast for analytical workloads by preserving attributes, adding independently readable Arrow column chunks with global/chunk statistics, and using the existing FlatGeobuf R-tree for exact bbox selection.

**Architecture:** An optional `analytics` feature adds one kind-2 `BODY_ANALYTICS` section with exact global metadata, numeric/null zone maps, absolute body offsets, and independently range-readable Arrow IPC column chunks with validity. `fgba-sql` uses DataFusion 48.0.1 and built-in `StreamingTableExec`: attribute-only work reads only required Arrow columns/chunks, while geometry output and legacy files stream the FlatGeobuf body. A5 bands remain separate, with explicit approximate viewport tables and a one-entry cache.

**Tech Stack:** Rust 2021/MSRV 1.89.0, DataFusion 48.0.1, Arrow/Arrow IPC 55.2.0, Tokio 1.54.1, futures 0.3.34, async-trait 0.1.92, flatgeobuf 6.0.1, geozero 0.15.1, existing `fgba`/`clap`/`thiserror`.

**Spec:** `docs/design/sql-analytics.md` (required: unified layout, source selection, and exact/approximate semantics)

## Global Constraints

- Preserve inclusive Rust 1.89.0 and `#![forbid(unsafe_code)]`. Verify the complete locked dependency graph on 1.89.0, not only the current compiler.
- No DuckDB, Parquet, GeoPackage conversion, SQL parser, custom `ExecutionPlan`, or full `ST_*`/CRS engine. Arrow IPC is used only as a column-chunk encoding.
- Keep `fgba` core/DataFusion-free and preserve default/wasm dependency shape. Arrow IPC is behind the optional `analytics` feature enabled by `fgba-sql` and the CLI.
- Keep kind-1 count bands, `FGBAGG01`, directory v1, and default output byte-identical when `--analytics` is absent. Kind 2 is additive; no footer/directory version change.
- Exact body SQL must never be silently answered from A5 bands or zone maps. Bbox pushdown is Exact only for stored bbox intersection with a valid index; zone maps are always Inexact with residual predicates.
- A whole-band total is exact only under bbox-midpoint attribution. A covering result is exactly an aggregate of selected cells, not exact spatial intersection and not a guaranteed bound.
- No unused analytical payload: global records feed `attribute_stats`, Arrow chunks feed actual projection/group/join execution, and chunk stats/offsets drive actual pruning.
- Preserve deterministic writes, input validation, error handling, existing contracts, and unrelated working-tree changes. Make only the specified targeted edits in `example/README.md`.
- Do not regenerate India, add remote SQL, or claim absolute memory bounds for hostile individual features or query operators.

---

## Session Research and Verified Facts

### Repository evidence

- `crates/fgba-cli/src/main.rs:43-113` is the command extension point. `convert.rs:1-6` says properties are dropped; lines 34-39 reject GeoParquet/GeoPackage.
- `crates/fgba/src/read.rs:41-48` has no band cache; `query` at 201-205 rereads a band every time. `http.rs:108-146` performs one request per read.
- `flatgeobuf 6.0.1` supports typed properties and `FgbWriter::add_column`; missing properties can be represented as SQL NULL.
- High-level `FgbReader` hides feature indices, but `PackedRTree::stream_search` exposes `{ offset, index }`; offset is relative to feature-data start.
- Existing India: 15,705,867 features, five bands (3, 5, 7, 9, 11), ~1.2 MiB trailer, only `OGC_FID` and `geom` in the body.
- Baseline `/home/sheece/.cargo/bin/cargo test --workspace`: 37 tests passed before planning.

### Compiled native probes

All probes were external to the repository and ran with rustc 1.99.0. Rust 1.89.0 remains a separate CI gate.

1. **DataFusion 48.0.1 custom provider:** `TableProvider` → built-in `StreamingTableExec` → custom `PartitionStream` → bounded receiver worker. Results were `SUM(value)=27` for values 8, 9, 10 and `COUNT(*)=3` for a limited subquery. The lock resolved Arrow 55.2.0, Tokio 1.54.1, futures 0.3.34, and async-trait 0.1.92.
2. **Band `MemTable` and output:** `SELECT SUM(count)` returned 12. `DataFrame::execute_stream` streamed batches to `arrow-csv`. `arrow-csv` emitted binary `01ff` and null as `""`.
3. **Arrow IPC:** an independent 1,066-byte chunk round-trip preserved three rows, two columns, and null values using `FileWriter::write`, `finish`, `into_inner()?`, and `FileReader`. This avoids a new column encoding.
4. **FlatGeobuf offsets:** 12 features produced 13 strictly increasing offsets with terminal `body_len`. Starting at offset 4 before constructing `select_all` returned the same feature as ordinal 4 and eight remaining rows.
5. **R-tree alignment:** every leaf `NodeItem` satisfied `data_start + leaf.offset == absolute_feature_offset` when streamed in lockstep with body features.

`FgbReader::select_all` consumes the first feature-length prefix during construction. A chunk source must use `start_at` before `FgbReader::open`/`select_all`; seeking afterward is invalid.

### Verified API choices

- DataFusion 48.0.1 declares Rust 1.82; 49.0.2 requires 1.85.1 and current 55.2.0 requires 1.94, so 48 preserves the existing contract.
- `StreamingTableExec::try_new(schema, partitions, projection, ordering, infinite, limit)` reuses the built-in finite plan. `PartitionStream::execute` returns `SendableRecordBatchStream` directly.
- `RecordBatchReceiverStreamBuilder` owns bounded channels and propagates errors/panics. Capacity 2 is the starting value; stop on failed `blocking_send` and after the first decode error.
- `SQLOptions` with DDL/DML/statements disabled still permits `EXPLAIN`; the 48.0.1 probe returned logical and physical plans.
- `TableProvider::scan` and `project_schema` take `Option<&Vec<usize>>` in 48.0.1. `supports_filters_pushdown` returns `Unsupported`, `Inexact`, or `Exact`.
- `StreamingTableExec` is distinct from `StreamTable`; the latter marks input unbounded and projects only after full batches.
- `geozero::ToWkb::to_wkb(dims)` performs WKB conversion. Enable `with-wkb` and derive dimensions from all four header flags (`has_z`, `has_m`, `has_t`, `has_tm`).
- `NodeItem::from_reader` is public. `PackedRTree::index_size` is public. `NodeItem` is 40 bytes, so leaf start is `data_start - features_count * 40`.
- Arrow IPC `FileWriter::try_new`, `write`, `finish`, and `into_inner()?` creates independent files; `FileReader::try_new` and stable `collect::<Result<Vec<_>, _>>()` reads them. Do not use unstable `Iterator::try_collect`.

### Unified kind-2 layout adopted in this plan

One kind-2 `BODY_ANALYTICS` section per file; directory `flags=0`, `meta_len=0`, descriptors sorted by column index. `A=column_count`, `R=features_count`, `K=chunk_rows`, `C=ceil(R/K)`. `K=4096` is a benchmark default, not a format constant.

```text
SectionHeader (32 bytes)
  u8 version=1, u8 flags=0, u16 reserved=0,
  u32 chunk_rows, u32 column_count, u32 reserved=0,
  u64 row_count, u64 chunk_count

ColumnDesc[A] (40 bytes each)
  u16 column_index, u8 fgb_type, u8 value_class, u32 flags,
  u64 stream_offset_relative_to_section,
  u64 stream_size = 56 + 24*C + 8*(C+1) + arrow_bytes,
  u64 arrow_offsets_offset_relative_to_section,
  u64 arrow_bytes

chunk_offsets[C+1] (u64 each)
  absolute file offsets; offset[0]=first feature prefix;
  offset[C]=body_len; strictly increasing

Per-column stream:
  GlobalStat (56 bytes)
    u64 non_null_count, u64 nan_count,
    u64 min_bits, u64 max_bits,
    u8[16] sum (i128/u128 LE; f64 in first 8, last 8 zero),
    u32 flags (bit0 SUM_VALID, bit1 HAS_INFINITY), u32 reserved=0

  ChunkStat[C] (24 bytes each)
    u64 min_bits, u64 max_bits,
    u32 non_null_count, u32 nan_count

  arrow_offsets[C+1] (u64 each)
    absolute offsets of independent Arrow IPC chunks;
    arrow_offsets[C] = section_start + stream_offset + stream_size

  ArrowIpcChunk[C] (variable)
    one single-field schema and exactly one RecordBatch with values+validity
```

Value classes: 0=opaque (Bool/String/Json/DateTime/Binary), 1=signed i64, 2=unsigned u64, 3=f64. Every property type gets Arrow values and null counts. Numeric classes additionally support min/max/sum and numeric zone maps. `ColumnDesc.flags` bit0 `HAS_ARROW_IPC=1`; all other bits are reserved zero.

Size formula: `32 + 40*A + 8*(C+1) + A*(56 + 24*C + 8*(C+1) + arrow_bytes)`. For one attribute this is **8,032 + arrow_bytes** at 1M rows and **122,952 + arrow_bytes** at 15,705,867 rows with `K=4096`. These are calculations, not performance results. Arrow IPC adds its independent per-chunk overhead; the 3-row/two-column probe was 1,066 bytes.

Semantic requirements:

- `null_count = rows - non_null_count`; NaN is not null. Bounds exist iff `non_null_count > nan_count`.
- Integer bounds never become f64. SUM uses checked i128/u128; result conversion is checked.
- f32 promotes losslessly to f64. Bounds include infinities but exclude NaN. Canonicalize min zero to `-0` and max zero to `+0`.
- Float global SUM uses sequential binary64 body order, skipping NULL without reassociation. NaN, infinity, or non-finite intermediate/final values clear `SUM_VALID` and zero sum bytes.
- Opaque classes have exact null counts but zero min/max/sum/flags, meaning no statistic. `attribute_stats` returns null for those bounds/sums while preserving exact `row_count`, `non_null_count`, and derived `null_count`.
- Global `attribute_stats` exposes `row_count`, `non_null_count`, derived `null_count`, `nan_count`, typed min/max, `sum_text`, `sum_valid`, and `has_infinity`. `sum_text` preserves full integer precision and finite binary64 text; it is exact metadata, not implicit SQL aggregate pushdown.
- Every count/offset/stream/Arrow range is validated before allocation. Missing/malformed analytics fall back to body reads without affecting body/kind-1 validity.
- `BuildReport::attribute_columns` counts every property column; `analytics_requested` distinguishes an ordinary build from a request with no properties. Do not emit an empty analytics section.

### Support/scope matrix

| Capability | Included | Deferred |
|---|---|---|
| Metadata-only `COUNT(*)` | Header/R-tree count and zero-column batch | Approximate count shortcuts |
| Exact global metadata | Global records + `attribute_stats` | Automatic aggregate substitution for every SQL function/type |
| Attribute projection/group/join | Required Arrow columns/chunks only | Dictionary/compression tuning |
| Selective filtering | R-tree + numeric/null zone maps + body offsets | String/binary zone maps and variable byte-capped chunks |
| Geometry/WKB/bbox output | Streaming FlatGeobuf body | Full spatial `ST_*`/CRS engine |
| Raw A5 analytics | `a5_band_<res>` `MemTable`s | Per-cell attribute cubes |
| Explicit approximate viewport SQL | `a5_viewport_approx` | Automatic exact-to-approximate rewrite |
| High-cardinality/group-heavy work | Exact DataFusion over Arrow/body rows | Memory-free grouping claims |
| Remote execution | Local files only | Remote SQL and browser/server API |

### Risks and mitigations

| Risk | Mitigation |
|---|---|
| Full dependency graph fails on Rust 1.89.0 | Exact direct pins, committed lockfile, dedicated 1.89.0 `--locked` CI job |
| Stats disagree with body or R-tree order | Validate every leaf/body offset during writing; abort requested analytics build on mismatch |
| Pruning changes results | Exact-only valid bbox pushdown, Inexact-only zone maps, NaN-conservative rules, full-scan parity tests, residual predicates |
| `select_all` prefix lifecycle mixes features | Use `start_at` before iterator construction and test first/middle/last chunk reads |
| Attributes lost before analytics can help | Complete property-preserving conversion first; analytics read existing property-rich FGB bodies |
| Large file read into memory | Reopenable sources, bounded batches/channels/chunks, no CLI `collect`, explicit per-feature/operator caveats |
| Global integer sum loses precision | i128/u128 storage and `sum_text`; never force through f64 |
| One cursor shared across workers | One source/adapter per partition execution; no shared mutable `Read + Seek` cursor |
| Range requests outweigh pruning | Coalesce ranges with correct row spans and measure body/Arrow bytes and requests |

---

## File Structure

| Area | Files |
|---|---|
| Property conversion | Modify `crates/fgba-cli/src/convert.rs`, `crates/fgba-cli/src/main.rs`, `crates/fgba-cli/tests/cli.rs` |
| Shared geometry | Create `crates/fgba/src/geometry.rs`; modify `lib.rs`, `write.rs` |
| Analytical format | Create `crates/fgba/src/analytics.rs`, `src/io.rs`, `tests/analytics.rs`; modify `Cargo.toml`, `format.rs`, `read.rs`, `write.rs`, `lib.rs`, `SPEC.md` |
| SQL crate | Create `crates/fgba-sql/Cargo.toml`, `README.md`, and `src/{lib,session,schema,table,stream,output,error,plan}.rs` |
| SQL tests | Create `crates/fgba-sql/tests/sql.rs`, `tests/pruning.rs` |
| CLI SQL | Create `crates/fgba-cli/src/query.rs`; modify `src/main.rs`, `Cargo.toml`, `tests/cli.rs` |
| Pruning | Create `crates/fgba-sql/src/prune.rs`; modify `schema.rs`, `table.rs`, `stream.rs` |
| Cache | Modify `crates/fgba/src/read.rs`, `src/http.rs`, `tests/http.rs`, `tests/vectors.rs` |
| Docs/CI | Modify `README.md`, `CHANGELOG.md`, `docs/guide/*`, `docs/spec.html`, `docs/index.html`, `example/README.md`, `.github/workflows/ci.yml`, root `Cargo.toml`, `Cargo.lock` |

`fgba-sql` modules have one responsibility: `schema` maps FGB to Arrow, `table` plans sources, `stream` decodes body/Arrow batches, `prune` evaluates conservative chunk elimination, `session` registers tables, `output` streams CSV, and `error` exposes typed failures.

---

## Reference Implementations

### A. Feature and dependency shape

Add to `crates/fgba/Cargo.toml`:

```toml
[dependencies]
arrow-ipc = { version = "=55.2.0", default-features = false, optional = true }

[features]
default = []
http = ["ureq"]
analytics = ["arrow-ipc"]

[package.metadata.docs.rs]
features = ["http", "analytics"]
```

`fgba-sql` uses `fgba = { version = "0.1.0", path = "../fgba", features = ["analytics"] }`; the CLI enables `features = ["http", "analytics"]`. Core `analytics.rs` is `#[cfg(feature = "analytics")]`. Default and wasm builds do not compile Arrow IPC.

Add `fgba-sql` to workspace `members` and use:

```toml
[dependencies]
async-trait = "=0.1.92"
arrow-csv = "=55.2.0"
arrow-ipc = { version = "=55.2.0", default-features = false }
datafusion = { version = "=48.0.1", default-features = false, features = ["recursive_protection"] }
fgba = { version = "0.1.0", path = "../fgba", features = ["analytics"] }
flatgeobuf = { version = "=6.0.1", default-features = false }
futures = "=0.3.34"
geozero = { version = "=0.15.1", default-features = false, features = ["with-wkb"] }
thiserror = "2"
tokio = { version = "=1.54.1", features = ["rt", "sync"] }

[dev-dependencies]
tempfile = "3.24.0"
```

Use `datafusion::arrow` for all Arrow arrays/schema/record batches. `arrow-ipc` is only the chunk codec; no Parquet feature is enabled.

### B. Thin provider returning the built-in plan

Use this exact 48.0.1 interface for `FgbaTable::scan`; it performs no I/O:

```rust
#[async_trait::async_trait]
impl TableProvider for FgbaTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| self.filter_support(expr))
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projection = projection.cloned();
        let projected = project_schema(&self.schema, projection.as_ref())?;
        let selection = self.selection_for(filters);
        let inexact = selection.has_inexact_attribute_filter();
        let partition = FgbPartition::new(
            self.source.clone(),
            self.output_schema.clone(),
            projected.clone(),
            projection,
            selection,
            self.batch_config,
            self.metrics.clone(),
        );
        Ok(Arc::new(StreamingTableExec::try_new(
            projected,
            vec![Arc::new(partition)],
            None,
            std::iter::empty::<datafusion::physical_expr::LexOrdering>(),
            false,
            if inexact { None } else { limit },
        )?))
    }
}
```

`filter_support` returns Exact only for four canonical bbox-envelope comparisons with finite ordered constants and a usable R-tree. Numeric/null zone-map predicates return Inexact. Everything else returns Unsupported. Metadata-only counts are handled separately by the exact logical-plan check in Reference Implementation F, never inferred from projection alone.

### C. Bounded partition worker

`FgbPartition::execute` constructs the worker without doing I/O. `scan.run` returns `Result<()>`, so the first decode/read error reaches DataFusion:

```rust
impl PartitionStream for FgbPartition {
    fn schema(&self) -> &SchemaRef {
        &self.projected_schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = RecordBatchReceiverStreamBuilder::new(
            self.projected_schema.clone(),
            CHANNEL_CAPACITY,
        );
        let tx = builder.tx();
        let mut scan = self.scanner.clone();
        builder.spawn_blocking(move || {
            scan.run(|batch| tx.blocking_send(Ok(batch)).is_err())
        });
        builder.build()
    }
}
```

The callback returns `true` only for cancellation, not for a successful batch. Each execution owns its source; no consumed iterator is cloned. `DataFrame::execute_stream` uses the same bounded path for results.

### D. Shared offset adapter

`crates/fgba/src/io.rs` exposes `SharedReader<R>` with `new`, `new_at`, `position`, `bytes_read`, and a thread-safe shared handle. It contains `Arc<Mutex<R>>`, atomic position/body-byte counters, and a one-shot `start_at: u64` (`u64::MAX` means no override). `new_at` intercepts only the initial `SeekFrom::Current` index skip and changes it to `SeekFrom::Start(start_at)`. All subsequent reads/seeks are normal and position/byte counters update. Mutex poisoning becomes `std::io::Error`, never a panic.

Required behavior:

```rust
#[test]
fn chunk_start_is_before_select_all_construction() -> TestResult {
    let offsets = write_twelve_feature_fixture()?;
    let shared = SharedReader::new_at(Cursor::new(fixture_bytes()?), offsets[4]);
    let mut iter = FgbReader::open(shared)?.select_all()?;
    assert_eq!(
        iter.next()?.map(|f| f.property::<String>("name")).transpose()?,
        Some("city7".to_string())
    );
    let mut remaining = 1;
    while iter.next()?.is_some() {
        remaining += 1;
    }
    assert_eq!(remaining, 8);
    Ok(())
}
```

Declare `name` with `add_column` before writing properties. Do not write property bytes without a schema.

### E. One-entry band cache

Add `cached_band: Option<AggBand>` to `Reader`, initialize `None`, and replace only `Reader::query`:

```rust
    pub fn query(&mut self, view: [f64; 4], res: u8) -> Result<Vec<(u64, u32)>> {
        let covering = covering(&view, res)?;
        if let Some(band) = self.cached_band.as_ref().filter(|band| band.res == res) {
            return Ok(band.query(&covering));
        }
        let band = self.read_band(res)?;
        let matches = band.query(&covering);
        self.cached_band = Some(band);
        Ok(matches)
    }
```

`read_band` remains an explicit fresh decode.

### F. Exact logical-plan count optimization

`FgbaSession::execute_dataframe` first calls `DataFrame::into_unoptimized_plan`. `metadata_count_plan` accepts only:

```text
Limit [optional; skip = 0, literal fetch]
  Projection [exactly one direct output/alias, Int64 output field]
    Aggregate [group_expr = [], one count(Int64(1)) aggregate]
      TableScan [features, projection = None or [], filters = [], fetch = None]
        OR
      Filter [valid canonical bbox]
        TableScan [same constraints]
```

The aggregate must have `func.name() == "count"`, one `Int64(1)` argument, `distinct = false`, and no filter/order/null treatment. Joins, grouping, `HAVING`, `DISTINCT`, `OFFSET`, `ORDER BY`, other aggregates, and unsupported filters return `None` and take the normal exact path. Output field name/type come from the plan schema, preserving aliases.

`metadata_count_plan` returns `Option<(RecordBatch, Option<usize>)>`. With no bbox it uses `OutputSchema::features_count`; with a valid bbox it runs only `PackedRTree::stream_search` in `spawn_blocking` and uses the result length, mapping `JoinError` to `DataFusionError`. Convert the count with `i64::try_from` and return an error on overflow. Build an `Int64Array` batch with the plan's field name and let `MemoryStream::with_fetch` apply the literal limit. This is logical-plan inspection, not SQL text parsing or a provider heuristic.

---

### Task 1: Preserve properties in existing converters

**Files:**
- Create: `crates/fgba/src/geometry.rs`
- Modify: `crates/fgba/src/lib.rs`, `crates/fgba/src/write.rs`
- Modify: `crates/fgba-cli/src/convert.rs`, `crates/fgba-cli/src/main.rs`
- Test: `crates/fgba-cli/tests/cli.rs`

**Interfaces:**
- Consumes: existing geometry readers, `FgbWriter::add_column`, `FeatureWriter::property`, and `BboxSink`.
- Produces: deterministic `FeatureData { geometry, properties }`, `PropertyValue`, schema predeclaration, and reusable `fgba::geometry::BboxSink`.

- [ ] **Step 1: Write failing property round-trip tests**

Add tests that convert input, reopen the FGB body with `FgbReader`, and read typed properties. Include:

1. GeoJSON `cities.geojson`: collect all 12 `name` values with `feature.property::<String>("name")`, sort them, and assert `city1` through `city12`.
2. GeoJSON null schema: two features share `height: 1.5` and `height: null`; assert the declared column is `Double` and nullable, and the second feature has no height property.
3. CSV `name,lon,lat`: assert `name` is String and `lon`/`lat` are Double with original values. WKT: assert `name` exists and `wkt` is not a property.
4. Shapefile with DBF: write a point and a `Record` with `name=alpha`, `height=12.5`; assert both typed values after conversion. Keep the existing missing-DBF test unchanged.
5. Type conflict: one GeoJSON feature has `height: 1`, another has `height: "tall"`; conversion fails with a readable conflict instead of coercing. A feature using reserved `geom_wkb` is also rejected.
6. Determinism: existing byte-identical build/convert tests still pass.

For the DBF fixture, use `shapefile::dbase` (no new direct dbase dependency):

```rust
let dbf_path = tmp.path("properties.dbf");
let fields = shapefile::dbase::TableWriterBuilder::new()
    .add_character_field(shapefile::dbase::FieldName::try_from("name")?, 32)
    .add_numeric_field(shapefile::dbase::FieldName::try_from("height")?, 18, 2)
    .build_with_dest(std::fs::File::create(&dbf_path)?);
let mut record = shapefile::dbase::Record::default();
record.insert(
    "name".to_string(),
    shapefile::dbase::FieldValue::Character(Some("alpha".to_string())),
);
record.insert(
    "height".to_string(),
    shapefile::dbase::FieldValue::Numeric(Some(12.5)),
);
let mut writer = fields;
writer.write_record(&record)?;
```

Use the existing `shapefile::ShapeWriter` for `properties.shp`, then `convert` and assert `alpha`/`12.5`.

- [ ] **Step 2: Run property tests to verify they fail**

Run:

```sh
/home/sheece/.cargo/bin/cargo test -p fgba-cli --test cli properties
/home/sheece/.cargo/bin/cargo test -p fgba-cli --test cli geojson_nullable
/home/sheece/.cargo/bin/cargo test -p fgba-cli --test cli type_conflict
```

Expected: FAIL because current output has no property columns and conflicts are not detected.

- [ ] **Step 3: Move and harden the shared bbox sink**

Move `BboxSink` from `write.rs` to public `crates/fgba/src/geometry.rs` with its existing tests and `midpoint()` behavior. Track whether any non-finite coordinate was seen and add `checked_bounds() -> Result<Option<[f64;4]>>`: it returns `Error::Format` for any non-finite coordinate, `Ok(None)` for empty geometry, and exact bounds otherwise. Add unit assertions for empty, finite, and NaN/inf inputs. Preserve existing build behavior: non-finite coordinates are ignored for midpoint construction; the SQL path uses `checked_bounds` and fails with the feature ordinal rather than receiving a partial exact bbox.

Export `pub mod geometry` from `lib.rs` and update `write.rs` to import the shared sink.

- [ ] **Step 4: Implement deterministic owned properties and schema**

Replace `Geoms = (Vec<Geometry<f64>>, usize)` with an owned feature list containing `BTreeMap<String, PropertyValue>`. Support all FGB `ColumnValue` variants through an owned enum with `column_type()` and `as_column_value()`.

Implement these exact schema rules and update `convert.rs`'s module documentation plus the CLI `Convert` help to say properties are preserved; GeoPackage/GeoParquet remain explicitly unsupported.

- Sort property names in a `BTreeMap`, infer one FGB type per name, and predeclare every column before writing. Reject exact and case-insensitive duplicate names and the reserved SQL geometry names (`geom_wkb`, `bbox_xmin`, `bbox_ymin`, `bbox_xmax`, `bbox_ymax`) with the conflicting property name. Reject schemas exceeding the FGB u16 property-index limit, and reject string/JSON/date-time/binary values whose encoded length exceeds u32::MAX with the property name.
- `nullable = missing || explicit_null`. An all-null property defaults to a nullable String column; do not fail type inference or assign an arbitrary numeric type.
- Only lossless widening is allowed (signed integer→Long, unsigned integer→ULong, Float→Double). Reject every signed/unsigned mixture and every string/numeric conflict with the property name; do not infer a potentially lossy common type.
- GeoJSON: preserve strings, booleans, signed/unsigned integers, and doubles. Test numeric conversion in the order `is_i64`, `is_u64`, `is_f64` so ordinary signed-range integers stay signed; only values outside i64 require ULong. Arrays/objects become JSON text; null becomes missing/null.
- CSV/TSV non-WKT fields remain strings; recognized x/y fields are also retained as doubles; WKT is excluded.
- Shapefile uses `shapefile::Reader::iter_shapes_and_records` when DBF exists and the existing shape-only path otherwise. Map `Character/Memo→String`, `Numeric/Currency/Double→Double`, `Float→Float`, `Integer→Int`, `Logical→Bool`, and `Date/DateTime→DateTime` ISO text. Format dates as `YYYY-MM-DD` using `Date::{year,month,day}` and datetimes as `YYYY-MM-DDTHH:MM:SS` using `DateTime::{date,time}` plus `Time::{hours,minutes,seconds}`; no additional date dependency.
- DBF `Record` is converted from `HashMap` to `BTreeMap` before schema inference.

`write_body` calls `add_column` for the complete schema, then writes each feature with the declared indices. Missing properties are omitted from property bytes; do not write a zero/empty-string sentinel. `FeatureWriter::property` errors inside the `add_feature_geom` closure are captured in an outer `Option` and returned immediately after the call; never discard them with `let _ = ...`.

- [ ] **Step 5: Run converter and existing tests**

Run:

```sh
/home/sheece/.cargo/bin/cargo test -p fgba-cli
/home/sheece/.cargo/bin/cargo test -p fgba
```

Expected: all new property tests and every existing test PASS, including missing DBF and deterministic builds.

- [ ] **Step 6: Commit**

```sh
git add crates/fgba/src/geometry.rs crates/fgba/src/lib.rs crates/fgba/src/write.rs crates/fgba-cli/src/convert.rs crates/fgba-cli/src/main.rs crates/fgba-cli/tests/cli.rs
git commit -m "feat: preserve feature properties during conversion"
```

---

### Task 2: Add the unified BODY_ANALYTICS format

**Files:**
- Create: `crates/fgba/src/analytics.rs`, `crates/fgba/src/io.rs`, `crates/fgba/tests/analytics.rs`
- Modify: `crates/fgba/Cargo.toml`, `crates/fgba/src/{format,read,write,lib}.rs`, `SPEC.md`
- Modify: `crates/fgba-cli/src/main.rs`, `crates/fgba-cli/tests/cli.rs`

**Interfaces:**
- Consumes: property-rich bodies, `NodeItem::from_reader`, `PackedRTree::index_size`, Arrow IPC `FileWriter`/`FileReader`, and the unified layout in the spec.
- Produces: `KIND_BODY_ANALYTICS=2`, `BuildOptions { analytics, chunk_rows }`, `build_with_options`, `BuildReport::{attribute_columns, analytics_requested}`, `Reader::{read_body_analytics_metadata, read_arrow_chunk}`, and `BodyAnalytics::metadata_bytes_read`.

- [ ] **Step 1: Write failing layout, codec, and semantic tests**

Create `crates/fgba/tests/analytics.rs` with `chunk_rows=2` and numeric/string/null/NaN/infinity cases. Assert:

- Kind 2 is present only with `BuildOptions { analytics: true, chunk_rows: 2 }` and at least one property column; the equivalent default build remains byte-identical to `build`.
- A property-free request reports `attribute_columns == 0` and `analytics_requested == true`, emits no kind-2 section, and has no `attribute_stats` SQL table.
- Header/descriptor counts, exact section size formula, `offset[C] == body_len`, strict body/Arrow offsets, `arrow_offsets[C] == section_start + stream_offset + stream_size`, and `arrow_offsets_offset == stream_offset + 56 + 24*C`.
- Global and chunk `non_null_count`/`nan_count` sums, integer min/max, float infinities, canonical zero signs, and `SUM_VALID` behavior.
- `row_count == FGB header features_count`, including body rows that band building skipped.
- Every Arrow IPC chunk has one field, one batch, the declared type, and the exact chunk row count; values and validity round-trip for numeric, string, JSON, DateTime, and Binary columns.
- Opaque classes preserve null counts but have null bounds/sums in `attribute_stats`.
- Truncation at every metadata prefix, duplicate columns, wrong stream sizes, out-of-body/Arrow offsets, count mismatches, unsupported types, reserved nonzero bytes, oversized chunks, and malformed IPC all fail cleanly without panicking.
- Ordinary `Reader::bands` and kind-1 queries are unchanged with kind 2 present.

- [ ] **Step 2: Add optional feature and bounded metadata/chunk readers**

Apply Reference Implementation A. Add `KIND_BODY_ANALYTICS=2` and the unified structures to `analytics.rs`: `BodyAnalytics`, `ColumnDesc`, `GlobalStat`, `ChunkStat`, `ValueClass`, `BodyRange`, and `ArrowChunk`. When the feature is disabled, `BuildOptions { analytics: true, .. }` returns `Error::Format("analytics support disabled")`; default builds and unknown-kind skipping remain unaffected.

`Reader::read_body_analytics_metadata` returns `Ok(None)` when kind 2 is absent. Otherwise it reads only bounded metadata blobs (header/descriptors/body offsets/global/chunk stats/Arrow offsets), never Arrow payload, and returns `BodyAnalytics { ..., metadata_bytes_read }`.

`Reader::read_arrow_chunk(column_index, chunk_index)` validates descriptor/range and returns exactly the bytes for that chunk with its identity. Enforce the 64 MiB per-chunk cap before allocation. Raw bytes are decoded and validated by `fgba-sql`, keeping core independent of DataFusion.

- [ ] **Step 3: Implement one-pass stats, offsets, and Arrow writing**

Add:

```rust
#[derive(Debug, Clone)]
pub struct BuildOptions {
    pub analytics: bool,
    pub chunk_rows: u32,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self { analytics: false, chunk_rows: 4096 }
    }
}

pub fn build_with_options(body: &[u8], options: &BuildOptions)
    -> Result<(Vec<u8>, BuildReport)>;
```

Keep `build(body)` as `build_with_options(body, &BuildOptions::default())`. Validate `chunk_rows > 0` before reading features.

Use the writer algorithm in the spec: compute `data_start`/`leaf_start` with checked arithmetic, open an **independent** leaf-reader cursor, stream leaf `NodeItem`s with `from_reader`, record exact feature offsets with `SharedReader`, update global/chunk stats for **every body row**, and assert every `data_start + leaf.offset == current_feature_offset`. Update attribute stats even when band geometry processing later skips an empty/invalid geometry; band totals and body stats must not share that filtering. Abort the requested build on mismatch before writing output. Include every body property column sorted by `column_index`.

Write one single-field `FileWriter` file per column/chunk with exactly one `RecordBatch`, preserving values and validity. Use `finish()` and `into_inner()?`. Reject chunks above 64 MiB with column/chunk identity. `metadata_bytes_read` is reported by the reader, not guessed from the directory section size.

Implement integer i128/u128 checked sums, sequential float accumulation and validity, NaN counts, null counts, finite bounds, canonical zero signs, and deterministic descriptor/record ordering. Do not store NaN payloads or nonzero reserved bytes. Add `BuildReport::attribute_columns` and `BuildReport::analytics_requested` (ordinary builds are `0`/`false`; a request with no properties is `0`/`true`). Emit kind 2 only when at least one property column exists; never emit an empty section.

- [ ] **Step 4: Expose flags and CLI verification**

Add `--analytics` and `--chunk-rows <N>` to `build`/`convert`, forwarded through `BuildOptions`. `verify` validates kind 2 and prints `analytics: N columns, M chunks — OK` when present. `print_report` prints the attribute-column count when positive and a clear `analytics: no property columns` notice when the flag was requested but `attribute_columns == 0`. Existing output remains unchanged without the flag.

- [ ] **Step 5: Run format, semantic, and regression tests**

Run:

```sh
/home/sheece/.cargo/bin/cargo test -p fgba --features analytics --test analytics
/home/sheece/.cargo/bin/cargo test --workspace
```

Expected: all layout, semantic, hostile-input, Arrow round-trip, and existing regression tests PASS. Also run `/home/sheece/.cargo/bin/cargo build --target wasm32-unknown-unknown -p fgba` to verify the default dependency shape is unchanged.

- [ ] **Step 6: Update the normative specification and commit**

Update `SPEC.md`: register kind 2, insert the unified layout/validation/semantics, replace the kind-1 attribute record with one count-only record (`band_flags == 0`, all other bits reserved), and move per-cell attribute aggregation to deferred work. Remove obsolete provisional-layout notes while retaining draft band-cap guidance. Keep footer/directory versions unchanged.

```sh
git add SPEC.md crates/fgba/Cargo.toml crates/fgba/src/analytics.rs crates/fgba/src/io.rs crates/fgba/src/format.rs crates/fgba/src/read.rs crates/fgba/src/write.rs crates/fgba/src/lib.rs crates/fgba/tests/analytics.rs crates/fgba-cli/src/main.rs crates/fgba-cli/tests/cli.rs
git commit -m "feat: add FGBA analytical columns and statistics"
```

---

### Task 3: Add native Arrow-backed `fgba-sql` and CLI query

**Files:**
- Create: `crates/fgba-sql/Cargo.toml`, `README.md`, `src/{lib,error,session,schema,table,stream,output,plan}.rs`, `tests/sql.rs`
- Create: `crates/fgba-cli/src/query.rs`
- Modify: root `Cargo.toml`, `Cargo.lock`, `crates/fgba-cli/Cargo.toml`, `crates/fgba-cli/src/main.rs`, `crates/fgba-cli/tests/cli.rs`

**Interfaces:**
- Consumes: `BodyAnalytics`, `Reader::read_arrow_chunk`, `FgbReader`, shared `BboxSink`, property-rich bodies, and Reference Implementations A–D.
- Produces: `fgba_sql::query_csv(path, sql, options, writer) -> Result<()>`, `FgbaSession { ctx, features }` from `session::register`, CLI `fgba query <INPUT> <SQL> [--res N] [--bbox ...]`, tables `features`, `a5_band_<res>`, `a5_viewport_approx`, and `attribute_stats`.

- [ ] **Step 1: Write failing native SQL integration tests**

Create `crates/fgba-sql/tests/sql.rs` with property-rich fixtures and a helper that executes SQL and collects only small test results (the CLI must not use this helper). Cover:

```rust
#[test]
fn exact_body_and_band_sql() -> TestResult {
    let path = cities_fgba_with_names()?;
    assert_eq!(scalar_i64(query(path, "SELECT COUNT(*) FROM features")?), 12);
    assert_eq!(
        scalar_u64(query(path, "SELECT SUM(count) FROM a5_band_2")?),
        12
    );
    let approx = query_with_bbox(
        path,
        "SELECT SUM(count) FROM a5_viewport_approx",
        2,
        [-10.0, 40.0, 10.0, 60.0],
    )?;
    assert_eq!(scalar_u64(approx), 8);
    Ok(())
}
```

Also test:

- `SELECT name FROM features WHERE name='city1'` returns `city1` and demonstrates typed properties.
- `geom_wkb` is Binary beginning with WKB byte order 1; bbox for `city1` is `[2.36,48.84,2.36,48.84]`.
- Two `features` scans in a self-join return 12, proving reopenable sources. A plain FGB fixture with `write_index: false` returns the same bbox-filtered rows through the full-scan fallback without an R-tree error; an empty FGB returns zero rows without attempting `select_bbox`.
- Exact-plan metadata counts: `SELECT COUNT(*) FROM features` returns 12 with zero body/Arrow payload reads; `COUNT(1) AS exact_rows LIMIT 2` preserves the alias and count; `LIMIT 0` returns no rows. Negative guards: `COUNT(*) WHERE name='city1'` returns 1, `COUNT(*) WHERE name!='city1'` returns 11, `COUNT(name)` returns 12, `COUNT(DISTINCT name)` returns 12, and `COUNT(*) GROUP BY name` returns 12 groups. These prove unsupported filters and other aggregate shapes never take the metadata shortcut.
- Attribute-only `GROUP BY name` reads only the `name` Arrow column, not other properties, WKB, or the FlatGeobuf body.
- All `ColumnType` mappings, nullable/missing properties, non-nullable missing errors, exact and case-insensitive duplicate names, geometry-field collisions, and invalid geometry ordinal errors.
- `LIMIT 1` on a 100,000-feature fixture reads at most the initial batch plus two queued batches, not all features.
- Read-only options reject `CREATE TABLE`, `COPY`, and `SET` but permit `EXPLAIN`.
- `attribute_stats` returns exact `row_count`, `non_null_count`, derived `null_count`, `nan_count`, typed min/max, `sum_text`, `sum_valid`, and `has_infinity` without reading Arrow chunks.

Run `/home/sheece/.cargo/bin/cargo test -p fgba-sql` before creating the crate and confirm it fails because the package does not exist.

- [ ] **Step 2: Create crate and Arrow schema/decoders**

Apply Reference Implementation A. Implement `OutputSchema` with source columns plus `geom_wkb` and four bbox fields, preserving source order and CRS metadata. Store `features_count` and `has_spatial_index` for the exact count/R-tree paths. Reject exact and case-insensitive duplicate property names, geometry-field collisions, and unknown types.

Implement a `ColumnValues` enum of owned vectors for every Arrow type, with `append_null`, typed `set(row, value)`, and `finish() -> ArrayRef`. Reject a runtime `ColumnValue`/header-type mismatch with the column name instead of panicking. `FeatureBatchBuilder` tracks projected output columns, row count, and current bytes. Use `RecordBatch::try_new_with_options` with `RecordBatchOptions::with_row_count` for zero-column batches.

Use `geozero::ToWkb` for WKB and shared bbox logic for bounds. Derive `CoordDimensions` from all four header flags (`has_z`, `has_m`, `has_t`, `has_tm`). Copy all property/string/binary data before advancing.

- [ ] **Step 3: Implement provider, body/Arrow sources, and metrics**

Implement `FgbaTable` using Reference Implementation B and `FgbPartition` using C. In this task, `supports_filters_pushdown` returns `Unsupported` for every filter, so DataFusion performs all filtering before Task 4 adds source pruning. `SourceMode::Body` streams FGB properties/WKB/bbox through `FeatureBatchBuilder`. `SourceMode::Columns` reads only selected Arrow chunks with `read_arrow_chunk`, validates one field/one batch/type/row count using `arrow_ipc::reader::FileReader`, and combines the projected arrays.

Create `SharedReader` in `crates/fgba/src/io.rs` (Reference Implementation D) and export `pub mod io`. Use it for every body source so `body_bytes_read` includes FGB header/index/body reads and `position` remains accurate across seeks. `analytics_bytes_read` comes from actual kind-2 metadata bytes plus Arrow chunk lengths; kind-1 band bytes are reported separately, not hidden in either counter.

Use one source per partition execution. Bound batches by 4,096 rows or 8 MiB and channel capacity 2. The first decode/read error stops the worker; `blocking_send` failure stops on cancellation. Keep `scan()` I/O-free.

Add `ScanMetrics` with cumulative `features_read`, `body_bytes_read`, `analytics_bytes_read`, `ranges_opened`, and `wkb_values_decoded` atomics and a `metrics()` snapshot method. `total_bytes_read()` sums the two byte counters. The metadata-only count shortcut lives in `plan.rs`, not in `FgbPartition`: `execute_dataframe` first inspects the logical plan with Reference Implementation F and returns a `MemoryStream` only for the exact count shape. Every other query takes the ordinary body/Arrow path.

- [ ] **Step 4: Register body, band, and global-stat tables and stream CSV**

`session::register` returns `FgbaSession { ctx: SessionContext, features: Arc<FgbaTable> }`, keeping production metrics accessible to tests and benchmarks. It handles FGBA and plain FGB. Register `features` always and every available kind-1 band as `a5_band_<res>` using `MemTable::try_new`. With bbox, register `a5_viewport_approx` from `Reader::query`; default to deepest band. Reject `--res` without `--bbox`, bbox without an available band, empty SQL, and invalid bbox input.

Register `attribute_stats` when kind 2 exists with `column_index: UInt16`, `name: Utf8`, `value_class: Utf8` (`opaque`/`signed`/`unsigned`/`float`), `row_count: UInt64`, `non_null_count: UInt64`, `null_count: UInt64` (`row_count - non_null_count`), `nan_count: UInt64`, typed nullable min/max columns, `sum_text: Utf8`, `sum_valid: Boolean`, and `has_infinity: Boolean`.

`query_csv` performs file/schema/stats registration synchronously, then creates a current-thread runtime with `Builder::new_current_thread().enable_all().build()?.block_on(...)` only for SQL execution and worker streaming. It calls `sql_with_options` with DDL/DML/statements disabled, passes the resulting `DataFrame` to `FgbaSession::execute_dataframe` for the exact logical-plan count check, and streams results through `arrow-csv`. Use `writer.write(&batch)`, not `collect`.

- [ ] **Step 5: Add the CLI command and failing-then-passing dispatch test**

Add this Clap variant and dispatch to `query::run`:

```rust
    /// Execute native read-only SQL over an FGB or FGBA file
    Query {
        /// Input FlatGeobuf/FGBA file
        input: PathBuf,
        /// SQL using features, a5_band_<res>, a5_viewport_approx, or attribute_stats
        sql: String,
        /// A5 resolution for --bbox (default: deepest band)
        #[arg(long)]
        res: Option<u8>,
        /// Covering bbox as minx,miny,maxx,maxy
        #[arg(long, allow_hyphen_values = true)]
        bbox: Option<String>,
    },
```

`query::run` validates nonempty SQL, calls `fgba_sql::query_csv` with stdout, and maps typed errors to one readable line. Reuse `parse_bbox`; do not accept a remote URL in this task.

- [ ] **Step 6: Run native SQL and full CLI tests**

Run:

```sh
/home/sheece/.cargo/bin/cargo test -p fgba-sql
/home/sheece/.cargo/bin/cargo test -p fgba-cli --test cli query
/home/sheece/.cargo/bin/cargo run -p fgba-cli -- query testdata/cities_12.a.fgb "SELECT COUNT(*) FROM features"
/home/sheece/.cargo/bin/cargo run -p fgba-cli -- query testdata/cities_12.a.fgb --bbox -10,40,10,60 "SELECT COUNT(*) AS cells, SUM(count) AS estimated FROM a5_viewport_approx"
```

Expected: native tests pass; CLI prints 12 for exact body rows and 1 cell/8 estimated features for the covering query.

- [ ] **Step 7: Commit**

```sh
git add Cargo.toml Cargo.lock crates/fgba-sql crates/fgba-cli/Cargo.toml crates/fgba-cli/src/main.rs crates/fgba-cli/src/query.rs crates/fgba-cli/tests/cli.rs
git commit -m "feat: add native FGBA SQL queries"
```

---

### Task 4: Drive exact pruning with Arrow stats and the R-tree

**Files:**
- Create: `crates/fgba-sql/src/prune.rs`, `crates/fgba-sql/tests/pruning.rs`
- Modify: `crates/fgba-sql/src/{table,stream,schema,error}.rs`
- Modify: `crates/fgba/src/io.rs`, `crates/fgba/tests/analytics.rs`

**Interfaces:**
- Consumes: `BodyAnalytics`, `SharedReader::new_at`, `read_arrow_chunk`, `PackedRTree::stream_search`, DataFusion expressions, and `ScanMetrics`.
- Produces: conservative `ChunkPruner`, coalesced `BodyRange { start, end, first_ordinal, end_ordinal }` selections, joint R-tree+attribute pruning, and exact full-scan-parity results.

- [ ] **Step 1: Write failing pruning and parity tests**

Use `chunk_rows=2` with numeric values deliberately clustered so some chunks can be skipped. Test:

1. `height > 100` and `height <= 100` return exactly the same rows as `SELECT ... FROM features` without analytics, while `features_read` and `total_bytes_read()` are lower on the selective query.
2. `IS NULL` and `IS NOT NULL` use `non_null_count` correctly; all-null chunks are not treated as empty for `COUNT`.
3. Integer values around and above 2^53 prune without f64 conversion errors; an f32 boundary literal with uncertain rounding remains a candidate and still returns full-scan-identical rows.
4. NaN-containing chunks remain candidates unless the predicate has a specific proof. NaN is not null. Non-finite literals without a proof also stay candidates.
5. Joint bbox+height filters use the R-tree and stats, return full-scan-identical rows, and leave a residual `FilterExec` for the attribute predicate in `EXPLAIN`.
6. Missing/malformed analytics fall back to the body and still return correct rows.
7. First/middle/last chunk reads and the terminal `body_len` are correct; no chunk reads the trailer. A merged range containing a skipped middle chunk still decodes every later row and preserves full-scan parity.
8. Inexact attribute filter plus `LIMIT` does not stop the source before residual filtering. Valid bbox-only pushdown is Exact and has no residual bbox filter; the exact-plan `COUNT(*)` path uses the R-tree result count without feature payload. A bbox plus `name='city1'` query returns 1 through the ordinary path, not the R-tree count shortcut.
9. Attribute-only pruning reads only selected Arrow columns/chunks; it never opens the FlatGeobuf body. Geometry projection falls back to the body with correct values.

Run the new tests before implementing `prune.rs`; expected FAIL because ranges/metrics are not used.

- [ ] **Step 2: Implement conservative expression pruning**

`ChunkPruner` recognizes only `Column op Literal` for numeric types and `IS NULL`/`IS NOT NULL`. Normalize literal-on-left comparisons and conjunctions. Compare a literal only when it converts exactly into the column's signed/unsigned/f64 class **and preserves the original FGB width semantics**; f32 comparisons with uncertain rounding/cast behavior stay `Candidate::Keep`. NaN/non-finite literals that lack a specific proof also stay Keep. Unsupported casts, mixed-class uncertainty, and every other expression yield `Candidate::Keep`. Provably disjoint signed/unsigned comparisons may skip, but no lossy cast is allowed.

Implement `Candidate::{Keep, Skip}` only. Never use NaN or zero as an empty-chunk marker. For floats, any NaN-containing chunk defaults to Keep unless the predicate has an explicit proof (for example a finite equality cannot match NaN, but range/inequality cases require care). Infinities remain in bounds. Null tests use `non_null_count` for every value class; NaN is not null.

Return only Inexact for zone maps. Residual filtering remains responsible for exact rows.

- [ ] **Step 3: Implement exact bbox and range planning in the worker**

Keep `scan()` I/O-free. The worker performs any R-tree search and range construction:

1. `PackedRTree::stream_search` yields relative offsets/ordinals. Convert to `data_start + offset` and `index / chunk_rows`.
2. Return Exact only for the four canonical bbox-envelope comparisons with finite ordered constants and `features_count > 0 && index_node_size > 0`. This is exact for stored bbox intersection, not geometry intersection. Everything else remains Inexact or Unsupported. `metadata_count_plan` handles the valid bbox-only `COUNT(*)` separately through the R-tree result count; it is never inferred from an empty provider projection.
3. Without bbox, evaluate surviving chunks directly from stats. With bbox, intersect R-tree candidates with surviving chunks. Keep bbox-only scans on the finer `select_bbox` path; combined pruning works at whole-chunk granularity, so any chunk containing an R-tree candidate is read in full and residual attribute predicates remove false positives.
4. Sort/coalesce adjacent `BodyRange`s, recording absolute start/end offsets and ordinals. Represent each as `{ start, end, first_ordinal, end_ordinal }` and compute rows as `end_ordinal - first_ordinal` (including any skipped rows inside a merged gap). Start with `RANGE_GAP_BYTES = 64 * 1024`: merge a gap only when it is at most 64 KiB, and benchmark other budgets rather than treating this as a format constant.
5. For body ranges, open a distinct source with `SharedReader::new_at(start)`, call `select_all`, and decode exactly `end_ordinal - first_ordinal` rows. For Arrow ranges, read only the selected columns/chunks with `read_arrow_chunk`. Extra rows inside a coalesced gap are discarded only by DataFusion's residual predicates, never by the row counter.

Do not allocate one partition per chunk. Start with one pruning worker; parallel range partitions require a later measured design.

- [ ] **Step 4: Run pruning and complete regression tests**

Run:

```sh
/home/sheece/.cargo/bin/cargo test -p fgba-sql --test pruning
/home/sheece/.cargo/bin/cargo test -p fgba --features analytics --test analytics
/home/sheece/.cargo/bin/cargo test --workspace
```

Expected: all parity, NaN/null, joint-filter, hostile-fallback, and existing tests PASS. Selective tests read fewer rows/bytes than full scans; no correctness assertion is weakened to obtain that improvement.

- [ ] **Step 5: Commit**

```sh
git add crates/fgba/src/io.rs crates/fgba/tests/analytics.rs crates/fgba-sql/src/prune.rs crates/fgba-sql/src/table.rs crates/fgba-sql/src/stream.rs crates/fgba-sql/src/schema.rs crates/fgba-sql/src/error.rs crates/fgba-sql/tests/pruning.rs
git commit -m "perf: prune FGBA analytical scans"
```

---

### Task 5: Add band caching, docs, benchmarks, and MSRV CI

**Files:**
- Modify: `crates/fgba/src/read.rs`, `src/http.rs`, `tests/http.rs`, `tests/vectors.rs`
- Create: `docs/guide/analytics.html`
- Modify: `README.md`, `CHANGELOG.md`, `crates/fgba/README.md`, `crates/fgba-cli/README.md`, `crates/fgba-sql/README.md`
- Modify: `docs/index.html`, `docs/spec.html`, `docs/guide/{quickstart,rust,hosting,converting,faq}.html`, `example/README.md`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: completed SQL/format/pruning behavior and existing static documentation.
- Produces: bounded warm band reads, user-facing native SQL docs, and reproducible correctness/performance gates.

- [ ] **Step 1: Write and fail the HTTP cache test**

Append to `crates/fgba/tests/http.rs`:

```rust
#[test]
fn repeated_stats_reuses_the_cached_band_request() -> TestResult {
    let port = serve(Mode::Ranges(vector_bytes()?))?;
    let url = format!("http://127.0.0.1:{port}/cities_12.a.fgb");
    let mut reader = Reader::open_url(&url)?;
    assert_eq!(reader.stats([-180.0, -85.0, 180.0, 85.0], 2)?.total, 12);
    assert_eq!(reader.stats([-10.0, 40.0, 10.0, 60.0], 2)?.total, 8);
    assert_eq!(reader.into_inner().requests(), 4);
    Ok(())
}
```

Run `/home/sheece/.cargo/bin/cargo test -p fgba --features http --test http repeated_stats_reuses_the_cached_band_request`. Expected: FAIL with 5 actual requests versus 4 expected.

- [ ] **Step 2: Implement the bounded cache and resolution regression test**

Apply Reference Implementation E. Update HTTP comments to say only repeated same-resolution queries are warm. Add the 241-point `[2,4]` regression test to `vectors.rs`: query res 2, then res 4 (assert more cells), then res 2 again and compare with the first result. Run all fgba feature tests.

- [ ] **Step 3: Publish native SQL documentation**

Create `docs/guide/analytics.html` using the existing static shell and guide navigation. Include only native commands:

```sh
cargo install fgba-cli
fgba query cities.a.fgb "SELECT COUNT(*) FROM features"
fgba query cities.a.fgb "SELECT \"name\", COUNT(*) FROM features GROUP BY \"name\""
fgba query cities.a.fgb --bbox -10,40,10,60 "SELECT COUNT(*) AS cells, SUM(count) AS estimated FROM a5_viewport_approx"
fgba build cities-metrics.fgb --analytics --chunk-rows 4096 -o cities-metrics.a.fgb
fgba query cities-metrics.a.fgb "SELECT name, row_count, non_null_count, null_count, min_f64, max_f64 FROM attribute_stats WHERE name='height'"
```

Use `cities-metrics.fgb` for a fixture with a numeric `height` column; the name-only 12-city fixture has an opaque `name` column and therefore no numeric bounds/sum.

Document:

- `features`, `a5_band_<res>`, `a5_viewport_approx`, and `attribute_stats` semantics and example SQL.
- Arrow chunk projection/group/join, metadata-only `COUNT(*)`, typed properties/WKB/bbox, and no `ST_*`/CRS engine.
- Exact body vs midpoint-bin vs covering results, NaN/null behavior, and `sum_text` as metadata.
- Kind-2 global/chunk/Arrow layout and optional `--analytics`; only actual property columns produce a section. No footer/version change.
- Why no second spatial index is created: the existing R-tree plus zone maps and body offsets covers candidate selection. Valid bbox is Exact for bbox intersection; zone maps always retain residual predicates.
- When zone maps fail to help (random/unclustered values), when to tune chunk size/range gaps, and why high-cardinality grouping is not memory-free.
- Current India only has `OGC_FID`/`geom`; source attributes require the original dataset. Do not claim they were recovered.

Link `analytics.html` from all five existing guide sidebars and the homepage. Update `docs/spec.html` with the unified layout from `SPEC.md`.

- [ ] **Step 4: Correct README, conversion, and India notes**

- Root/crate/CLI READMEs: add `fgba query`, `fgba-sql`, and `--analytics`; remove any implication that DuckDB/GeoParquet is required. Replace the old provisional attribute-band note with kind-2 analytical columns/statistics.
- `docs/guide/converting.html`: replace the geometry-only warning with deterministic property preservation rules, and document the analytics flag.
- `docs/guide/rust.html`: replace the provisional attribute-band note with kind-2 global/chunk/Arrow statistics and the body fallback rule.
- `CHANGELOG.md`: add native SQL, property preservation, kind-2 analytics, and warm-band caching; remove the unsupported GeoPackage/GeoParquet conversion claim and the obsolete provisional kind-1 attribute-layout entry.
- `crates/fgba-sql/README.md`: document table names, streaming output, read-only SQL options, local-only scope, and memory caveats.
- `example/README.md`: disclose that the existing object has only `OGC_FID` and `geom`. In the existing optional rebuild command, change `SELECT geom` to `SELECT *` and add `SET geometry_always_xy = true;` so a future rebuild retains properties with explicit axis order. Do not run the rebuild or introduce DuckDB into the native SQL path.

- [ ] **Step 5: Add reproducible benchmark checks and MSRV CI**

Extend `crates/fgba-sql/tests/pruning.rs` with a repeatable benchmark report helper (not timing assertions) that prints before/after **deltas** of `features_read`, `body_bytes_read`, `analytics_bytes_read`, and `ranges_opened` for full/R-tree/stats/joint scans, plus band/stats trailer sizes from section metadata. Counters are cumulative per table; never present cumulative totals as per-query values. Keep correctness assertions authoritative; record timings separately in the final report without promising a percentage.

Add this CI job to `.github/workflows/ci.yml` while retaining stable checks:

```yaml
  msrv:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: 1.89.0
      - run: cargo test --workspace --locked
```

Run all final checks:

```sh
/home/sheece/.cargo/bin/cargo fmt --all --check
/home/sheece/.cargo/bin/cargo clippy --workspace --all-targets -- -D warnings
/home/sheece/.cargo/bin/cargo test --workspace --locked
/home/sheece/.cargo/bin/cargo build --target wasm32-unknown-unknown -p fgba
```

Expected: all PASS. `Cargo.lock` contains DataFusion 48.0.1 and no DuckDB/Parquet package introduced by this work.

- [ ] **Step 6: Commit**

```sh
git add README.md CHANGELOG.md crates/fgba/README.md crates/fgba-cli/README.md crates/fgba-sql/README.md crates/fgba/src/read.rs crates/fgba/src/http.rs crates/fgba/tests/http.rs crates/fgba/tests/vectors.rs docs example/README.md .github/workflows/ci.yml docs/design/sql-analytics.md docs/plans/2026-10-25-sql-analytics.md
git commit -m "docs: guide native FGBA analytics and indexing"
```

---

## Final Review Checklist

- [ ] `features` performs exact native SQL with typed properties, streaming batches, and reopenable sources.
- [ ] Metadata-only `COUNT(*)` and `attribute_stats` avoid unnecessary payload reads.
- [ ] Attribute projection/group/join reads only selected Arrow columns/chunks with validity; geometry output uses the body fallback.
- [ ] `a5_band_<res>` and `a5_viewport_approx` preserve integral IDs and exact-vs-estimated semantics.
- [ ] Conversion no longer drops attributes, and missing-DBF/deterministic behavior still passes.
- [ ] Kind 2 is optional, bounded, validated, and connected to both `attribute_stats` and actual Arrow/range execution.
- [ ] Every R-tree leaf/body offset is verified during writing; terminal offset equals `body_len`.
- [ ] Valid bbox pushdown is Exact only for bbox intersection; zone-map pruning is Inexact and returns full-scan-identical rows with residual filters.
- [ ] No custom physical plan, DuckDB, GeoParquet, directory/footer version change, or per-cell attribute cube was added.
- [ ] Two same-resolution HTTP stats calls cost one band request; resolution replacement is correct.
- [ ] All stable, wasm, and Rust 1.89.0 `--locked` checks pass, and unrelated working-tree changes remain untouched.
