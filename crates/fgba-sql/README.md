# fgba-sql

Native, read-only DataFusion SQL over **local** FlatGeobuf (`.fgb`) and FGBA
files. The `fgba query` CLI command uses this crate; no external database or
intermediate analytical file is required. Remote URLs are outside its scope.

```rust
let options = fgba_sql::QueryOptions::default();
fgba_sql::query_csv(
    std::path::Path::new("cities.a.fgb"),
    "SELECT name, COUNT(*) AS n FROM features GROUP BY name",
    options,
    std::io::stdout(),
)?;
```

## Tables

- `features`: exact body rows with typed properties, nullable `geom_wkb`, and
  nullable numeric `bbox_xmin`, `bbox_ymin`, `bbox_xmax`, `bbox_ymax`. Bbox
  predicates mean bbox intersection, not exact geometry intersection. Planar
  `st_length`, `st_area`, and `st_npoints(Binary)` UDFs operate on supported WKB
  without CRS transformation; stored `geom_length`, `geom_area`, and
  `geom_npoints` columns expose kind-3 metrics when available. There is no
  CRS-aware geometry engine.
- `a5_band_<res>`: integral `cell_id`/`count` records from each available A5
  band. Counts are exact for bbox-midpoint-attributed cells, not regional
  geometry-intersection counts.
- `a5_viewport_approx`: the cells selected by `Reader::query` when
  `QueryOptions::bbox` is set; `res` selects a band, defaulting to the deepest.
  Its covering sum is an estimate, never substituted for exact body SQL.
- `attribute_stats`: exact global kind-2 metadata when that optional section
  exists, including typed bounds, counts, `sum_text`, and validity flags.
  `sum_text` preserves metadata precision and is not a replacement for native
  SQL aggregation. It does not read Arrow payloads.

Attribute-only projection, filtering, grouping, and joins use independently
readable kind-2 Arrow IPC chunks and validity where possible. Produce that
optional section with `fgba build input.fgb --analytics --chunk-rows 4096 -o
input.a.fgb` (or `fgba convert --analytics`). Geometry output and files without
usable analytics stream the exact FlatGeobuf body. Numeric zone maps only
eliminate candidates; residual predicates always remain.

## Execution and limits

Every execution owns its reopenable source. `query_csv` streams result batches
as CSV with one header and does not collect the query result. Source batches
are bounded by `QueryOptions::batch_rows` and `batch_bytes`; these are source
batching controls, not a whole-query memory limit. DataFusion operators such as
high-cardinality `GROUP BY`, distinct, joins, and sorts still need their normal
working memory, and one oversized value can exceed a batch's byte target.

SQL is constrained with DataFusion `SQLOptions`: DDL, DML, and session
statements are disabled; read-only queries and `EXPLAIN` remain available.

For the standalone browser artifact, `EagerSession` registers bounded
one-partition in-memory RecordBatches and executes the same DataFusion engine
and geometry UDFs through direct async streams. `fgba-wasm` adds a conservative
source allowlist, physical-plan gate, hard source/result caps, and a worker
watchdog. That path is bounded eager materialization, not the native streaming
path and not an unbounded lazy/remote SQL service. See
[`docs/tilify-integration.md`](https://github.com/afterrealism/fgba/blob/main/docs/tilify-integration.md).

Only the narrowly matched `Projection → Aggregate(count(1)) → TableScan`
logical plan, optionally with a literal limit or canonical bbox filter, uses
metadata counts. An unfiltered count comes from the body header; a valid
bbox-only count uses the FlatGeobuf packed R-tree. Empty provider projections
alone never trigger this shortcut, and every other plan follows the exact
execution path.
