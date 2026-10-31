# FGBA — FlatGeobuf Aggregates

**Binary Specification, Draft v1 (2026-10-18)**

Status: Draft. Directory version 1. Comment/change process: see §11.

This document provides adequate detail to create a new implementation of FGBA
without the aid of the reference implementation.

---

## 1. Purpose

[FlatGeobuf](https://flatgeobuf.org) (FGB) is a cloud-optimized vector format:
its packed Hilbert R-tree answers bounding-box queries over HTTP range
requests. It holds a single resolution, so low-zoom rendering of large
datasets reads nearly the whole file, and there is no way to answer "how many
features are in this region" without reading the features.

FGBA is an **extension of a FlatGeobuf file**: a trailer appended after the
FGB data section holding **aggregate bands** — per-resolution arrays of
`(A5 cell, count)` records — and an optional **BODY_ANALYTICS** section with
exact property statistics and independently readable Arrow IPC column chunks.
A reader can draw a counted, equal-area cell overview at any zoom, answer count
analytics, or scan analytical properties without decoding row-oriented feature
geometry; at high zoom it reads full-resolution features from the body, exactly
as a plain FGB reader does today.

Design principles, in order:

1. **Valid FGB invariant** — an FGBA file is always a valid FGB file. Readers
   that only understand FGB read all features (the "still a valid LAZ"
   principle of [COPC](https://copc.io)).
2. **Footer discovery** — extensions are found via a fixed-size footer at
   end-of-file (the Parquet / PMTiles / FGBO pattern).
3. **Reuse, don't reinvent** — geometry and the body's spatial index are
   plain FGB; cells are [A5](https://a5geo.org) cells. Analytical values use
   standard Arrow IPC files, not a parallel column encoding. The only new
   fixed encodings are the band/directory/analytics records.
4. **Deterministic build** — identical input and options produce byte-
   identical output (CDN caching, diff distribution, verification).
5. **Write-once** — no appends or partial updates; rebuild instead.

Non-goals: editing in place, per-cell attribute aggregation (deferred), a
DuckDB/GeoParquet migration, tiling/overviews of geometry (that is
[FGBO](https://github.com/reearth/flatgeobuf-overviews)'s role; the directory
is designed to compose with it, see §10.4).

## 2. Terminology

- **body** — the leading, fully valid FlatGeobuf file: magic bytes, Header,
  packed Hilbert R-tree Index, Data.
- **trailer** — everything after the body's Data section: sentinel, sections,
  directory, footer.
- **section** — one typed byte range in the trailer, listed in the directory.
- **aggregate band** (or **band**) — a section holding the dataset's feature
  counts per A5 cell at one A5 resolution.
- **cell** — an A5 cell identifier (64-bit unsigned integer). A5 cells at one
  resolution are equal-area pentagons; cell IDs follow a space-filling curve,
  so spatially adjacent cells are numerically adjacent.
- **covering** — the set of cells at one resolution intersecting a query
  region.
- **ladder** — the mapping from map viewport size to the band resolution a
  renderer draws.
- **base resolution** — the finest band resolution, chosen at build time.

## 3. Normative references

- *FlatGeobuf specification* — <https://flatgeobuf.org> (body layout, Header
  fields including `features_count` and `index_node_size`).
- *A5 — a pentagonal discrete global grid* — <https://a5geo.org/docs>
  (cell IDs, resolutions 0–30, equal-area property, space-filling curve
  order). A5 cell IDs are frozen from A5 v1.0.
- *RFC 9110 §14, "Range Requests"* — HTTP delivery.

The key words MUST, SHOULD, MAY are to be interpreted as in RFC 2119.

## 4. File layout

All integers are little-endian. Offsets are absolute file offsets.

```
┌────────────────────────────────────────────────────┐
│ FlatGeobuf body (magic / Header / Index / Data)    │ ← valid FGB
├────────────────────────────────────────────────────┤
│ sentinel (4 bytes = FF FF FF FF)                   │ ─┐
│ section 0 .. section n                             │  │ FGBA trailer
│ directory (variable)                               │  │ (FGB readers
│ footer (fixed 32 bytes, ends the file)             │ ─┘ never reach it)
└────────────────────────────────────────────────────┘
```

Constraints:

- The body's Header MUST have `features_count > 0` and a spatial index
  (`index_node_size > 0`). (Both optional in plain FGB; required here so
  sequential readers stop at the sentinel and index readers never touch the
  trailer.)
- The file is write-once. Implementations MUST NOT append to or partially
  rewrite an FGBA file; rebuild it instead.

### 4.1 Sentinel

The first 4 bytes of the trailer are the fixed value `FF FF FF FF`. A
non-conforming sequential reader that ignores `features_count` interprets
this as a feature length prefix of ~4 GiB and fails immediately, instead of
silently misparsing trailer bytes as features. (Verified behavior of major
implementations: FGBO COMPAT.md — Rust and GDAL stop at `features_count`;
the JavaScript full-scan path yields all features then errors on the
sentinel.)

## 5. Directory

The directory is a table of kind-tagged section entries. Readers MUST skip
sections whose kind they do not recognize, and MUST NOT fail on them.

| Field | Type | Notes |
|---|---|---|
| version | u8 | `1` |
| n_sections | u8 | number of entries |
| **entry** × n_sections | | |
| — kind | u16 | section type, registry below |
| — flags | u16 | reserved; writers MUST emit `0`, readers MUST ignore the value |
| — offset | u64 | absolute byte offset of the section |
| — size | u64 | byte size of the section |
| — meta_len | u16 | byte length of `meta` |
| — meta | u8[meta_len] | per-kind metadata |

Kind registry:

| kind | section | meta |
|---|---|---|
| 1 | aggregate band (§6) | `[u8 res]` — the band's A5 resolution; a mismatch with the band section's own `res` field makes the trailer invalid (§10.2) |
| 2 | BODY_ANALYTICS (§6A), optional, at most one | empty; directory `flags=0`, `meta_len=0` |
| 3 | GEOMETRY_METRICS (§6B), optional, at most one | empty; directory `flags=0`, `meta_len=0` |
| 4–0x7FFF | reserved | — |
| 0x8000–0xFFFF | vendor/private extensions | — |

Entries SHOULD be ordered by ascending offset. A directory whose entries
overlap, lie outside the file, or fail CRC (§7) makes the trailer invalid:
readers MUST treat the file as plain FGB (§10.2), never error on the body.

Rationale for the kind-tagged form: a file can only have one EOF footer, so a
single extensible directory is what allows future section kinds (simplified
overviews, clipped segments, per-vertex importance — cf. FGBO) to coexist
with aggregate bands in one file.

## 6. Aggregate band sections

A band section holds every **occupied** A5 cell at its resolution (cells with
zero features are absent), sorted by ascending cell ID:

| Field | Type | Notes |
|---|---|---|
| grid | u8 | `1` = A5. Other values reserved. |
| res | u8 | A5 resolution of the band, 0–30 |
| band_flags | u16 | MUST be `0`; every bit is reserved for future use |
| total_features | u64 | features binned into this band |
| num_cells | u64 | records that follow |
| records | record[num_cells] | ascending `cell_id` |

The only v1 record, used when `band_flags == 0` (12 bytes), is `u64 cell_id`,
`u32 count`. Per-cell attribute aggregation is deferred; it MUST NOT be
enabled with kind-1 flag bits. A future demonstrated requirement needs a new
section kind with grouped internal streams.

Validation (readers MUST):

- `grid == 1`; `res ≤ 30`; `band_flags == 0`;
- the section size equals `header + num_cells × record_len` exactly;
- `cell_id`s are strictly ascending and valid A5 cells at `res`
  (`a5::is_valid_cell` semantics);
- every `count > 0`.

A band is a lossless partition of the dataset: the sum of `count` over all
records equals `total_features`. Features are binned by the midpoint of their
bounding box (§8), so each feature is counted in exactly one cell per band.

## 6A. BODY_ANALYTICS sections (optional)

A writer MAY emit one kind-2 section before the kind-1 bands. It contains all
FlatGeobuf body property columns in ascending `column_index` order. A body with
no properties MUST NOT have an empty kind-2 section. Ordinary builds and old
readers are unaffected: kind 2 is additive, and neither the footer nor the
directory version changes.

When GEOMETRY_METRICS (§6B) is present, it MUST use the same `R`, `K`, and `C`
as this section. The reference writer places kind 2 first, kind 3 second, and
the kind-1 bands last. Readers locate every optional section by its directory
kind and MUST NOT depend on directory or physical section order.

Let `A = column_count`, `R = features_count`, `K = chunk_rows > 0`, and
`C = ceil(R/K)`. The reference CLI defaults to `--chunk-rows auto` and chooses
`K` from `R`; an explicit positive `--chunk-rows` overrides that choice. `K` is
stored in the section, not fixed by the format. All fixed records are
little-endian. Except for the two fields explicitly marked section-relative,
offsets are absolute file offsets.

### 6A.1 Header, descriptors, and body offsets

```text
SectionHeader (32 bytes)
  0   u8  encoding_version = 1
  1   u8  flags = 0
  2   u16 reserved = 0
  4   u32 chunk_rows                 # > 0
  8   u32 column_count               # > 0 when the section exists
  12  u32 reserved = 0
  16  u64 row_count                  # MUST equal FGB features_count
  24  u64 chunk_count                # MUST equal ceil(row_count/chunk_rows)

ColumnDesc[A] (40 bytes each)
  0   u16 column_index               # FGB header column index
  2   u8  fgb_type                   # exact FGB ColumnType
  3   u8  value_class                # classes below
  4   u32 flags                      # bit 0 HAS_ARROW_IPC = 1; others zero
  8   u64 stream_offset              # relative to section start
  16  u64 stream_size
  24  u64 arrow_offsets_offset       # relative to section start
  32  u64 arrow_bytes                # sum of Arrow chunk byte lengths

chunk_offsets[C+1] (u64 each, immediately after descriptors)
  Absolute offsets of feature length prefixes, strictly increasing.
  offset[0] = first feature length-prefix in FGB Data.
  offset[C] = body_len (the trailer sentinel offset).
  Chunk i owns rows [i*K, min((i+1)*K,R))
  and body bytes [offset[i], offset[i+1]).
```

The value classes are `0 = opaque` (Bool/String/Json/DateTime/Binary),
`1 = signed i64`, `2 = unsigned u64`, and `3 = f64`. Every property type has
Arrow values and exact null counts. Numeric classes also have bounds, sums, and
zone maps. Opaque bounds/sums/flags are zero and mean **no statistic**, not a
zero-valued result.

Arrow arrays use the original values and validity, with these physical types:
Byte/UByte → Int8/UInt8, Bool → Boolean, Short/UShort → Int16/UInt16,
Int/UInt → Int32/UInt32, Long/ULong → Int64/UInt64, Float/Double →
Float32/Float64, String/Json/DateTime → Utf8, and Binary → Binary.

### 6A.2 Per-column streams

Each descriptor locates one contiguous stream:

```text
GlobalStat (56 bytes)
  0   u64 non_null_count             # includes NaN and infinities
  8   u64 nan_count                  # zero for integer and opaque classes
  16  u64 min_bits                   # i64/u64 value or f64 bits
  24  u64 max_bits                   # zero bounds when no statistic exists
  32  u8[16] sum                     # i128/u128 LE; f64 in first 8 bytes
  48  u32 flags                      # bit 0 SUM_VALID; bit 1 HAS_INFINITY
  52  u32 reserved = 0

ChunkStat[C] (24 bytes each)
  0   u64 min_bits
  8   u64 max_bits
  16  u32 non_null_count
  20  u32 nan_count

arrow_offsets[C+1] (u64 each)
  Absolute offsets of independent Arrow IPC chunks.

ArrowIpcChunk[C] (variable length)
  One complete Arrow IPC file per chunk: a single-field schema and exactly
  one RecordBatch containing the declared values and validity.
```

The following identities are mandatory:

```text
stream_size = 56 + 24*C + 8*(C+1) + arrow_bytes
arrow_offsets_offset = stream_offset + 56 + 24*C
arrow_offsets[C] = section_start + stream_offset + stream_size
```

`arrow_offsets[C]` is the absolute end of the column stream. In particular,
`stream_offset` and `arrow_offsets_offset` are section-relative, but every
Arrow offset is absolute. Arrow chunks are non-empty, strictly increasing,
individually no larger than 64 MiB, and independently range-readable. A reader
need not read another column or any Arrow payload to decode the fixed metadata.
The RECOMMENDED combined metadata cap is 16 MiB; it is separate from the
per-Arrow-chunk cap.

### 6A.3 Statistical semantics

- `null_count = row_count - non_null_count`. NaN is not null. Bounds exist iff
  `non_null_count > nan_count`. All-null/all-NaN chunks still contain their
  rows and MUST NOT be treated as empty for count or null predicates.
- Integer bounds sign-/zero-extend to i64/u64 and MUST NOT be converted through
  f64. Integer sums use checked i128/u128 arithmetic; overflow MUST NOT wrap.
- f32 promotes losslessly to f64 for statistics. Bounds include infinities but
  exclude NaN. Canonicalize a zero minimum to `-0` and a zero maximum to `+0`.
- Float global SUM is sequential binary64 accumulation in body order, skipping
  NULL without reassociation. Any NaN, infinity, or non-finite intermediate or
  final result clears `SUM_VALID` and zeroes all sum bytes. `HAS_INFINITY`
  records infinity independently of sum validity.
- Empty/all-null numeric sum storage is zero with count zero; consumers decide
  the appropriate empty aggregate result. Opaque globals have zero NaN count,
  bounds, sum, and flags. Consumers expose null, rather than zero, for opaque
  bounds/sums.
- Reserved fields are zero. Descriptors, records, chunks, and Arrow batches are
  deterministic. NaN payloads are not stored in statistics, and parallel or
  reassociated float reduction is forbidden.

### 6A.4 Writing and validation

An analytics writer computes `data_start` and `leaf_start` with checked
arithmetic, streams leaf `NodeItem`s independently, and records exact body
feature offsets. For every body ordinal it MUST verify
`data_start + leaf.offset == current_feature_offset`. It updates statistics
for every body row, including rows whose empty/invalid geometry is skipped by
kind-1 band construction. `row_count` MUST equal the body `features_count`, and
a requested build MUST abort on any mismatch before emitting output.

Before allocation, readers bound every count/offset by the directory section
size and applicable metadata/chunk cap. Readers MUST validate supported unique
ordered column indices, exact FGB type/class matches, reserved bytes, exact
stream formulas and nonoverlapping ranges, chunk/global count consistency,
strict body offsets ending at `body_len`, strict Arrow ranges and terminal
offsets, and each chunk length in `1..=64 MiB`. Arrow consumers additionally
require one field, one RecordBatch, the declared type, and the chunk row count.

Missing, unsupported, truncated, or malformed analytics disables only this
optional acceleration and falls back to the body. It MUST NOT make the body or
kind-1 bands unreadable (§10.2).

## 6B. GEOMETRY_METRICS sections (optional)

A writer MAY emit one kind-3 section only when a kind-2 section exists. It
contains exactly seven derived geometry columns on the kind-2 chunk grid. A
property-free body, which cannot have kind 2 (§6A), therefore MUST NOT have
kind 3. Neither the footer magic nor the directory version changes.

Let `R`, `K`, and `C` be the kind-2 `row_count`, `chunk_rows`, and `chunk_count`.
The kind-3 values MUST equal those fields, and `R` MUST equal FGB
`features_count`. Chunk `i` owns the same feature ordinals
`[i*K, min((i+1)*K,R))` in both sections. All fixed records are little-endian.
Except for `stream_offset`, offsets are absolute file offsets.

### 6B.1 Header and metric descriptors

```text
SectionHeader (32 bytes) — field positions and rules from §6A.1
  encoding_version=1, flags=0, reserved fields=0
  chunk_rows=K, column_count=7
  row_count=R, chunk_count=C

MetricDesc[7] (24 bytes each)
  0   u16 metric_id                   # unique, ascending 0..6
  2   u8  arrow_type                  # 0=Float64, 1=UInt32
  3   u8  reserved = 0
  4   u32 flags                      # bit 0 HAS_ARROW_IPC = 1; others zero
  8   u64 stream_offset              # relative to section start
  16  u64 stream_size                # complete metric stream size
```

The descriptor order and physical types are:

| metric_id | name | Arrow type | Meaning |
|---|---|---|---|
| 0 | `length` | Float64 | planar Euclidean length |
| 1 | `area` | Float64 | planar unsigned area |
| 2 | `npoints` | UInt32 | vertex count |
| 3 | `bbox_xmin` | Float64 | minimum x |
| 4 | `bbox_ymin` | Float64 | minimum y |
| 5 | `bbox_xmax` | Float64 | maximum x |
| 6 | `bbox_ymax` | Float64 | maximum y |

Unlike §6A, kind 3 has no body-offset array: chunk ownership is defined
entirely by equality with kind 2. The first metric stream starts immediately
after the seven descriptors.

### 6B.2 Per-metric streams

Each descriptor locates one contiguous stream with the exact §6A.2 records:

```text
GlobalStat (56 bytes)
ChunkStat[C] (24 bytes each)
arrow_offsets[C+1] (u64 each)
ArrowIpcChunk[C] (variable length)
```

`GlobalStat`, `ChunkStat`, flags, null/NaN handling, bounds, sums, and reserved
fields have the §6A.2 and §6A.3 semantics. Float sums are sequential binary64
accumulation in body order, skipping null without reassociation. `npoints`
uses unsigned integer statistics and a checked `u128` global sum. Every Arrow
IPC chunk is a complete independent file with one nullable field named as in
the table and exactly one RecordBatch of the declared physical type.

For each metric, define
`M = 56 + 24*C + 8*(C+1)`. These identities are mandatory:

```text
arrow_offsets_offset = stream_offset + 56 + 24*C   # derived, not stored
stream_size = M + sum(Arrow chunk byte lengths)
arrow_offsets[0] = section_start + stream_offset + M
arrow_offsets[C] = section_start + stream_offset + stream_size
```

Metric streams are contiguous and nonoverlapping in descriptor order; the end
of the seventh stream equals the section size. Arrow offsets are absolute,
strictly increasing, and each chunk length is in `1..=64 MiB`. Fixed metadata
is bounded by the directory section size and the same recommended 16 MiB cap
as §6A. A reader need not read Arrow payloads to decode fixed metadata.

### 6B.3 Geometry and statistical semantics

Metrics use planar (Euclidean, degree-unit) arithmetic over WGS84 longitude
and latitude. Length is the sum of segment lengths in LineStrings and in every
Polygon exterior and interior ring; Points contribute zero. Area is unsigned
shoelace area: the absolute exterior-ring area minus each absolute interior
ring area, with per-Polygon and multi-part accumulation matching
`geo::Area::unsigned_area`. Ring closure coordinates count as vertices.
`npoints` counts every XY vertex in all parts, including interior rings and
GeometryCollections, and MUST be checked against `u32::MAX`. The four bbox
metrics are the geometry's exact finite coordinate envelope. A requested
build MUST reject non-finite input coordinates rather than invent an envelope.

An empty or absent geometry has logical values `length=NaN`, `area=NaN`,
`npoints=0`, and `bbox=[NaN,NaN,NaN,NaN]`, with **all seven Arrow validity bits
set to null**. Null is the stored representation; NaN/zero placeholders are
not non-null values. Such rows contribute to `row_count`, but not to
`non_null_count`, bounds, or sums. A non-empty Point has non-null zero length
and area. Empty geometries are still counted by kind 2 and do not move the
shared chunk grid.

### 6B.4 Writing and validation

The writer MUST feed property and geometry values in the same body-order pass,
flush all columns at the same chunk boundaries, and use identical in-memory
and streaming encodings. It MUST verify kind-2 body counts/offsets before
emitting kind 3. A kind-3-only file is invalid.

Before allocation, readers MUST bound counts and offsets by the directory
section size and metadata cap. Readers MUST validate the header, exactly seven
unique ordered metric IDs and physical types, reserved bytes, `R`/`K`/`C`
against kind 2 and FGB `features_count`, exact stream formulas and contiguous
nonoverlapping streams, chunk/global count consistency, strict Arrow ranges
and terminal offsets, and chunk lengths in `1..=64 MiB`. Arrow consumers
additionally require the declared nullable field, one RecordBatch, and the
shared chunk row count. Metadata lookup is by section kind and is independent
of directory order.

Malformed or truncated kind 3 disables only geometry-metric acceleration. It
MUST NOT make the body, kind-1 bands, or valid kind-2 analytics unreadable
(§10.2).

## 7. Footer

The last 32 bytes of the file:

| Bytes | Content |
|---|---|
| 0–8 | magic `FGBAGG01` |
| 8–16 | directory offset (u64) |
| 16–24 | directory size (u64) |
| 24–28 | CRC-32 (IEEE 802.3, poly `0xEDB88320`, as in PNG/zip) of the directory bytes |
| 28–32 | reserved; writers MUST emit `0`, readers MUST ignore the value |

A reader range-reads the last 32 bytes once: magic match → FGBA, fetch the
directory (and verify CRC); no match → plain FGB. This is the only mandatory
overhead versus plain FGB, and it is one small read at EOF.

## 8. Write protocol

Producers MUST:

1. Write the body as a valid FlatGeobuf with `features_count` and a spatial
   index, features Hilbert-sorted (any conforming FGB writer does this).
2. Bin every feature by the **midpoint of its bounding box**:
   `cell = a5.lonlat_to_cell(midpoint, res)` per band resolution. (Points:
   the point itself. The midpoint convention keeps binning O(1) per feature;
   large features are attributed to the cell holding their center, matching
   A5's supercell semantics.)
3. Choose the **base resolution**: the smallest `res ≥ 2` with
   `a5.get_num_cells(res) ≥ 4 × feature_count`, capped at 16.
4. Write bands at `{base, base−2, base−4, … }` while the resolution stays
   `≥ 2`, at most 6 bands, ascending by resolution. (For small datasets the
   ladder may not reach res 2 — e.g. `base = 3` yields the single band
   `{3}`. Renderers drawing a view coarser than the coarsest available band
   simply use that coarsest band; the cells render small but correct.)
5. Sort each band's records by ascending cell ID. Within a record set, bytes
   MUST be deterministic: identical input and options MUST produce
   byte-identical files.
6. When property analytics are requested and properties exist, write kind 2
   before the bands. When geometry metrics are also requested, write kind 3
   immediately after kind 2 and before the bands, using the same chunk grid.
   Do not emit either optional section for a property-free body.
7. Compute the directory CRC over the exact directory bytes.

*Informative:* measured build cost ≈ 1 s per million point features for five
bands on a 2026 laptop; total trailer size ≈ 2.4 MiB per million features
(~2% of an uncompressed FGB body).

## 9. Read protocol

### 9.1 Local files

Open → seek to EOF−32 → footer → directory (verify CRC) → per query: pick the
band (§9.4), read its section (or the covering's record spans — records are
curve-sorted, so a viewport covering is a few contiguous runs; measured 1–7
runs), binary-search records by cell ID.

### 9.2 HTTP range requests

1. Probe `Range: bytes=0-0` once: the server MUST answer `206 Partial
   Content` (its `Content-Range` yields the file length). A `200` answer
   means the host does not do range requests; fail with a message naming
   range support (S3, R2, nginx, and most static hosts qualify).
2. Read the footer and directory once per session; cache them.
3. Fetch each needed band when required, whole when its size is below the
   reader's band cap (RECOMMENDED default 8 MiB — measured 1.4 MiB per million
   features at the deepest band); above the cap, binary-search the remote
   records. Cache policy belongs to the reader, not the format.

The reference native `Reader::query`/`stats` retains one most-recently-used
band: repeated same-resolution viewports cost no additional band request and
changing resolution evicts the previous entry. `read_band(res)` always performs
a fresh decode. Browser clients may instead use a separately bounded sparse or
multi-band cache; no multi-band or cross-session cache is implied by FGBA.

Measured (loopback HTTP, 1M-feature file, reference native reader): 3 requests
to open, 1 request per newly selected band, **0 requests per viewport while the
same band remains cached**.

Cross-origin browser reads need CORS: the `Range` header triggers a preflight;
hosts SHOULD cache `OPTIONS` responses (see flatgeobuf.org's CDN notes).

### 9.3 Non-blocking environments (wasm)

Readers without blocking I/O SHOULD use a two-phase loop: compute the byte
ranges needed (footer, directory, band slice), fetch them asynchronously,
then decode. Chunk caching makes warm views free.

The reference `fgba-wasm` crate implements this pattern with a sparse range
cache, strict 200/206 validation, A5/GeoJSON rendering, and bounded eager
`fgba-sql`/DataFusion execution over in-memory RecordBatches. It first probes
`bytes=0-0` to learn the size and then requests an explicit final-32-byte
footer span, so it does not depend on suffix-range Fetch behavior. Its 8 MiB
range, 64 MiB sparse-cache, and SQL source/result limits are implementation
limits, not changes to the binary format. See `docs/tilify-integration.md` for
the actual artifact and current support boundary.

`crates/fgba/examples/wasm/main.js` is an older manual pure-JavaScript parser
demo. It illustrates the read pattern but is not the standalone Rust/WASM
artifact and provides no SQL runtime.

### 9.4 The ladder and the switch (rendering guidance)

A renderer showing aggregates SHOULD:

0. Clamp viewport latitudes to the Web Mercator range (±85.0511°) before
   computing coverings: an A5 covering of a ring touching ±90° degenerates
   (the polar quintants are partially omitted). Whole-world queries use
   `[-180, -85.0511, 180, 85.0511]`.

1. Map the viewport width to a band resolution: the coarsest available band
   whose average cell edge (`a5.cell_edge_length_avg(res)`) covers at least
   ~48 px of the view. (The reference ladder spans res 2–15 so that files
   with a base resolution up to 16 are served at every zoom; a view finer
   than the deepest band always reads the body.)
2. Estimate real matches: sum the counts of the deepest band's cells
   covering the viewport (use *overlapping* containment — center-containment
   misses boundary cells at fine zooms).
3. If that sum exceeds the draw limit (order 50k features), draw the band:
   one pentagon (`a5.cell_to_boundary`) per occupied cell, styled by `count`.
   Otherwise read real features from the body with a normal FGB bbox query.
   Above the base resolution, always read the body.

Sparse datasets therefore never show cells; dense city cores stay aggregated
until deep zoom. The same covering-sum query answers count analytics without
touching feature bytes.

For viewports covering at least half the sphere, readers SHOULD filter the
band's own cells by conservative longitude/latitude bounding-box overlap
instead of building direct or complement spherical ring coverings, which are
ambiguous at that scale. This preserves *overlapping* containment as an
upper-bound estimate: a boundary cell may be retained even when its exact
polygon misses the viewport rectangle.

## 10. Compatibility

### 10.1 With FGB readers

An FGBA file is a valid FGB file. Index-driven reads (bbox / HTTP range)
structurally never reach the trailer. Sequential readers that respect
`features_count` stop at the sentinel. Measured matrix (FGBO COMPAT.md, same
construction): Rust flatgeobuf 6.0.1 ✅, GDAL/OGR 3.12 ✅, flatgeobuf JS 4.4.0
index paths ✅; the JS full-scan path yields all features, then fails fast on
the sentinel ⚠️ (an upstream `featuresCount` fix is the remedy).

### 10.2 Invalid trailers

A reader that finds a bad footer CRC, a malformed directory, or an invalid
band MUST fall back to reading the body as plain FGB and SHOULD surface a
warning. The body is never hostage to the trailer. An implementation MAY
surface the trailer failure as an error to *its* caller instead of silently
falling back, provided the body remains readable through the same handle.
Optional sections are independently readable: a malformed kind-3 section does
not invalidate the body, kind-1 bands, or kind 2, and a malformed kind-2
section does not invalidate the body or kind-1 bands.

### 10.3 Re-export caveat

Re-saving through other tools (e.g. `ogr2ogr`) drops the trailer, returning a
plain FGB — same known caveat as COPC. Distribute the file as built.

### 10.4 Composition with FGBO

FGBO (FlatGeobuf Overviews) appends its own trailer discovered by its own
footer. A file can only have one EOF footer, so an FGBO+FGBA composite is
defined as: FGBO sections listed as entries of the FGBA directory under
registered kinds (to be assigned), discovered by the FGBA footer. This is why
the FGBA directory is a kind-tagged table rather than a fixed struct.

## 11. Versioning and change policy

- The footer magic's last two bytes are the format version (`01`).
  Implementations reading a file with a higher version than they support MUST
  fall back to the body.
- The directory's `version` byte versions the directory encoding
  independently.
- New section kinds are additive and do not change the version.
- Reference-implementation crates follow SemVer; the format itself changes
  only by a new magic/version and a documented migration.
- A5 cell IDs are frozen by the A5 project from v1.0, so records written
  today refer to the same cells forever.

Draft v1 provisional (will change before v1 freeze): the band-cap guidance
(§9.2). Per-cell attribute aggregation is deferred to a demonstrated
requirement and a new section kind; kind 1 remains count-only.

## 12. Security considerations

Trailers are hostile input. Implementations MUST bound every count and length
before allocating (`dir_size ≤ 1 MiB` RECOMMENDED; `num_cells` bounded by the
section size), MUST NOT recurse on input data, and MUST handle truncated
reads as errors, not partial success. URL readers SHOULD restrict schemes
(https; http only for loopback/private networks) and follow redirects
cautiously.

## Appendix A. Test vector (normative)

`testdata/cities_12.a.fgb` — 12 point features (4 London, 4 Paris, 4 New York
City), one band at A5 resolution 2, no attribute aggregates. Body: 1672 bytes
written by flatgeobuf 6.0.1. Trailer, directory and footer (105 bytes, file
offset 1672 = `0x688`), complete:

```
00000688  ff ff ff ff 01 02 00 00 0c 00 00 00 00 00 00 00
00000698  02 00 00 00 00 00 00 00 00 00 00 00 00 00 80 26
000006a8  04 00 00 00 00 00 00 00 00 00 80 63 08 00 00 00
000006b8  01 01 01 00 00 00 8c 06 00 00 00 00 00 00 2c 00
000006c8  00 00 00 00 00 00 01 00 02 46 47 42 41 47 47 30
000006d8  31 b8 06 00 00 00 00 00 00 19 00 00 00 00 00 00
000006e8  00 cb 73 fc 41 00 00 00 00
```

Annotated walk:

| Offset | Bytes | Meaning |
|---|---|---|
| 0x688 | `ff ff ff ff` | sentinel |
| 0x68c | `01` | band: grid = A5 |
| 0x68d | `02` | band: res = 2 |
| 0x68e | `00 00` | band flags: count only |
| 0x690 | `0c … 00` | total_features = 12 |
| 0x698 | `02 … 00` | num_cells = 2 |
| 0x6a0 | `00 00 00 00 00 00 80 26`, `04 00 00 00` | cell `0x2680000000000000`, count 4 (the NYC points) |
| 0x6ac | `00 00 00 00 00 00 80 63`, `08 00 00 00` | cell `0x6380000000000000`, count 8 (London + Paris) |
| 0x6b8 | `01`, `01` | directory: version 1, one section |
| 0x6ba | `01 00`, `00 00` | kind 1 (aggregate band), flags 0 |
| 0x6be | `8c 06 … 00` | section offset = 1676 (`0x68c`) |
| 0x6c6 | `2c 00 … 00` | section size = 44 |
| 0x6ce | `01 00`, `02` | meta_len 1, meta = res 2 |
| 0x6d1 | `46 47 42 41 47 47 30 31` | footer magic `FGBAGG01` |
| 0x6d9 | `b8 06 … 00` | dir_offset = 1720 (`0x6b8`) |
| 0x6e1 | `19 … 00` | dir_size = 25 |
| 0x6e9 | `cb 73 fc 41` | directory CRC-32 = `0x41fc73cb` |
| 0x6ed | `00 00 00 00` | reserved |

Total file size 1777 bytes; the counts sum to 12 = `total_features`.

## Appendix B. Measured performance (informative)

Synthetic 1M-point dataset (117 MiB body), reference implementation, 2026
laptop, release build:

| Metric | Value |
|---|---|
| build: FGB encode + 5 bands | 2.5 s + 1.1 s |
| trailer size (5 bands) | 2.43 MiB (~2% of body) |
| aggregate query per viewport | 2–91 µs; 1–7 contiguous record runs |
| HTTP: open (probe + footer + directory) | 3 requests, 120 B |
| HTTP: first view per band | 1 request (band size, ≤ 1.4 MiB) |
| HTTP: subsequent viewports | 0 requests |
| plain-FGB body read, hemisphere view (135k features) | 51 ms local; tens of MB remotely |

---
*FGBA is developed at <https://github.com/afterrealism/fgba>. FlatGeobuf is by
Björn Harrtell; A5 is by Felix Palmer; the trailer conventions follow FGBO
(reearth). Apache Arrow's format documentation was the structural model for
this document.*
