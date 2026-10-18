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
`(A5 cell, count)` records. A reader can draw a counted, equal-area cell
overview at any zoom, or answer count analytics, reading only kilobytes; at
high zoom it reads full-resolution features from the body, exactly as a plain
FGB reader does today.

Design principles, in order:

1. **Valid FGB invariant** — an FGBA file is always a valid FGB file. Readers
   that only understand FGB read all features (the "still a valid LAZ"
   principle of [COPC](https://copc.io)).
2. **Footer discovery** — extensions are found via a fixed-size footer at
   end-of-file (the Parquet / PMTiles / FGBO pattern).
3. **Reuse, don't reinvent** — geometry and the body's spatial index are
   plain FGB; cells are [A5](https://a5geo.org) cells. The only new encodings
   are the band records and the directory.
4. **Deterministic build** — identical input and options produce byte-
   identical output (CDN caching, diff distribution, verification).
5. **Write-once** — no appends or partial updates; rebuild instead.

Non-goals: editing in place, attribute aggregation richer than
count/sum/min/max (reserved), tiling/overviews of geometry (that is
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
| 2–0x7FFF | reserved | — |
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
| band_flags | u16 | bit 0: records carry attribute aggregates |
| total_features | u64 | features binned into this band |
| num_cells | u64 | records that follow |
| records | record[num_cells] | ascending `cell_id` |

Record, `band_flags & 0 == 0` (12 bytes): `u64 cell_id`, `u32 count`.

Record, `band_flags & 1 == 1` (36 bytes): `u64 cell_id`, `u32 count`,
then per aggregated attribute (one attribute in v1): `f64 sum`,
`f64 min`, `f64 max`. *(Draft note: the attribute-aggregate layout is
provisional and not yet frozen; see §11.)*

Validation (readers MUST):

- `grid == 1`; `res ≤ 30`;
- the section size equals `header + num_cells × record_len` exactly;
- `cell_id`s are strictly ascending and valid A5 cells at `res`
  (`a5::is_valid_cell` semantics);
- every `count > 0`.

A band is a lossless partition of the dataset: the sum of `count` over all
records equals `total_features`. Features are binned by the midpoint of their
bounding box (§8), so each feature is counted in exactly one cell per band.

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
6. Compute the directory CRC over the exact directory bytes.

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
3. Fetch each needed band once, whole, when its size is below the reader's
   band cap (RECOMMENDED default 8 MiB — measured 1.4 MiB per million
   features at the deepest band); above the cap, binary-search the remote
   records. Cache bands per session.

Measured (loopback HTTP, 1M-feature file): 3 requests to open, 1 request per
new band, **0 requests per viewport once the band is cached**.

Cross-origin browser reads need CORS: the `Range` header triggers a preflight;
hosts SHOULD cache `OPTIONS` responses (see flatgeobuf.org's CDN notes).

### 9.3 Non-blocking environments (wasm)

Readers without blocking I/O SHOULD use a two-phase loop: compute the byte
ranges needed (footer, directory, band slice), fetch them asynchronously,
then decode. Chunk caching makes warm views free.

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

Draft v1 provisional (will change before v1 freeze): the attribute-aggregate
record layout (§6), and the band-cap guidance (§9.2).

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
