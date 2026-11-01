//! FGBA reader over any `Read + Seek` byte source (SPEC §9.1).
//!
//! Opening a source is one small read at EOF (the footer) plus one directory
//! read, CRC-verified. Bands are read whole per section and binary-searched
//! per covering cell. The FlatGeobuf body is normally left to the caller via
//! [`Reader::body`]; a malformed optional section may require one exceptional
//! body scan to recover its exact terminal offset (SPEC §6B.4/§10.2).
//!
//! HTTP range reading and a wasm two-phase reader are separate layers
//! (SPEC §9.2–9.3) and intentionally not in this core module.

use crate::band::AggBand;
use crate::error::{Error, Result};
use crate::format::{
    crc32, Directory, Footer, FOOTER_SIZE, KIND_AGGREGATE_BAND, KIND_BODY_ANALYTICS,
    KIND_GEOMETRY_METRICS, MAX_DIR_SIZE, SENTINEL,
};
use crate::io::SharedReader;
#[cfg(feature = "analytics")]
use crate::{ArrowChunk, BodyAnalytics, GeometryMetrics};
use flatgeobuf::packed_r_tree::{NodeItem, PackedRTree};
use flatgeobuf::FgbReader;
use std::io::{BufReader, Read, Seek, SeekFrom};

/// Public metadata of one aggregate band section, from the directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BandMeta {
    /// A5 resolution of the band.
    pub res: u8,
    /// Absolute byte offset of the band section.
    pub offset: u64,
    /// Byte size of the band section.
    pub size: u64,
}

/// Aggregate statistics over one viewport: how many occupied cells and how
/// many features the covering matched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BandStats {
    /// Occupied cells matching the viewport covering.
    pub cells: usize,
    /// Sum of the matched cells' counts (estimated real feature matches).
    pub total: u64,
}

#[derive(Default)]
struct OptionalSectionErrors {
    analytics: Option<String>,
    geometry_metrics: Option<String>,
}

impl OptionalSectionErrors {
    fn is_optional(kind: u16) -> bool {
        matches!(kind, KIND_BODY_ANALYTICS | KIND_GEOMETRY_METRICS)
    }

    fn record(&mut self, kind: u16, error: String) {
        let slot = match kind {
            KIND_BODY_ANALYTICS => &mut self.analytics,
            KIND_GEOMETRY_METRICS => &mut self.geometry_metrics,
            _ => return,
        };
        if slot.is_none() {
            *slot = Some(error);
        }
    }

    fn any(&self) -> bool {
        self.analytics.is_some() || self.geometry_metrics.is_some()
    }
}

/// An FGBA reader over a `Read + Seek` source (a file, an in-memory cursor,
/// or any custom byte source such as an HTTP range reader).
pub struct Reader<R: Read + Seek> {
    inner: R,
    directory: Directory,
    /// Byte offset where the FlatGeobuf body ends and the trailer sentinel
    /// begins, derived from the first section when structurally sound or
    /// recovered by an exceptional body scan for malformed optional sections.
    body_len: u64,
    cached_band: Option<AggBand>,
    #[cfg(feature = "analytics")]
    analytics_structural_error: Option<String>,
    #[cfg(feature = "analytics")]
    analytics: Option<BodyAnalytics>,
    #[cfg(feature = "analytics")]
    analytics_loaded: bool,
    #[cfg(feature = "analytics")]
    geometry_metrics_structural_error: Option<String>,
    #[cfg(feature = "analytics")]
    geometry_metrics: Option<GeometryMetrics>,
    #[cfg(feature = "analytics")]
    geometry_metrics_loaded: bool,
}

impl<R: Read + Seek> Reader<R> {
    /// Open a source: read the 32-byte footer at EOF, then the directory,
    /// and verify the directory CRC-32.
    ///
    /// * No footer magic → [`Error::NotFgba`] (treat the source as plain
    ///   FlatGeobuf; SPEC §10.2).
    /// * Truncated, oversized, overlapping, out-of-file, or CRC-failing
    ///   trailer structures → [`Error::Format`]. Every length is bounded
    ///   before allocation (`dir_size <= `[`MAX_DIR_SIZE`], entries bounded
    ///   by the directory bytes, sections bounded by the file).
    pub fn open(mut inner: R) -> Result<Reader<R>> {
        let len = inner.seek(SeekFrom::End(0))?;
        if len < FOOTER_SIZE as u64 {
            return Err(Error::NotFgba);
        }
        inner.seek(SeekFrom::Start(len - FOOTER_SIZE as u64))?;
        let mut footer_bytes = [0u8; FOOTER_SIZE];
        inner.read_exact(&mut footer_bytes)?;
        let footer = Footer::decode(&footer_bytes)?;

        // Bound the directory before allocating its buffer.
        if footer.dir_size == 0 || footer.dir_size > MAX_DIR_SIZE as u64 {
            return Err(Error::Format(format!(
                "directory size {} out of bounds (1..={MAX_DIR_SIZE})",
                footer.dir_size
            )));
        }
        let dir_end = footer
            .dir_offset
            .checked_add(footer.dir_size)
            .ok_or_else(|| Error::Format("directory offset + size overflows".to_string()))?;
        if dir_end != len - FOOTER_SIZE as u64 {
            return Err(Error::Format(format!(
                "directory [{}, {dir_end}) does not end at the footer ({})",
                footer.dir_offset,
                len - FOOTER_SIZE as u64
            )));
        }
        let dir_len = usize::try_from(footer.dir_size)
            .map_err(|_| Error::Format("directory size does not fit this platform".to_string()))?;
        let mut dir_bytes = vec![0u8; dir_len];
        inner.seek(SeekFrom::Start(footer.dir_offset))?;
        inner.read_exact(&mut dir_bytes)?;
        if crc32(&dir_bytes) != footer.dir_crc32 {
            return Err(Error::Format("directory CRC-32 mismatch".to_string()));
        }
        let directory = Directory::decode(&dir_bytes)?;

        // Optional sections fail independently. Core/unknown entries retain
        // the existing fatal structural validation (SPEC §6A.4/§6B.4/§10.2).
        let mut optional_errors = OptionalSectionErrors::default();
        let mut spans: Vec<Option<(u64, u64)>> = Vec::with_capacity(directory.sections.len());
        for entry in &directory.sections {
            let optional = OptionalSectionErrors::is_optional(entry.kind);
            let end = match entry.offset.checked_add(entry.size) {
                Some(end) => end,
                None if optional => {
                    optional_errors
                        .record(entry.kind, "section offset + size overflows".to_string());
                    spans.push(None);
                    continue;
                }
                None => return Err(Error::Format("section offset + size overflows".to_string())),
            };
            if end > footer.dir_offset {
                let error = format!(
                    "section [{}, {end}) lies outside the trailer (directory at {})",
                    entry.offset, footer.dir_offset
                );
                if optional {
                    optional_errors.record(entry.kind, error);
                } else {
                    return Err(Error::Format(error));
                }
            }
            if optional && entry.size == 0 {
                optional_errors.record(entry.kind, "optional section is empty".to_string());
            }
            spans.push(Some((entry.offset, end)));
        }
        for (index, entry) in directory.sections.iter().enumerate() {
            for (other_index, other) in directory.sections.iter().enumerate().skip(index + 1) {
                let (Some((start, end)), Some((other_start, other_end))) =
                    (spans[index], spans[other_index])
                else {
                    continue;
                };
                if start >= other_end || other_start >= end {
                    continue;
                }
                let error = format!("sections at {start:#x} and {other_start:#x} overlap");
                match (
                    OptionalSectionErrors::is_optional(entry.kind),
                    OptionalSectionErrors::is_optional(other.kind),
                ) {
                    (true, true) if entry.kind == other.kind => {
                        optional_errors.record(entry.kind, error);
                    }
                    (true, true) => {
                        // Kind 3 depends on kind 2. Disable the overlapping
                        // kind-3 candidate without condemning independent
                        // kind-2 metadata; if kind 2 is also bad, kind 3
                        // remains disabled through that dependency.
                        optional_errors.record(KIND_GEOMETRY_METRICS, error);
                    }
                    (true, false) => optional_errors.record(entry.kind, error),
                    (false, true) => optional_errors.record(other.kind, error),
                    (false, false) => return Err(Error::Format(error)),
                }
            }
        }

        let first_start = spans
            .iter()
            .flatten()
            .map(|&(start, _)| start)
            .min()
            .unwrap_or(footer.dir_offset);
        let first_is_optional = directory.sections.iter().zip(&spans).any(|(entry, span)| {
            OptionalSectionErrors::is_optional(entry.kind)
                && span.map(|(start, _)| start) == Some(first_start)
        });
        let body_len = if first_is_optional {
            match first_start.checked_sub(SENTINEL.len() as u64) {
                Some(candidate) if optional_body_len_matches(&mut inner, candidate)? => candidate,
                _ => {
                    for (entry, span) in directory.sections.iter().zip(&spans) {
                        if OptionalSectionErrors::is_optional(entry.kind)
                            && span.map(|(start, _)| start) == Some(first_start)
                        {
                            optional_errors.record(
                                entry.kind,
                                "optional section offset is inconsistent with the FlatGeobuf body terminal and sentinel"
                                    .to_string(),
                            );
                        }
                    }
                    recover_body_len(&mut inner, footer.dir_offset)?
                }
            }
        } else if optional_errors.any() {
            recover_body_len(&mut inner, footer.dir_offset)?
        } else {
            first_start
                .checked_sub(SENTINEL.len() as u64)
                .ok_or_else(|| {
                    Error::Format("first section starts before the trailer sentinel".to_string())
                })?
        };
        if optional_errors.any() {
            validate_recovered_sections(&directory, &spans, body_len, &mut optional_errors)?;
        }
        Ok(Reader {
            inner,
            directory,
            body_len,
            cached_band: None,
            #[cfg(feature = "analytics")]
            analytics_structural_error: optional_errors.analytics,
            #[cfg(feature = "analytics")]
            analytics: None,
            #[cfg(feature = "analytics")]
            analytics_loaded: false,
            #[cfg(feature = "analytics")]
            geometry_metrics_structural_error: optional_errors.geometry_metrics,
            #[cfg(feature = "analytics")]
            geometry_metrics: None,
            #[cfg(feature = "analytics")]
            geometry_metrics_loaded: false,
        })
    }

    /// Metadata of every aggregate band in the trailer, in directory order.
    ///
    /// Sections of unknown kind are skipped, never failed on (SPEC §5); a
    /// band entry without its resolution byte in `meta` is malformed and
    /// also skipped.
    pub fn bands(&self) -> Vec<BandMeta> {
        self.directory
            .sections
            .iter()
            .filter(|s| s.kind == KIND_AGGREGATE_BAND)
            .filter_map(|s| {
                s.meta.first().map(|&res| BandMeta {
                    res,
                    offset: s.offset,
                    size: s.size,
                })
            })
            .collect()
    }

    /// The decoded directory, including sections of unknown kind.
    pub fn directory(&self) -> &Directory {
        &self.directory
    }

    /// Byte offset where the FlatGeobuf body ends (the trailer sentinel).
    pub fn body_len(&self) -> u64 {
        self.body_len
    }

    /// Read and fully validate the aggregate band at resolution `res`
    /// (SPEC §6 reader rules). This always performs a fresh decode, even when
    /// the same resolution is cached for viewport queries.
    pub fn read_band(&mut self, res: u8) -> Result<AggBand> {
        let entry = self
            .directory
            .sections
            .iter()
            .find(|s| s.kind == KIND_AGGREGATE_BAND && s.meta.first() == Some(&res))
            .ok_or_else(|| Error::Format(format!("no aggregate band at resolution {res}")))?;
        let (offset, size) = (entry.offset, entry.size);
        let len = usize::try_from(size)
            .map_err(|_| Error::Format("band size does not fit this platform".to_string()))?;
        let mut bytes = vec![0u8; len];
        self.inner.seek(SeekFrom::Start(offset))?;
        self.inner.read_exact(&mut bytes)?;
        let band = AggBand::from_bytes(&bytes)?;
        if band.res != res {
            return Err(Error::Format(format!(
                "band header resolution {} disagrees with directory meta {res}",
                band.res
            )));
        }
        Ok(band)
    }

    /// Aggregate query: the band records at resolution `res` whose cells
    /// cover the viewport `[min_x, min_y, max_x, max_y]` (lon/lat degrees).
    ///
    /// The covering uses *overlapping* containment — center-containment
    /// misses boundary cells at fine zooms (SPEC §9.4).
    ///
    /// Viewports covering at least half the sphere instead keep band cells
    /// whose conservative lon/lat bounding boxes overlap the viewport. This
    /// avoids ambiguous spherical ring coverings while preserving overlapping
    /// containment semantics (SPEC §9.4).
    pub fn query(&mut self, view: [f64; 4], res: u8) -> Result<Vec<(u64, u32)>> {
        if use_cell_filter(&view) {
            if let Some(band) = self.cached_band.as_ref().filter(|band| band.res == res) {
                return band_cells_in_view(&band.cells, &view, res);
            }
            let band = self.read_band(res)?;
            let matches = band_cells_in_view(&band.cells, &view, res)?;
            self.cached_band = Some(band);
            return Ok(matches);
        }
        let covering = covering(&view, res)?;
        if let Some(band) = self.cached_band.as_ref().filter(|band| band.res == res) {
            return Ok(band.query(&covering));
        }
        let band = self.read_band(res)?;
        let matches = band.query(&covering);
        self.cached_band = Some(band);
        Ok(matches)
    }

    /// Count-only aggregate query: occupied cells and estimated feature
    /// total over the viewport, without returning the records.
    pub fn stats(&mut self, view: [f64; 4], res: u8) -> Result<BandStats> {
        let matches = self.query(view, res)?;
        Ok(BandStats {
            cells: matches.len(),
            total: matches.iter().map(|&(_, count)| u64::from(count)).sum(),
        })
    }

    #[cfg(feature = "analytics")]
    fn analytics_metadata(&mut self) -> Result<Option<&BodyAnalytics>> {
        if let Some(error) = &self.analytics_structural_error {
            return Err(Error::Format(error.clone()));
        }
        if !self.analytics_loaded {
            let mut entries = self
                .directory
                .sections
                .iter()
                .filter(|section| section.kind == KIND_BODY_ANALYTICS);
            let entry = entries.next().cloned();
            if entries.next().is_some() {
                return Err(Error::Format(
                    "analytics: more than one BODY_ANALYTICS section".to_string(),
                ));
            }
            self.analytics = match entry {
                Some(entry) => {
                    crate::analytics::read_metadata(&mut self.inner, &entry, self.body_len)
                        .map(Some)?
                }
                None => None,
            };
            self.analytics_loaded = true;
        }
        Ok(self.analytics.as_ref())
    }

    /// Read and validate BODY_ANALYTICS metadata without reading Arrow payloads.
    ///
    /// Returns `Ok(None)` when no kind-2 section exists. A malformed optional
    /// section is an error here, but the same reader can still read the body
    /// and kind-1 bands because those paths seek independently.
    #[cfg(feature = "analytics")]
    pub fn read_body_analytics_metadata(&mut self) -> Result<Option<BodyAnalytics>> {
        Ok(self.analytics_metadata()?.cloned())
    }

    /// Read and validate GEOMETRY_METRICS metadata by directory kind.
    ///
    /// Returns `Ok(None)` when no kind-3 section exists. Kind 3 requires a
    /// valid kind-2 section and inherits its chunk grid. An error here does
    /// not affect subsequent body, band, or kind-2 reads.
    #[cfg(feature = "analytics")]
    pub fn geometry_metrics(&mut self) -> Result<Option<&GeometryMetrics>> {
        if let Some(error) = &self.geometry_metrics_structural_error {
            return Err(Error::Format(error.clone()));
        }
        if !self.geometry_metrics_loaded {
            let mut entries = self
                .directory
                .sections
                .iter()
                .filter(|section| section.kind == KIND_GEOMETRY_METRICS);
            let entry = entries.next().cloned();
            if entries.next().is_some() {
                return Err(Error::Format(
                    "geometry metrics: more than one GEOMETRY_METRICS section".to_string(),
                ));
            }
            self.geometry_metrics = match entry {
                Some(entry) => {
                    let analytics = self.analytics_metadata()?.cloned().ok_or_else(|| {
                        Error::Format(
                            "geometry metrics: GEOMETRY_METRICS requires BODY_ANALYTICS".into(),
                        )
                    })?;
                    crate::geom_analytics::read_metadata(&mut self.inner, &entry, &analytics)
                        .map(Some)?
                }
                None => None,
            };
            self.geometry_metrics_loaded = true;
        }
        Ok(self.geometry_metrics.as_ref())
    }

    /// Read and clone validated GEOMETRY_METRICS metadata without Arrow payloads.
    #[cfg(feature = "analytics")]
    pub fn read_geometry_metrics_metadata(&mut self) -> Result<Option<GeometryMetrics>> {
        Ok(self.geometry_metrics()?.cloned())
    }

    /// Read one independently range-readable Arrow IPC chunk by column/chunk identity.
    ///
    /// Metadata and the 64 MiB chunk cap are validated before allocating the
    /// returned buffer. Decoding the Arrow file is deliberately left to the
    /// query layer.
    #[cfg(feature = "analytics")]
    pub fn read_arrow_chunk(&mut self, column_index: u16, chunk_index: u64) -> Result<ArrowChunk> {
        let (start, end) = self
            .analytics_metadata()?
            .ok_or_else(|| Error::Format("analytics: section is absent".to_string()))?
            .arrow_range(column_index, chunk_index)?;
        let bytes = self.read_arrow_bytes(
            start,
            end,
            &format!("analytics Arrow chunk {column_index}/{chunk_index}"),
        )?;
        Ok(ArrowChunk {
            column_index,
            chunk_index,
            bytes,
        })
    }

    /// Read one Arrow IPC chunk using an already loaded metadata table.
    ///
    /// This avoids reopening the footer, directory, and analytical metadata for
    /// every chunk. The metadata is still validated before any range is read.
    #[cfg(feature = "analytics")]
    pub fn read_arrow_chunk_with_metadata(
        &mut self,
        metadata: &BodyAnalytics,
        column_index: u16,
        chunk_index: u64,
    ) -> Result<ArrowChunk> {
        let (start, end) = metadata.arrow_range(column_index, chunk_index)?;
        let bytes = self.read_arrow_bytes(
            start,
            end,
            &format!("analytics Arrow chunk {column_index}/{chunk_index}"),
        )?;
        Ok(ArrowChunk {
            column_index,
            chunk_index,
            bytes,
        })
    }

    /// Read one geometry metric Arrow chunk by metric/chunk identity.
    #[cfg(feature = "analytics")]
    pub fn read_geometry_arrow_chunk(
        &mut self,
        metric_id: u16,
        chunk_index: u64,
    ) -> Result<ArrowChunk> {
        let (start, end) = self
            .geometry_metrics()?
            .ok_or_else(|| Error::Format("geometry metrics: section is absent".to_string()))?
            .arrow_range(metric_id, chunk_index)?;
        let bytes = self.read_arrow_bytes(
            start,
            end,
            &format!("geometry Arrow chunk {metric_id}/{chunk_index}"),
        )?;
        Ok(ArrowChunk {
            column_index: metric_id,
            chunk_index,
            bytes,
        })
    }

    /// Read one geometry metric Arrow chunk using already loaded metadata.
    #[cfg(feature = "analytics")]
    pub fn read_geometry_arrow_chunk_with_metadata(
        &mut self,
        metadata: &GeometryMetrics,
        metric_id: u16,
        chunk_index: u64,
    ) -> Result<ArrowChunk> {
        let (start, end) = metadata.arrow_range(metric_id, chunk_index)?;
        let bytes = self.read_arrow_bytes(
            start,
            end,
            &format!("geometry Arrow chunk {metric_id}/{chunk_index}"),
        )?;
        Ok(ArrowChunk {
            column_index: metric_id,
            chunk_index,
            bytes,
        })
    }

    #[cfg(feature = "analytics")]
    fn read_arrow_bytes(&mut self, start: u64, end: u64, context: &str) -> Result<Vec<u8>> {
        let len = end
            .checked_sub(start)
            .ok_or_else(|| Error::Format(format!("{context} range is inverted")))?;
        if len == 0 || len > crate::analytics::MAX_ARROW_CHUNK_BYTES {
            return Err(Error::Format(format!("{context} has invalid size {len}")));
        }
        let len = usize::try_from(len)
            .map_err(|_| Error::Format(format!("{context} does not fit this platform")))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(len)
            .map_err(|_| Error::Format(format!("{context} allocation is too large")))?;
        bytes.resize(len, 0);
        self.inner.seek(SeekFrom::Start(start))?;
        self.inner.read_exact(&mut bytes).map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::Format(format!("{context} is truncated: {error}"))
            } else {
                Error::Io(error)
            }
        })?;
        Ok(bytes)
    }

    /// The underlying source, positioned at offset 0, for reading the
    /// FlatGeobuf body.
    ///
    /// Body reads are the caller's business: the body is a plain FlatGeobuf
    /// file — open it with the `flatgeobuf` crate (e.g.
    /// `flatgeobuf::FgbReader::open(reader.body()?)`) and use bbox queries
    /// as usual. Read only offsets below [`Reader::body_len`]; everything
    /// beyond is the trailer.
    pub fn body(&mut self) -> Result<&mut R> {
        self.inner.seek(SeekFrom::Start(0))?;
        Ok(&mut self.inner)
    }

    /// Consume the reader and return the underlying source.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Reader<std::io::BufReader<std::fs::File>> {
    /// Open a local FGBA file, buffered.
    pub fn open_path(path: &std::path::Path) -> Result<Reader<std::io::BufReader<std::fs::File>>> {
        let file = std::fs::File::open(path)?;
        Reader::open(std::io::BufReader::new(file))
    }
}

fn sentinel_matches<R: Read + Seek>(inner: &mut R, body_len: u64) -> Result<bool> {
    inner.seek(SeekFrom::Start(body_len))?;
    let mut sentinel = [0u8; SENTINEL.len()];
    match inner.read_exact(&mut sentinel) {
        Ok(()) => Ok(sentinel == SENTINEL),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(Error::Io(error)),
    }
}

/// Verify an optional-first candidate against the last leaf in body order.
/// This reads the header, one NodeItem, one feature length, and the sentinel;
/// it never iterates the feature body.
fn optional_body_len_matches<R: Read + Seek>(inner: &mut R, candidate: u64) -> Result<bool> {
    if !sentinel_matches(inner, candidate)? {
        return Ok(false);
    }

    inner.seek(SeekFrom::Start(0))?;
    let shared = SharedReader::new(&mut *inner);
    let fgb = match FgbReader::open(shared.handle()) {
        Ok(fgb) => fgb,
        Err(_) => return Ok(false),
    };
    let row_count = fgb.header().features_count();
    let node_size = fgb.header().index_node_size();
    let index_start = shared.position();
    drop(fgb);
    drop(shared);

    // Besides being structurally necessary, this bound keeps the upstream
    // index-size calculation within u64 for every admitted row count.
    if row_count == 0 || node_size < 2 || row_count > candidate / 40 {
        return Ok(false);
    }
    let Ok(rows) = usize::try_from(row_count) else {
        return Ok(false);
    };
    let Ok(index_size) = u64::try_from(PackedRTree::index_size(rows, node_size)) else {
        return Ok(false);
    };
    let Some(data_start) = index_start.checked_add(index_size) else {
        return Ok(false);
    };
    let Some(leaf_bytes) = row_count.checked_mul(40) else {
        return Ok(false);
    };
    let Some(leaf_start) = data_start.checked_sub(leaf_bytes) else {
        return Ok(false);
    };
    if data_start > candidate || leaf_start < index_start {
        return Ok(false);
    }
    let Some(last_leaf) = (row_count - 1)
        .checked_mul(40)
        .and_then(|offset| leaf_start.checked_add(offset))
    else {
        return Ok(false);
    };

    inner.seek(SeekFrom::Start(last_leaf))?;
    let node = match NodeItem::from_reader(&mut *inner) {
        Ok(node) => node,
        Err(_) => return Ok(false),
    };
    let Some(last_feature_start) = data_start.checked_add(node.offset) else {
        return Ok(false);
    };
    let Some(prefix_end) = last_feature_start.checked_add(4) else {
        return Ok(false);
    };
    if prefix_end > candidate {
        return Ok(false);
    }

    inner.seek(SeekFrom::Start(last_feature_start))?;
    let mut prefix = [0u8; 4];
    match inner.read_exact(&mut prefix) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(false),
        Err(error) => return Err(Error::Io(error)),
    }
    let feature_len = u64::from(u32::from_le_bytes(prefix));
    let Some(terminal) = prefix_end.checked_add(feature_len) else {
        return Ok(false);
    };
    Ok(terminal == candidate)
}

/// Preserve sparse-reader I/O errors so async callers can supply the missing
/// range and retry; only actual FlatGeobuf format failures become Format.
fn recover_body_error(context: &str, error: flatgeobuf::Error) -> Error {
    match error {
        flatgeobuf::Error::IO(io_error) => Error::Io(io_error),
        other => Error::Format(format!("{context}: {other}")),
    }
}

/// Recover the exact Data-section terminal from FlatGeobuf's own feature
/// iteration. The directory boundary and sentinel are independent checks;
/// neither a section offset nor a guessed feature length supplies `body_len`.
fn recover_body_len<R: Read + Seek>(inner: &mut R, dir_offset: u64) -> Result<u64> {
    inner.seek(SeekFrom::Start(0))?;
    let shared = SharedReader::new(&mut *inner);
    let fgb = FgbReader::open(shared.handle())
        .map_err(|error| recover_body_error("cannot recover FlatGeobuf body", error))?;
    let (expected_rows, index_node_size) = {
        let header = fgb.header();
        (header.features_count(), header.index_node_size())
    };
    if expected_rows == 0 || index_node_size == 0 {
        return Err(Error::Format(
            "cannot recover FlatGeobuf body without a feature count and spatial index".to_string(),
        ));
    }
    let rows = usize::try_from(expected_rows)
        .map_err(|_| Error::Format("FlatGeobuf body row count does not fit usize".to_string()))?;
    let index_size = u64::try_from(PackedRTree::index_size(rows, index_node_size))
        .map_err(|_| Error::Format("FlatGeobuf body index size overflows u64".to_string()))?;
    drop(fgb);

    // FlatGeobuf's FeatureIter treats every prefix-read error as EOF. That is
    // incorrect for a sparse source: WouldBlock must reach the async caller so
    // it can supply the range and retry. Count length-delimited features
    // directly instead of allowing a cache miss to become a false zero-row EOF.
    let mut body = BufReader::with_capacity(1024 * 1024, shared.handle());
    body.seek(SeekFrom::Current(i64::try_from(index_size).map_err(
        |_| Error::Format("FlatGeobuf body index size exceeds i64".to_string()),
    )?))?;
    let mut body_len = shared.position();
    for row in 0..expected_rows {
        let mut prefix = [0_u8; 4];
        body.read_exact(&mut prefix)?;
        let feature_len = u64::from(u32::from_le_bytes(prefix));
        let skipped = std::io::copy(&mut (&mut body).take(feature_len), &mut std::io::sink())?;
        if skipped != feature_len {
            return Err(Error::Format(format!(
                "FlatGeobuf body feature {row} is truncated: {skipped} of {feature_len} bytes"
            )));
        }
        body_len = body_len
            .checked_add(4)
            .and_then(|position| position.checked_add(feature_len))
            .ok_or_else(|| {
                Error::Format("FlatGeobuf body terminal offset overflows".to_string())
            })?;
    }
    drop(body);
    drop(shared);

    let sentinel_end = body_len
        .checked_add(SENTINEL.len() as u64)
        .ok_or_else(|| Error::Format("recovered body sentinel offset overflows".to_string()))?;
    if sentinel_end > dir_offset {
        return Err(Error::Format(format!(
            "recovered body sentinel ends at {sentinel_end}, beyond the directory at {dir_offset}"
        )));
    }
    if !sentinel_matches(inner, body_len)? {
        return Err(Error::Format(format!(
            "FlatGeobuf body terminal at {body_len} is not followed by the trailer sentinel"
        )));
    }
    Ok(body_len)
}

/// With an exact body boundary, no core or unknown section may intersect the
/// recovered body or sentinel. Optional sections in that range are only
/// disabled; their directory entries never move the recovered boundary.
fn validate_recovered_sections(
    directory: &Directory,
    spans: &[Option<(u64, u64)>],
    body_len: u64,
    optional_errors: &mut OptionalSectionErrors,
) -> Result<()> {
    let sections_start = body_len
        .checked_add(SENTINEL.len() as u64)
        .ok_or_else(|| Error::Format("recovered section boundary overflows".to_string()))?;
    for (entry, span) in directory.sections.iter().zip(spans) {
        let Some((start, _)) = *span else {
            continue;
        };
        if start >= sections_start {
            continue;
        }
        let error = format!(
            "section at {start:#x} begins before the recovered trailer sections at {sections_start:#x}"
        );
        if OptionalSectionErrors::is_optional(entry.kind) {
            optional_errors.record(entry.kind, error);
        } else {
            return Err(Error::Format(error));
        }
    }
    Ok(())
}

fn validate_viewport(view: &[f64; 4]) -> Result<()> {
    if !view.iter().all(|v| v.is_finite()) {
        return Err(Error::Format(format!(
            "viewport coordinates must be finite, got {view:?}"
        )));
    }
    Ok(())
}

/// Conservative per-cell rect filter for viewports covering at least half the
/// sphere, where spherical ring coverings are ambiguous (SPEC §9.4).
pub(crate) fn band_cells_in_view(
    cells: &[(u64, u32)],
    view: &[f64; 4],
    _res: u8,
) -> Result<Vec<(u64, u32)>> {
    validate_viewport(view)?;
    let [min_x, min_y, max_x, max_y] = *view;
    let mut out = Vec::new();
    for &(cell, count) in cells {
        // An in-view center proves overlap; all other cells retain the exact
        // conservative boundary-bbox test.
        let center = a5::cell_to_lonlat(cell).map_err(Error::A5)?;
        let x = center.longitude();
        let y = center.latitude();
        if min_x <= x && x <= max_x && min_y <= y && y <= max_y {
            out.push((cell, count));
            continue;
        }
        let ring = a5::cell_to_boundary(cell, None).map_err(Error::A5)?;
        let (mut x0, mut y0, mut x1, mut y1) = (f64::MAX, f64::MAX, f64::MIN, f64::MIN);
        for point in &ring {
            x0 = x0.min(point.longitude());
            x1 = x1.max(point.longitude());
            y0 = y0.min(point.latitude());
            y1 = y1.max(point.latitude());
        }
        if x0 <= max_x && x1 >= min_x && y0 <= max_y && y1 >= min_y {
            out.push((cell, count));
        }
    }
    Ok(out)
}

/// A5 covering of a viewport at `res` (uncompacted, curve-sorted), with
/// overlapping containment (SPEC §9.4).
///
/// Viewports covering at least half the sphere are ambiguous for a single
/// spherical ring, so [`Reader::query`] uses [`band_cells_in_view`] instead.
fn covering(view: &[f64; 4], res: u8) -> Result<Vec<u64>> {
    validate_viewport(view)?;
    let [min_x, min_y, max_x, max_y] = *view;
    rect_covering_cells(min_x, min_y, max_x, max_y, res)
}

/// Cells covering one axis-aligned rectangle, uncompacted. Rectangles wider
/// than 180° are split in half so no single ring spans more than a
/// hemisphere (where the spherical polygon becomes ambiguous).
fn rect_covering_cells(
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
    res: u8,
) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    let mut push = |lo: f64, hi: f64| -> Result<()> {
        // Slivers thinner than this are degenerate for the spherical ring:
        // an antimeridian-spanning band of ~zero width can match every cell
        // on the globe. Treat them as empty.
        const MIN_DEG: f64 = 1e-6;
        if hi - lo < MIN_DEG || max_y - min_y < MIN_DEG {
            return Ok(());
        }
        let ring = vec![
            a5::LonLat::new(lo, min_y),
            a5::LonLat::new(hi, min_y),
            a5::LonLat::new(hi, max_y),
            a5::LonLat::new(lo, max_y),
        ];
        let compacted = a5::polygon_to_cells(
            &[ring],
            i32::from(res),
            Some(a5::PolygonToCellsOptions {
                containment: a5::Containment::Overlapping,
            }),
        )
        .map_err(Error::A5)?;
        out.extend(a5::uncompact(&compacted).map_err(Error::A5)?);
        Ok(())
    };
    let width = max_x - min_x;
    if width > 180.0 {
        let mid = min_x + width / 2.0;
        push(min_x, mid)?;
        push(mid, max_x)?;
    } else {
        push(min_x, max_x)?;
    }
    Ok(out)
}

/// Viewport spherical area fraction at which per-cell bbox filtering replaces
/// ring coverings, whose spherical orientation becomes ambiguous at this scale.
const CELL_FILTER_AREA_FRACTION: f64 = 0.5;

fn use_cell_filter(view: &[f64; 4]) -> bool {
    viewport_area_fraction(view) >= CELL_FILTER_AREA_FRACTION
}

fn viewport_area_fraction(view: &[f64; 4]) -> f64 {
    let lon_width = (view[2] - view[0]).clamp(0.0, 360.0);
    let min_lat = view[1].clamp(-90.0, 90.0).to_radians();
    let max_lat = view[3].clamp(-90.0, 90.0).to_radians();
    (lon_width / 360.0) * ((max_lat.sin() - min_lat.sin()) / 2.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_branch_selector_uses_spherical_area_at_hemisphere_boundary() {
        // Planar area is exactly 0.5, but spherical area is about 0.707.
        assert!(use_cell_filter(&[-180.0, -45.0, 180.0, 45.0]));
        // An exact hemisphere must not build a 180-degree ring.
        assert!(use_cell_filter(&[-180.0, 0.0, 180.0, 90.0]));
        // This also distinguishes spherical area from merely changing the
        // planar comparison to >=: planar area is only 5/12 here.
        assert!(use_cell_filter(&[-180.0, -45.0, 180.0, 30.0]));

        // Regional viewports retain the direct-covering branch.
        assert!(!use_cell_filter(&[-30.0, 40.0, 20.0, 60.0]));
    }

    #[test]
    fn near_world_viewport_returns_full_band_total() -> Result<()> {
        // A5 1.0.0 has 240 res-2 cells, not the 122 assumed by the plan draft.
        let mut ids = a5::cell_to_children(a5::WORLD_CELL, Some(2)).map_err(Error::A5)?;
        ids.sort_unstable();
        let cells: Vec<(u64, u32)> = ids.into_iter().map(|cell| (cell, 1)).collect();
        assert_eq!(cells.len(), 240);

        let view = [-179.999, -79.776, 179.997, 82.499]; // trails_lines extent
        assert!(use_cell_filter(&view));
        let kept = band_cells_in_view(&cells, &view, 2)?;
        let total: u64 = kept.iter().map(|&(_, count)| u64::from(count)).sum();
        assert!(total >= 216, "kept only {total}/240 res-2 cells");
        Ok(())
    }

    fn center_free_reference(cells: &[(u64, u32)], view: &[f64; 4]) -> Result<Vec<(u64, u32)>> {
        validate_viewport(view)?;
        let [min_x, min_y, max_x, max_y] = *view;
        let mut out = Vec::new();
        for &(cell, count) in cells {
            let ring = a5::cell_to_boundary(cell, None).map_err(Error::A5)?;
            let (mut x0, mut y0, mut x1, mut y1) = (f64::MAX, f64::MAX, f64::MIN, f64::MIN);
            for point in &ring {
                x0 = x0.min(point.longitude());
                x1 = x1.max(point.longitude());
                y0 = y0.min(point.latitude());
                y1 = y1.max(point.latitude());
            }
            if x0 <= max_x && x1 >= min_x && y0 <= max_y && y1 >= min_y {
                out.push((cell, count));
            }
        }
        Ok(out)
    }

    #[test]
    fn center_fast_path_matches_center_free_reference() -> Result<()> {
        let mut res2_ids = a5::cell_to_children(a5::WORLD_CELL, Some(2)).map_err(Error::A5)?;
        res2_ids.sort_unstable();
        let res2_cells: Vec<(u64, u32)> = res2_ids
            .into_iter()
            .enumerate()
            .map(|(index, cell)| (cell, (index % 31 + 1) as u32))
            .collect();
        assert_eq!(res2_cells.len(), 240);

        let mut res4_ids = a5::cell_to_children(a5::WORLD_CELL, Some(4)).map_err(Error::A5)?;
        res4_ids.sort_unstable();
        let res4_cells: Vec<(u64, u32)> = res4_ids
            .into_iter()
            .step_by(7)
            .enumerate()
            .map(|(index, cell)| (cell, (index % 31 + 1) as u32))
            .collect();
        assert!(res4_cells.len() > res2_cells.len());

        let viewports = [
            ("near-world", [-179.999, -79.776, 179.997, 82.499]),
            ("clamped-polar", [-180.0, -85.0, 180.0, 85.0]),
            ("hemisphere-boundary", [-180.0, 0.0, 180.0, 90.0]),
        ];
        for (res, cells) in [(2, res2_cells.as_slice()), (4, res4_cells.as_slice())] {
            for (name, view) in viewports {
                let actual = band_cells_in_view(cells, &view, res)?;
                let expected = center_free_reference(cells, &view)?;
                assert_eq!(actual, expected, "{name} viewport at res {res}");
            }
        }
        Ok(())
    }
}
