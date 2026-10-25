//! FGBA writer: bin features into aggregate bands and assemble the trailer
//! (sentinel, band sections, directory, footer) per SPEC §8.
//!
//! Builds are deterministic: identical input produces byte-identical output
//! (cell IDs are sorted before run-length encoding).

use crate::band::AggBand;
use crate::error::{Error, Result};
use crate::format::{
    crc32, Directory, Footer, SectionEntry, KIND_AGGREGATE_BAND, KIND_BODY_ANALYTICS,
    KIND_GEOMETRY_METRICS, SENTINEL,
};
use crate::geometry::BboxSink;
use crate::io::{copy_exact, SharedReader};
use flatgeobuf::geozero::GeozeroGeometry;
use flatgeobuf::{FallibleStreamingIterator, FgbReader};
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom, Write};

#[cfg(test)]
use std::cell::Cell;

/// Maximum A5 resolution chosen as the base (finest) band (SPEC §8.3).
pub const MAX_BASE_RES: u8 = 16;

/// Minimum band resolution; bands stop descending at 2 (SPEC §8.4).
pub const MIN_BAND_RES: u8 = 2;

/// Maximum number of bands per file (SPEC §8.4).
pub const MAX_BANDS: usize = 6;

/// Optional outputs and chunk sizing for [`build_with_options`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BuildOptions {
    /// Emit BODY_ANALYTICS when at least one property column exists.
    pub analytics: bool,
    /// Also emit GEOMETRY_METRICS on the BODY_ANALYTICS chunk grid.
    /// Requires `analytics`; neither section is emitted for property-free bodies.
    pub geometry_metrics: bool,
    /// Rows per analytical chunk; 0 selects a size from the body's feature count.
    pub chunk_rows: u32,
}

/// Resolve analytical chunk rows, using automatic sizing when `requested` is 0.
/// Explicit nonzero requests are preserved, including values outside the
/// automatic 4096–1,048,576 row range.
pub fn effective_chunk_rows(row_count: u64, requested: u32) -> u32 {
    if requested != 0 {
        return requested;
    }
    let automatic = row_count
        .div_ceil(8192)
        .checked_next_power_of_two()
        .unwrap_or(1 << 20)
        .clamp(4096, 1 << 20);
    automatic as u32
}

/// Summary of a [`build`] run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildReport {
    /// Features binned into the bands (valid geometries only).
    pub features: u64,
    /// Band resolutions written, ascending (finest last).
    pub bands: Vec<u8>,
    /// Features skipped because their geometry was empty or invalid.
    pub skipped: usize,
    /// Byte size of the appended trailer (sentinel through footer).
    pub trailer_bytes: usize,
    /// Property columns included in requested analytics; zero for ordinary builds.
    pub attribute_columns: u32,
    /// Whether analytical output was requested, including property-free bodies.
    pub analytics_requested: bool,
}

/// Build the trailer bytes for a set of feature midpoints
/// (`(longitude, latitude)` in degrees), per SPEC §8: pick the base
/// resolution, bin every midpoint into each band, sort records by ascending
/// cell ID, then assemble sentinel + band sections + directory + footer.
///
/// `total_hint` only reserves allocation capacity; the real count comes from
/// the iterator, so a wrong hint never corrupts the output.
///
/// The returned trailer is standalone: directory offsets are absolute file
/// offsets computed as if the trailer began at file offset 0. To append a
/// trailer to a real FlatGeobuf body use [`build`], which knows the body
/// length.
pub fn trailer_for(
    midpoints: impl Iterator<Item = (f64, f64)>,
    total_hint: Option<u64>,
) -> Result<Vec<u8>> {
    // A wrong (hostile or stale) hint must never cause a huge allocation.
    let cap = total_hint
        .and_then(|h| usize::try_from(h).ok())
        .unwrap_or(0)
        .min(1 << 20);
    let mut points: Vec<(f64, f64)> = Vec::with_capacity(cap);
    points.extend(midpoints);
    let (bytes, _, _) = trailer_at_with_analytics(points, 0, None, None)?;
    Ok(bytes)
}

/// Compute the band resolutions for a dataset of `count` features (SPEC
/// §8.3–8.4): the base resolution is the smallest `res >= 2` with
/// `a5::get_num_cells(res) >= 4 * count`, capped at [`MAX_BASE_RES`]; bands
/// are `{base, base-2, ...}` while `>= `[`MIN_BAND_RES`], at most
/// [`MAX_BANDS`], returned ascending.
pub fn band_ladder(count: u64) -> Vec<u8> {
    let target = count.saturating_mul(4);
    let mut base = MIN_BAND_RES;
    while base < MAX_BASE_RES && a5::get_num_cells(i32::from(base)) < target {
        base += 1;
    }
    let mut res: Vec<u8> = Vec::new();
    let mut r = base;
    loop {
        res.push(r);
        if r < MIN_BAND_RES + 2 || res.len() >= MAX_BANDS {
            break;
        }
        r -= 2;
    }
    res.reverse();
    res
}

trait SectionPayload {
    fn size(&self) -> u64;
    fn write_to(&mut self, out: &mut dyn Write) -> Result<u64>;
}

impl SectionPayload for Vec<u8> {
    fn size(&self) -> u64 {
        self.len() as u64
    }

    fn write_to(&mut self, out: &mut dyn Write) -> Result<u64> {
        out.write_all(self)?;
        Ok(self.len() as u64)
    }
}

#[cfg(feature = "analytics")]
impl SectionPayload for crate::analytics::EncodedAnalytics {
    fn size(&self) -> u64 {
        self.size()
    }

    fn write_to(&mut self, out: &mut dyn Write) -> Result<u64> {
        self.write_to(out)
    }
}

#[cfg(feature = "analytics")]
impl SectionPayload for crate::geom_analytics::EncodedGeometry {
    fn size(&self) -> u64 {
        self.size()
    }

    fn write_to(&mut self, out: &mut dyn Write) -> Result<u64> {
        self.write_to(out)
    }
}

struct TrailerSummary {
    bands: Vec<u8>,
    binned: u64,
    bytes: u64,
}

/// Assemble the trailer with optional, already encoded kind-2 and kind-3 sections.
fn trailer_at_with_analytics(
    points: Vec<(f64, f64)>,
    base_offset: u64,
    mut analytics: Option<Vec<u8>>,
    mut geometry: Option<Vec<u8>>,
) -> Result<(Vec<u8>, Vec<u8>, u64)> {
    let mut out = Vec::new();
    let summary = assemble_trailer(
        &points,
        base_offset,
        analytics
            .as_mut()
            .map(|bytes| bytes as &mut dyn SectionPayload),
        geometry
            .as_mut()
            .map(|bytes| bytes as &mut dyn SectionPayload),
        &mut out,
    )?;
    Ok((out, summary.bands, summary.binned))
}

fn write_counted(out: &mut dyn Write, bytes: &[u8], written: &mut u64) -> Result<()> {
    out.write_all(bytes)?;
    *written = written
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| Error::Format("trailer size overflows".into()))?;
    Ok(())
}

fn absolute_offset(base_offset: u64, written: u64, what: &str) -> Result<u64> {
    base_offset
        .checked_add(written)
        .ok_or_else(|| Error::Format(format!("{what} offset overflows")))
}

fn write_optional_section(
    payload: Option<&mut dyn SectionPayload>,
    kind: u16,
    what: &str,
    base_offset: u64,
    written: &mut u64,
    entries: &mut Vec<SectionEntry>,
    out: &mut dyn Write,
) -> Result<()> {
    let Some(payload) = payload else {
        return Ok(());
    };
    let size = payload.size();
    let offset = absolute_offset(base_offset, *written, what)?;
    let actual = payload.write_to(out)?;
    if actual != size {
        return Err(Error::Format(format!(
            "{what} size disagrees with its layout"
        )));
    }
    *written = written
        .checked_add(actual)
        .ok_or_else(|| Error::Format("trailer size overflows".into()))?;
    entries.push(SectionEntry {
        kind,
        flags: 0,
        offset,
        size,
        meta: Vec::new(),
    });
    Ok(())
}

fn assemble_trailer(
    points: &[(f64, f64)],
    base_offset: u64,
    analytics: Option<&mut dyn SectionPayload>,
    geometry: Option<&mut dyn SectionPayload>,
    out: &mut dyn Write,
) -> Result<TrailerSummary> {
    let ladder = band_ladder(points.len() as u64);
    let mut entries = Vec::with_capacity(
        ladder.len() + usize::from(analytics.is_some()) + usize::from(geometry.is_some()),
    );
    let mut written = 0u64;
    write_counted(out, &SENTINEL, &mut written)?;
    write_optional_section(
        analytics,
        KIND_BODY_ANALYTICS,
        "analytics section",
        base_offset,
        &mut written,
        &mut entries,
        out,
    )?;
    write_optional_section(
        geometry,
        KIND_GEOMETRY_METRICS,
        "geometry section",
        base_offset,
        &mut written,
        &mut entries,
        out,
    )?;
    let mut binned_total = 0u64;
    let mut cell_buf: Vec<u64> = Vec::new();
    for (i, &res) in ladder.iter().enumerate() {
        cell_buf.clear();
        cell_buf.reserve(points.len());
        for &(lon, lat) in points {
            if let Ok(cell) = a5::lonlat_to_cell(a5::LonLat::new(lon, lat), i32::from(res)) {
                cell_buf.push(cell);
            }
        }
        cell_buf.sort_unstable();
        let total = cell_buf.len() as u64;
        let mut cells: Vec<(u64, u32)> = Vec::new();
        let mut runs = cell_buf.iter();
        if let Some(&first) = runs.next() {
            let (mut cur, mut n) = (first, 1u32);
            for &cell in runs {
                if cell == cur {
                    n = n
                        .checked_add(1)
                        .ok_or_else(|| Error::Format("cell count overflows u32".to_string()))?;
                } else {
                    cells.push((cur, n));
                    cur = cell;
                    n = 1;
                }
            }
            cells.push((cur, n));
        }
        if i == 0 {
            binned_total = total;
        }
        let band = AggBand { res, total, cells };
        let bytes = band.to_bytes()?;
        let offset = absolute_offset(base_offset, written, "band section")?;
        entries.push(SectionEntry {
            kind: KIND_AGGREGATE_BAND,
            flags: 0,
            offset,
            size: bytes.len() as u64,
            meta: vec![res],
        });
        write_counted(out, &bytes, &mut written)?;
    }

    let dir_offset = absolute_offset(base_offset, written, "directory")?;
    let dir_bytes = Directory { sections: entries }.encode()?;
    let footer = Footer {
        dir_offset,
        dir_size: dir_bytes.len() as u64,
        dir_crc32: crc32(&dir_bytes),
    };
    write_counted(out, &dir_bytes, &mut written)?;
    write_counted(out, &footer.encode(), &mut written)?;
    Ok(TrailerSummary {
        bands: ladder,
        binned: binned_total,
        bytes: written,
    })
}

/// Append an FGBA trailer to a FlatGeobuf body, returning the complete FGBA
/// file and a [`BuildReport`].
///
/// The body must be a valid FlatGeobuf with `features_count > 0` and a
/// spatial index (`index_node_size > 0`), the SPEC §4 constraints that keep
/// plain FGB readers away from the trailer. Every feature is binned by the
/// midpoint of its bounding box (SPEC §8.2); features with empty or invalid
/// geometry are skipped and counted in [`BuildReport::skipped`]. The body
/// bytes are copied verbatim, so the output is always a valid FGB file.
pub fn build(body: &[u8]) -> Result<(Vec<u8>, BuildReport)> {
    build_with_options(body, &BuildOptions::default())
}

/// Build with optional BODY_ANALYTICS while preserving [`build`] defaults.
pub fn build_with_options(body: &[u8], options: &BuildOptions) -> Result<(Vec<u8>, BuildReport)> {
    if options.geometry_metrics && !options.analytics {
        return Err(Error::Format(
            "geometry metrics require BODY_ANALYTICS".to_string(),
        ));
    }
    #[cfg(not(feature = "analytics"))]
    if options.analytics {
        return Err(Error::Format("analytics support disabled".to_string()));
    }
    let mut cursor = Cursor::new(body);
    let fgb = FgbReader::open(&mut cursor)
        .map_err(|e| Error::Format(format!("body is not a valid FlatGeobuf: {e}")))?;
    let header = fgb.header();
    if header.features_count() == 0 {
        return Err(Error::Format(
            "body header has no features_count (SPEC §4 requires it); rebuild the FlatGeobuf with a feature count".to_string(),
        ));
    }
    if header.index_node_size() == 0 {
        return Err(Error::Format(
            "body header has no spatial index (SPEC §4 requires it); rebuild the FlatGeobuf with an index".to_string(),
        ));
    }
    #[cfg(feature = "analytics")]
    let chunk_rows = effective_chunk_rows(header.features_count(), options.chunk_rows);
    let mut iter = fgb
        .select_all()
        .map_err(|e| Error::Format(format!("cannot iterate body features: {e}")))?;

    let mut midpoints: Vec<(f64, f64)> = Vec::new();
    let mut skipped = 0usize;
    while let Some(feature) = iter
        .next()
        .map_err(|e| Error::Format(format!("cannot read body feature: {e}")))?
    {
        bin_feature(feature, &mut midpoints, &mut skipped);
    }

    let count = midpoints.len() as u64;
    #[cfg(feature = "analytics")]
    let analytics = if options.analytics {
        let section_start = u64::try_from(body.len())
            .ok()
            .and_then(|len| len.checked_add(4))
            .ok_or_else(|| Error::Format("analytics section offset overflows".to_string()))?;
        crate::analytics::encode_section(body, section_start, chunk_rows, options.geometry_metrics)?
    } else {
        None
    };
    #[cfg(feature = "analytics")]
    let (analytics, geometry, attribute_columns) = match analytics {
        Some((bytes, geometry, columns)) => (Some(bytes), geometry, columns),
        None => (None, None, 0),
    };
    #[cfg(not(feature = "analytics"))]
    let (analytics, geometry, attribute_columns) = (None, None, 0);
    let (trailer, bands, _binned) =
        trailer_at_with_analytics(midpoints, body.len() as u64, analytics, geometry)?;
    let mut out = Vec::with_capacity(body.len() + trailer.len());
    out.extend_from_slice(body);
    out.extend_from_slice(&trailer);
    Ok((
        out,
        BuildReport {
            features: count,
            bands,
            skipped,
            trailer_bytes: trailer.len(),
            attribute_columns,
            analytics_requested: options.analytics,
        },
    ))
}

/// Bin one feature by its geometry midpoint, or count it as skipped.
fn bin_feature(
    feature: &flatgeobuf::FgbFeature,
    midpoints: &mut Vec<(f64, f64)>,
    skipped: &mut usize,
) {
    let mut sink = BboxSink::new();
    match feature.process_geom(&mut sink) {
        Ok(()) => match sink.midpoint() {
            Some(mid) => midpoints.push(mid),
            None => *skipped += 1,
        },
        Err(_) => *skipped += 1,
    }
}

struct PreparedStreaming<R: Read + Seek> {
    shared: SharedReader<BufReader<R>>,
    midpoints: Vec<(f64, f64)>,
    skipped: usize,
    #[cfg(feature = "analytics")]
    sections: Option<crate::analytics::BuiltSections>,
    #[cfg(feature = "analytics")]
    attribute_columns: u32,
}

fn prepare_streaming<R: Read + Seek>(
    reader: R,
    body_len: u64,
    options: &BuildOptions,
    spooled: bool,
) -> Result<PreparedStreaming<R>> {
    if options.geometry_metrics && !options.analytics {
        return Err(Error::Format(
            "geometry metrics require BODY_ANALYTICS".to_string(),
        ));
    }
    #[cfg(not(feature = "analytics"))]
    if options.analytics {
        return Err(Error::Format("analytics support disabled".to_string()));
    }
    let shared = SharedReader::new(BufReader::new(reader));
    let fgb = FgbReader::open(shared.handle())
        .map_err(|e| Error::Format(format!("body is not a valid FlatGeobuf: {e}")))?;
    let header = fgb.header();
    if header.features_count() == 0 {
        return Err(Error::Format(
            "body header has no features_count (SPEC §4 requires it); rebuild the FlatGeobuf with a feature count".to_string(),
        ));
    }
    if header.index_node_size() == 0 {
        return Err(Error::Format(
            "body header has no spatial index (SPEC §4 requires it); rebuild the FlatGeobuf with an index".to_string(),
        ));
    }
    #[cfg(feature = "analytics")]
    let chunk_rows = effective_chunk_rows(header.features_count(), options.chunk_rows);
    let mut iter = fgb
        .select_all()
        .map_err(|e| Error::Format(format!("cannot iterate body features: {e}")))?;

    let mut midpoints: Vec<(f64, f64)> = Vec::new();
    let mut skipped = 0usize;
    while let Some(feature) = iter
        .next()
        .map_err(|e| Error::Format(format!("cannot read body feature: {e}")))?
    {
        bin_feature(feature, &mut midpoints, &mut skipped);
    }
    drop(iter);
    let actual_body_len = shared.position();
    if actual_body_len != body_len {
        return Err(Error::Format(format!(
            "FlatGeobuf body terminal {actual_body_len} does not match supplied body length {body_len}"
        )));
    }
    #[cfg(feature = "analytics")]
    let sections = if options.analytics {
        let section_start = body_len
            .checked_add(4)
            .ok_or_else(|| Error::Format("analytics section offset overflows".to_string()))?;
        crate::analytics::encode_section_data(
            shared.handle(),
            body_len,
            section_start,
            chunk_rows,
            options.geometry_metrics,
            spooled,
        )?
    } else {
        None
    };
    #[cfg(feature = "analytics")]
    let attribute_columns = sections
        .as_ref()
        .map_or(0, |sections| sections.column_count);
    #[cfg(not(feature = "analytics"))]
    let _ = (body_len, spooled);
    Ok(PreparedStreaming {
        shared,
        midpoints,
        skipped,
        #[cfg(feature = "analytics")]
        sections,
        #[cfg(feature = "analytics")]
        attribute_columns,
    })
}

fn assemble_prepared<R: Read + Seek>(
    prepared: &mut PreparedStreaming<R>,
    body_len: u64,
    out: &mut dyn Write,
) -> Result<TrailerSummary> {
    #[cfg(feature = "analytics")]
    if let Some(sections) = prepared.sections.as_mut() {
        let crate::analytics::BuiltSections {
            analytics,
            geometry,
            ..
        } = sections;
        return assemble_trailer(
            &prepared.midpoints,
            body_len,
            Some(analytics),
            geometry
                .as_mut()
                .map(|section| section as &mut dyn SectionPayload),
            out,
        );
    }
    assemble_trailer(&prepared.midpoints, body_len, None, None, out)
}

fn prepared_report<R: Read + Seek>(
    prepared: &PreparedStreaming<R>,
    options: &BuildOptions,
    bands: Vec<u8>,
    trailer_bytes: usize,
) -> BuildReport {
    #[cfg(feature = "analytics")]
    let attribute_columns = prepared.attribute_columns;
    #[cfg(not(feature = "analytics"))]
    let attribute_columns = 0;
    BuildReport {
        features: prepared.midpoints.len() as u64,
        bands,
        skipped: prepared.skipped,
        trailer_bytes,
        attribute_columns,
        analytics_requested: options.analytics,
    }
}

#[cfg(test)]
thread_local! {
    static VEC_TRAILER_CALLS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(all(test, feature = "analytics"))]
fn vec_trailer_call_count() -> usize {
    VEC_TRAILER_CALLS.with(Cell::get)
}

/// Streaming trailer build: read features from any `Read + Seek` FlatGeobuf
/// source and return the FGBA trailer bytes for the body of `body_len` bytes,
/// without materializing the body in memory.
///
/// Band construction retains one midpoint vector (16 bytes per valid feature).
/// Analytics uses a second seekable pass with property and R-tree-leaf buffers
/// bounded by one chunk; this Vec-returning API retains memory-backed chunks.
pub fn build_trailer_streaming<R: Read + Seek>(
    reader: R,
    body_len: u64,
    options: &BuildOptions,
) -> Result<(Vec<u8>, BuildReport)> {
    #[cfg(test)]
    VEC_TRAILER_CALLS.with(|count| count.set(count.get() + 1));
    let mut prepared = prepare_streaming(reader, body_len, options, false)?;
    let mut trailer = Vec::new();
    let summary = assemble_prepared(&mut prepared, body_len, &mut trailer)?;
    let trailer_bytes = trailer.len();
    let report = prepared_report(&prepared, options, summary.bands, trailer_bytes);
    Ok((trailer, report))
}

/// Build a complete FGBA file from a seekable FlatGeobuf body and write it
/// directly to `writer`, without materializing complete sections or the trailer.
/// The body is copied exactly `body_len` bytes. Arrow chunks are spooled to
/// anonymous temporary files; band records use one temporary Vec per band.
pub fn build_to_writer<R: Read + Seek, W: Write>(
    reader: R,
    body_len: u64,
    options: &BuildOptions,
    writer: W,
) -> Result<BuildReport> {
    let mut prepared = prepare_streaming(reader, body_len, options, true)?;
    let mut writer = writer;
    let mut source = prepared.shared.handle();
    source.seek(SeekFrom::Start(0))?;
    copy_exact(&mut source, body_len, &mut writer, "body")?;
    let summary = assemble_prepared(&mut prepared, body_len, &mut writer)?;
    writer.flush()?;
    let trailer_bytes = usize::try_from(summary.bytes)
        .map_err(|_| Error::Format("trailer size does not fit this platform".into()))?;
    Ok(prepared_report(
        &prepared,
        options,
        summary.bands,
        trailer_bytes,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_chunk_rows_targets_8k_chunks() {
        assert_eq!(effective_chunk_rows(847_984, 0), 4096); // small: floor
        assert_eq!(effective_chunk_rows(255_381_003, 0), 1 << 15);
        assert_eq!(effective_chunk_rows(u64::MAX / 2, 0), 1 << 20); // cap
        assert_eq!(effective_chunk_rows(255_381_003, 4096), 4096); // explicit wins
    }

    #[test]
    fn adaptive_chunk_rows_handles_zero_boundaries_and_overflow() {
        const FLOOR_THRESHOLD: u64 = 4096 * 8192;

        assert_eq!(BuildOptions::default().chunk_rows, 0);
        assert_eq!(effective_chunk_rows(0, 0), 4096);
        assert_eq!(effective_chunk_rows(1, 0), 4096);
        assert_eq!(effective_chunk_rows(FLOOR_THRESHOLD, 0), 4096);
        assert_eq!(effective_chunk_rows(FLOOR_THRESHOLD + 1, 0), 8192);
        assert_eq!(effective_chunk_rows(u64::MAX, 0), 1 << 20);
        assert_eq!(effective_chunk_rows(u64::MAX, u32::MAX), u32::MAX);
    }

    #[test]
    fn band_ladder_follows_spec() {
        // SPEC Appendix A: 12 features → 4*12 = 48 <= get_num_cells(2) = 240.
        assert_eq!(band_ladder(12), vec![2]);
        assert_eq!(band_ladder(0), vec![2]);
        assert_eq!(band_ladder(1), vec![2]);
        // 4*count exceeds res 2 (240) at 61 features: base moves to res 3
        // (960 cells), and the ladder keeps only res >= 2.
        assert_eq!(band_ladder(61), vec![3]);
        assert_eq!(band_ladder(241), vec![2, 4]); // 4*241 = 964 > 960 → base 4, then res 2
                                                  // Large counts: base capped at 16, at most 6 bands, step 2, ascending.
        assert_eq!(band_ladder(1_000_000_000), vec![4, 6, 8, 10, 12, 14]);
        assert_eq!(band_ladder(1_000_000_000_000), vec![6, 8, 10, 12, 14, 16]);
    }

    #[test]
    fn trailer_for_is_deterministic_and_self_consistent() {
        let points = [(2.35f64, 48.85f64), (-0.11, 51.51), (-73.99, 40.72)];
        let a = trailer_for(points.iter().copied(), Some(3)).unwrap_or_default();
        let b = trailer_for(points.iter().copied(), None).unwrap_or_default();
        assert_eq!(a, b, "identical input must produce identical bytes");
        assert_eq!(&a[0..4], &SENTINEL);
        // Standalone trailer: footer sits at len-32 and points within it.
        let footer = Footer::decode(&a[a.len() - 32..]).unwrap_or(Footer {
            dir_offset: 0,
            dir_size: 0,
            dir_crc32: 0,
        });
        assert!(footer.dir_offset >= 4 && footer.dir_offset < a.len() as u64);
        let dir = Directory::decode(
            &a[footer.dir_offset as usize..(footer.dir_offset + footer.dir_size) as usize],
        );
        assert_eq!(dir.ok().map(|d| d.sections.len()), Some(1));
    }

    #[test]
    fn sort_based_bands_match_btreemap_bytes() -> Result<()> {
        // Deterministic pseudo-random points; the fixture was generated by the
        // original per-band BTreeMap aggregation before replacing it.
        let mut state = 0x2545F4914F6CDD1Du64;
        let mut rng = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let pts: Vec<(f64, f64)> = (0..50_000)
            .map(|_| {
                (
                    (rng() % 360_000) as f64 / 1000.0 - 180.0,
                    (rng() % 180_000) as f64 / 1000.0 - 90.0,
                )
            })
            .collect();
        let before = include_bytes!("../tests/fixtures/trailer_50k_golden.bin");
        let after = trailer_for(pts.into_iter(), Some(50_000))?;
        assert_eq!(after.as_slice(), &before[..]);
        Ok(())
    }

    #[cfg(feature = "analytics")]
    struct TestPoint(f64, f64);

    #[cfg(feature = "analytics")]
    impl GeozeroGeometry for TestPoint {
        fn process_geom<P: flatgeobuf::geozero::GeomProcessor>(
            &self,
            processor: &mut P,
        ) -> flatgeobuf::geozero::error::Result<()> {
            processor.point_begin(0)?;
            processor.xy(self.0, self.1, 0)?;
            processor.point_end(0)
        }
    }

    #[cfg(feature = "analytics")]
    fn spool_body() -> std::result::Result<Vec<u8>, Box<dyn std::error::Error>> {
        use flatgeobuf::geozero::{ColumnValue, PropertyProcessor};
        use flatgeobuf::{ColumnType, FgbWriter, GeometryType};

        let mut writer = FgbWriter::create("spool", GeometryType::Point)?;
        writer.add_column("name", ColumnType::String, |_, _| {});
        for row in 0..4 {
            let name = format!("city{row}");
            let mut property_error = None;
            writer.add_feature_geom(TestPoint(row as f64, row as f64), |feature| {
                if let Err(error) = feature.property(0, "name", &ColumnValue::String(&name)) {
                    property_error = Some(error);
                }
            })?;
            if let Some(error) = property_error {
                return Err(error.into());
            }
        }
        let mut body = Vec::new();
        writer.write(&mut body)?;
        Ok(body)
    }

    #[cfg(feature = "analytics")]
    struct FailAfter {
        remaining: usize,
    }

    #[cfg(feature = "analytics")]
    impl Write for FailAfter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.is_empty() {
                return Ok(0);
            }
            if self.remaining == 0 {
                return Err(std::io::Error::other("injected file-output failure"));
            }
            let written = bytes.len().min(self.remaining);
            self.remaining -= written;
            Ok(written)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[cfg(feature = "analytics")]
    #[test]
    fn file_build_spools_cleans_up_and_never_calls_vec_trailer(
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let body = spool_body()?;
        let options = BuildOptions {
            analytics: true,
            geometry_metrics: true,
            chunk_rows: 2,
        };
        let (expected, expected_report) = build_with_options(&body, &options)?;
        let vec_calls = vec_trailer_call_count();
        let created = crate::analytics::spool_create_count();
        let live = crate::analytics::live_spool_count();
        let mut actual = Vec::new();
        let report = build_to_writer(Cursor::new(&body), body.len() as u64, &options, &mut actual)?;
        assert_eq!(actual, expected);
        assert_eq!(report, expected_report);
        assert_eq!(vec_trailer_call_count(), vec_calls);
        assert_eq!(crate::analytics::spool_create_count(), created + 8);
        assert_eq!(crate::analytics::live_spool_count(), live);

        let error = match build_to_writer(
            Cursor::new(&body),
            body.len() as u64,
            &options,
            FailAfter {
                remaining: body.len() + 4 + 32,
            },
        ) {
            Ok(_) => return Err("injected file output unexpectedly succeeded".into()),
            Err(error) => error,
        };
        assert!(matches!(error, Error::Io(_)));
        assert_eq!(vec_trailer_call_count(), vec_calls);
        assert_eq!(crate::analytics::spool_create_count(), created + 16);
        assert_eq!(crate::analytics::live_spool_count(), live);
        Ok(())
    }
}
