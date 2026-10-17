//! FGBA writer: bin features into aggregate bands and assemble the trailer
//! (sentinel, band sections, directory, footer) per SPEC §8.
//!
//! Builds are deterministic: identical input produces byte-identical output
//! (records come from a `BTreeMap`, so no hash-ordering leaks in).

use crate::band::AggBand;
use crate::error::{Error, Result};
use crate::format::{crc32, Directory, Footer, SectionEntry, KIND_AGGREGATE_BAND, SENTINEL};
use flatgeobuf::geozero::{error::Result as GeozeroResult, GeomProcessor, GeozeroGeometry};
use flatgeobuf::{FallibleStreamingIterator, FgbReader};
use std::collections::BTreeMap;
use std::io::Cursor;

/// Maximum A5 resolution chosen as the base (finest) band (SPEC §8.3).
pub const MAX_BASE_RES: u8 = 16;

/// Minimum band resolution; bands stop descending at 2 (SPEC §8.4).
pub const MIN_BAND_RES: u8 = 2;

/// Maximum number of bands per file (SPEC §8.4).
pub const MAX_BANDS: usize = 6;

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
    let (bytes, _, _) = trailer_at(midpoints, total_hint, 0)?;
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

/// Assemble the trailer with directory offsets relative to `base_offset`
/// (the body length). Returns the trailer bytes, the band resolutions
/// (ascending), and the number of midpoints successfully binned per band.
fn trailer_at(
    midpoints: impl Iterator<Item = (f64, f64)>,
    total_hint: Option<u64>,
    base_offset: u64,
) -> Result<(Vec<u8>, Vec<u8>, u64)> {
    // A wrong (hostile or stale) hint must never cause a huge allocation.
    let cap = total_hint
        .and_then(|h| usize::try_from(h).ok())
        .unwrap_or(0)
        .min(1 << 20);
    let mut points: Vec<(f64, f64)> = Vec::with_capacity(cap);
    points.extend(midpoints);
    let ladder = band_ladder(points.len() as u64);

    let mut out = Vec::new();
    out.extend_from_slice(&SENTINEL);
    let mut entries = Vec::with_capacity(ladder.len());
    let mut binned_total = 0u64;
    for (i, &res) in ladder.iter().enumerate() {
        let mut map: BTreeMap<u64, u32> = BTreeMap::new();
        for &(lon, lat) in &points {
            if let Ok(cell) = a5::lonlat_to_cell(a5::LonLat::new(lon, lat), i32::from(res)) {
                *map.entry(cell).or_insert(0) += 1;
            }
        }
        let total: u64 = map.values().map(|&c| u64::from(c)).sum();
        if i == 0 {
            binned_total = total;
        }
        let band = AggBand {
            res,
            total,
            cells: map.into_iter().collect(), // BTreeMap iterates ascending
        };
        let bytes = band.to_bytes()?;
        let offset = base_offset + out.len() as u64;
        entries.push(SectionEntry {
            kind: KIND_AGGREGATE_BAND,
            flags: 0,
            offset,
            size: bytes.len() as u64,
            meta: vec![res],
        });
        out.extend_from_slice(&bytes);
    }

    let dir_offset = base_offset + out.len() as u64;
    let dir_bytes = Directory { sections: entries }.encode()?;
    let footer = Footer {
        dir_offset,
        dir_size: dir_bytes.len() as u64,
        dir_crc32: crc32(&dir_bytes),
    };
    out.extend_from_slice(&dir_bytes);
    out.extend_from_slice(&footer.encode());
    Ok((out, ladder, binned_total))
}

/// Geometry processor collecting the bounding box of one feature.
///
/// Non-finite coordinates are ignored; a feature whose geometry yields no
/// finite coordinate (empty geometries error out of geozero and are skipped
/// by the caller) ends with `count == 0`.
struct BboxSink {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
    count: u64,
}

impl BboxSink {
    fn new() -> Self {
        BboxSink {
            min_x: f64::INFINITY,
            min_y: f64::INFINITY,
            max_x: f64::NEG_INFINITY,
            max_y: f64::NEG_INFINITY,
            count: 0,
        }
    }

    /// The bounding-box midpoint, or `None` when no coordinate was seen.
    /// Written overflow-free (`min + (max - min) / 2`) for hostile input.
    fn midpoint(&self) -> Option<(f64, f64)> {
        if self.count == 0 {
            return None;
        }
        Some((
            self.min_x + (self.max_x - self.min_x) / 2.0,
            self.min_y + (self.max_y - self.min_y) / 2.0,
        ))
    }
}

impl GeomProcessor for BboxSink {
    fn xy(&mut self, x: f64, y: f64, _idx: usize) -> GeozeroResult<()> {
        if x.is_finite() && y.is_finite() {
            self.min_x = self.min_x.min(x);
            self.min_y = self.min_y.min(y);
            self.max_x = self.max_x.max(x);
            self.max_y = self.max_y.max(y);
            self.count += 1;
        }
        Ok(())
    }
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
    let mut iter = fgb
        .select_all()
        .map_err(|e| Error::Format(format!("cannot iterate body features: {e}")))?;

    let mut midpoints: Vec<(f64, f64)> = Vec::new();
    let mut skipped = 0usize;
    while let Some(feature) = iter
        .next()
        .map_err(|e| Error::Format(format!("cannot read body feature: {e}")))?
    {
        let mut sink = BboxSink::new();
        match feature.process_geom(&mut sink) {
            Ok(()) => match sink.midpoint() {
                Some(mid) => midpoints.push(mid),
                None => skipped += 1,
            },
            Err(_) => skipped += 1,
        }
    }

    let count = midpoints.len() as u64;
    let (trailer, bands, _binned) =
        trailer_at(midpoints.iter().copied(), Some(count), body.len() as u64)?;
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
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn bbox_sink_tracks_min_max() {
        let mut sink = BboxSink::new();
        assert_eq!(sink.midpoint(), None);
        let coords = [(2.0, 48.0), (4.0, 52.0), (3.0, 49.0)];
        for (i, &(x, y)) in coords.iter().enumerate() {
            sink.xy(x, y, i).unwrap_or(());
        }
        assert_eq!(sink.midpoint(), Some((3.0, 50.0)));
        // Non-finite coordinates are ignored, never poisoning the bounds.
        sink.xy(f64::NAN, f64::INFINITY, 3).unwrap_or(());
        assert_eq!(sink.midpoint(), Some((3.0, 50.0)));
    }
}
