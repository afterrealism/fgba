//! Aggregate band sections: `(A5 cell, count)` records at one resolution
//! (SPEC §6), plus the viewport-ladder guidance (SPEC §9.4).
//!
//! A band is a lossless partition of the binned dataset: records are sorted
//! by ascending cell ID (A5's space-filling curve order, so viewport
//! coverings are a few contiguous runs) and the counts sum to
//! `total_features`.

use crate::error::{Error, Result};

/// Band section header size in bytes: grid (1) + res (1) + flags (2) +
/// total_features (8) + num_cells (8).
pub const BAND_HEADER_SIZE: usize = 20;

/// Record size in bytes for count-only bands (`band_flags == 0`):
/// `u64 cell_id` + `u32 count`.
pub const BAND_RECORD_SIZE: usize = 12;

/// `grid` value for the A5 pentagonal grid; the only grid in v1.
pub const GRID_A5: u8 = 1;

/// Largest A5 resolution (SPEC §6, per the A5 normative reference).
pub const MAX_RES: u8 = 30;

/// Aggregate band section header (SPEC §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BandHeader {
    /// Grid identifier; [`GRID_A5`] (`1`) is the only supported value.
    pub grid: u8,
    /// A5 resolution of the band, 0–30.
    pub res: u8,
    /// Band flags; v1 supports only `0` (count-only records).
    pub band_flags: u16,
    /// Features binned into this band; equals the sum of record counts.
    pub total_features: u64,
    /// Number of records that follow.
    pub num_cells: u64,
}

/// One aggregate band in memory: every occupied A5 cell at resolution
/// [`AggBand::res`], sorted by ascending cell ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggBand {
    /// A5 resolution of the band, 0–30.
    pub res: u8,
    /// Features binned into this band; equals the sum of all counts.
    pub total: u64,
    /// `(cell_id, count)` records, strictly ascending by `cell_id`,
    /// every `count > 0`.
    pub cells: Vec<(u64, u32)>,
}

impl AggBand {
    /// Encode as a complete band section (header + records, SPEC §6).
    ///
    /// Validates the same invariants as [`AggBand::from_bytes`] so writers
    /// cannot emit a section readers would reject.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        validate_records(self.res, &self.cells)?;
        let mut out = Vec::with_capacity(BAND_HEADER_SIZE + self.cells.len() * BAND_RECORD_SIZE);
        out.push(GRID_A5);
        out.push(self.res);
        out.extend_from_slice(&0u16.to_le_bytes()); // band_flags
        out.extend_from_slice(&self.total.to_le_bytes());
        out.extend_from_slice(&(self.cells.len() as u64).to_le_bytes());
        for &(cell_id, count) in &self.cells {
            out.extend_from_slice(&cell_id.to_le_bytes());
            out.extend_from_slice(&count.to_le_bytes());
        }
        Ok(out)
    }

    /// Decode and fully validate a band section (SPEC §6 reader rules):
    /// `grid == 1`, `res <= 30`, `band_flags == 0`, the section size equals
    /// header + `num_cells` records exactly, cell IDs are strictly ascending
    /// valid A5 cells at `res`, and every `count > 0`.
    ///
    /// `num_cells` is bounded by the actual section size before allocation.
    pub fn from_bytes(bytes: &[u8]) -> Result<AggBand> {
        if bytes.len() < BAND_HEADER_SIZE {
            return Err(Error::Format(format!(
                "band section truncated: {} bytes, need at least {BAND_HEADER_SIZE}",
                bytes.len()
            )));
        }
        let grid = bytes[0];
        if grid != GRID_A5 {
            return Err(Error::Format(format!(
                "unsupported band grid {grid} (want {GRID_A5})"
            )));
        }
        let res = bytes[1];
        if res > MAX_RES {
            return Err(Error::Format(format!(
                "band resolution {res} out of range (max {MAX_RES})"
            )));
        }
        let band_flags = u16::from_le_bytes([bytes[2], bytes[3]]);
        if band_flags != 0 {
            return Err(Error::Format(format!(
                "unsupported band_flags {band_flags} (v1 supports count-only bands)"
            )));
        }
        let read_u64 = |at: usize| -> u64 {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&bytes[at..at + 8]);
            u64::from_le_bytes(arr)
        };
        let header = BandHeader {
            grid,
            res,
            band_flags,
            total_features: read_u64(4),
            num_cells: read_u64(12),
        };
        // The record region must be exactly num_cells records long; this also
        // bounds num_cells by the real byte length before any allocation.
        let record_bytes = bytes.len() - BAND_HEADER_SIZE;
        if record_bytes % BAND_RECORD_SIZE != 0 {
            return Err(Error::Format(format!(
                "band record region {record_bytes} bytes is not a multiple of {BAND_RECORD_SIZE}"
            )));
        }
        let num_cells = record_bytes / BAND_RECORD_SIZE;
        if header.num_cells != num_cells as u64 {
            return Err(Error::Format(format!(
                "band declares {} cells but the section holds {num_cells}",
                header.num_cells
            )));
        }
        let mut cells = Vec::with_capacity(num_cells);
        let mut p = BAND_HEADER_SIZE;
        for _ in 0..num_cells {
            let cell_id = read_u64(p);
            let mut count_bytes = [0u8; 4];
            count_bytes.copy_from_slice(&bytes[p + 8..p + 12]);
            let count = u32::from_le_bytes(count_bytes);
            cells.push((cell_id, count));
            p += BAND_RECORD_SIZE;
        }
        validate_records(res, &cells)?;
        let total = header.total_features;
        let sum: u64 = cells.iter().map(|&(_, c)| u64::from(c)).sum();
        if sum != total {
            return Err(Error::Format(format!(
                "band counts sum to {sum}, not total_features {total}"
            )));
        }
        Ok(AggBand { res, total, cells })
    }

    /// Return the records whose cell ID is in `covering`, by binary search
    /// per covering cell, sorted by ascending cell ID with duplicates
    /// removed.
    pub fn query(&self, covering: &[u64]) -> Vec<(u64, u32)> {
        let mut hits: Vec<usize> = Vec::new();
        for &cell in covering {
            if let Ok(i) = self.cells.binary_search_by_key(&cell, |&(id, _)| id) {
                hits.push(i);
            }
        }
        hits.sort_unstable();
        hits.dedup();
        hits.iter().map(|&i| self.cells[i]).collect()
    }
}

/// Shared record validation for encoding and decoding.
fn validate_records(res: u8, cells: &[(u64, u32)]) -> Result<()> {
    if res > MAX_RES {
        return Err(Error::Format(format!(
            "band resolution {res} out of range (max {MAX_RES})"
        )));
    }
    let mut prev: Option<u64> = None;
    for &(cell_id, count) in cells {
        if count == 0 {
            return Err(Error::Format(format!(
                "band cell {cell_id:#x} has a zero count"
            )));
        }
        if prev.is_some_and(|p| cell_id <= p) {
            return Err(Error::Format(format!(
                "band cell IDs not strictly ascending at {cell_id:#x}"
            )));
        }
        if !a5::is_valid_cell(cell_id) {
            return Err(Error::Format(format!(
                "band cell {cell_id:#x} is not a valid A5 cell"
            )));
        }
        let cell_res = a5::get_resolution(cell_id);
        if cell_res != i32::from(res) {
            return Err(Error::Format(format!(
                "band cell {cell_id:#x} is a resolution-{cell_res} A5 cell, not resolution {res}"
            )));
        }
        prev = Some(cell_id);
    }
    Ok(())
}

/// Default rendering ladder (SPEC §9.4 guidance): map the viewport width in
/// degrees to the A5 band resolution a renderer should draw.
///
/// The mapping picks the coarsest band whose cells still cover roughly 48 px
/// of a typical view: `>60°` → res 2, `>16°` → res 3, `>4°` → res 5,
/// `>0.5°` → res 7, `>0.125°` → res 9, `>0.03°` → res 11, `>0.008°` → res 13,
/// otherwise res 15. Non-finite and negative widths map to the deepest band
/// (15), the conservative choice. The ladder continues to res 15 because the
/// build protocol (SPEC §8) permits a base resolution up to 16.
///
/// This is informative guidance, not a format rule; applications may pick
/// bands any way they like, and above the base resolution they should read
/// real features from the body.
pub fn ladder_resolution(view_width_deg: f64) -> u8 {
    if view_width_deg > 60.0 {
        2
    } else if view_width_deg > 16.0 {
        3
    } else if view_width_deg > 4.0 {
        5
    } else if view_width_deg > 0.5 {
        7
    } else if view_width_deg > 0.125 {
        9
    } else if view_width_deg > 0.03 {
        11
    } else if view_width_deg > 0.008 {
        13
    } else {
        15
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two res-2 cells of the normative test vector (SPEC Appendix A).
    const NYC: u64 = 0x2680_0000_0000_0000;
    const EUROPE: u64 = 0x6380_0000_0000_0000;

    fn vector_band() -> AggBand {
        AggBand {
            res: 2,
            total: 12,
            cells: vec![(NYC, 4), (EUROPE, 8)],
        }
    }

    #[test]
    fn band_round_trip_matches_spec_layout() {
        let bytes = vector_band().to_bytes().unwrap_or_default();
        assert_eq!(bytes.len(), 44);
        assert_eq!(&bytes[0..4], &[1, 2, 0, 0]);
        assert_eq!(
            u64::from_le_bytes(bytes[4..12].try_into().unwrap_or_default()),
            12
        );
        assert_eq!(
            u64::from_le_bytes(bytes[12..20].try_into().unwrap_or_default()),
            2
        );
        let back = AggBand::from_bytes(&bytes).ok();
        assert_eq!(back, Some(vector_band()));
    }

    #[test]
    fn query_binary_searches_the_covering() {
        let band = vector_band();
        assert_eq!(band.query(&[NYC]), vec![(NYC, 4)]);
        assert_eq!(band.query(&[EUROPE, NYC]), vec![(NYC, 4), (EUROPE, 8)]);
        assert_eq!(band.query(&[NYC, NYC]), vec![(NYC, 4)]); // duplicates counted once
        assert!(band.query(&[0x3380_0000_0000_0000]).is_empty()); // a res-2 cell, not occupied
        assert!(band.query(&[]).is_empty());
    }

    #[test]
    fn from_bytes_rejects_hostile_input() {
        let good = vector_band().to_bytes().unwrap_or_default();

        // Truncation at every point errors, never panics.
        for cut in 0..good.len() {
            assert!(AggBand::from_bytes(&good[..cut]).is_err(), "cut at {cut}");
        }
        // Trailing garbage breaks the exact-size rule.
        let mut padded = good.clone();
        padded.push(0);
        assert!(AggBand::from_bytes(&padded).is_err());

        let mut bad = good.clone();
        bad[0] = 7; // unknown grid
        assert!(AggBand::from_bytes(&bad).is_err());
        let mut bad = good.clone();
        bad[1] = 31; // res out of range
        assert!(AggBand::from_bytes(&bad).is_err());
        let mut bad = good.clone();
        bad[2] = 1; // attribute-aggregate records unsupported in v1
        assert!(AggBand::from_bytes(&bad).is_err());
        let mut bad = good.clone();
        bad[12] = 3; // num_cells inflated: 3 records do not fit 44 bytes
        assert!(AggBand::from_bytes(&bad).is_err());
        let mut bad = good.clone();
        bad[28] = 0; // first record's count := 0 (count starts at header 20 + cell 8)
        assert!(AggBand::from_bytes(&bad).is_err());

        // Unsorted cell IDs.
        let unsorted = AggBand {
            res: 2,
            total: 12,
            cells: vec![(EUROPE, 8), (NYC, 4)],
        };
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&good[..BAND_HEADER_SIZE]);
        for &(id, c) in &unsorted.cells {
            bytes.extend_from_slice(&id.to_le_bytes());
            bytes.extend_from_slice(&c.to_le_bytes());
        }
        assert!(AggBand::from_bytes(&bytes).is_err());
        // Encoding an unsorted/invalid band fails too.
        assert!(unsorted.to_bytes().is_err());

        // total_features mismatch with the record sum.
        let mut bad = good.clone();
        bad[4] = 13;
        assert!(AggBand::from_bytes(&bad).is_err());
    }

    #[test]
    fn ladder_matches_spec_guidance() {
        assert_eq!(ladder_resolution(120.0), 2);
        assert_eq!(ladder_resolution(61.0), 2);
        assert_eq!(ladder_resolution(60.0), 3);
        assert_eq!(ladder_resolution(32.0), 3);
        assert_eq!(ladder_resolution(16.0), 5);
        assert_eq!(ladder_resolution(8.0), 5);
        assert_eq!(ladder_resolution(4.0), 7);
        assert_eq!(ladder_resolution(1.0), 7);
        assert_eq!(ladder_resolution(0.5), 9);
        assert_eq!(ladder_resolution(0.125), 11);
        assert_eq!(ladder_resolution(0.03), 13);
        assert_eq!(ladder_resolution(0.008), 15);
        assert_eq!(ladder_resolution(0.0), 15);
        assert_eq!(ladder_resolution(-1.0), 15);
        assert_eq!(ladder_resolution(f64::NAN), 15);
        assert_eq!(ladder_resolution(f64::INFINITY), 2);
    }
}
