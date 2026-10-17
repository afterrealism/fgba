//! FGBA reader over any `Read + Seek` byte source (SPEC §9.1).
//!
//! Opening a source is one small read at EOF (the footer) plus one directory
//! read, CRC-verified. Bands are read whole per section and binary-searched
//! per covering cell. The FlatGeobuf body is never parsed here: it belongs
//! to the caller via the `flatgeobuf` crate (see [`Reader::body`]).
//!
//! HTTP range reading and a wasm two-phase reader are separate layers
//! (SPEC §9.2–9.3) and intentionally not in this core module.

use crate::band::AggBand;
use crate::error::{Error, Result};
use crate::format::{
    crc32, Directory, Footer, FOOTER_SIZE, KIND_AGGREGATE_BAND, MAX_DIR_SIZE, SENTINEL,
};
use std::io::{Read, Seek, SeekFrom};

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

/// An FGBA reader over a `Read + Seek` source (a file, an in-memory cursor,
/// or any custom byte source such as an HTTP range reader).
pub struct Reader<R: Read + Seek> {
    inner: R,
    directory: Directory,
    /// Byte offset where the FlatGeobuf body ends and the trailer sentinel
    /// begins, derived from the first section (or the directory when the
    /// trailer holds no sections).
    body_len: u64,
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

        // Sections must lie inside the trailer (before the directory) and
        // must not overlap (SPEC §5: overlapping or out-of-file entries make
        // the trailer invalid).
        let mut spans: Vec<(u64, u64)> = Vec::with_capacity(directory.sections.len());
        for entry in &directory.sections {
            let end = entry
                .offset
                .checked_add(entry.size)
                .ok_or_else(|| Error::Format("section offset + size overflows".to_string()))?;
            if end > footer.dir_offset {
                return Err(Error::Format(format!(
                    "section [{}, {end}) lies outside the trailer (directory at {})",
                    entry.offset, footer.dir_offset
                )));
            }
            spans.push((entry.offset, end));
        }
        spans.sort_unstable();
        for pair in spans.windows(2) {
            if pair[0].1 > pair[1].0 {
                return Err(Error::Format(format!(
                    "sections at {:#x} and {:#x} overlap",
                    pair[0].0, pair[1].0
                )));
            }
        }

        let first_section = spans
            .first()
            .map(|&(start, _)| start)
            .unwrap_or(footer.dir_offset);
        let body_len = first_section.saturating_sub(SENTINEL.len() as u64);
        Ok(Reader {
            inner,
            directory,
            body_len,
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
    /// (SPEC §6 reader rules).
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
    /// Viewport latitudes of exactly ±90° (or longitudes spanning the
    /// antimeridian) can make the A5 covering degenerate; clamp the viewport
    /// to a hair inside the poles (e.g. ±85°) for a world view.
    pub fn query(&mut self, view: [f64; 4], res: u8) -> Result<Vec<(u64, u32)>> {
        let band = self.read_band(res)?;
        let covering = covering(&view, res)?;
        Ok(band.query(&covering))
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

/// A5 covering of a viewport at `res` (uncompacted, curve-sorted), with
/// overlapping containment (SPEC §9.4).
fn covering(view: &[f64; 4], res: u8) -> Result<Vec<u64>> {
    let [min_x, min_y, max_x, max_y] = *view;
    if ![min_x, min_y, max_x, max_y].iter().all(|v| v.is_finite()) {
        return Err(Error::Format(format!(
            "viewport coordinates must be finite, got {view:?}"
        )));
    }
    let ring = vec![
        a5::LonLat::new(min_x, min_y),
        a5::LonLat::new(max_x, min_y),
        a5::LonLat::new(max_x, max_y),
        a5::LonLat::new(min_x, max_y),
    ];
    let compacted = a5::polygon_to_cells(
        &[ring],
        i32::from(res),
        Some(a5::PolygonToCellsOptions {
            containment: a5::Containment::Overlapping,
        }),
    )
    .map_err(Error::A5)?;
    a5::uncompact(&compacted).map_err(Error::A5)
}
