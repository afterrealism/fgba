//! In-memory byte source for two-phase reads (SPEC §9.3).
//!
//! Non-blocking environments (wasm) cannot drive a blocking `Read + Seek`
//! source over the network: they compute the byte ranges they need (footer,
//! directory, band slice), fetch them asynchronously, and only then decode.
//! [`SliceReader`] adapts such fetched bytes to `Read + Seek`, so the same
//! [`Reader`](crate::Reader) used for local files and HTTP range reading
//! works unchanged on them.

use std::io::{Cursor, Read, Seek, SeekFrom};

/// A `Read + Seek` source over in-memory bytes (SPEC §9.3).
///
/// This is a named, stable-API wrapper over `std::io::Cursor<Vec<u8>>`: the
/// two-phase wasm reader fetches its ranges into a `Vec<u8>` and hands them
/// here; `Reader<SliceReader>` then decodes footers, directories, and bands
/// exactly as it would from a file.
#[derive(Debug, Clone, Default)]
pub struct SliceReader {
    inner: Cursor<Vec<u8>>,
}

impl SliceReader {
    /// Wrap fetched bytes as a seekable source, positioned at offset 0.
    pub fn new(bytes: Vec<u8>) -> SliceReader {
        SliceReader {
            inner: Cursor::new(bytes),
        }
    }

    /// Total number of bytes in the source.
    pub fn len(&self) -> u64 {
        self.inner.get_ref().len() as u64
    }

    /// Whether the source holds no bytes.
    pub fn is_empty(&self) -> bool {
        self.inner.get_ref().is_empty()
    }
}

impl Read for SliceReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Seek for SliceReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}
