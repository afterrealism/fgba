//! Error type shared by the FGBA reader and writer.

use thiserror::Error;

/// Errors returned by FGBA reads and writes.
#[derive(Debug, Error)]
pub enum Error {
    /// Underlying I/O failure while reading or writing bytes.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    /// The file has no FGBA footer magic; it is a plain FlatGeobuf file.
    ///
    /// Per SPEC §10.2 a reader should fall back to reading the body as plain
    /// FlatGeobuf; the body is never hostage to the trailer.
    #[error("not an FGBA file (missing FGBAGG01 footer magic)")]
    NotFgba,
    /// The trailer (footer, directory, or a band section) is malformed,
    /// truncated, or fails validation.
    #[error("invalid FGBA trailer: {0}")]
    Format(String),
    /// The A5 grid library rejected a coordinate, cell, or covering.
    #[error("a5 grid error: {0}")]
    A5(String),
}

/// Convenience alias for FGBA results.
pub type Result<T> = std::result::Result<T, Error>;
