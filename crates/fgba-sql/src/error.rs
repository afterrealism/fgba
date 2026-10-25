//! Errors exposed by the SQL layer.

use datafusion::error::DataFusionError;

/// SQL registration, decoding, planning, and output failures.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Local file or writer I/O failed.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    /// FGBA trailer, band, or analytical-column access failed.
    #[error(transparent)]
    Fgba(#[from] fgba::Error),
    /// FlatGeobuf header or feature decoding failed.
    #[error("FlatGeobuf error: {0}")]
    FlatGeobuf(#[from] flatgeobuf::Error),
    /// Arrow array, IPC, or CSV encoding failed.
    #[error("Arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    /// DataFusion planning or execution failed.
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    /// A caller supplied an invalid SQL or query option.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// Input bytes or their runtime schema are inconsistent.
    #[error("format error: {0}")]
    Format(String),
}

/// Convenient result alias for this crate.
pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for DataFusionError {
    fn from(error: Error) -> Self {
        match error {
            Error::DataFusion(error) => error,
            other => DataFusionError::Execution(other.to_string()),
        }
    }
}
