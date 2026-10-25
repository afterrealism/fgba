//! Reusable native SQL over local FlatGeobuf and FGBA files.

#![forbid(unsafe_code)]

pub mod error;
pub mod output;
pub mod plan;
pub mod prune;
pub mod schema;
pub mod session;
pub mod stream;
pub mod table;
mod udf_geom;

pub use error::{Error, Result};
pub use output::query_csv;
pub use schema::OutputSchema;
pub use session::{FgbaSession, QueryOptions};
pub use stream::ScanMetrics;
pub use table::FgbaTable;
