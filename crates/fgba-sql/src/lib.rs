//! Reusable DataFusion SQL over local FlatGeobuf/FGBA files and memory batches.

#![forbid(unsafe_code)]

pub mod batch;
pub mod error;
pub mod memory;
#[cfg(not(target_arch = "wasm32"))]
pub mod output;
#[cfg(not(target_arch = "wasm32"))]
pub mod plan;
pub mod prune;
pub mod schema;
#[cfg(not(target_arch = "wasm32"))]
pub mod session;
#[cfg(not(target_arch = "wasm32"))]
pub mod stream;
#[cfg(not(target_arch = "wasm32"))]
pub mod table;
mod udf_geom;

pub use error::{Error, Result};
pub use memory::EagerSession;
#[cfg(not(target_arch = "wasm32"))]
pub use output::query_csv;
pub use schema::OutputSchema;
#[cfg(not(target_arch = "wasm32"))]
pub use session::{FgbaSession, QueryOptions};
#[cfg(not(target_arch = "wasm32"))]
pub use stream::ScanMetrics;
#[cfg(not(target_arch = "wasm32"))]
pub use table::FgbaTable;
