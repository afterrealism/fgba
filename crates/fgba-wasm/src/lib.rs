//! Sparse signed-range FGBA visualization for browser workers.

#![forbid(unsafe_code)]

pub mod arrow;
pub mod range;
mod reader;
pub mod sql;

pub use reader::SparseDataset;
