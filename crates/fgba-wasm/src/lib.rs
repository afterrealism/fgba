//! Sparse signed-range FGBA visualization for browser workers.

#![forbid(unsafe_code)]

pub mod range;
mod reader;

pub use reader::SparseDataset;
