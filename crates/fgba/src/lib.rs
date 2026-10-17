//! FGBA — FlatGeobuf Aggregates: a valid FlatGeobuf file plus an aggregate trailer.
//!
//! Specification: <https://github.com/afterrealism/fgba/blob/main/SPEC.md>.
//! Implemented in Task A2; this crate root exists so the workspace compiles.
#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    clippy::todo,
    clippy::unreachable
)]
#![warn(missing_docs)]

/// Format version written into the directory.
pub const DIRECTORY_VERSION: u8 = 1;
