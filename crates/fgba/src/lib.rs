//! FGBA — FlatGeobuf Aggregates: a valid FlatGeobuf file plus an aggregate
//! trailer of curve-sorted `(A5 cell, count)` bands.
//!
//! Specification: <https://github.com/afterrealism/fgba/blob/main/SPEC.md>
//! (Draft v1). The file layout is
//!
//! ```text
//! [FlatGeobuf body] [sentinel FF FF FF FF] [band sections] [directory] [32-byte footer]
//! ```
//!
//! so every FGBA file is also a valid FlatGeobuf file. Readers open a file
//! with one small read at EOF (the footer) plus one CRC-verified directory
//! read; aggregate queries then read whole band sections and binary-search
//! them, never touching feature bytes.
//!
//! Reading is transport-agnostic: local files and any `Read + Seek` source
//! work out of the box, the `http` feature adds `HttpRangeReader` for HTTP
//! range requests (SPEC §9.2), and [`SliceReader`] adapts asynchronously
//! fetched bytes for non-blocking environments such as wasm (SPEC §9.3).
//!
//! # Example
//!
//! Build a tiny FlatGeobuf in memory, append the aggregate trailer, and run
//! a count query over a viewport:
//!
//! ```
//! use flatgeobuf::geozero::{GeomProcessor, GeozeroGeometry};
//! use flatgeobuf::{FgbWriter, GeometryType};
//! use std::io::Cursor;
//!
//! // A minimal point geometry for the FlatGeobuf writer.
//! struct Point(f64, f64);
//! impl GeozeroGeometry for Point {
//!     fn process_geom<P: GeomProcessor>(&self, p: &mut P) -> flatgeobuf::geozero::error::Result<()> {
//!         p.point_begin(0)?;
//!         p.xy(self.0, self.1, 0)?;
//!         p.point_end(0)
//!     }
//! }
//!
//! // Write a three-point FlatGeobuf body (Paris, London, New York).
//! let mut writer = FgbWriter::create("cities", GeometryType::Point)?;
//! writer.add_feature_geom(Point(2.35, 48.85), |_feat| {})?;
//! writer.add_feature_geom(Point(-0.11, 51.51), |_feat| {})?;
//! writer.add_feature_geom(Point(-73.99, 40.72), |_feat| {})?;
//! let mut body = Vec::new();
//! writer.write(&mut body)?;
//!
//! // Append the aggregate trailer: one band at A5 resolution 2.
//! let (file, report) = fgba::build(&body)?;
//! assert_eq!(report.features, 3);
//! assert_eq!(report.bands, vec![2]);
//!
//! // Open it back and answer "how many features in this viewport".
//! let mut reader = fgba::Reader::open(Cursor::new(file))?;
//! assert_eq!(reader.bands().len(), 1);
//! let stats = reader.stats([-180.0, -85.0, 180.0, 85.0], 2)?;
//! assert_eq!(stats.total, 3);
//! assert_eq!(stats.cells, 2); // London + Paris share one res-2 cell
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
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

#[cfg(feature = "analytics")]
pub mod analytics;
pub mod band;
pub mod error;
pub mod format;
#[cfg(feature = "analytics")]
pub mod geom_analytics;
#[cfg(feature = "analytics")]
mod geom_metrics;
pub mod geometry;
#[cfg(feature = "http")]
pub mod http;
pub mod io;
pub mod read;
pub mod slice;
pub mod write;

#[cfg(feature = "analytics")]
pub use analytics::{
    ArrowChunk, BodyAnalytics, BodyRange, ChunkStat, ColumnAnalytics, ColumnDesc, GlobalStat,
    ValueClass,
};
pub use band::{ladder_resolution, AggBand, BandHeader};
pub use error::{Error, Result};
pub use format::{Directory, Footer, SectionEntry};
#[cfg(feature = "analytics")]
pub use geom_analytics::{GeometryMetrics, MetricColumn, MetricDesc};
#[cfg(feature = "analytics")]
pub use geom_metrics::{feature_metrics, FeatureMetrics, MetricsSink};
#[cfg(feature = "http")]
pub use http::HttpRangeReader;
pub use read::{BandMeta, BandStats, Reader};
pub use slice::SliceReader;
pub use write::{
    build, build_to_writer, build_trailer_streaming, build_with_options, effective_chunk_rows,
    trailer_for, BuildOptions, BuildReport,
};
