//! `fgba` — command line tool for FGBA (FlatGeobuf Aggregates) files:
//! convert source formats, build trailers, inspect, query, verify, serve
//! over HTTP, and benchmark aggregate queries. Library: crate `fgba`.

#![forbid(unsafe_code)]

mod bench;
mod convert;
mod query;
mod serve;

use arrow_ipc::reader::FileReader;
use arrow_schema::DataType;
use clap::{Parser, Subcommand};
use flatgeobuf::FgbReader;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use fgba::{BodyAnalytics, BuildOptions, Error as FgbaError, GeometryMetrics, Reader};

/// CLI entry point: dispatch and turn errors into one readable line + exit 1.
fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("error: {msg}");
            ExitCode::FAILURE
        }
    }
}

/// FGBA (FlatGeobuf Aggregates) command line tool.
#[derive(Parser)]
#[command(
    name = "fgba",
    version,
    about = "Build, inspect, query, verify, serve and benchmark FGBA (FlatGeobuf Aggregates) files"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

fn parse_chunk_rows(value: &str) -> Result<u32, String> {
    if value == "auto" {
        return Ok(0);
    }
    value
        .parse::<u32>()
        .ok()
        .filter(|&rows| rows > 0)
        .ok_or_else(|| "expected 'auto' or a positive integer".to_string())
}

fn parse_scan_threads(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|&threads| threads > 0)
        .ok_or_else(|| "expected a positive integer".to_string())
}

#[derive(Subcommand)]
enum Cmd {
    /// Append an FGBA aggregate trailer to a FlatGeobuf file
    Build {
        /// Input FlatGeobuf file (.fgb); an existing trailer is rebuilt
        input: PathBuf,
        /// Output file (default: input name ending in .a.fgb)
        #[arg(short, long)]
        out: Option<PathBuf>,
        /// Emit analytical property columns and statistics
        #[arg(long)]
        analytics: bool,
        /// Emit derived geometry metrics (also enables --analytics)
        #[arg(long, conflicts_with = "no_geometry_metrics")]
        geometry_metrics: bool,
        /// Do not emit geometry metrics when --analytics is present
        #[arg(long)]
        no_geometry_metrics: bool,
        /// Rows per analytical chunk, or 'auto' to adapt to the feature count
        #[arg(long, default_value = "auto", value_parser = parse_chunk_rows)]
        chunk_rows: u32,
    },
    /// Convert GeoJSON, CSV/TSV, Shapefile or FlatGeobuf into FGBA with properties preserved; GeoParquet/GeoPackage unsupported
    Convert {
        /// Input file (.geojson/.json, .csv/.tsv, .shp, .fgb)
        input: PathBuf,
        /// Output FGBA file
        #[arg(short, long)]
        out: PathBuf,
        /// Emit analytical property columns and statistics
        #[arg(long)]
        analytics: bool,
        /// Emit derived geometry metrics (also enables --analytics)
        #[arg(long, conflicts_with = "no_geometry_metrics")]
        geometry_metrics: bool,
        /// Do not emit geometry metrics when --analytics is present
        #[arg(long)]
        no_geometry_metrics: bool,
        /// Rows per analytical chunk, or 'auto' to adapt to the feature count
        #[arg(long, default_value = "auto", value_parser = parse_chunk_rows)]
        chunk_rows: u32,
    },
    /// Show body header, aggregate bands and sizes of a file
    Info {
        /// Input file (or use --url)
        input: Option<PathBuf>,
        /// Read over HTTP range requests instead of a local file
        #[arg(long)]
        url: Option<String>,
    },
    /// Count occupied cells and estimated features inside a bounding box
    Stats {
        /// Input FGBA file
        input: PathBuf,
        /// Viewport as minx,miny,maxx,maxy (lon/lat degrees)
        #[arg(long, allow_hyphen_values = true)]
        bbox: String,
        /// A5 band resolution (default: the file's deepest band)
        #[arg(long)]
        res: Option<u8>,
    },
    /// Execute native read-only SQL over an FGB or FGBA file
    Query {
        /// Input FlatGeobuf/FGBA file
        input: PathBuf,
        /// SQL using features, a5_band_<res>, a5_viewport_approx, or attribute_stats
        sql: String,
        /// A5 resolution for --bbox (default: deepest band)
        #[arg(long)]
        res: Option<u8>,
        /// Covering bbox as minx,miny,maxx,maxy
        #[arg(long, allow_hyphen_values = true)]
        bbox: Option<String>,
        /// DataFusion target partitions for analytical chunk scans
        #[arg(long, value_parser = parse_scan_threads)]
        scan_threads: Option<usize>,
    },
    /// Structurally verify the footer, directory and every band
    Verify {
        /// Input FGBA file
        input: PathBuf,
    },
    /// Serve a file or directory over loopback HTTP with range support
    Serve {
        /// File or directory to serve
        path: PathBuf,
        /// Loopback port to bind
        #[arg(long, default_value_t = 8080)]
        port: u16,
    },
    /// Time aggregate queries at each zoom-ladder rung over the file extent
    Bench {
        /// Input FGBA file (or use --url)
        input: Option<PathBuf>,
        /// Bench over HTTP range requests instead of a local file
        #[arg(long)]
        url: Option<String>,
    },
}

fn analytics_options(
    analytics: bool,
    geometry_metrics: bool,
    no_geometry_metrics: bool,
    chunk_rows: u32,
) -> BuildOptions {
    let analytics = analytics || geometry_metrics;
    BuildOptions {
        analytics,
        geometry_metrics: analytics && !no_geometry_metrics,
        chunk_rows,
    }
}

fn run() -> Result<(), String> {
    match Cli::parse().cmd {
        Cmd::Build {
            input,
            out,
            analytics,
            geometry_metrics,
            no_geometry_metrics,
            chunk_rows,
        } => cmd_build(
            &input,
            out.as_deref(),
            &analytics_options(analytics, geometry_metrics, no_geometry_metrics, chunk_rows),
        ),
        Cmd::Convert {
            input,
            out,
            analytics,
            geometry_metrics,
            no_geometry_metrics,
            chunk_rows,
        } => convert::run(
            &input,
            &out,
            &analytics_options(analytics, geometry_metrics, no_geometry_metrics, chunk_rows),
        ),
        Cmd::Info { input, url } => cmd_info(&source(input, url)?),
        Cmd::Stats { input, bbox, res } => cmd_stats(&input, &bbox, res),
        Cmd::Query {
            input,
            sql,
            res,
            bbox,
            scan_threads,
        } => query::run(&input, &sql, res, bbox.as_deref(), scan_threads),
        Cmd::Verify { input } => cmd_verify(&input),
        Cmd::Serve { path, port } => serve::run(&path, port),
        Cmd::Bench { input, url } => bench::run(&source(input, url)?),
    }
}

/// A local file or a remote URL, for commands that read either.
pub(crate) enum Source {
    Path(PathBuf),
    Url(String),
}

/// Resolve the mutually exclusive positional-input / --url pair.
fn source(input: Option<PathBuf>, url: Option<String>) -> Result<Source, String> {
    match (input, url) {
        (Some(p), None) => Ok(Source::Path(p)),
        (None, Some(u)) => Ok(Source::Url(u)),
        (None, None) => Err("give an input file or --url <http(s)://…>".to_string()),
        (Some(_), Some(_)) => Err("give either an input file or --url, not both".to_string()),
    }
}

impl Source {
    /// Human-readable label for messages.
    pub(crate) fn label(&self) -> String {
        match self {
            Source::Path(p) => p.display().to_string(),
            Source::Url(u) => u.clone(),
        }
    }
}

// ---------- build ----------

fn cmd_build(input: &Path, out: Option<&Path>, options: &BuildOptions) -> Result<(), String> {
    let label = input.display().to_string();
    let default;
    let out = match out {
        Some(p) => p,
        None => {
            default = default_out(input);
            &default
        }
    };
    let t0 = Instant::now();
    let (report, total) =
        build_streaming_to_file(input, out, options).map_err(|e| format!("{label}: {e}"))?;
    let wall = t0.elapsed();
    println!("built {}", out.display());
    print_report(&report, total, wall);
    Ok(())
}

/// Where the FlatGeobuf body of `input` ends: the body length from an
/// existing FGBA trailer, or the full file length for a plain FlatGeobuf.
/// Never loads the file into memory.
fn body_length(input: &Path) -> Result<(u64, bool), String> {
    let mut file = std::fs::File::open(input).map_err(|e| e.to_string())?;
    let file_len = file.metadata().map_err(|e| e.to_string())?.len();
    let mut probe = std::io::BufReader::new(&mut file);
    match fgba::Reader::open(&mut probe) {
        Ok(reader) => Ok((reader.body_len(), true)),
        Err(FgbaError::NotFgba) => Ok((file_len, false)),
        Err(e) => Err(format!("input has a corrupt FGBA trailer: {e}")),
    }
}

/// Streaming build for large files: validate and encode through seekable
/// passes, then stream the body and spooled sections directly into the atomic
/// output temporary file. No complete section or trailer is retained in RAM.
fn build_streaming_to_file(
    input: &Path,
    out: &Path,
    options: &BuildOptions,
) -> Result<(fgba::BuildReport, u64), String> {
    let (body_len, had_trailer) = body_length(input)?;
    if had_trailer {
        eprintln!("input already has an FGBA trailer; rebuilding from its body");
    }
    let src = std::fs::File::open(input).map_err(|e| e.to_string())?;
    let report = write_atomic_with(out, |dst| {
        fgba::build_to_writer(src, body_len, options, dst)
            .map_err(|error| std::io::Error::other(format!("cannot build trailer: {error}")))
    })
    .map_err(|e| format!("cannot write {}: {e}", out.display()))?;
    let total = body_len + report.trailer_bytes as u64;
    Ok((report, total))
}

/// Copy exactly `body_len` source bytes, append `trailer`, and flush.
#[cfg(test)]
fn copy_body_and_trailer(
    src: impl Read,
    body_len: u64,
    trailer: &[u8],
    dst: &mut dyn Write,
) -> std::io::Result<()> {
    let copied = std::io::copy(&mut src.take(body_len), dst)?;
    if copied != body_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("copied {copied} body bytes, expected exactly {body_len}"),
        ));
    }
    dst.write_all(trailer)?;
    dst.flush()
}

/// Build the trailer for `body` and write the complete FGBA file to `out`,
/// atomically. Returns the report and the total file size.
pub(crate) fn build_to_file_at(
    body: &[u8],
    out: &Path,
    options: &BuildOptions,
) -> Result<(fgba::BuildReport, u64), String> {
    let (file, report) = fgba::build_with_options(body, options)
        .map_err(|e| format!("cannot build trailer: {e}"))?;
    let total = file.len() as u64;
    write_atomic(out, &file).map_err(|e| format!("cannot write {}: {e}", out.display()))?;
    Ok((report, total))
}

/// Print the build report lines shared by `build` and `convert`.
pub(crate) fn print_report(report: &fgba::BuildReport, total: u64, wall: Duration) {
    println!(
        "  features: {} ({} skipped)",
        report.features, report.skipped
    );
    if report.attribute_columns > 0 {
        println!("  attribute columns: {}", report.attribute_columns);
    } else if report.analytics_requested {
        println!("  analytics: no property columns");
    }
    println!(
        "  bands: {}",
        report
            .bands
            .iter()
            .map(|r| r.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("  trailer: {}", fmt_bytes(report.trailer_bytes as u64));
    println!("  total size: {}", fmt_bytes(total));
    println!("  wall time: {}", fmt_duration(wall));
}

/// The default `build` output: `x.fgb` → `x.a.fgb`, otherwise append.
fn default_out(input: &Path) -> PathBuf {
    let s = input.to_string_lossy();
    match s.strip_suffix(".fgb") {
        Some(stem) => PathBuf::from(format!("{stem}.a.fgb")),
        None => PathBuf::from(format!("{s}.a.fgb")),
    }
}

/// Strip an existing FGBA trailer (write-once format: rebuild, never edit,
/// SPEC §4). Returns the bare body and whether a trailer was present.
pub(crate) fn strip_existing_trailer(bytes: Vec<u8>) -> Result<(Vec<u8>, bool), String> {
    let mut cursor = Cursor::new(bytes);
    let body_len = match Reader::open(&mut cursor) {
        Ok(reader) => Some(reader.body_len()),
        Err(FgbaError::NotFgba) => None,
        Err(e) => return Err(format!("input has a corrupt FGBA trailer: {e}")),
    };
    let mut bytes = cursor.into_inner();
    match body_len {
        Some(len) => {
            let len =
                usize::try_from(len).map_err(|_| "file too large for this platform".to_string())?;
            bytes.truncate(len);
            Ok((bytes, true))
        }
        None => Ok((bytes, false)),
    }
}

// ---------- info ----------

fn cmd_info(src: &Source) -> Result<(), String> {
    let label = src.label();
    match src {
        Source::Path(path) => match Reader::open_path(path) {
            Ok(reader) => info_fgba(reader, &label).map(|_| ()),
            Err(FgbaError::NotFgba) => {
                let file = std::fs::File::open(path).map_err(|e| format!("{label}: {e}"))?;
                let fgb = FgbReader::open(BufReader::new(file))
                    .map_err(|e| format!("{label}: cannot read FlatGeobuf header: {e}"))?;
                info_plain(fgb, &label);
                Ok(())
            }
            Err(e) => Err(format!("{label}: {e}")),
        },
        Source::Url(url) => match Reader::open_url(url) {
            Ok(reader) => {
                let http = info_fgba(reader, &label)?;
                println!(
                    "  http: {} requests, {} read",
                    http.requests(),
                    fmt_bytes(http.bytes_read())
                );
                Ok(())
            }
            Err(FgbaError::NotFgba) => {
                let http = fgba::HttpRangeReader::open(url).map_err(|e| format!("{url}: {e}"))?;
                let fgb = FgbReader::open(http)
                    .map_err(|e| format!("{url}: cannot read FlatGeobuf header: {e}"))?;
                info_plain(fgb, &label);
                Ok(())
            }
            Err(e) => Err(format!("{label}: {e}")),
        },
    }
}

/// Print the FGBA view: body header, sizes, band table. Returns the inner
/// byte source so HTTP callers can report request counters.
fn info_fgba<R: Read + Seek>(mut reader: Reader<R>, label: &str) -> Result<R, String> {
    let total_len = reader
        .body()
        .map_err(|e| format!("{label}: {e}"))?
        .seek(SeekFrom::End(0))
        .map_err(|e| format!("{label}: cannot seek: {e}"))?;
    let header = header_info(
        FgbReader::open(reader.body().map_err(|e| format!("{label}: {e}"))?)
            .map_err(|e| format!("{label}: cannot read FlatGeobuf header: {e}"))?,
    );
    let body_len = reader.body_len();
    let bands = reader.bands();
    println!("{label} — FGBA");
    print_header(&header);
    println!(
        "  sizes: body {}, trailer {}, total {}",
        fmt_bytes(body_len),
        fmt_bytes(total_len - body_len),
        fmt_bytes(total_len)
    );
    println!("  bands: {}", bands.len());
    for band in &bands {
        match reader.read_band(band.res) {
            Ok(b) => println!(
                "    res {:>2}: {} cells, {}",
                band.res,
                b.cells.len(),
                fmt_bytes(band.size)
            ),
            Err(e) => println!("    res {:>2}: invalid band ({e})", band.res),
        }
    }
    Ok(reader.into_inner())
}

/// Print the plain-FlatGeobuf view (no trailer).
fn info_plain<R: Read>(fgb: FgbReader<R>, label: &str) {
    let header = header_info(fgb);
    println!("{label} — plain FlatGeobuf (no FGBA trailer)");
    print_header(&header);
}

/// Body header facts worth printing.
pub(crate) struct HeaderInfo {
    features: u64,
    geometry: String,
    index_node_size: u16,
    name: Option<String>,
    /// `[min_x, min_y, max_x, max_y]` when the header carries an envelope.
    pub(crate) envelope: Option<[f64; 4]>,
}

/// Extract the header facts from an open FlatGeobuf reader.
pub(crate) fn header_info<R: Read>(fgb: FgbReader<R>) -> HeaderInfo {
    let h = fgb.header();
    let envelope = h
        .envelope()
        .and_then(|v| (v.len() == 4).then(|| [v.get(0), v.get(1), v.get(2), v.get(3)]));
    HeaderInfo {
        features: h.features_count(),
        geometry: format!("{:?}", h.geometry_type()),
        index_node_size: h.index_node_size(),
        name: h.name().map(str::to_owned),
        envelope,
    }
}

fn print_header(h: &HeaderInfo) {
    let name = match h.name.as_deref() {
        Some(n) if !n.is_empty() => format!(" {n:?}"),
        _ => String::new(),
    };
    println!(
        "  body: FlatGeobuf{name}, {} features, {}, index node size {}",
        h.features, h.geometry, h.index_node_size
    );
    println!("  envelope: {}", fmt_env(h.envelope));
}

fn fmt_env(envelope: Option<[f64; 4]>) -> String {
    match envelope {
        Some([a, b, c, d]) => format!("[{a}, {b}, {c}, {d}]"),
        None => "none".to_string(),
    }
}

// ---------- stats ----------

fn cmd_stats(input: &Path, bbox: &str, res: Option<u8>) -> Result<(), String> {
    let label = input.display().to_string();
    let bbox = parse_bbox(bbox)?;
    let mut reader = Reader::open_path(input).map_err(|e| format!("{label}: {e}"))?;
    let res = match res {
        Some(r) => r,
        None => reader
            .bands()
            .iter()
            .map(|b| b.res)
            .max()
            .ok_or_else(|| format!("{label}: no aggregate bands"))?,
    };
    let stats = reader
        .stats(bbox, res)
        .map_err(|e| format!("{label}: {e}"))?;
    let [a, b, c, d] = bbox;
    println!("bbox [{a}, {b}, {c}, {d}] res {res}");
    println!("  cells matched: {}", stats.cells);
    println!("  estimated features: {}", stats.total);
    Ok(())
}

/// Parse `minx,miny,maxx,maxy` into a viewport, rejecting junk early.
pub(crate) fn parse_bbox(s: &str) -> Result<[f64; 4], String> {
    let parts: Vec<&str> = s.split(',').collect();
    if parts.len() != 4 {
        return Err(format!("--bbox wants minx,miny,maxx,maxy, got {s:?}"));
    }
    let mut v = [0.0; 4];
    for (i, part) in parts.iter().enumerate() {
        v[i] = part
            .trim()
            .parse::<f64>()
            .map_err(|_| format!("--bbox: {:?} is not a number", part.trim()))?;
    }
    if v.iter().any(|c| !c.is_finite()) {
        return Err("--bbox: coordinates must be finite".to_string());
    }
    if v[0] > v[2] || v[1] > v[3] {
        return Err(format!("--bbox: min must be <= max, got {s:?}"));
    }
    Ok(v)
}

// ---------- verify ----------

fn cmd_verify(input: &Path) -> Result<(), String> {
    let label = input.display().to_string();
    println!("verify {label}");
    let mut failures: Vec<String> = Vec::new();

    // The body is checked independently: the trailer never holds it hostage
    // (SPEC §10.2).
    let body_features = match std::fs::File::open(input)
        .map(BufReader::new)
        .map_err(|e| e.to_string())
        .and_then(|f| FgbReader::open(f).map_err(|e| format!("cannot read FlatGeobuf body: {e}")))
    {
        Ok(fgb) => {
            let h = header_info(fgb);
            println!("  body: {} features, {} — OK", h.features, h.geometry);
            Some(h.features)
        }
        Err(e) => {
            println!("  FAIL body: {e}");
            failures.push(format!("body: {e}"));
            None
        }
    };

    match Reader::open_path(input) {
        Ok(mut reader) => {
            println!("  footer + directory: CRC verified — OK");
            let bands = reader.bands();
            if bands.is_empty() {
                println!("  FAIL directory: no aggregate band sections");
                failures.push("directory: no aggregate band sections".to_string());
            }
            for band in &bands {
                match reader.read_band(band.res) {
                    Ok(b) => {
                        // read_band validated grid/res/flags, exact size,
                        // ascending valid cells, count > 0, sum == total.
                        println!(
                            "  band res {}: {} cells, {} features — OK",
                            band.res,
                            b.cells.len(),
                            b.total
                        );
                    }
                    Err(e) => {
                        println!("  FAIL band res {}: {e}", band.res);
                        failures.push(format!("band res {}: {e}", band.res));
                    }
                }
            }
            match reader.read_body_analytics_metadata() {
                Ok(Some(analytics)) => match validate_analytics(&mut reader, &analytics) {
                    Ok(()) => println!(
                        "  analytics: {} columns, {} chunks — OK",
                        analytics.column_count, analytics.chunk_count
                    ),
                    Err(error) => {
                        println!("  FAIL analytics: {error}");
                        failures.push(format!("analytics: {error}"));
                    }
                },
                Ok(None) => {}
                Err(error) => {
                    println!("  FAIL analytics: {error}");
                    failures.push(format!("analytics: {error}"));
                }
            }
            match reader.read_geometry_metrics_metadata() {
                Ok(Some(metrics)) => match validate_geometry_metrics(&mut reader, &metrics) {
                    Ok(()) => println!(
                        "  geometry metrics: {} columns, {} chunks — OK",
                        metrics.columns.len(),
                        metrics.chunk_count
                    ),
                    Err(error) => {
                        println!("  FAIL geometry metrics: {error}");
                        failures.push(format!("geometry metrics: {error}"));
                    }
                },
                Ok(None) => {}
                Err(error) => {
                    println!("  FAIL geometry metrics: {error}");
                    failures.push(format!("geometry metrics: {error}"));
                }
            }
            if failures.is_empty() {
                println!(
                    "OK: valid FGBA, {} features, {} band(s)",
                    body_features.unwrap_or(0),
                    bands.len()
                );
            }
        }
        Err(FgbaError::NotFgba) => {
            println!("  FAIL footer: no FGBAGG01 magic (not an FGBA file)");
            failures.push("footer: no FGBAGG01 magic (not an FGBA file)".to_string());
        }
        Err(e) => {
            println!("  FAIL footer/directory: {e}");
            failures.push(format!("footer/directory: {e}"));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!("verify: {} check(s) failed", failures.len()))
    }
}

/// Validate every independent Arrow IPC file against its column/chunk metadata.
fn validate_analytics<R: Read + Seek>(
    reader: &mut Reader<R>,
    analytics: &BodyAnalytics,
) -> Result<(), String> {
    for column in &analytics.columns {
        let identity = column.descriptor.column_index;
        let expected_type = fgba::analytics::arrow_data_type(column.descriptor.fgb_type)
            .map_err(|error| format!("column {identity}: {error}"))?;
        for chunk_index in 0..analytics.chunk_count {
            let context = format!("column {identity}, chunk {chunk_index}");
            let chunk = reader
                .read_arrow_chunk_with_metadata(analytics, identity, chunk_index)
                .map_err(|error| format!("{context}: {error}"))?;
            let expected_rows = analytics
                .chunk_row_count(chunk_index)
                .ok_or_else(|| format!("{context}: chunk row count is unavailable"))?;
            validate_arrow_ipc(
                chunk.bytes,
                &column.name,
                &expected_type,
                column.nullable,
                expected_rows,
                &context,
            )?;
        }
    }
    Ok(())
}

/// Validate every kind-3 Arrow IPC file against the shared chunk grid.
fn validate_geometry_metrics<R: Read + Seek>(
    reader: &mut Reader<R>,
    metrics: &GeometryMetrics,
) -> Result<(), String> {
    for column in &metrics.columns {
        let identity = column.descriptor.metric_id;
        let name = fgba::geom_analytics::metric_name(identity)
            .ok_or_else(|| format!("metric {identity}: unknown metric ID"))?;
        let expected_type = fgba::geom_analytics::metric_data_type(identity)
            .map_err(|error| format!("metric {identity}: {error}"))?;
        for chunk_index in 0..metrics.chunk_count {
            let context = format!("metric {identity}, chunk {chunk_index}");
            let chunk = reader
                .read_geometry_arrow_chunk_with_metadata(metrics, identity, chunk_index)
                .map_err(|error| format!("{context}: {error}"))?;
            let expected_rows = metrics
                .chunk_row_count(chunk_index)
                .ok_or_else(|| format!("{context}: chunk row count is unavailable"))?;
            validate_arrow_ipc(
                chunk.bytes,
                name,
                &expected_type,
                true,
                expected_rows,
                &context,
            )?;
        }
    }
    Ok(())
}

fn validate_arrow_ipc(
    bytes: Vec<u8>,
    expected_name: &str,
    expected_type: &DataType,
    expected_nullable: bool,
    expected_rows: u32,
    context: &str,
) -> Result<(), String> {
    let ipc = FileReader::try_new(Cursor::new(bytes), None)
        .map_err(|error| format!("{context}: malformed Arrow IPC: {error}"))?;
    if ipc.schema().fields().len() != 1 || ipc.num_batches() != 1 {
        return Err(format!("{context}: expected one field and one RecordBatch"));
    }
    let schema = ipc.schema();
    let field = &schema.fields()[0];
    if field.name() != expected_name
        || field.data_type() != expected_type
        || field.is_nullable() != expected_nullable
    {
        return Err(format!(
            "{context}: Arrow schema disagrees with the section"
        ));
    }
    let batches = ipc
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| format!("{context}: cannot decode Arrow IPC: {error}"))?;
    if batches.len() != 1 || batches[0].num_columns() != 1 {
        return Err(format!("{context}: expected exactly one RecordBatch"));
    }
    if batches[0].num_rows() != expected_rows as usize {
        return Err(format!(
            "{context}: Arrow rows {} != expected {expected_rows}",
            batches[0].num_rows()
        ));
    }
    Ok(())
}

// ---------- shared helpers ----------

/// Write `bytes` to `path` atomically: same-directory temp file + rename.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    write_atomic_with(path, |dst| dst.write_all(bytes))
}

/// Write to a unique same-directory temporary file, then close it and rename
/// it over `path`. Any error removes the temporary file and leaves `path` as-is.
fn write_atomic_with<T>(
    path: &Path,
    write: impl FnOnce(&mut dyn Write) -> std::io::Result<T>,
) -> std::io::Result<T> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    let (tmp, file) = loop {
        let mut tmp_name = path.as_os_str().to_owned();
        tmp_name.push(format!(
            ".tmp-{}-{}",
            std::process::id(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ));
        let tmp = PathBuf::from(tmp_name);
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
        {
            Ok(file) => break (tmp, file),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    };

    let result = (|| {
        let mut dst = std::io::BufWriter::new(file);
        let value = write(&mut dst)?;
        dst.flush()?;
        drop(dst);
        std::fs::rename(&tmp, path)?;
        Ok(value)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// Human-readable byte count, deterministic (no locale grouping).
pub(crate) fn fmt_bytes(n: u64) -> String {
    if n < 1024 {
        format!("{n} B")
    } else if n < 1 << 20 {
        format!("{:.1} KiB", n as f64 / 1024.0)
    } else {
        format!("{:.1} MiB", n as f64 / (1 << 20) as f64)
    }
}

/// Human-readable duration, deterministic.
pub(crate) fn fmt_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs >= 1.0 {
        format!("{secs:.2} s")
    } else if secs >= 0.001 {
        format!("{:.2} ms", secs * 1e3)
    } else {
        format!("{:.2} µs", secs * 1e6)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        copy_body_and_trailer, validate_analytics, validate_geometry_metrics, write_atomic_with,
    };
    use fgba::{BuildOptions, Reader};
    use std::io::{Cursor, Read, Seek, Write};
    use std::ops::Range;
    use std::path::{Path, PathBuf};

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> std::io::Result<Self> {
            let path = std::env::temp_dir()
                .join(format!("fgba-cli-atomic-write-test-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path)?;
            Ok(Self(path))
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct MetadataReadMonitor<R> {
        inner: R,
        metadata_ranges: Vec<Range<u64>>,
        metadata_reads: usize,
    }

    impl<R: Read + Seek> Read for MetadataReadMonitor<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let start = self.inner.stream_position()?;
            let count = self.inner.read(buf)?;
            let end = start + count as u64;
            if count > 0
                && self
                    .metadata_ranges
                    .iter()
                    .any(|range| start < range.end && range.start < end)
            {
                self.metadata_reads += 1;
            }
            Ok(count)
        }
    }

    impl<R: Seek> Seek for MetadataReadMonitor<R> {
        fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
            self.inner.seek(position)
        }
    }

    struct FailingReader;

    impl Read for FailingReader {
        fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("injected copy failure"))
        }
    }

    struct FailingWriter<'a> {
        dst: &'a mut dyn Write,
        writes_before_failure: usize,
        fail_flush: bool,
    }

    impl Write for FailingWriter<'_> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.writes_before_failure == 0 {
                return Err(std::io::Error::other("injected write failure"));
            }
            let allowed = buf.len().min(self.writes_before_failure);
            let written = self.dst.write(&buf[..allowed])?;
            self.writes_before_failure -= written;
            Ok(written)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            if self.fail_flush {
                Err(std::io::Error::other("injected flush failure"))
            } else {
                self.dst.flush()
            }
        }
    }

    fn assert_atomic_failure(
        dir: &Path,
        write: impl FnOnce(&mut dyn Write) -> std::io::Result<()>,
    ) -> TestResult<std::io::Error> {
        let out = dir.join("out.fgb");
        let original = b"pre-existing output";
        std::fs::write(&out, original)?;
        let error = write_atomic_with(&out, write)
            .err()
            .ok_or("atomic write unexpectedly succeeded")?;
        assert_eq!(std::fs::read(&out)?, original);
        let entries = std::fs::read_dir(dir)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<Vec<_>>>()?;
        assert_eq!(
            entries,
            vec![std::ffi::OsString::from("out.fgb")],
            "temporary artifacts were not cleaned up"
        );
        Ok(error)
    }

    #[test]
    fn verify_validators_use_loaded_metadata() -> TestResult {
        let vector = include_bytes!("../tests/data/cities_12.a.fgb");
        let body_len = Reader::open(Cursor::new(vector))?.body_len() as usize;
        let (file, _) = fgba::build_with_options(
            &vector[..body_len],
            &BuildOptions {
                analytics: true,
                geometry_metrics: true,
                chunk_rows: 2,
            },
        )?;
        let mut loaded = Reader::open(Cursor::new(file.as_slice()))?;
        let analytics = loaded
            .read_body_analytics_metadata()?
            .ok_or("missing BODY_ANALYTICS")?;
        let metrics = loaded
            .read_geometry_metrics_metadata()?
            .ok_or("missing GEOMETRY_METRICS")?;
        let analytics_payload = analytics
            .columns
            .first()
            .and_then(|column| column.arrow_offsets.first())
            .copied()
            .ok_or("missing analytics Arrow payload")?;
        let geometry_payload = metrics
            .columns
            .first()
            .and_then(|column| column.arrow_offsets.first())
            .copied()
            .ok_or("missing geometry Arrow payload")?;
        let monitored = MetadataReadMonitor {
            inner: Cursor::new(file.as_slice()),
            metadata_ranges: vec![
                analytics.section_start..analytics_payload,
                metrics.section_start..geometry_payload,
            ],
            metadata_reads: 0,
        };

        // Use a fresh reader with no cached metadata. Any convenience-method
        // metadata lookup would be observed in the monitored section ranges.
        let mut reader = Reader::open(monitored)?;
        validate_analytics(&mut reader, &analytics)?;
        validate_geometry_metrics(&mut reader, &metrics)?;
        let monitored = reader.into_inner();
        assert_eq!(
            monitored.metadata_reads, 0,
            "verification reloaded analytical metadata"
        );
        Ok(())
    }

    #[test]
    fn streaming_atomic_failures_preserve_existing_output() -> TestResult {
        let tmp = TestDir::new()?;
        let body = b"body";

        // A source error after a partial copy cannot replace the output.
        assert_atomic_failure(&tmp.0, |dst| {
            let src = Cursor::new(&body[..]).chain(FailingReader);
            copy_body_and_trailer(src, 8, b"trailer", dst)
        })?;

        // A trailer write error after the complete body cannot replace it.
        assert_atomic_failure(&tmp.0, |dst| {
            let mut failing = FailingWriter {
                dst,
                writes_before_failure: body.len(),
                fail_flush: false,
            };
            copy_body_and_trailer(
                Cursor::new(&body[..]),
                body.len() as u64,
                b"trailer",
                &mut failing,
            )
        })?;

        // A flush error after all bytes were accepted cannot replace it.
        assert_atomic_failure(&tmp.0, |dst| {
            let mut failing = FailingWriter {
                dst,
                writes_before_failure: usize::MAX,
                fail_flush: true,
            };
            copy_body_and_trailer(
                Cursor::new(&body[..]),
                body.len() as u64,
                b"trailer",
                &mut failing,
            )
        })?;

        // EOF before the exact expected body length is also an error.
        let error = assert_atomic_failure(&tmp.0, |dst| {
            copy_body_and_trailer(
                Cursor::new(&body[..]),
                body.len() as u64 + 1,
                b"trailer",
                dst,
            )
        })?;
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        assert!(error.to_string().contains("expected exactly 5"));
        Ok(())
    }
}
