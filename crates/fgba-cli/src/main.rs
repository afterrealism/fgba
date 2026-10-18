//! `fgba` — command line tool for FGBA (FlatGeobuf Aggregates) files:
//! convert source formats, build trailers, inspect, query, verify, serve
//! over HTTP, and benchmark aggregate queries. Library: crate `fgba`.

#![forbid(unsafe_code)]

mod bench;
mod convert;
mod serve;

use clap::{Parser, Subcommand};
use flatgeobuf::FgbReader;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use fgba::{Error as FgbaError, Reader};

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

#[derive(Subcommand)]
enum Cmd {
    /// Append an FGBA aggregate trailer to a FlatGeobuf file
    Build {
        /// Input FlatGeobuf file (.fgb); an existing trailer is rebuilt
        input: PathBuf,
        /// Output file (default: input name ending in .a.fgb)
        #[arg(short, long)]
        out: Option<PathBuf>,
    },
    /// Convert GeoJSON, CSV/TSV, Shapefile or FlatGeobuf into FGBA
    Convert {
        /// Input file (.geojson/.json, .csv/.tsv, .shp, .fgb)
        input: PathBuf,
        /// Output FGBA file
        #[arg(short, long)]
        out: PathBuf,
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

fn run() -> Result<(), String> {
    match Cli::parse().cmd {
        Cmd::Build { input, out } => cmd_build(&input, out.as_deref()),
        Cmd::Convert { input, out } => convert::run(&input, &out),
        Cmd::Info { input, url } => cmd_info(&source(input, url)?),
        Cmd::Stats { input, bbox, res } => cmd_stats(&input, &bbox, res),
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

fn cmd_build(input: &Path, out: Option<&Path>) -> Result<(), String> {
    let label = input.display().to_string();
    let bytes = std::fs::read(input).map_err(|e| format!("{label}: {e}"))?;
    let (body, had_trailer) = strip_existing_trailer(bytes)?;
    if had_trailer {
        eprintln!("{label}: input already has an FGBA trailer; rebuilding from its body");
    }
    let default;
    let out = match out {
        Some(p) => p,
        None => {
            default = default_out(input);
            &default
        }
    };
    let t0 = Instant::now();
    let (report, total) = build_to_file_at(&body, out)?;
    let wall = t0.elapsed();
    println!("built {}", out.display());
    print_report(&report, total, wall);
    Ok(())
}

/// Build the trailer for `body` and write the complete FGBA file to `out`,
/// atomically. Returns the report and the total file size.
pub(crate) fn build_to_file_at(
    body: &[u8],
    out: &Path,
) -> Result<(fgba::BuildReport, u64), String> {
    let (file, report) = fgba::build(body).map_err(|e| format!("cannot build trailer: {e}"))?;
    let total = file.len() as u64;
    write_atomic(out, &file).map_err(|e| format!("cannot write {}: {e}", out.display()))?;
    Ok((report, total))
}

/// Print the build report lines shared by `build` and `convert`.
pub(crate) fn print_report(report: &fgba::BuildReport, total: u64, wall: Duration) {
    println!("  features: {} ({} skipped)", report.features, report.skipped);
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
            let len = usize::try_from(len)
                .map_err(|_| "file too large for this platform".to_string())?;
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
                let http = fgba::HttpRangeReader::open(url)
                    .map_err(|e| format!("{url}: {e}"))?;
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
    let envelope = h.envelope().and_then(|v| {
        (v.len() == 4).then(|| [v.get(0), v.get(1), v.get(2), v.get(3)])
    });
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
        .and_then(|f| {
            FgbReader::open(f).map_err(|e| format!("cannot read FlatGeobuf body: {e}"))
        }) {
        Ok(fgb) => {
            let h = header_info(fgb);
            println!(
                "  body: {} features, {} — OK",
                h.features, h.geometry
            );
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
                        if let Some(features) = body_features {
                            if b.total != features {
                                let msg = format!(
                                    "band res {}: total {} != body features_count {features}",
                                    band.res, b.total
                                );
                                println!("  FAIL {msg}");
                                failures.push(msg);
                            }
                        }
                    }
                    Err(e) => {
                        println!("  FAIL band res {}: {e}", band.res);
                        failures.push(format!("band res {}: {e}", band.res));
                    }
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

// ---------- shared helpers ----------

/// Write `bytes` to `path` atomically: same-directory temp file + rename.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut tmp_name = path.as_os_str().to_owned();
    tmp_name.push(format!(".tmp-{}", std::process::id()));
    let tmp = PathBuf::from(tmp_name);
    let result = std::fs::write(&tmp, bytes).and_then(|()| std::fs::rename(&tmp, path));
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
