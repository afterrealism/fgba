//! CLI integration tests: convert → build/verify/info/stats round-trips,
//! determinism, `serve` smoke over loopback HTTP, and bench — spawning the
//! real `fgba` binary via `CARGO_BIN_EXE_fgba`.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// The GeoJSON fixture: the normative vector's 12 points (SPEC Appendix A).
const CITIES_GEOJSON: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/cities.geojson");

/// The normative FGBA vector.
const CITIES_VECTOR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../testdata/cities_12.a.fgb");

fn fgba(args: &[&str]) -> std::io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_fgba")).args(args).output()
}

fn assert_ok(out: &Output) {
    assert!(
        out.status.success(),
        "expected success, got {:?}\nstdout: {}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn assert_fail(out: &Output) {
    assert!(
        !out.status.success(),
        "expected failure, got success\nstdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
}

/// A unique temp directory per test, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> std::io::Result<TempDir> {
        let dir = std::env::temp_dir().join(format!(
            "fgba-cli-test-{}-{tag}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        Ok(TempDir(dir))
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Kill the spawned server even when a test assertion fails.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// Convert the cities fixture and return the output path.
fn convert_cities(tmp: &TempDir) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let out = tmp.path("cities.a.fgb");
    let o = fgba(&["convert", CITIES_GEOJSON, "-o", &out.to_string_lossy()])?;
    assert_ok(&o);
    assert!(out.exists());
    Ok(out)
}

#[test]
fn convert_verify_info_stats_roundtrip() -> TestResult {
    let tmp = TempDir::new("roundtrip")?;
    let out = convert_cities(&tmp)?;

    // Convert prints the report.
    let o = fgba(&["convert", CITIES_GEOJSON, "-o", &out.to_string_lossy()])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("12 features, 0 skipped"), "{text}");
    assert!(text.contains("bands: 2"), "{text}");

    // Verify exits 0 with a summary.
    let o = fgba(&["verify", &out.to_string_lossy()])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("footer + directory: CRC verified"), "{text}");
    assert!(text.contains("band res 2: 2 cells, 12 features"), "{text}");
    assert!(text.contains("OK: valid FGBA, 12 features, 1 band"), "{text}");

    // Info reports FGBA, the body header and the band table.
    let o = fgba(&["info", &out.to_string_lossy()])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("— FGBA"), "{text}");
    assert!(text.contains("12 features, Point"), "{text}");
    assert!(text.contains("res  2: 2 cells"), "{text}");

    // Stats over the world viewport at the default (deepest) band.
    let o = fgba(&["stats", &out.to_string_lossy(), "--bbox", "-180,-85,180,85"])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("res 2"), "{text}");
    assert!(text.contains("cells matched: 2"), "{text}");
    assert!(text.contains("estimated features: 12"), "{text}");

    // Stats over a region holding only the 8 European points.
    let o = fgba(&["stats", &out.to_string_lossy(), "--bbox", "-5,45,10,55"])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("estimated features: 8"));

    // Explicit res.
    let o = fgba(&[
        "stats",
        &out.to_string_lossy(),
        "--bbox",
        "-180,-85,180,85",
        "--res",
        "2",
    ])?;
    assert_ok(&o);
    Ok(())
}

#[test]
fn build_and_convert_are_deterministic() -> TestResult {
    let tmp = TempDir::new("determinism")?;
    let a = tmp.path("a.a.fgb");
    let b = tmp.path("b.a.fgb");
    for out in [&a, &b] {
        let o = fgba(&["convert", CITIES_GEOJSON, "-o", &out.to_string_lossy()])?;
        assert_ok(&o);
    }
    assert_eq!(
        std::fs::read(&a)?,
        std::fs::read(&b)?,
        "convert must be byte-identical on identical input"
    );

    // `build` on an FGBA input rebuilds from the body, byte-identically.
    let rebuilt = tmp.path("rebuilt.a.fgb");
    let o = fgba(&["build", &a.to_string_lossy(), "-o", &rebuilt.to_string_lossy()])?;
    assert_ok(&o);
    assert!(stderr(&o).contains("already has an FGBA trailer"));
    assert_eq!(std::fs::read(&a)?, std::fs::read(&rebuilt)?);

    // `build` without -o defaults to <stem>.a.fgb next to the input.
    let plain = tmp.path("plain.fgb");
    let full = std::fs::read(&a)?;
    let reader = fgba::Reader::open_path(&a)?;
    let body_len = reader.body_len() as usize;
    std::fs::write(&plain, &full[..body_len])?;
    let o = fgba(&["build", &plain.to_string_lossy()])?;
    assert_ok(&o);
    assert_eq!(std::fs::read(tmp.path("plain.a.fgb"))?, std::fs::read(&a)?);
    Ok(())
}

#[test]
fn verify_names_failures_and_exits_1() -> TestResult {
    let tmp = TempDir::new("verify-fail")?;
    let good = convert_cities(&tmp)?;

    // A plain FlatGeobuf (no trailer): footer failure named, exit 1.
    let reader = fgba::Reader::open_path(&good)?;
    let body_len = reader.body_len() as usize;
    let plain = tmp.path("plain.fgb");
    std::fs::write(&plain, &std::fs::read(&good)?[..body_len])?;
    let o = fgba(&["verify", &plain.to_string_lossy()])?;
    assert_fail(&o);
    assert!(stdout(&o).contains("FAIL footer"), "{}", stdout(&o));
    assert!(stderr(&o).contains("check(s) failed"));

    // A corrupted trailer (one flipped directory byte): CRC failure named.
    let mut bytes = std::fs::read(&good)?;
    let at = bytes.len() - 33; // last directory byte
    bytes[at] ^= 0xFF;
    let bad = tmp.path("bad.a.fgb");
    std::fs::write(&bad, &bytes)?;
    let o = fgba(&["verify", &bad.to_string_lossy()])?;
    assert_fail(&o);
    assert!(stdout(&o).contains("CRC"), "{}", stdout(&o));
    Ok(())
}

#[test]
fn convert_reads_csv_xy_and_wkt() -> TestResult {
    let tmp = TempDir::new("csv")?;
    let csv_path = tmp.path("cities.csv");
    std::fs::write(
        &csv_path,
        "name,lon,lat\na,2.36,48.84\nb,-0.14,51.49\nc,-73.97,40.73\n",
    )?;
    let out = tmp.path("csv.a.fgb");
    let o = fgba(&["convert", &csv_path.to_string_lossy(), "-o", &out.to_string_lossy()])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("3 features"), "{}", stdout(&o));
    let o = fgba(&["verify", &out.to_string_lossy()])?;
    assert_ok(&o);

    let wkt_path = tmp.path("cities.tsv");
    std::fs::write(
        &wkt_path,
        "name\twkt\na\tPOINT (2.36 48.84)\nb\tPOINT (-0.14 51.49)\n",
    )?;
    let out = tmp.path("wkt.a.fgb");
    let o = fgba(&["convert", &wkt_path.to_string_lossy(), "-o", &out.to_string_lossy()])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("2 features"), "{}", stdout(&o));

    // A column-less table fails with a readable error.
    let bad_path = tmp.path("bad.csv");
    std::fs::write(&bad_path, "a,b\n1,2\n")?;
    let o = fgba(&["convert", &bad_path.to_string_lossy(), "-o", &tmp.path("bad.a.fgb").to_string_lossy()])?;
    assert_fail(&o);
    assert!(stderr(&o).contains("no geometry column"), "{}", stderr(&o));
    Ok(())
}

#[test]
fn convert_reads_shapefile_points() -> TestResult {
    let tmp = TempDir::new("shp")?;
    let shp = tmp.path("cities.shp");
    let mut writer = shapefile::ShapeWriter::from_path(&shp)?;
    for &(x, y) in &[(2.36, 48.84), (-0.14, 51.49), (-73.97, 40.73)] {
        writer.write_shape(&shapefile::Point::new(x, y))?;
    }
    drop(writer);

    let out = tmp.path("shp.a.fgb");
    let o = fgba(&["convert", &shp.to_string_lossy(), "-o", &out.to_string_lossy()])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("3 features"), "{}", stdout(&o));
    let o = fgba(&["verify", &out.to_string_lossy()])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("3 features"), "{}", stdout(&o));
    let o = fgba(&["stats", &out.to_string_lossy(), "--bbox", "-180,-85,180,85"])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("estimated features: 3"), "{}", stdout(&o));
    Ok(())
}

#[test]
fn convert_names_unimplemented_formats() -> TestResult {
    let tmp = TempDir::new("not-yet")?;
    for (name, want) in [
        ("x.parquet", "GeoParquet input is not implemented yet"),
        ("x.gpkg", "GeoPackage input is not implemented yet"),
        ("x.shx", "cannot detect input format"),
    ] {
        let input = tmp.path(name);
        std::fs::write(&input, b"")?;
        let o = fgba(&["convert", &input.to_string_lossy(), "-o", &tmp.path("o.a.fgb").to_string_lossy()])?;
        assert_fail(&o);
        assert!(stderr(&o).contains(want), "{name}: {}", stderr(&o));
    }
    Ok(())
}

/// A free loopback port (best effort: released before the server binds it).
fn free_port() -> std::io::Result<u16> {
    Ok(TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port())
}

#[test]
fn serve_smoke_ranges_traversal_and_fgba_reads() -> TestResult {
    let tmp = TempDir::new("serve")?;
    convert_cities(&tmp)?;
    let port = free_port()?;

    let child = Command::new(env!("CARGO_BIN_EXE_fgba"))
        .args(["serve", &tmp.0.to_string_lossy(), "--port", &port.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let _server = Server(child);

    // The fgba HTTP reader drives the server: retry until it is up.
    let url = format!("http://127.0.0.1:{port}/cities.a.fgb");
    let mut opened = None;
    for _ in 0..50 {
        match fgba::Reader::open_url(&url) {
            Ok(reader) => {
                opened = Some(reader);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    let mut reader = match opened {
        Some(r) => r,
        None => return Err(format!("server never came up on :{port}").into()),
    };
    assert_eq!(reader.bands().len(), 1);
    let stats = reader.stats([-180.0, -85.0, 180.0, 85.0], 2)?;
    assert_eq!(stats.total, 12);
    assert_eq!(stats.cells, 2);

    // Path traversal is refused with 403.
    let status = response_status(port, "GET /../../Cargo.toml HTTP/1.1\r\nHost: x\r\n\r\n")?;
    assert_eq!(status, "HTTP/1.1 403 Forbidden");
    Ok(())
}

/// One raw HTTP request, returning the response status line.
fn response_status(port: u16, request: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.write_all(request.as_bytes())?;
    let mut buf = Vec::new();
    stream.take(4096).read_to_end(&mut buf)?;
    Ok(String::from_utf8_lossy(&buf)
        .lines()
        .next()
        .unwrap_or("")
        .to_string())
}

#[test]
fn serve_status_codes() -> TestResult {
    let tmp = TempDir::new("serve-status")?;
    convert_cities(&tmp)?;
    let port = free_port()?;
    let child = Command::new(env!("CARGO_BIN_EXE_fgba"))
        .args(["serve", &tmp.0.to_string_lossy(), "--port", &port.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let _server = Server(child);

    // Wait until the port accepts connections.
    let mut up = false;
    for _ in 0..50 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            up = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(up, "server never came up on :{port}");

    let status = response_status(port, "GET /../../Cargo.toml HTTP/1.1\r\nHost: x\r\n\r\n")?;
    assert_eq!(status, "HTTP/1.1 403 Forbidden");
    let status = response_status(
        port,
        "GET /cities.a.fgb HTTP/1.1\r\nHost: x\r\nRange: bytes=0-0\r\n\r\n",
    )?;
    assert_eq!(status, "HTTP/1.1 206 Partial Content");
    let status = response_status(port, "GET /nope.fgb HTTP/1.1\r\nHost: x\r\n\r\n")?;
    assert_eq!(status, "HTTP/1.1 404 Not Found");
    let status = response_status(
        port,
        "GET /cities.a.fgb HTTP/1.1\r\nHost: x\r\nRange: bytes=99999-100000\r\n\r\n",
    )?;
    assert_eq!(status, "HTTP/1.1 416 Range Not Satisfiable");
    let status = response_status(port, "POST /cities.a.fgb HTTP/1.1\r\nHost: x\r\n\r\n")?;
    assert_eq!(status, "HTTP/1.1 405 Method Not Allowed");
    Ok(())
}

#[test]
fn bench_runs_on_a_small_file() -> TestResult {
    let tmp = TempDir::new("bench")?;
    let out = convert_cities(&tmp)?;
    let o = fgba(&["bench", &out.to_string_lossy()])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("band res"), "{text}");
    assert!(text.contains("         2"), "{text}"); // the res-2 band row
    Ok(())
}

#[test]
fn info_accepts_plain_fgb() -> TestResult {
    let tmp = TempDir::new("plain-info")?;
    let good = convert_cities(&tmp)?;
    let reader = fgba::Reader::open_path(&good)?;
    let body_len = reader.body_len() as usize;
    let plain = tmp.path("plain.fgb");
    std::fs::write(&plain, &std::fs::read(&good)?[..body_len])?;
    let o = fgba(&["info", &plain.to_string_lossy()])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("plain FlatGeobuf"), "{text}");
    assert!(text.contains("12 features, Point"), "{text}");
    Ok(())
}

#[test]
fn info_and_bench_require_input_or_url() -> TestResult {
    let o = fgba(&["info"])?;
    assert_fail(&o);
    assert!(stderr(&o).contains("input file or --url"), "{}", stderr(&o));
    let o = fgba(&["bench"])?;
    assert_fail(&o);
    Ok(())
}

/// The normative vector passes verify and info unchanged.
#[test]
fn normative_vector_roundtrip() -> TestResult {
    assert!(Path::new(CITIES_VECTOR).exists(), "missing {CITIES_VECTOR}");
    let o = fgba(&["verify", CITIES_VECTOR])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("OK: valid FGBA, 12 features, 1 band"));
    let o = fgba(&["info", CITIES_VECTOR])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("body 1.6 KiB"), "{text}"); // 1672 B body, SPEC Appendix A
    assert!(text.contains("trailer 105 B"), "{text}"); // 105 B trailer, SPEC Appendix A
    assert!(text.contains("res  2: 2 cells, 44 B"), "{text}");
    Ok(())
}
