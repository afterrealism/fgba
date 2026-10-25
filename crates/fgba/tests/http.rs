//! HTTP range-request integration tests (SPEC §9.2): the loopback range
//! server is ported from the validated experiment harness
//! (`docs/reference/trailer.rs`), bound to an ephemeral port.

#![cfg(feature = "http")]

use fgba::{BandMeta, Reader};
use std::io::{Read, Write};
use std::net::TcpListener;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// The normative vector file (SPEC Appendix A).
fn vector_bytes() -> std::io::Result<Vec<u8>> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/cities_12.a.fgb");
    std::fs::read(path)
}

/// How the stub server answers requests.
enum Mode {
    /// RFC 9110 ranges: 206 to `Range` requests, 200 otherwise.
    Ranges(Vec<u8>),
    /// Ignore `Range`; always answer 200 with the full body.
    FullOnly(Vec<u8>),
    /// Answer 206 but close the connection after half the promised bytes.
    ShortRead(Vec<u8>),
}

impl Mode {
    fn body(&self) -> &[u8] {
        match self {
            Mode::Ranges(b) | Mode::FullOnly(b) | Mode::ShortRead(b) => b,
        }
    }
}

/// Start a sequential one-request-per-connection server on an ephemeral
/// loopback port and return the port. The detached thread serves until the
/// test process exits.
fn serve(mode: Mode) -> std::io::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    let _ = std::thread::spawn(move || {
        for s in listener.incoming().flatten() {
            handle(s, &mode);
        }
    });
    Ok(port)
}

/// Parse an explicit `a-b` byte range, clamped to the body length.
fn parse_range(value: &str, len: usize) -> Option<(usize, usize)> {
    let (a, b) = value.split_once('-')?;
    let a: usize = a.parse().ok()?;
    let b: usize = b.parse().ok()?;
    if a >= len || a > b {
        return None;
    }
    Some((a, b.min(len - 1)))
}

fn handle(mut s: std::net::TcpStream, mode: &Mode) {
    let mut head = Vec::with_capacity(512);
    let mut byte = [0u8; 1];
    while head.len() < 8192 {
        if Read::read(&mut s, &mut byte).unwrap_or(0) == 0 {
            return;
        }
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let text = String::from_utf8_lossy(&head);
    let range = text
        .lines()
        .filter(|l| l.to_ascii_lowercase().starts_with("range:"))
        .find_map(|l| l.split_once("bytes=").map(|(_, v)| v.trim().to_string()));
    let full = mode.body();
    match mode {
        Mode::FullOnly(_) => {
            let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", full.len());
            let _ = s.write_all(head.as_bytes());
            let _ = s.write_all(full);
        }
        Mode::Ranges(_) => {
            if let Some((a, b)) = range.and_then(|r| parse_range(&r, full.len())) {
                let slice = &full[a..=b];
                let head = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {a}-{b}/{}\r\nAccept-Ranges: bytes\r\n\r\n",
                    slice.len(),
                    full.len()
                );
                let _ = s.write_all(head.as_bytes());
                let _ = s.write_all(slice);
            } else {
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n",
                    full.len()
                );
                let _ = s.write_all(head.as_bytes());
                let _ = s.write_all(full);
            }
        }
        Mode::ShortRead(_) => {
            // Promise the full requested range, then close after half of it.
            let (a, b) = range
                .and_then(|r| parse_range(&r, full.len()))
                .unwrap_or((0, full.len().saturating_sub(1)));
            let promised = b - a + 1;
            let head = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {promised}\r\nContent-Range: bytes {a}-{b}/{}\r\n\r\n",
                full.len()
            );
            let _ = s.write_all(head.as_bytes());
            let _ = s.write_all(&full[a..a + promised / 2]);
        }
    }
}

/// SPEC §9.2: probe + footer + directory to open, one request per band
/// read — the whole session must stay within the measured budget.
#[test]
fn http_open_read_stats_within_request_budget() -> TestResult {
    let port = serve(Mode::Ranges(vector_bytes()?))?;
    let url = format!("http://127.0.0.1:{port}/cities_12.a.fgb");

    let mut reader = Reader::open_url(&url)?;
    assert_eq!(
        reader.bands(),
        vec![BandMeta {
            res: 2,
            offset: 1676,
            size: 44
        }]
    );
    let band = reader.read_band(2)?;
    assert_eq!(band.cells.len(), 2);
    let sum: u64 = band.cells.iter().map(|&(_, c)| u64::from(c)).sum();
    assert_eq!(sum, 12);

    // A London/Paris viewport matches only the shared European res-2 cell.
    let stats = reader.stats([-10.0, 40.0, 10.0, 60.0], 2)?;
    assert_eq!(stats.cells, 1);
    assert_eq!(stats.total, 8);

    let src = reader.into_inner();
    println!(
        "http session: {} requests, {} bytes",
        src.requests(),
        src.bytes_read()
    );
    assert!(
        src.requests() <= 5,
        "expected at most 5 requests (probe + footer + directory + 2 band reads), got {}",
        src.requests()
    );
    Ok(())
}

/// A 200-only host does not do range requests: open fails, naming them.
#[test]
fn full_only_server_is_rejected_naming_ranges() -> TestResult {
    let port = serve(Mode::FullOnly(vector_bytes()?))?;
    let url = format!("http://127.0.0.1:{port}/cities_12.a.fgb");
    match Reader::open_url(&url) {
        Ok(_) => Err("open_url must fail against a 200-only server".into()),
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("range"),
                "error must name range support, got: {msg}"
            );
            Ok(())
        }
    }
}

/// A server that promises N bytes and sends fewer yields a clean error
/// through `read_exact`, never a panic.
#[test]
fn short_read_server_errors_without_panic() -> TestResult {
    let port = serve(Mode::ShortRead(vector_bytes()?))?;
    let url = format!("http://127.0.0.1:{port}/cities_12.a.fgb");
    assert!(Reader::open_url(&url).is_err());
    Ok(())
}

/// SPEC §12: schemes are restricted before any network traffic — https
/// always, http for loopback hosts only.
#[test]
fn scheme_restrictions_are_enforced() {
    assert!(fgba::HttpRangeReader::open("ftp://example.com/f.a.fgb").is_err());
    assert!(fgba::HttpRangeReader::open("file:///etc/passwd").is_err());
    assert!(fgba::HttpRangeReader::open("http://example.com/f.a.fgb").is_err());
    assert!(fgba::HttpRangeReader::open("no-scheme").is_err());
}

#[test]
fn repeated_stats_reuses_the_cached_band_request() -> TestResult {
    let port = serve(Mode::Ranges(vector_bytes()?))?;
    let url = format!("http://127.0.0.1:{port}/cities_12.a.fgb");
    let mut reader = Reader::open_url(&url)?;
    assert_eq!(reader.stats([-180.0, -85.0, 180.0, 85.0], 2)?.total, 12);
    assert_eq!(reader.stats([-10.0, 40.0, 10.0, 60.0], 2)?.total, 8);
    assert_eq!(reader.into_inner().requests(), 4);
    Ok(())
}
