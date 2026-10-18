//! `fgba serve`: a minimal static file server with HTTP range request
//! support (RFC 9110 §14), bound to loopback — enough to exercise FGBA
//! over HTTP (SPEC §9.2). Ported and hardened from
//! `docs/reference/trailer.rs`'s `serve`.
//!
//! Hardening over the reference: GET/HEAD only, percent-decoded paths,
//! `..`/backslash/absolute paths refused, symlink escapes refused via
//! canonicalization under the served root, suffix (`bytes=-N`) and open
//! (`bytes=N-`) ranges, `416` on unsatisfiable ranges, empty-file safety,
//! per-connection I/O timeouts, and only the requested span is read from
//! disk. Requests are logged to stderr. One request per connection,
//! sequential connections — this is a test server, not a web server.

use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Request-head byte cap; anything larger is not one of our clients.
const MAX_HEAD: usize = 16 * 1024;

/// Per-connection read/write timeout so a stalled client cannot park the
/// sequential server forever.
const IO_TIMEOUT: Duration = Duration::from_secs(30);

/// Serve `path` (a file, or a directory's files) on `127.0.0.1:port`.
pub(crate) fn run(path: &Path, port: u16) -> Result<(), String> {
    let root = path
        .canonicalize()
        .map_err(|e| format!("serve: {}: {e}", path.display()))?;
    let single_file = root.is_file();
    let listener = TcpListener::bind(("127.0.0.1", port))
        .map_err(|e| format!("serve: cannot bind 127.0.0.1:{port}: {e}"))?;
    let addr = listener.local_addr().map_err(|e| format!("serve: {e}"))?;
    eprintln!(
        "serving {} on http://{addr} (range requests; ctrl-c to stop)",
        root.display()
    );
    for stream in listener.incoming() {
        match stream {
            Ok(s) => handle(s, &root, single_file),
            Err(e) => eprintln!("accept failed: {e}"),
        }
    }
    Ok(())
}

/// One connection: read the head, answer, close.
fn handle(mut stream: TcpStream, root: &Path, single_file: bool) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "?".to_string());
    let _ = stream.set_read_timeout(Some(IO_TIMEOUT));
    let _ = stream.set_write_timeout(Some(IO_TIMEOUT));

    let Some(head) = read_head(&mut stream) else {
        return;
    };
    let text = String::from_utf8_lossy(&head);
    let mut lines = text.lines();
    let mut parts = lines.next().unwrap_or("").split_whitespace();
    let method = parts.next().unwrap_or("");
    let raw_target = parts.next().unwrap_or("/");

    // Only GET and HEAD are meaningful here.
    let head_only = match method {
        "GET" => false,
        "HEAD" => true,
        _ => {
            let n = respond(&mut stream, 405, "Method Not Allowed");
            log(&peer, method, raw_target, 405, n);
            return;
        }
    };

    // Resolve the target to a file under the root, refusing escapes.
    let file = match resolve(root, single_file, raw_target) {
        Ok(f) => f,
        Err(status) => {
            let (text, code) = match status {
                403 => ("Forbidden", 403),
                _ => ("Not Found", 404),
            };
            let n = respond(&mut stream, code, text);
            log(&peer, method, raw_target, code, n);
            return;
        }
    };
    let meta = match std::fs::metadata(&file) {
        Ok(m) => m,
        Err(_) => {
            let n = respond(&mut stream, 404, "Not Found");
            log(&peer, method, raw_target, 404, n);
            return;
        }
    };
    let len = meta.len();

    match range_header(&text, len) {
        Range::None => {
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {len}\r\nAccept-Ranges: bytes\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n"
            );
            let sent = send_file(&mut stream, &file, 0, len, head_only, &head);
            log(&peer, method, raw_target, 200, sent.unwrap_or(0));
        }
        Range::Span(start, end) => {
            let count = end - start + 1;
            let head = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {count}\r\nContent-Range: bytes {start}-{end}/{len}\r\nAccept-Ranges: bytes\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n"
            );
            let sent = send_file(&mut stream, &file, start, count, head_only, &head);
            log(&peer, method, raw_target, 206, sent.unwrap_or(0));
        }
        Range::Unsatisfiable => {
            let head = format!(
                "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{len}\r\nContent-Length: 0\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
            );
            let sent = send_head(&mut stream, &head);
            log(&peer, method, raw_target, 416, sent.unwrap_or(0));
        }
    }
}

/// What the request's Range header asks for.
enum Range {
    /// No (parseable) Range header — RFC 9110: ignore invalid ranges.
    None,
    /// Inclusive byte span, clamped to the file.
    Span(u64, u64),
    /// Well-formed but unsatisfiable → 416.
    Unsatisfiable,
}

/// Parse `Range: bytes=…` against a file of `len` bytes. Supports
/// `a-b`, `a-` and suffix `-b`; only the first range of a list is used.
fn range_header(head: &str, len: u64) -> Range {
    let Some(value) = head
        .lines()
        .find_map(|l| {
            l.split_once(':')
                .filter(|(k, _)| k.eq_ignore_ascii_case("range"))
        })
        .map(|(_, v)| v.trim().to_string())
    else {
        return Range::None;
    };
    let Some(spec) = value
        .strip_prefix("bytes=")
        .or_else(|| value.strip_prefix("Bytes="))
        .map(str::trim)
    else {
        return Range::None;
    };
    let first = spec.split(',').next().unwrap_or("").trim();
    let Some((a, b)) = first.split_once('-') else {
        return Range::None;
    };
    if len == 0 {
        return Range::Unsatisfiable;
    }
    let last = len - 1;
    if a.is_empty() {
        // Suffix range: the last b bytes.
        match b.parse::<u64>() {
            Ok(0) | Err(_) => Range::Unsatisfiable,
            Ok(n) => Range::Span(len.saturating_sub(n).min(last), last),
        }
    } else {
        let Ok(start) = a.parse::<u64>() else {
            return Range::None;
        };
        if start > last {
            return Range::Unsatisfiable;
        }
        let end = if b.is_empty() {
            last
        } else {
            match b.parse::<u64>() {
                Ok(v) => v.min(last),
                Err(_) => return Range::None,
            }
        };
        if start > end {
            Range::Unsatisfiable
        } else {
            Range::Span(start, end)
        }
    }
}

/// Resolve a request target to a canonical file path under `root`.
/// Err(403) for traversal attempts, Err(404) for anything not a file.
fn resolve(root: &Path, single_file: bool, raw_target: &str) -> Result<PathBuf, u16> {
    if single_file {
        return Ok(root.to_path_buf());
    }
    let decoded = url_decode(raw_target.split(['?', '#']).next().unwrap_or(""));
    if decoded.contains('\\') {
        return Err(403u16);
    }
    let mut joined = root.to_path_buf();
    for component in decoded.split('/') {
        match component {
            "" | "." => {}
            ".." => return Err(403u16),
            c => joined.push(c),
        }
    }
    let canonical = joined.canonicalize().map_err(|_| 404u16)?;
    if !canonical.starts_with(root) {
        return Err(403u16);
    }
    if !canonical.is_file() {
        return Err(404u16); // no directory listing
    }
    Ok(canonical)
}

/// Percent-decode `%XX` bytes once; anything else passes through.
fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let decoded = if bytes[i] == b'%' {
            bytes
                .get(i + 1..i + 3)
                .and_then(|h| std::str::from_utf8(h).ok())
                .and_then(|h| u8::from_str_radix(h, 16).ok())
        } else {
            None
        };
        match decoded {
            Some(b) => {
                out.push(b);
                i += 3;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Read the request head byte by byte until `\r\n\r\n`; None on EOF,
/// timeout, or a head over MAX_HEAD.
fn read_head(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut head = Vec::with_capacity(512);
    let mut byte = [0u8; 1];
    while head.len() < MAX_HEAD {
        if stream.read(&mut byte).ok()? == 0 {
            return None;
        }
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            return Some(head);
        }
    }
    None
}

/// Write a bare status response with no body.
fn respond(stream: &mut TcpStream, code: u16, text: &str) -> u64 {
    let head = format!(
        "HTTP/1.1 {code} {text}\r\nContent-Length: 0\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
    );
    send_head(stream, &head).unwrap_or(0)
}

/// Write a response head; bytes sent.
fn send_head(stream: &mut TcpStream, head: &str) -> std::io::Result<u64> {
    stream.write_all(head.as_bytes())?;
    Ok(head.len() as u64)
}

/// Write the head, then stream `count` bytes of `file` starting at
/// `offset` (nothing for HEAD). Returns body bytes sent.
fn send_file(
    stream: &mut TcpStream,
    file: &Path,
    offset: u64,
    count: u64,
    head_only: bool,
    head: &str,
) -> std::io::Result<u64> {
    send_head(stream, head)?;
    if head_only || count == 0 {
        return Ok(0);
    }
    let mut f = std::fs::File::open(file)?;
    f.seek(SeekFrom::Start(offset))?;
    let mut limited = f.take(count);
    std::io::copy(&mut limited, stream)
}

fn log(peer: &str, method: &str, target: &str, status: u16, bytes: u64) {
    eprintln!("{peer} {method} {target} → {status} ({bytes} B)");
}
