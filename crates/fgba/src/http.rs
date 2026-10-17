//! HTTP range-request reading of FGBA files (SPEC §9.2).
//!
//! [`HttpRangeReader`] adapts a remote FGBA file to a blocking
//! `Read + Seek` source over RFC 9110 range requests, so the core
//! [`Reader`] works on it unchanged:
//!
//! 1. [`HttpRangeReader::open`] probes `Range: bytes=0-0` once. The server
//!    MUST answer `206 Partial Content`; its `Content-Range` header yields
//!    the file length. A `200` answer means the host does not do range
//!    requests and is an error naming range support (SPEC §9.2 step 1).
//! 2. Every `read` is exactly one `Range` request for the wanted span;
//!    `seek` only moves the local cursor. The core `Reader` reads the
//!    footer and directory once per session and each band once, so a full
//!    open + one band costs 4 requests (measured in SPEC §9.2/Appendix B).
//!
//! Hardening (SPEC §12): a 60 s global per-request timeout, at most 5
//! followed redirects, URL schemes restricted to `https` (with `http`
//! allowed for loopback hosts only), and https→http downgrades refused —
//! for `https` URLs the agent enforces https on every hop, redirects
//! included. A non-206 answer mid-read is an error naming range support,
//! never a silent full-body read.

use crate::error::{Error, Result};
use crate::read::Reader;
use std::io::{Read, Seek, SeekFrom};
use std::time::Duration;

/// Global timeout (connect + transfer) applied to every HTTP request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Redirects followed per request (SPEC §12: follow redirects cautiously).
const MAX_REDIRECTS: u32 = 5;

/// A blocking `Read + Seek` byte source over HTTP range requests
/// (SPEC §9.2), with request and transfer counters for diagnostics.
#[derive(Debug)]
pub struct HttpRangeReader {
    url: String,
    agent: ureq::Agent,
    /// Total file length from the open probe's `Content-Range`.
    len: u64,
    pos: u64,
    requests: usize,
    bytes_read: u64,
}

impl HttpRangeReader {
    /// Open a remote FGBA file: validate the URL scheme, then probe
    /// `Range: bytes=0-0` and parse the file length from `Content-Range`.
    ///
    /// Errors ([`Error::Http`]) when the scheme is not `https` (or `http`
    /// for a loopback host), when the probe is not answered
    /// `206 Partial Content` — the error names range support — or when the
    /// `Content-Range` header is missing or unparseable.
    pub fn open(url: &str) -> Result<HttpRangeReader> {
        let https = https_required(url)?;
        // For https URLs, `https_only(true)` refuses any http hop, so an
        // https→http redirect (downgrade) fails instead of being followed.
        let config = ureq::Agent::config_builder()
            .timeout_global(Some(REQUEST_TIMEOUT))
            .max_redirects(MAX_REDIRECTS)
            .https_only(https)
            .build();
        let agent = ureq::Agent::new_with_config(config);

        let res = agent
            .get(url)
            .header("Range", "bytes=0-0")
            .call()
            .map_err(|e| Error::Http(format!("range probe of {url:?} failed: {e}")))?;
        let status = res.status().as_u16();
        if status != 206 {
            return Err(Error::Http(format!(
                "server answered {status} to a Range: bytes=0-0 probe; reading FGBA over HTTP \
                 requires a host with range request support (206 Partial Content, SPEC §9.2)"
            )));
        }
        let content_range = res
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
            .ok_or_else(|| {
                Error::Http("206 answer to the range probe has no Content-Range header".to_string())
            })?;
        let len = content_range_len(&content_range)?;
        Ok(HttpRangeReader {
            url: url.to_string(),
            agent,
            len,
            pos: 0,
            requests: 1,
            bytes_read: 1,
        })
    }

    /// HTTP requests issued so far, including the open probe.
    pub fn requests(&self) -> usize {
        self.requests
    }

    /// Response-body bytes fetched so far, including the open probe byte.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }
}

impl Read for HttpRangeReader {
    /// Fetch `buf` with one `Range` request for `[pos, pos + want)`.
    ///
    /// A short transfer returns a short read (callers using `read_exact`
    /// see `UnexpectedEof`); a non-206 answer is an error naming range
    /// support, never a silently accepted full body.
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || self.pos >= self.len {
            return Ok(0);
        }
        let want = (buf.len() as u64).min(self.len - self.pos);
        let last = self.pos + want - 1;
        let res = self
            .agent
            .get(&self.url)
            .header("Range", format!("bytes={}-{last}", self.pos))
            .call()
            .map_err(std::io::Error::other)?;
        let status = res.status().as_u16();
        if status != 206 {
            return Err(std::io::Error::other(format!(
                "server answered {status} to a mid-read range request; reading FGBA over HTTP \
                 requires a host with range request support (206 Partial Content, SPEC §9.2)"
            )));
        }
        let mut body = res.into_body().into_reader();
        let mut filled = 0usize;
        while filled < want as usize {
            let n = body.read(&mut buf[filled..want as usize])?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        self.pos += filled as u64;
        self.requests += 1;
        self.bytes_read += filled as u64;
        Ok(filled)
    }
}

impl Seek for HttpRangeReader {
    /// Move the local cursor only; no request is issued until the next read.
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let next = match pos {
            SeekFrom::Start(offset) => i128::from(offset),
            SeekFrom::Current(delta) => i128::from(self.pos) + i128::from(delta),
            SeekFrom::End(delta) => i128::from(self.len) + i128::from(delta),
        };
        if next < 0 || next > i128::from(u64::MAX) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("seek to out-of-range position {next}"),
            ));
        }
        self.pos = next as u64;
        Ok(self.pos)
    }
}

impl Reader<HttpRangeReader> {
    /// Open an FGBA file over HTTP range requests (SPEC §9.2): probe the
    /// server, then read the footer and directory through the range reader.
    ///
    /// The URL must be `https`, or `http` for a loopback host (SPEC §12).
    pub fn open_url(url: &str) -> Result<Reader<HttpRangeReader>> {
        Reader::open(HttpRangeReader::open(url)?)
    }
}

/// Validate the URL scheme (SPEC §12): `https` is always allowed; `http`
/// only for loopback hosts (`localhost`, `127.0.0.0/8`, `[::1]`). Returns
/// whether the agent must enforce https on every hop including redirects.
fn https_required(url: &str) -> Result<bool> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| Error::Http(format!("url {url:?} has no scheme")))?;
    if scheme.eq_ignore_ascii_case("https") {
        return Ok(true);
    }
    if !scheme.eq_ignore_ascii_case("http") {
        return Err(Error::Http(format!(
            "unsupported url scheme {scheme:?}: https is required (http is allowed for loopback \
             hosts only, SPEC §12)"
        )));
    }
    let authority = rest.split('/').next().unwrap_or("");
    let host = authority.rsplit('@').next().unwrap_or(authority);
    let host = if let Some(v6) = host.strip_prefix('[') {
        v6.split(']').next().unwrap_or("")
    } else {
        host.split(':').next().unwrap_or("")
    };
    let loopback =
        host.eq_ignore_ascii_case("localhost") || host.starts_with("127.") || host == "::1";
    if loopback {
        Ok(false)
    } else {
        Err(Error::Http(format!(
            "refusing plain-http url {url:?}: http is allowed for loopback hosts only (SPEC §12)"
        )))
    }
}

/// Parse the total length out of a `Content-Range` header
/// (`bytes 0-0/1777`); a `*` total is an error.
fn content_range_len(header: &str) -> Result<u64> {
    // `rsplit` always yields one item; an empty or headerless total fails
    // the integer parse below.
    let total = header.rsplit('/').next().map(str::trim).unwrap_or("");
    if total == "*" {
        return Err(Error::Http(format!(
            "Content-Range {header:?} does not state the file length"
        )));
    }
    total
        .parse::<u64>()
        .map_err(|_| Error::Http(format!("unparseable Content-Range {header:?}")))
}
