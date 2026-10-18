//! Read an FGBA file over HTTP range requests (SPEC §9.2): print the
//! aggregate bands, one count query, and the HTTP request/byte counters.
//!
//! **Requires the `http` feature** — it is off by default, so run with:
//!
//! ```sh
//! cargo run -p fgba --example http_reader --features http -- <url>
//! ```
//!
//! Try it against the test vector on loopback:
//!
//! ```sh
//! cargo run -p fgba-cli -- serve testdata/cities_12.a.fgb &
//! cargo run -p fgba --example http_reader --features http -- http://127.0.0.1:8080/
//! ```

use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let Some(url) = std::env::args().nth(1) else {
        println!("usage: http_reader <url-of-an-.a.fgb>  (build with --features http)");
        println!("example: serve the test vector with `fgba serve testdata/cities_12.a.fgb`,");
        println!("then pass http://127.0.0.1:8080/ — https is required for remote hosts.");
        return Ok(());
    };

    // Opening costs 3 requests: the range probe, the 32-byte footer, the
    // CRC-verified directory (SPEC §9.2, Appendix B).
    let mut reader = fgba::Reader::open_url(&url)?;
    println!("{url} — FGBA over HTTP range requests");
    println!("body length: {} bytes", reader.body_len());

    let bands = reader.bands();
    if bands.is_empty() {
        println!("no aggregate bands in this file");
        return Ok(());
    }
    for band in &bands {
        let b = reader.read_band(band.res)?;
        println!(
            "  band res {}: {} cells, {} features ({} bytes)",
            band.res,
            b.cells.len(),
            b.total,
            band.size
        );
    }

    // One count query at the deepest available band resolution.
    let res = bands.iter().map(|b| b.res).max().unwrap_or(0);
    let stats = reader.stats([-180.0, -85.0, 180.0, 85.0], res)?;
    println!(
        "stats over the world @ res {res}: {} cell(s), ~{} features",
        stats.cells, stats.total
    );

    let http = reader.into_inner();
    println!(
        "http: {} requests, {} bytes read",
        http.requests(),
        http.bytes_read()
    );
    Ok(())
}
