//! `fgba bench`: time aggregate (`stats`) queries at each zoom-ladder rung
//! over the file's own extent, with half-width-stepped sample viewports
//! (SPEC §9.4). Local files report wall time; HTTP sources additionally
//! report the range reader's request/byte counters.

use fgba::{ladder_resolution, Reader};
use flatgeobuf::FgbReader;
use std::io::{Read, Seek};
use std::time::Instant;

use crate::{fmt_bytes, fmt_duration, header_info, Source};

/// Web Mercator latitude clamp for viewport coverings (SPEC §9.4 step 0).
const MAX_LAT: f64 = 85.0511;

/// Sample viewports per rung are capped so bench stays fast on big extents.
const MAX_VIEWPORTS: usize = 64;

/// Halvings of the extent width tried when matching a band to a ladder rung.
const MAX_HALVINGS: u32 = 24;

pub(crate) fn run(src: &Source) -> Result<(), String> {
    let label = src.label();
    match src {
        Source::Path(path) => {
            let reader = Reader::open_path(path).map_err(|e| format!("{label}: {e}"))?;
            bench(reader, &label).map(|_| ())
        }
        Source::Url(url) => {
            let reader = Reader::open_url(url).map_err(|e| format!("{label}: {e}"))?;
            let http = bench(reader, &label)?;
            println!(
                "http: {} requests, {} read (incl. open)",
                http.requests(),
                fmt_bytes(http.bytes_read())
            );
            Ok(())
        }
    }
}

/// Run the ladder and print the table. Returns the inner byte source so
/// HTTP callers can read the transport counters.
fn bench<R: Read + Seek>(mut reader: Reader<R>, label: &str) -> Result<R, String> {
    let header = header_info(
        FgbReader::open(reader.body().map_err(|e| format!("{label}: {e}"))?)
            .map_err(|e| format!("{label}: cannot read FlatGeobuf header: {e}"))?,
    );
    let [min_x, min_y, max_x, max_y] = header
        .envelope
        .ok_or_else(|| format!("{label}: body header has no envelope; nothing to bench"))?;
    let extent_w = max_x - min_x;
    if !extent_w.is_finite() || extent_w <= 0.0 {
        return Err(format!(
            "{label}: degenerate envelope [{min_x}, {min_y}, {max_x}, {max_y}]"
        ));
    }
    let bands = reader.bands();
    if bands.is_empty() {
        return Err(format!("{label}: no aggregate bands; nothing to bench"));
    }

    println!("bench: {label}");
    println!(
        "  extent: [{min_x}, {min_y}, {max_x}, {max_y}] ({} features)",
        header.features
    );
    println!("  band res   viewport°   viewports   wall total   wall/viewport   est. features");
    for band in &bands {
        let width = ladder_width(band.res, extent_w);
        let viewports = sample_viewports([min_x, min_y, max_x, max_y], width);
        let n = viewports.len();
        let t0 = Instant::now();
        let mut features = 0u64;
        for view in &viewports {
            let stats = reader
                .stats(*view, band.res)
                .map_err(|e| format!("{label}: stats failed at res {}: {e}", band.res))?;
            features += stats.total;
        }
        let wall = t0.elapsed();
        let per = wall.div_f64(n.max(1) as f64);
        println!(
            "  {:>8} {:>11.4} {:>11} {:>11} {:>14} {:>14}",
            band.res,
            width,
            n,
            fmt_duration(wall),
            fmt_duration(per),
            features
        );
    }
    Ok(reader.into_inner())
}

/// The viewport width whose ladder rung (SPEC §9.4, `ladder_resolution`) is
/// `res`: the widest halving of the extent width that maps to it. Bands
/// finer than the ladder's deepest rung bench at the smallest width tried.
fn ladder_width(res: u8, extent_w: f64) -> f64 {
    let mut w = extent_w;
    for _ in 0..MAX_HALVINGS {
        if ladder_resolution(w) == res {
            return w;
        }
        w /= 2.0;
    }
    w
}

/// Viewports of `width` degrees over the extent, stepped by half widths,
/// latitudes clamped to the Web Mercator range.
fn sample_viewports(extent: [f64; 4], width: f64) -> Vec<[f64; 4]> {
    let [min_x, min_y, max_x, max_y] = extent;
    let min_y = min_y.clamp(-MAX_LAT, MAX_LAT);
    let max_y = max_y.clamp(-MAX_LAT, MAX_LAT);
    let step = (width / 2.0).max(f64::EPSILON);
    let mut out = Vec::new();
    let mut y = min_y;
    while y < max_y && out.len() < MAX_VIEWPORTS {
        let mut x = min_x;
        while x < max_x && out.len() < MAX_VIEWPORTS {
            out.push([x, y, (x + width).min(max_x), (y + width).min(max_y)]);
            x += step;
        }
        y += step;
    }
    if out.is_empty() {
        out.push([min_x, min_y, max_x, max_y]);
    }
    out
}
