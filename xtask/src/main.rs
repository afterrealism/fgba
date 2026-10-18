//! xtask: test-vector tooling for the FGBA workspace.
//!
//! - `cargo xtask gen-testdata` — rebuild `testdata/cities_12.a.fgb` from the
//!   12 known coordinates with the library's `build()` and check the produced
//!   trailer against the committed file (deterministic build, SPEC §8.5).
//! - `cargo xtask verify-vectors` — assert the normative SPEC Appendix A
//!   numbers against the committed file through the public `fgba` API.

use fgba::{BandMeta, Reader};
use flatgeobuf::geozero::{GeomProcessor, GeozeroGeometry};
use flatgeobuf::{FgbWriter, GeometryType};
use std::error::Error;
use std::io::Cursor;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("error: {msg}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("gen-testdata") => gen_testdata(args.any(|a| a == "--write")),
        Some("verify-vectors") => verify_vectors(),
        _ => Err("usage: cargo xtask gen-testdata [--write] | verify-vectors".into()),
    }
}

/// The 12 points of the normative vector: 4 Paris, 4 London, 4 New York City
/// (SPEC Appendix A, `crates/fgba/tests/vectors.rs`).
const POINTS: [(f64, f64); 12] = [
    (2.36, 48.84),
    (2.37, 48.85),
    (2.33, 48.86),
    (2.34, 48.87),
    (-0.14, 51.49),
    (-0.09, 51.51),
    (-0.11, 51.52),
    (-0.12, 51.5),
    (-73.97, 40.73),
    (-73.98, 40.72),
    (-74.0, 40.71),
    (-74.02, 40.7),
];

/// The normative vector's two occupied res-2 cells (SPEC Appendix A).
const NYC_CELL: u64 = 0x2680_0000_0000_0000;
const EUROPE_CELL: u64 = 0x6380_0000_0000_0000;

/// Minimal point geometry for the FlatGeobuf writer.
struct Point(f64, f64);

impl GeozeroGeometry for Point {
    fn process_geom<P: GeomProcessor>(&self, p: &mut P) -> flatgeobuf::geozero::error::Result<()> {
        p.point_begin(0)?;
        p.xy(self.0, self.1, 0)?;
        p.point_end(0)
    }
}

/// The vector's path inside the workspace, independent of the caller's cwd.
fn vector_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../testdata/cities_12.a.fgb")
}

/// Per-crate copies of the normative vector: tests inside a packaged .crate
/// cannot reach `../../testdata`, so each published crate carries its own.
fn crate_copies() -> [PathBuf; 2] {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    [
        root.join("crates/fgba/tests/data/cities_12.a.fgb"),
        root.join("crates/fgba-cli/tests/data/cities_12.a.fgb"),
    ]
}

/// Keep the per-crate copies byte-identical to the canonical vector: refresh
/// them when `write`, otherwise fail on drift.
fn sync_copies(canonical: &[u8], write: bool) -> Result<(), Box<dyn Error>> {
    for path in crate_copies() {
        if std::fs::read(&path).ok().as_deref() == Some(canonical) {
            continue;
        }
        if write {
            std::fs::write(&path, canonical)?;
            println!("synced {}", path.display());
        } else {
            return Err(format!(
                "{} differs from testdata/cities_12.a.fgb — run `cargo xtask gen-testdata --write`",
                path.display()
            )
            .into());
        }
    }
    Ok(())
}

/// Write the 12 points as a FlatGeobuf body in memory (no properties: the
/// committed vector additionally carries a `name` column, so its body is not
/// reproduced from coordinates alone — the trailer comparison is structural).
fn fgb_body() -> Result<Vec<u8>, Box<dyn Error>> {
    let mut writer = FgbWriter::create("cities", GeometryType::Point)?;
    for &(lon, lat) in &POINTS {
        writer.add_feature_geom(Point(lon, lat), |_feat| {})?;
    }
    let mut body = Vec::new();
    writer.write(&mut body)?;
    Ok(body)
}

/// `cargo xtask gen-testdata [--write]`: rebuild the vector and check that
/// the produced trailer equals the committed file's (from the sentinel
/// onward, SPEC Appendix A). Without `--write` the committed file is left
/// untouched; with `--write` it is replaced atomically.
fn gen_testdata(write: bool) -> Result<(), Box<dyn Error>> {
    let path = vector_path();
    let body = fgb_body()?;
    let (file, report) = fgba::build(&body)?;
    println!(
        "rebuilt: body {} B + trailer {} B = {} B (bands {:?})",
        body.len(),
        report.trailer_bytes,
        file.len(),
        report.bands
    );

    let committed = std::fs::read(&path).ok();
    match committed {
        Some(committed) => {
            println!("committed: {} B", committed.len());
            compare_trailers(&committed, &file)?;
            if committed == file {
                println!(
                    "unchanged: {} matches the deterministic build",
                    path.display()
                );
                sync_copies(&committed, write)?;
                return Ok(());
            }
            if !write {
                println!(
                    "bodies differ (the committed vector's body carries a `name` column\n\
                     written by a different FlatGeobuf options set); the trailer sections\n\
                     match. Leaving the normative vector untouched — pass --write to replace it\n\
                     (verify-vectors' Appendix A numbers are tied to the committed body)."
                );
                sync_copies(&committed, false)?;
                return Ok(());
            }
        }
        None => println!("no committed file at {}", path.display()),
    }

    // Atomic write: same-directory temp file + rename.
    let tmp = path.with_extension("a.fgb.tmp");
    std::fs::write(&tmp, &file)?;
    std::fs::rename(&tmp, &path)?;
    println!("wrote {} ({} B)", path.display(), file.len());
    sync_copies(&file, true)?;
    Ok(())
}

/// Check that two complete FGBA files carry the same trailer: identical band
/// sections, and identical directory entries apart from base-shifted absolute
/// offsets (the body may differ when another FlatGeobuf writer produced it).
fn compare_trailers(committed: &[u8], rebuilt: &[u8]) -> Result<(), Box<dyn Error>> {
    let mut old = Reader::open(Cursor::new(committed))?;
    let mut new = Reader::open(Cursor::new(rebuilt))?;
    let delta = i128::from(new.body_len()) - i128::from(old.body_len());
    if delta == 0 {
        let start = usize::try_from(old.body_len())?;
        if committed[start..] == rebuilt[start..] {
            println!(
                "trailer (from the sentinel onward): byte-identical, {} B",
                committed.len() - start
            );
            return Ok(());
        }
    }

    let old_sections = old.directory().sections.clone();
    let new_sections = new.directory().sections.clone();
    check(
        old_sections.len() == new_sections.len(),
        format!(
            "directory section count differs: {} vs {}",
            old_sections.len(),
            new_sections.len()
        ),
    )?;
    for (o, n) in old_sections.iter().zip(&new_sections) {
        check(
            (o.kind, o.flags, o.size, &o.meta) == (n.kind, n.flags, n.size, &n.meta),
            format!("directory entry mismatch: {o:?} vs {n:?}"),
        )?;
        check(
            i128::from(n.offset) - i128::from(o.offset) == delta,
            format!("section offset shift is not the body-length delta {delta}: {o:?} vs {n:?}"),
        )?;
    }
    for band in old.bands() {
        check(
            old.read_band(band.res)? == new.read_band(band.res)?,
            format!("band res {} content diverged", band.res),
        )?;
    }
    println!(
        "trailers match section-by-section (offsets shifted by {delta}):\n  \
         committed {} B, rebuilt {} B",
        committed.len(),
        rebuilt.len()
    );
    Ok(())
}

/// `cargo xtask verify-vectors`: assert every normative SPEC Appendix A
/// number against the committed vector.
fn verify_vectors() -> Result<(), Box<dyn Error>> {
    let path = vector_path();
    let bytes = std::fs::read(&path)?;
    println!("verify {} ({} B)", path.display(), bytes.len());
    sync_copies(&bytes, false)?;

    check(
        bytes.len() == 1777,
        format!("file size {} != 1777", bytes.len()),
    )?;
    println!("  total file size: 1777 B — OK");

    // Footer (SPEC §7): magic, directory offset/size, directory CRC-32.
    let footer = fgba::Footer::decode(
        bytes
            .get(bytes.len() - 32..)
            .ok_or("file shorter than the 32-byte footer")?,
    )?;
    check(
        footer.dir_offset == 1720,
        format!("dir offset {} != 1720", footer.dir_offset),
    )?;
    check(
        footer.dir_size == 25,
        format!("dir size {} != 25", footer.dir_size),
    )?;
    check(
        footer.dir_crc32 == 0x41fc_73cb,
        format!("dir CRC {:#010x} != 0x41fc73cb", footer.dir_crc32),
    )?;
    println!("  footer: dir offset 1720, dir size 25, CRC 0x41fc73cb — OK");

    let mut reader = Reader::open(Cursor::new(&bytes))?;
    check(
        reader.body_len() == 1672,
        format!("body length {} != 1672", reader.body_len()),
    )?;
    check(
        reader.bands()
            == vec![BandMeta {
                res: 2,
                offset: 1676,
                size: 44,
            }],
        format!("bands {:?} != [res 2 @ 1676, 44 B]", reader.bands()),
    )?;
    println!("  body 1672 B; band res 2 at offset 1676, size 44 — OK");

    let band = reader.read_band(2)?;
    check(band.total == 12, format!("band total {} != 12", band.total))?;
    check(
        band.cells == vec![(NYC_CELL, 4), (EUROPE_CELL, 8)],
        format!("band cells {:?} != [(NYC, 4), (Europe, 8)]", band.cells),
    )?;
    println!("  cells: 0x2680… count 4 (NYC), 0x6380… count 8 (London+Paris) — OK");

    let stats = reader.stats([-180.0, -85.0, 180.0, 85.0], 2)?;
    check(
        stats.cells == 2 && stats.total == 12,
        format!("world stats {stats:?} != 2 cells / 12 features"),
    )?;
    println!("  world stats: 2 cells, 12 features — OK");
    println!("OK: testdata/cities_12.a.fgb matches SPEC Appendix A");
    Ok(())
}

fn check(ok: bool, msg: String) -> Result<(), Box<dyn Error>> {
    if ok {
        Ok(())
    } else {
        Err(msg.into())
    }
}
