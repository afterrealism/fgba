//! Read/write round trip: build a small FlatGeobuf in memory, append the
//! FGBA aggregate trailer with [`fgba::build`], write `cities.a.fgb` to a
//! temporary directory, reopen it from disk with [`fgba::Reader::open_path`],
//! and answer count queries from the trailer alone.
//!
//! Run with: `cargo run -p fgba --example read_write`

use flatgeobuf::geozero::{GeomProcessor, GeozeroGeometry};
use flatgeobuf::{FgbWriter, GeometryType};
use std::error::Error;

/// The 12 points of the normative test vector (SPEC Appendix A):
/// 4 Paris, 4 London, 4 New York City.
const CITIES: [(f64, f64); 12] = [
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

/// Minimal point geometry for the FlatGeobuf writer.
struct Point(f64, f64);

impl GeozeroGeometry for Point {
    fn process_geom<P: GeomProcessor>(&self, p: &mut P) -> flatgeobuf::geozero::error::Result<()> {
        p.point_begin(0)?;
        p.xy(self.0, self.1, 0)?;
        p.point_end(0)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    // 1. Write the 12 cities as an in-memory FlatGeobuf body (Hilbert-sorted,
    //    with a spatial index and a feature count, as SPEC §4 requires).
    let mut writer = FgbWriter::create("cities", GeometryType::Point)?;
    for &(lon, lat) in &CITIES {
        writer.add_feature_geom(Point(lon, lat), |_feat| {})?;
    }
    let mut body = Vec::new();
    writer.write(&mut body)?;
    println!(
        "body: {} cities, {} bytes of FlatGeobuf",
        CITIES.len(),
        body.len()
    );

    // 2. Append the aggregate trailer (one band at A5 resolution 2 here).
    let (file, report) = fgba::build(&body)?;
    let ladder = report
        .bands
        .iter()
        .map(|r| r.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    println!(
        "trailer: {} bytes, band(s) at A5 resolution {ladder}",
        report.trailer_bytes
    );

    // 3. Write cities.a.fgb to a temporary directory and reopen it from disk.
    let path = std::env::temp_dir().join("cities.a.fgb");
    std::fs::write(&path, &file)?;
    let mut reader = fgba::Reader::open_path(&path)?;
    println!(
        "wrote {} ({} bytes) and reopened it",
        path.display(),
        file.len()
    );
    println!(
        "body length: {} bytes (the trailer starts there)",
        reader.body_len()
    );
    for band in reader.bands() {
        let b = reader.read_band(band.res)?;
        println!(
            "  band res {}: {} cells, {} features, {} bytes at offset {}",
            band.res,
            b.cells.len(),
            b.total,
            band.size,
            band.offset
        );
    }

    // 4. Count analytics over Europe: London + Paris share one res-2 cell.
    let europe = reader.stats([-11.0, 35.0, 31.0, 71.0], 2)?;
    println!(
        "stats over Europe [-11, 35, 31, 71] @ res 2: {} cell(s), ~{} features",
        europe.cells, europe.total
    );

    // 5. A raw query over New York returns the (cell, count) records.
    let nyc = reader.query([-75.0, 39.0, -72.0, 42.0], 2)?;
    println!("query over New York [-75, 39, -72, 42] @ res 2:");
    for (cell, count) in nyc {
        println!("  cell {cell:#018x}: {count} features");
    }

    // 6. The whole world, answered from a few dozen trailer bytes.
    let world = reader.stats([-180.0, -85.0, 180.0, 85.0], 2)?;
    println!(
        "stats over the world: {} cell(s), ~{} features",
        world.cells, world.total
    );
    Ok(())
}
