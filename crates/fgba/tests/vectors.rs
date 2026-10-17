//! Integration tests against the normative test vector (SPEC.md Appendix A,
//! `testdata/cities_12.a.fgb`) and round-trips through `build` + `Reader`.

use fgba::band::AggBand;
use fgba::error::Error;
use fgba::format::{crc32, Directory, Footer, SectionEntry, KIND_AGGREGATE_BAND, SENTINEL};
use fgba::{build, ladder_resolution, BandMeta, BandStats, Reader};
use flatgeobuf::geozero::{GeomProcessor, GeozeroGeometry};
use flatgeobuf::{FallibleStreamingIterator, FgbReader, FgbWriter, FgbWriterOptions, GeometryType};
use std::io::Cursor;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// The 12 points of the normative vector: 4 Paris, 4 London, 4 New York.
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

/// Write the 12 points as a FlatGeobuf body in memory.
fn fgb_body() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut writer = FgbWriter::create("cities", GeometryType::Point)?;
    for &(lon, lat) in &POINTS {
        writer.add_feature_geom(Point(lon, lat), |_feat| {})?;
    }
    let mut body = Vec::new();
    writer.write(&mut body)?;
    Ok(body)
}

/// The normative vector file.
fn vector_bytes() -> std::io::Result<Vec<u8>> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../testdata/cities_12.a.fgb"
    );
    std::fs::read(path)
}

#[test]
fn round_trip_build_open_query() -> TestResult {
    let body = fgb_body()?;
    let body_len = body.len() as u64;
    let (file, report) = build(&body)?;
    assert_eq!(report.features, 12);
    assert_eq!(report.skipped, 0);
    assert_eq!(report.bands, vec![2]); // 4*12 = 48 <= 240 cells at res 2
    assert_eq!(report.trailer_bytes, file.len() - body.len());

    let mut reader = Reader::open(Cursor::new(&file))?;
    assert_eq!(reader.body_len(), body_len);
    assert_eq!(
        reader.bands(),
        vec![BandMeta {
            res: 2,
            offset: body_len + 4,
            size: 44
        }]
    );

    // The band is a lossless partition of the dataset (SPEC §6).
    let band = reader.read_band(2)?;
    assert_eq!(band.res, 2);
    assert_eq!(band.total, 12);
    assert_eq!(band.cells.len(), 2);
    let sum: u64 = band.cells.iter().map(|&(_, c)| u64::from(c)).sum();
    assert_eq!(sum, 12);

    // Aggregate queries over viewports (SPEC §9.4 covering semantics).
    // NB: a ring at exactly ±90° latitude is degenerate for the A5 covering
    // (the poles are points); the world view here stops at ±85°.
    let world = [-180.0, -85.0, 180.0, 85.0];
    let got = reader.query(world, 2)?;
    assert_eq!(got, vec![(NYC_CELL, 4), (EUROPE_CELL, 8)]);
    let stats = reader.stats(world, 2)?;
    assert_eq!(stats.cells, 2);
    assert_eq!(stats.total, 12);
    // A viewport around New York matches only the NYC cell.
    let nyc = reader.stats([-75.0, 39.0, -72.0, 42.0], 2)?;
    assert_eq!(nyc, BandStats { cells: 1, total: 4 });

    // The band section our writer emits is byte-identical to the normative
    // vector's (deterministic build, SPEC §8.5). The directory and footer
    // embed absolute offsets, so they necessarily differ when the body
    // bytes differ; the band section does not depend on the body.
    let vector = vector_bytes()?;
    assert_eq!(
        &file[body.len() + 4..body.len() + 4 + 44],
        &vector[1676..1720],
        "band section bytes diverge from the vector"
    );

    // The result is still a valid FlatGeobuf file (SPEC §10.1): a plain
    // sequential FGB reader sees exactly the 12 features and stops.
    let mut fgb = FgbReader::open(reader.body()?).and_then(FgbReader::select_all)?;
    let mut features = 0u64;
    while fgb.next()?.is_some() {
        features += 1;
    }
    assert_eq!(features, 12);
    Ok(())
}

#[test]
fn normative_vector_appendix_a() -> TestResult {
    let bytes = vector_bytes()?;
    assert_eq!(bytes.len(), 1777);

    // Footer (SPEC §7): magic, directory offset/size, directory CRC-32.
    let footer = Footer::decode(&bytes[bytes.len() - 32..])?;
    assert_eq!(footer.dir_offset, 1720);
    assert_eq!(footer.dir_size, 25);
    assert_eq!(footer.dir_crc32, 0x41fc_73cb);

    let mut reader = Reader::open(Cursor::new(&bytes))?;
    assert_eq!(reader.body_len(), 1672);
    assert_eq!(
        reader.bands(),
        vec![BandMeta {
            res: 2,
            offset: 1676,
            size: 44
        }]
    );

    // The band section, exactly as annotated in Appendix A.
    let band = reader.read_band(2)?;
    assert_eq!(band.total, 12);
    assert_eq!(band.cells, vec![(NYC_CELL, 4), (EUROPE_CELL, 8)]);

    // Count analytics over the world without touching feature bytes.
    let stats = reader.stats([-180.0, -85.0, 180.0, 85.0], 2)?;
    assert_eq!(stats.total, 12);
    Ok(())
}

#[test]
fn hostile_truncation_never_panics() -> TestResult {
    let (file, _) = build(&fgb_body()?)?;
    let body_len = file.len() - 105; // trailer is 105 bytes for this dataset
    let band_mid = body_len + 4 + 20 + 6; // mid first record
    let dir_start = body_len + 4 + 44;
    let cuts = [
        0,
        3,
        7,
        body_len,       // body only, no trailer at all
        body_len + 3,   // mid sentinel
        body_len + 7,   // band header truncated
        band_mid,       // mid band records
        dir_start,      // directory missing
        dir_start + 12, // mid directory entry
        file.len() - 1, // footer truncated
    ];
    for cut in cuts {
        let result = Reader::open(Cursor::new(&file[..cut]));
        assert!(result.is_err(), "cut at {cut} must fail");
    }
    Ok(())
}

#[test]
fn hostile_corruption_is_rejected() -> TestResult {
    let (file, _) = build(&fgb_body()?)?;
    let len = file.len();

    // Wrong footer magic → plain FlatGeobuf, not a format error.
    let mut bad = file.clone();
    bad[len - 32] = b'X';
    assert!(matches!(
        Reader::open(Cursor::new(bad)),
        Err(Error::NotFgba)
    ));

    // A plain FlatGeobuf body has no footer at all.
    assert!(matches!(
        Reader::open(Cursor::new(fgb_body()?)),
        Err(Error::NotFgba)
    ));

    // Corrupt the directory CRC field → Format error.
    let mut bad = file.clone();
    bad[len - 8] ^= 0xFF;
    assert!(matches!(
        Reader::open(Cursor::new(bad)),
        Err(Error::Format(_))
    ));

    // Corrupt a directory byte (CRC no longer matches) → Format error.
    let mut bad = file.clone();
    bad[len - 32 - 25] ^= 0xFF;
    assert!(matches!(
        Reader::open(Cursor::new(bad)),
        Err(Error::Format(_))
    ));

    // Inflate num_cells in the band section (CRC covers only the directory,
    // so the file opens; the band itself must fail validation on read).
    let band_start = file.len() - 105 + 4;
    let mut bad = file.clone();
    bad[band_start + 12] = 7; // num_cells := 7, section holds 2
    let mut reader = Reader::open(Cursor::new(bad))?;
    assert!(matches!(reader.read_band(2), Err(Error::Format(_))));

    // Unsorted cell IDs: swap the two 12-byte records in the band section.
    let mut bad = file.clone();
    let r0 = band_start + 20;
    let (first, second) = bad.split_at_mut(r0 + 12);
    let (rec0, rec1) = (&mut first[r0..r0 + 12], &mut second[0..12]);
    rec0.swap_with_slice(rec1);
    let mut reader = Reader::open(Cursor::new(bad))?;
    assert!(matches!(reader.read_band(2), Err(Error::Format(_))));

    // Reading a resolution that has no band is a clean error.
    let mut reader = Reader::open(Cursor::new(&file))?;
    assert!(matches!(reader.read_band(9), Err(Error::Format(_))));
    Ok(())
}

#[test]
fn unknown_directory_kind_is_preserved_and_skipped() -> TestResult {
    // Hand-encode a trailer with a vendor section (kind 0x8001) plus one band.
    let body = fgb_body()?;
    let band = AggBand {
        res: 2,
        total: 12,
        cells: vec![(NYC_CELL, 4), (EUROPE_CELL, 8)],
    }
    .to_bytes()?;

    let mut file = body.clone();
    file.extend_from_slice(&SENTINEL);
    let band_offset = file.len() as u64;
    file.extend_from_slice(&band);
    let vendor_offset = file.len() as u64;
    file.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]); // vendor payload
    let dir_offset = file.len() as u64;
    let directory = Directory {
        sections: vec![
            SectionEntry {
                kind: KIND_AGGREGATE_BAND,
                flags: 0,
                offset: band_offset,
                size: band.len() as u64,
                meta: vec![2],
            },
            SectionEntry {
                kind: 0x8001,
                flags: 0,
                offset: vendor_offset,
                size: 4,
                meta: vec![0x01, 0x02, 0x03],
            },
        ],
    };
    let dir_bytes = directory.encode()?;
    let footer = Footer {
        dir_offset,
        dir_size: dir_bytes.len() as u64,
        dir_crc32: crc32(&dir_bytes),
    };
    file.extend_from_slice(&dir_bytes);
    file.extend_from_slice(&footer.encode());

    let mut reader = Reader::open(Cursor::new(&file))?;
    // Unknown kind preserved verbatim in the directory...
    assert_eq!(reader.directory().sections.len(), 2);
    let vendor = &reader.directory().sections[1];
    assert_eq!(vendor.kind, 0x8001);
    assert_eq!(vendor.meta, vec![0x01, 0x02, 0x03]);
    // ...and skipped by the band accessors (SPEC §5).
    assert_eq!(
        reader.bands(),
        vec![BandMeta {
            res: 2,
            offset: band_offset,
            size: 44
        }]
    );
    assert_eq!(reader.read_band(2)?.total, 12);
    Ok(())
}

#[test]
fn invalid_bodies_are_rejected() -> TestResult {
    assert!(build(&[]).is_err());
    assert!(build(b"not a flatgeobuf").is_err());

    // No spatial index: SPEC §4 requires index_node_size > 0.
    let mut writer = FgbWriter::create_with_options(
        "cities",
        GeometryType::Point,
        FgbWriterOptions {
            write_index: false,
            ..Default::default()
        },
    )?;
    writer.add_feature_geom(Point(2.35, 48.85), |_feat| {})?;
    let mut body = Vec::new();
    writer.write(&mut body)?;
    assert!(matches!(build(&body), Err(Error::Format(_))));

    // No features: SPEC §4 requires features_count > 0.
    let writer = FgbWriter::create("cities", GeometryType::Point)?;
    let mut body = Vec::new();
    writer.write(&mut body)?;
    assert!(matches!(build(&body), Err(Error::Format(_))));
    Ok(())
}

#[test]
fn ladder_spot_checks() {
    assert_eq!(ladder_resolution(120.0), 2);
    assert_eq!(ladder_resolution(32.0), 3);
    assert_eq!(ladder_resolution(8.0), 5);
    assert_eq!(ladder_resolution(1.0), 7);
    assert_eq!(ladder_resolution(0.125), 9);
}
