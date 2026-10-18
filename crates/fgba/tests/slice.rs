//! `SliceReader` (SPEC §9.3): the same `Reader` decode path as files and
//! HTTP, over in-memory bytes fetched in a first, asynchronous phase.

use fgba::{BandMeta, Reader, SliceReader};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// The normative vector file (SPEC Appendix A).
fn vector_bytes() -> std::io::Result<Vec<u8>> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/cities_12.a.fgb");
    std::fs::read(path)
}

#[test]
fn slice_reader_decodes_the_vector_like_a_file() -> TestResult {
    let bytes = vector_bytes()?;
    let src = SliceReader::new(bytes.clone());
    assert_eq!(src.len(), bytes.len() as u64);
    assert!(!src.is_empty());

    let mut reader = Reader::open(src)?;
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

    assert_eq!(SliceReader::new(Vec::new()).len(), 0);
    assert!(SliceReader::new(Vec::new()).is_empty());
    Ok(())
}
