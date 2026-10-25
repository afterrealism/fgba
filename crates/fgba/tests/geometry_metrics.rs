//! GEOMETRY_METRICS layout, values, compatibility, and validation tests.

#![cfg(feature = "analytics")]

use arrow_array::{Array, Float64Array, RecordBatch, UInt32Array};
use arrow_ipc::reader::FileReader;
use fgba::analytics::MAX_ARROW_CHUNK_BYTES;
use fgba::format::{
    crc32, Directory, Footer, SectionEntry, KIND_AGGREGATE_BAND, KIND_BODY_ANALYTICS,
    KIND_GEOMETRY_METRICS, SENTINEL,
};
use fgba::{
    build_to_writer, build_trailer_streaming, build_with_options, BuildOptions, Error,
    GeometryMetrics, Reader,
};
use flatgeobuf::geozero::PropertyProcessor;
use flatgeobuf::{
    ColumnType, FallibleStreamingIterator, FeatureProperties, FgbReader, FgbWriter, GeometryType,
};
use geo::{Geometry, LineString, MultiPolygon, Polygon};
use std::io::{Cursor, Read, Seek, SeekFrom};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy)]
struct Expected {
    length: f64,
    area: f64,
    npoints: u32,
    bbox: [f64; 4],
}

fn ring(points: &[(f64, f64)]) -> LineString<f64> {
    LineString::from(points.to_vec())
}

fn polygon(exterior: &[(f64, f64)], holes: &[&[(f64, f64)]]) -> Polygon<f64> {
    Polygon::new(
        ring(exterior),
        holes.iter().map(|hole| ring(hole)).collect(),
    )
}

fn geometries() -> Vec<Geometry<f64>> {
    let shell = [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)];
    let hole = [(1.0, 1.0), (1.0, 2.0), (2.0, 2.0), (2.0, 1.0), (1.0, 1.0)];
    let unit = [
        (10.0, 0.0),
        (11.0, 0.0),
        (11.0, 1.0),
        (10.0, 1.0),
        (10.0, 0.0),
    ];
    vec![
        Geometry::MultiPolygon(MultiPolygon(vec![polygon(
            &[(0.0, 0.0), (3.0, 0.0), (0.0, 0.0)],
            &[],
        )])),
        Geometry::MultiPolygon(MultiPolygon(vec![polygon(
            &[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)],
            &[],
        )])),
        Geometry::MultiPolygon(MultiPolygon(vec![
            polygon(&shell, &[&hole]),
            polygon(&unit, &[]),
        ])),
        Geometry::MultiPolygon(MultiPolygon(vec![polygon(&unit, &[])])),
        Geometry::MultiPolygon(MultiPolygon(Vec::new())),
        Geometry::MultiPolygon(MultiPolygon(vec![polygon(&[(7.0, 8.0), (7.0, 8.0)], &[])])),
    ]
}

fn expected() -> [Option<Expected>; 6] {
    [
        Some(Expected {
            length: 6.0,
            area: 0.0,
            npoints: 3,
            bbox: [0.0, 0.0, 3.0, 0.0],
        }),
        Some(Expected {
            length: 4.0,
            area: 1.0,
            npoints: 5,
            bbox: [0.0, 0.0, 1.0, 1.0],
        }),
        Some(Expected {
            length: 24.0,
            area: 16.0,
            npoints: 15,
            bbox: [0.0, 0.0, 11.0, 4.0],
        }),
        Some(Expected {
            length: 4.0,
            area: 1.0,
            npoints: 5,
            bbox: [10.0, 0.0, 11.0, 1.0],
        }),
        None,
        Some(Expected {
            length: 0.0,
            area: 0.0,
            npoints: 2,
            bbox: [7.0, 8.0, 7.0, 8.0],
        }),
    ]
}

fn expected_in_body_order(body: &[u8]) -> TestResult<Vec<Option<Expected>>> {
    let expected = expected();
    let mut features = FgbReader::open(Cursor::new(body))?.select_all()?;
    let mut ordered = Vec::new();
    while let Some(feature) = features.next()? {
        let id = usize::try_from(feature.property::<i64>("id")?)?;
        ordered.push(*expected.get(id).ok_or("expected feature ID is absent")?);
    }
    Ok(ordered)
}

fn body(with_properties: bool) -> TestResult<Vec<u8>> {
    let mut writer = FgbWriter::create("geometry-metrics", GeometryType::MultiPolygon)?;
    if with_properties {
        writer.add_column("id", ColumnType::Long, |_, _| {});
    }
    for (row, geometry) in geometries().into_iter().enumerate() {
        writer.add_feature_geom(geometry, |feature| {
            if with_properties {
                let _ =
                    feature.property(0, "id", &flatgeobuf::geozero::ColumnValue::Long(row as i64));
            }
        })?;
    }
    let mut bytes = Vec::new();
    writer.write(&mut bytes)?;
    Ok(bytes)
}

fn options(chunk_rows: u32, geometry_metrics: bool) -> BuildOptions {
    BuildOptions {
        analytics: true,
        geometry_metrics,
        chunk_rows,
    }
}

fn assert_file_build_matches(body: &[u8], options: &BuildOptions) -> TestResult {
    let (expected, expected_report) = build_with_options(body, options)?;
    let mut actual = Vec::new();
    let report = build_to_writer(Cursor::new(body), body.len() as u64, options, &mut actual)?;
    assert_eq!(report, expected_report);
    assert_eq!(actual, expected);
    Ok(())
}

fn decode_chunk(bytes: Vec<u8>) -> TestResult<RecordBatch> {
    let reader = FileReader::try_new(Cursor::new(bytes), None)?;
    assert_eq!(reader.schema().fields().len(), 1);
    assert_eq!(reader.num_batches(), 1);
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(batches.len(), 1);
    Ok(batches.into_iter().next().ok_or("missing batch")?)
}

fn open_geometry(file: &[u8]) -> TestResult<GeometryMetrics> {
    Ok(Reader::open(Cursor::new(file))?
        .read_geometry_metrics_metadata()?
        .ok_or("missing GEOMETRY_METRICS")?)
}

#[test]
fn multi_chunk_metrics_round_trip_and_exact_stats() -> TestResult {
    let body = body(true)?;
    let expected = expected_in_body_order(&body)?;
    let (file, _) = build_with_options(&body, &options(2, true))?;
    let body_analytics = Reader::open(Cursor::new(&file))?
        .read_body_analytics_metadata()?
        .ok_or("missing BODY_ANALYTICS")?;
    let metrics = open_geometry(&file)?;
    assert_eq!(metrics.row_count, 6);
    assert_eq!(metrics.chunk_rows, 2);
    assert_eq!(metrics.chunk_count, 3);
    assert_eq!(metrics.columns.len(), 7);
    assert_eq!(metrics.chunk_rows, body_analytics.chunk_rows);
    assert_eq!(metrics.chunk_count, body_analytics.chunk_count);
    assert_eq!(metrics.row_count, body_analytics.row_count);
    let entry = geometry_entry(&file)?;
    assert_eq!(file[entry.offset as usize], 1);
    let metadata_size = 56 + 24 * metrics.chunk_count + 8 * (metrics.chunk_count + 1);
    let stream_bytes = metrics
        .columns
        .iter()
        .fold(0u64, |sum, column| sum + column.descriptor.stream_size);
    assert_eq!(entry.size, 32 + 24 * 7 + stream_bytes);
    let mut expected_stream = 32 + 24 * 7;
    for (index, column) in metrics.columns.iter().enumerate() {
        let descriptor = column.descriptor;
        assert_eq!(descriptor.metric_id as usize, index);
        assert_eq!(descriptor.arrow_type, if index == 2 { 1 } else { 0 });
        assert_eq!(descriptor.stream_offset, expected_stream);
        expected_stream += descriptor.stream_size;
        let arrow_bytes = column
            .arrow_offsets
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .sum::<u64>();
        assert!(column
            .arrow_offsets
            .windows(2)
            .all(|pair| pair[1] > pair[0] && pair[1] - pair[0] <= MAX_ARROW_CHUNK_BYTES));
        assert_eq!(descriptor.stream_size, metadata_size + arrow_bytes);
        assert_eq!(
            column.arrow_offsets.first(),
            Some(&(entry.offset + descriptor.stream_offset + metadata_size))
        );
        assert_eq!(
            column.arrow_offsets.last(),
            Some(&(entry.offset + descriptor.stream_offset + descriptor.stream_size))
        );
    }
    assert_eq!(expected_stream, entry.size);

    let expected_length = expected
        .iter()
        .flatten()
        .fold(0.0, |sum, row| sum + row.length);
    let expected_area = expected
        .iter()
        .flatten()
        .fold(0.0, |sum, row| sum + row.area);
    let expected_npoints = expected
        .iter()
        .flatten()
        .fold(0u128, |sum, row| sum + u128::from(row.npoints));
    assert_eq!(expected_length, 38.0);
    assert_eq!(expected_area, 18.0);
    assert_eq!(expected_npoints, 30);
    assert_eq!(
        f64::from_le_bytes(metrics.columns[0].global.sum[..8].try_into()?),
        expected_length
    );
    assert_eq!(
        f64::from_le_bytes(metrics.columns[1].global.sum[..8].try_into()?),
        expected_area
    );
    assert_eq!(
        u128::from_le_bytes(metrics.columns[2].global.sum),
        expected_npoints
    );

    let mut reader = Reader::open(Cursor::new(&file))?;
    let mut float_values: [Vec<Option<f64>>; 7] = std::array::from_fn(|_| Vec::new());
    let mut npoints_values = Vec::new();
    for chunk in 0..metrics.chunk_count {
        for metric in 0..7u16 {
            let arrow_chunk = reader.read_geometry_arrow_chunk(metric, chunk)?;
            assert_eq!(arrow_chunk.column_index, metric);
            assert_eq!(arrow_chunk.chunk_index, chunk);
            let batch = decode_chunk(arrow_chunk.bytes)?;
            assert_eq!(batch.num_rows(), 2);
            let schema = batch.schema();
            let field = &schema.fields()[0];
            assert_eq!(
                field.name(),
                fgba::geom_analytics::metric_name(metric).ok_or("missing metric name")?
            );
            assert_eq!(
                field.data_type(),
                &fgba::geom_analytics::metric_data_type(metric)?
            );
            assert!(field.is_nullable());
            if metric == 2 {
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or("npoints is not UInt32")?;
                npoints_values.extend(
                    (0..array.len()).map(|row| array.is_valid(row).then(|| array.value(row))),
                );
            } else {
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or("metric is not Float64")?;
                float_values[metric as usize].extend(
                    (0..array.len()).map(|row| array.is_valid(row).then(|| array.value(row))),
                );
            }
        }
    }
    let arrow_length_sum = float_values[0]
        .iter()
        .flatten()
        .fold(0.0, |sum, value| sum + value);
    assert_eq!(
        arrow_length_sum, expected_length,
        "single body-order SUM(length)"
    );
    for (row, expected) in expected.iter().enumerate() {
        assert_eq!(npoints_values[row], expected.map(|row| row.npoints));
        for metric in 0..7u16 {
            if metric == 2 {
                continue;
            }
            let expected_value = expected.map(|row| match metric {
                0 => row.length,
                1 => row.area,
                3 => row.bbox[0],
                4 => row.bbox[1],
                5 => row.bbox[2],
                6 => row.bbox[3],
                _ => 0.0,
            });
            assert_eq!(float_values[metric as usize][row], expected_value);
        }
    }

    for column in &metrics.columns {
        assert_eq!(
            column
                .chunks
                .iter()
                .map(|chunk| u64::from(chunk.non_null_count))
                .sum::<u64>(),
            column.global.non_null_count
        );
        assert_eq!(column.global.non_null_count, 5);
        let chunks: Vec<_> = column
            .chunks
            .iter()
            .filter(|chunk| chunk.has_bounds())
            .collect();
        if column.descriptor.metric_id == 2 {
            let min = chunks
                .iter()
                .map(|chunk| chunk.min_bits)
                .min()
                .ok_or("no min")?;
            let max = chunks
                .iter()
                .map(|chunk| chunk.max_bits)
                .max()
                .ok_or("no max")?;
            assert_eq!((column.global.min_bits, column.global.max_bits), (min, max));
        } else {
            let min = chunks
                .iter()
                .map(|chunk| f64::from_bits(chunk.min_bits))
                .fold(f64::INFINITY, f64::min);
            let max = chunks
                .iter()
                .map(|chunk| f64::from_bits(chunk.max_bits))
                .fold(f64::NEG_INFINITY, f64::max);
            assert_eq!(f64::from_bits(column.global.min_bits), min);
            assert_eq!(f64::from_bits(column.global.max_bits), max);
        }
    }
    Ok(())
}

#[test]
fn streaming_and_in_memory_metrics_are_byte_identical() -> TestResult {
    let body = body(true)?;
    let (expected, expected_report) = build_with_options(&body, &options(2, true))?;
    let (trailer, report) =
        build_trailer_streaming(Cursor::new(&body), body.len() as u64, &options(2, true))?;
    assert_eq!(report, expected_report);
    assert_eq!(trailer.as_slice(), &expected[body.len()..]);
    assert_file_build_matches(&body, &options(2, true))?;
    Ok(())
}

#[test]
fn kind_two_only_and_property_free_outputs_have_no_kind_three() -> TestResult {
    let body_bytes = body(true)?;
    let (kind_two, _) = build_with_options(&body_bytes, &options(2, false))?;
    let mut reader = Reader::open(Cursor::new(&kind_two))?;
    assert!(!reader
        .directory()
        .sections
        .iter()
        .any(|entry| entry.kind == KIND_GEOMETRY_METRICS));
    assert!(reader.read_geometry_metrics_metadata()?.is_none());
    assert!(reader.read_body_analytics_metadata()?.is_some());
    let (with_metrics, _) = build_with_options(&body_bytes, &options(2, true))?;
    let mut metric_reader = Reader::open(Cursor::new(&with_metrics))?;
    assert_eq!(reader.read_band(2)?, metric_reader.read_band(2)?);
    assert_eq!(
        reader.stats([-180.0, -85.0, 180.0, 85.0], 2)?,
        metric_reader.stats([-180.0, -85.0, 180.0, 85.0], 2)?
    );

    let property_free_body = body(false)?;
    let (property_free, _) = build_with_options(&property_free_body, &options(2, true))?;
    let mut reader = Reader::open(Cursor::new(&property_free))?;
    assert!(reader.read_body_analytics_metadata()?.is_none());
    assert!(reader.read_geometry_metrics_metadata()?.is_none());
    assert!(!reader
        .directory()
        .sections
        .iter()
        .any(|entry| entry.kind == KIND_GEOMETRY_METRICS));
    let mut features = FgbReader::open(reader.body()?)?.select_all()?;
    let mut rows = 0;
    while features.next()?.is_some() {
        rows += 1;
    }
    assert_eq!(rows, 6);
    assert_file_build_matches(&body_bytes, &options(2, false))?;
    assert_file_build_matches(&property_free_body, &options(2, true))?;
    Ok(())
}

fn metric_file() -> TestResult<Vec<u8>> {
    Ok(build_with_options(&body(true)?, &options(2, true))?.0)
}

fn geometry_entry(file: &[u8]) -> TestResult<SectionEntry> {
    let reader = Reader::open(Cursor::new(file))?;
    let entries: Vec<_> = reader
        .directory()
        .sections
        .iter()
        .filter(|section| section.kind == KIND_GEOMETRY_METRICS)
        .cloned()
        .collect();
    assert_eq!(entries.len(), 1);
    Ok(entries.into_iter().next().ok_or("missing geometry entry")?)
}

fn read_directory(file: &[u8]) -> TestResult<(Footer, Directory)> {
    let footer = Footer::decode(&file[file.len() - 32..])?;
    let start = footer.dir_offset as usize;
    let directory = Directory::decode(&file[start..start + footer.dir_size as usize])?;
    Ok((footer, directory))
}

fn replace_directory(file: &[u8], directory: &Directory) -> TestResult<Vec<u8>> {
    let (old_footer, _) = read_directory(file)?;
    let start = old_footer.dir_offset as usize;
    let encoded = directory.encode()?;
    let footer = Footer {
        dir_offset: old_footer.dir_offset,
        dir_size: encoded.len() as u64,
        dir_crc32: crc32(&encoded),
    };
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&file[..start]);
    bytes.extend_from_slice(&encoded);
    bytes.extend_from_slice(&footer.encode());
    Ok(bytes)
}

fn assert_body_and_band_readable<R: Read + Seek>(
    reader: &mut Reader<R>,
    expect_analytics: bool,
) -> TestResult {
    assert_eq!(
        reader.read_body_analytics_metadata()?.is_some(),
        expect_analytics
    );
    reader.read_band(2)?;
    let mut features = FgbReader::open(reader.body()?)?.select_all()?;
    let mut rows = 0;
    while features.next()?.is_some() {
        rows += 1;
    }
    assert_eq!(rows, 6);
    Ok(())
}

fn put_u32(bytes: &mut [u8], at: usize, value: u32) {
    bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], at: usize, value: u64) {
    bytes[at..at + 8].copy_from_slice(&value.to_le_bytes());
}

#[test]
fn directory_order_is_independent_and_kind_three_requires_kind_two() -> TestResult {
    let file = metric_file()?;
    let (_, mut directory) = read_directory(&file)?;
    directory.sections.reverse();
    let reordered = replace_directory(&file, &directory)?;
    let mut reader = Reader::open(Cursor::new(&reordered))?;
    let metrics = reader
        .read_geometry_metrics_metadata()?
        .ok_or("missing reordered geometry metrics")?;
    assert_eq!(metrics.row_count, 6);
    assert_eq!(metrics.chunk_count, 3);
    assert_body_and_band_readable(&mut reader, true)?;

    let (_, mut without_kind_two) = read_directory(&file)?;
    without_kind_two
        .sections
        .retain(|section| section.kind != KIND_BODY_ANALYTICS);
    let invalid = replace_directory(&file, &without_kind_two)?;
    let mut reader = Reader::open(Cursor::new(&invalid))?;
    assert!(matches!(
        reader.read_geometry_metrics_metadata(),
        Err(Error::Format(_))
    ));
    assert_body_and_band_readable(&mut reader, false)?;

    let (_, mut invalid_flags) = read_directory(&file)?;
    for section in &mut invalid_flags.sections {
        if section.kind == KIND_GEOMETRY_METRICS {
            section.flags = 1;
        }
    }
    let invalid = replace_directory(&file, &invalid_flags)?;
    let mut reader = Reader::open(Cursor::new(&invalid))?;
    assert!(matches!(
        reader.read_geometry_metrics_metadata(),
        Err(Error::Format(_))
    ));
    assert_body_and_band_readable(&mut reader, true)
}

#[test]
fn malformed_counts_descriptors_stats_and_overlaps_fail_cleanly() -> TestResult {
    let file = metric_file()?;
    let start = geometry_entry(&file)?.offset as usize;
    let first_stream = start + 32 + 7 * 24;
    let byte_cases = [
        (start, 2),           // version
        (start + 1, 1),       // header flags
        (start + 2, 1),       // header reserved
        (start + 32, 1),      // metric ID order
        (start + 32 + 2, 99), // Arrow type
        (start + 32 + 3, 1),  // descriptor reserved
    ];
    for (at, value) in byte_cases {
        let mut bad = file.clone();
        bad[at] = value;
        let mut reader = Reader::open(Cursor::new(bad))?;
        assert!(
            matches!(
                reader.read_geometry_metrics_metadata(),
                Err(Error::Format(_))
            ),
            "byte corruption at {at} was accepted"
        );
        assert_body_and_band_readable(&mut reader, true)?;
    }
    let u32_cases = [
        (start + 4, 3),                     // chunk_rows != kind 2
        (start + 8, 6),                     // not seven descriptors
        (start + 12, 1),                    // header reserved
        (start + 32 + 4, 0),                // missing Arrow flag
        (first_stream + 52, 1),             // global reserved
        (first_stream + 56 + 16, u32::MAX), // wrong chunk non-null count
    ];
    for (at, value) in u32_cases {
        let mut bad = file.clone();
        put_u32(&mut bad, at, value);
        let mut reader = Reader::open(Cursor::new(bad))?;
        assert!(
            matches!(
                reader.read_geometry_metrics_metadata(),
                Err(Error::Format(_))
            ),
            "u32 corruption at {at} was accepted"
        );
        assert_body_and_band_readable(&mut reader, true)?;
    }
    let u64_cases = [
        (start + 16, 5),                         // wrong row_count
        (start + 24, 4),                         // wrong chunk_count
        (start + 32 + 8, 0),                     // wrong stream offset
        (start + 32 + 16, 1),                    // wrong stream size
        (start + 32 + 24 + 8, 200),              // overlapping second stream
        (first_stream, u64::MAX),                // wrong global non-null count
        (first_stream + 8, u64::MAX),            // wrong global NaN count
        (first_stream + 56 + 3 * 24 + 3 * 8, 0), // terminal Arrow offset
    ];
    for (at, value) in u64_cases {
        let mut bad = file.clone();
        put_u64(&mut bad, at, value);
        let mut reader = Reader::open(Cursor::new(bad))?;
        assert!(
            matches!(
                reader.read_geometry_metrics_metadata(),
                Err(Error::Format(_))
            ),
            "u64 corruption at {at} was accepted"
        );
        assert_body_and_band_readable(&mut reader, true)?;
    }
    Ok(())
}

fn assert_structural_fallback(
    valid_file: &[u8],
    malformed_file: &[u8],
    body_len: u64,
    kind_two_survives: bool,
) -> TestResult {
    let world = [-180.0, -85.0, 180.0, 85.0];
    let mut expected = Reader::open(Cursor::new(valid_file))?;
    let expected_band = expected.read_band(2)?;
    let expected_query = expected.query(world, 2)?;
    let expected_stats = expected.stats(world, 2)?;

    let mut reader = Reader::open(Cursor::new(malformed_file))?;
    assert_eq!(reader.body_len(), body_len);
    if kind_two_survives {
        assert!(reader.read_body_analytics_metadata()?.is_some());
    } else {
        assert!(matches!(
            reader.read_body_analytics_metadata(),
            Err(Error::Format(_))
        ));
    }
    assert!(matches!(
        reader.read_geometry_metrics_metadata(),
        Err(Error::Format(_))
    ));
    assert_eq!(reader.read_band(2)?, expected_band);
    assert_eq!(reader.query(world, 2)?, expected_query);
    assert_eq!(reader.stats(world, 2)?, expected_stats);
    let mut features = FgbReader::open(reader.body()?)?.select_all()?;
    let mut rows = 0;
    while features.next()?.is_some() {
        rows += 1;
    }
    assert_eq!(rows, 6);
    Ok(())
}

#[test]
fn kind_three_size_overflow_and_out_of_file_disable_only_kind_three() -> TestResult {
    let body = body(true)?;
    let body_len = body.len() as u64;
    let file = build_with_options(&body, &options(2, true))?.0;
    let (footer, _) = read_directory(&file)?;
    for invalid_size in [u64::MAX, footer.dir_offset] {
        let (_, mut directory) = read_directory(&file)?;
        let entry = directory
            .sections
            .iter_mut()
            .find(|section| section.kind == KIND_GEOMETRY_METRICS)
            .ok_or("missing geometry entry")?;
        entry.size = invalid_size;
        let malformed = replace_directory(&file, &directory)?;
        assert_structural_fallback(&file, &malformed, body_len, true)?;
    }
    Ok(())
}

#[test]
fn kind_three_overlapping_a_band_preserves_the_band_and_kind_two() -> TestResult {
    let body = body(true)?;
    let body_len = body.len() as u64;
    let file = build_with_options(&body, &options(2, true))?.0;
    let (_, mut directory) = read_directory(&file)?;
    let band = directory
        .sections
        .iter()
        .find(|section| section.kind == KIND_AGGREGATE_BAND)
        .cloned()
        .ok_or("missing band")?;
    let entry = directory
        .sections
        .iter_mut()
        .find(|section| section.kind == KIND_GEOMETRY_METRICS)
        .ok_or("missing geometry entry")?;
    entry.offset = band.offset;
    entry.size = band.size;
    directory.sections.reverse();
    let malformed = replace_directory(&file, &directory)?;
    assert_structural_fallback(&file, &malformed, body_len, true)
}

#[test]
fn kind_three_overlapping_kind_two_preserves_kind_two() -> TestResult {
    let body = body(true)?;
    let body_len = body.len() as u64;
    let file = build_with_options(&body, &options(2, true))?.0;
    let (_, mut directory) = read_directory(&file)?;
    let analytics = directory
        .sections
        .iter()
        .find(|section| section.kind == KIND_BODY_ANALYTICS)
        .cloned()
        .ok_or("missing analytics")?;
    let entry = directory
        .sections
        .iter_mut()
        .find(|section| section.kind == KIND_GEOMETRY_METRICS)
        .ok_or("missing geometry entry")?;
    entry.offset = analytics.offset;
    entry.size = analytics.size;
    directory.sections.reverse();
    let malformed = replace_directory(&file, &directory)?;
    assert_structural_fallback(&file, &malformed, body_len, true)
}

#[test]
fn structurally_malformed_kind_two_preserves_body_and_bands() -> TestResult {
    let body = body(true)?;
    let body_len = body.len() as u64;
    let file = build_with_options(&body, &options(2, true))?.0;
    let (_, mut directory) = read_directory(&file)?;
    let entry = directory
        .sections
        .iter_mut()
        .find(|section| section.kind == KIND_BODY_ANALYTICS)
        .ok_or("missing analytics entry")?;
    entry.size = u64::MAX;
    let malformed = replace_directory(&file, &directory)?;
    assert_structural_fallback(&file, &malformed, body_len, false)
}

#[test]
fn in_body_optional_offset_recovers_the_exact_body_terminal() -> TestResult {
    let body = body(true)?;
    let body_len = body.len() as u64;
    let file = build_with_options(&body, &options(2, true))?.0;
    let invalid_offset = body_len / 2;
    assert_ne!(
        &file[invalid_offset as usize..invalid_offset as usize + SENTINEL.len()],
        &SENTINEL
    );
    let (_, mut directory) = read_directory(&file)?;
    let entry = directory
        .sections
        .iter_mut()
        .find(|section| section.kind == KIND_GEOMETRY_METRICS)
        .ok_or("missing geometry entry")?;
    entry.offset = invalid_offset;
    entry.size = 1;
    directory.sections.reverse();
    let malformed = replace_directory(&file, &directory)?;
    assert_structural_fallback(&file, &malformed, body_len, true)
}

struct SectionCut<R> {
    inner: R,
    section_start: u64,
    section_end: u64,
    cut: u64,
}

impl<R: Read + Seek> Read for SectionCut<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let position = self.inner.stream_position()?;
        if position >= self.section_start && position < self.section_end && position >= self.cut {
            return Ok(0);
        }
        let allowed = if position >= self.section_start && position < self.cut {
            usize::try_from(self.cut - position)
                .unwrap_or(buf.len())
                .min(buf.len())
        } else {
            buf.len()
        };
        self.inner.read(&mut buf[..allowed])
    }
}

impl<R: Seek> Seek for SectionCut<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

#[test]
fn every_geometry_metadata_truncation_preserves_body_bands_and_kind_two() -> TestResult {
    let file = metric_file()?;
    let entry = geometry_entry(&file)?;
    let metadata_len = open_geometry(&file)?.metadata_bytes_read;
    for cut in 0..metadata_len {
        let source = SectionCut {
            inner: Cursor::new(&file),
            section_start: entry.offset,
            section_end: entry.offset + entry.size,
            cut: entry.offset + cut,
        };
        let mut reader = Reader::open(source)?;
        assert!(
            matches!(
                reader.read_geometry_metrics_metadata(),
                Err(Error::Format(_))
            ),
            "metadata cut {cut} was accepted"
        );
        assert_body_and_band_readable(&mut reader, true)?;
    }
    Ok(())
}

#[test]
fn truncated_arrow_payload_and_invalid_ranges_fail_cleanly() -> TestResult {
    let file = metric_file()?;
    let entry = geometry_entry(&file)?;
    let metrics = open_geometry(&file)?;
    let payload_start = metrics.columns[6].arrow_offsets[0];
    let source = SectionCut {
        inner: Cursor::new(&file),
        section_start: entry.offset,
        section_end: entry.offset + entry.size,
        cut: payload_start + 1,
    };
    let mut reader = Reader::open(source)?;
    assert!(reader.read_geometry_metrics_metadata()?.is_some());
    assert!(matches!(
        reader.read_geometry_arrow_chunk(6, 0),
        Err(Error::Format(_))
    ));
    assert_body_and_band_readable(&mut reader, true)?;

    let mut metrics = open_geometry(&file)?;
    let first = metrics.columns[0].arrow_offsets[0];
    metrics.columns[0].arrow_offsets[1] = first + MAX_ARROW_CHUNK_BYTES + 1;
    assert!(metrics.arrow_range(0, 0).is_err());
    metrics.columns[0].arrow_offsets[1] = first;
    assert!(metrics.arrow_range(0, 0).is_err());

    let mut reader = Reader::open(Cursor::new(&file))?;
    assert!(reader.read_geometry_arrow_chunk(99, 0).is_err());
    assert!(reader.read_geometry_arrow_chunk(0, 99).is_err());
    Ok(())
}

#[test]
fn geometry_metrics_require_analytics_and_default_to_false() -> TestResult {
    assert!(!BuildOptions::default().geometry_metrics);
    let body = body(true)?;
    let invalid = BuildOptions {
        analytics: false,
        geometry_metrics: true,
        chunk_rows: 2,
    };
    assert!(matches!(
        build_with_options(&body, &invalid),
        Err(Error::Format(_))
    ));
    assert!(matches!(
        build_trailer_streaming(Cursor::new(&body), body.len() as u64, &invalid),
        Err(Error::Format(_))
    ));
    Ok(())
}
