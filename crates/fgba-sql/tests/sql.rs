//! End-to-end SQL tests using real FlatGeobuf bodies and DataFusion execution.

use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::SQLOptions;
use fgba::{build_with_options, BuildOptions, Reader};
use fgba_sql::schema::{CRS_CODE_METADATA, CRS_WKT_METADATA};
use fgba_sql::{query_csv, session, QueryOptions, ScanMetrics};
use flatgeobuf::geozero::{ColumnValue, GeomProcessor, GeozeroGeometry, PropertyProcessor};
use flatgeobuf::{ColumnType, FgbCrs, FgbWriter, FgbWriterOptions, GeometryType};
use futures::StreamExt;
use std::path::{Path, PathBuf};

// Tests collect only small results. Production query_csv always streams.
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy)]
struct Point(f64, f64);

impl GeozeroGeometry for Point {
    fn process_geom<P: GeomProcessor>(
        &self,
        processor: &mut P,
    ) -> flatgeobuf::geozero::error::Result<()> {
        processor.point_begin(0)?;
        processor.xy(self.0, self.1, 0)?;
        processor.point_end(0)
    }
}

fn body(
    columns: &[(&str, ColumnType, bool)],
    points: &[(f64, f64)],
    rows: Vec<Vec<Option<ColumnValue<'_>>>>,
    write_index: bool,
) -> TestResult<Vec<u8>> {
    if points.len() != rows.len() {
        return Err("point/property row counts differ".into());
    }
    let mut writer = FgbWriter::create_with_options(
        "sql-test",
        GeometryType::Point,
        FgbWriterOptions {
            write_index,
            crs: FgbCrs {
                code: 4326,
                wkt: Some("test CRS WKT"),
                ..FgbCrs::default()
            },
            ..FgbWriterOptions::default()
        },
    )?;
    for (name, kind, nullable) in columns {
        writer.add_column(name, *kind, |_, args| args.nullable = *nullable);
    }
    for (row, properties) in rows.into_iter().enumerate() {
        let mut property_error = None;
        writer.add_feature_geom(Point(points[row].0, points[row].1), |feature| {
            for (column, value) in properties.into_iter().enumerate() {
                let Some(value) = value else {
                    continue;
                };
                if let Err(error) = feature.property(column, columns[column].0, &value) {
                    property_error = Some(error);
                    return;
                }
            }
        })?;
        if let Some(error) = property_error {
            return Err(error.into());
        }
    }
    let mut bytes = Vec::new();
    writer.write(&mut bytes)?;
    Ok(bytes)
}

fn analytics_file(dir: &Path, body_bytes: &[u8]) -> TestResult<PathBuf> {
    let (file, _) = build_with_options(
        body_bytes,
        &BuildOptions {
            analytics: true,
            geometry_metrics: false,
            chunk_rows: 3,
        },
    )?;
    let path = dir.join("fixture.a.fgb");
    std::fs::write(&path, file)?;
    Ok(path)
}

const CITY_POINTS: [(f64, f64); 12] = [
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

fn cities_body(write_index: bool) -> TestResult<Vec<u8>> {
    let names = (1..=12)
        .map(|index| format!("city{index}"))
        .collect::<Vec<_>>();
    let payloads = (0..12)
        .map(|index| format!("payload-{index}-{}", "x".repeat(64)))
        .collect::<Vec<_>>();
    let rows = (0..12)
        .map(|index| {
            vec![
                Some(ColumnValue::String(names[index].as_str())),
                Some(ColumnValue::Long(index as i64)),
                Some(ColumnValue::String(payloads[index].as_str())),
            ]
        })
        .collect();
    body(
        &[
            ("name", ColumnType::String, false),
            ("height", ColumnType::Long, true),
            ("payload", ColumnType::String, true),
        ],
        &CITY_POINTS,
        rows,
        write_index,
    )
}

fn cities_fixture() -> TestResult<(tempfile::TempDir, PathBuf)> {
    let dir = tempfile::tempdir()?;
    let path = analytics_file(dir.path(), &cities_body(true)?)?;
    Ok((dir, path))
}

fn corrupt_band(path: &Path, res: u8) -> TestResult<PathBuf> {
    let band = Reader::open_path(path)?
        .bands()
        .into_iter()
        .find(|meta| meta.res == res)
        .ok_or_else(|| format!("missing band resolution {res}"))?;
    let mut bytes = std::fs::read(path)?;
    let offset = usize::try_from(band.offset)?;
    // Invalidate the grid byte without touching directory metadata or its CRC.
    *bytes
        .get_mut(offset)
        .ok_or("band offset is outside the file")? = 0;
    let corrupted = path.with_file_name(format!("corrupt-band-{res}.a.fgb"));
    std::fs::write(&corrupted, bytes)?;
    Ok(corrupted)
}

struct Execution {
    batches: Vec<RecordBatch>,
    before: ScanMetrics,
    after: ScanMetrics,
}

fn read_only() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

fn execute_with(path: &Path, sql: &str, options: QueryOptions) -> TestResult<Execution> {
    let session = session::register(path, &options)?;
    let before = session.features.metrics();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let batches = runtime.block_on(async {
        let df = session.ctx.sql_with_options(sql, read_only()).await?;
        let mut stream = session.execute_dataframe(df).await?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        Ok::<Vec<RecordBatch>, fgba_sql::Error>(batches)
    })?;
    let after = session.features.metrics();
    Ok(Execution {
        batches,
        before,
        after,
    })
}

fn execute(path: &Path, sql: &str) -> TestResult<Execution> {
    execute_with(path, sql, QueryOptions::default())
}

fn csv(path: &Path, sql: &str, options: QueryOptions) -> TestResult<String> {
    let mut output = Vec::new();
    query_csv(path, sql, options, &mut output)?;
    Ok(String::from_utf8(output)?)
}

fn one_batch(batches: &[RecordBatch]) -> TestResult<&RecordBatch> {
    if batches.len() == 1 {
        Ok(&batches[0])
    } else {
        Err(format!("expected one batch, got {}", batches.len()).into())
    }
}

fn scalar_i64(batches: &[RecordBatch]) -> TestResult<i64> {
    let batch = one_batch(batches)?;
    if batch.num_rows() != 1 {
        return Err("expected one scalar row".into());
    }
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("not an Int64 result")?;
    Ok(values.value(0))
}

fn scalar_u64(batches: &[RecordBatch]) -> TestResult<u64> {
    let batch = one_batch(batches)?;
    if batch.num_rows() != 1 {
        return Err("expected one scalar row".into());
    }
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or("not a UInt64 result")?;
    Ok(values.value(0))
}

fn scalar_string(batches: &[RecordBatch]) -> TestResult<String> {
    let batch = one_batch(batches)?;
    if batch.num_rows() != 1 {
        return Err("expected one scalar row".into());
    }
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("not a Utf8 result")?;
    Ok(values.value(0).to_owned())
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn delta(before: ScanMetrics, after: ScanMetrics) -> ScanMetrics {
    ScanMetrics {
        features_read: after.features_read - before.features_read,
        body_bytes_read: after.body_bytes_read - before.body_bytes_read,
        analytics_bytes_read: after.analytics_bytes_read - before.analytics_bytes_read,
        ranges_opened: after.ranges_opened - before.ranges_opened,
        wkb_values_decoded: after.wkb_values_decoded - before.wkb_values_decoded,
    }
}

fn array<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> TestResult<&'a T> {
    let index = batch.schema().index_of(name)?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("wrong array type for {name}").into())
}

#[test]
fn exact_body_band_viewport_and_typed_wkb_sql() -> TestResult {
    let (_dir, path) = cities_fixture()?;
    let count = execute(&path, "SELECT COUNT(*) FROM features")?;
    assert_eq!(scalar_i64(&count.batches)?, 12);
    assert_eq!(delta(count.before, count.after), ScanMetrics::default());
    assert_eq!(
        scalar_u64(&execute(&path, "SELECT SUM(count) FROM a5_band_2")?.batches)?,
        12
    );
    assert_eq!(
        scalar_string(&execute(&path, "SELECT name FROM features WHERE name='city1'")?.batches)?,
        "city1"
    );
    assert_eq!(
        csv(
            &path,
            "SELECT COUNT(*) AS cells, SUM(count) AS estimated FROM a5_viewport_approx",
            QueryOptions {
                bbox: Some([-10.0, 40.0, 10.0, 60.0]),
                res: Some(2),
                ..QueryOptions::default()
            },
        )?,
        "cells,estimated\n1,8\n"
    );

    let geometry = execute(
        &path,
        "SELECT geom_wkb, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax FROM features WHERE name='city1'",
    )?;
    let batch = one_batch(&geometry.batches)?;
    let wkb = array::<BinaryArray>(batch, "geom_wkb")?;
    assert_eq!(wkb.value(0).first(), Some(&1));
    for (name, expected) in [
        ("bbox_xmin", 2.36),
        ("bbox_ymin", 48.84),
        ("bbox_xmax", 2.36),
        ("bbox_ymax", 48.84),
    ] {
        assert_eq!(array::<Float64Array>(batch, name)?.value(0), expected);
    }
    assert_eq!(
        delta(geometry.before, geometry.after).wkb_values_decoded,
        12
    );
    assert!(delta(geometry.before, geometry.after).body_bytes_read > 0);
    Ok(())
}

#[test]
fn lazy_sql_registration_does_not_read_unreferenced_a5_bands() -> TestResult {
    let (_dir, path) = cities_fixture()?;
    let corrupted = corrupt_band(&path, 2)?;
    let options = QueryOptions {
        bbox: Some([-10.0, 40.0, 10.0, 60.0]),
        res: Some(2),
        ..QueryOptions::default()
    };
    let sql = "SELECT COUNT(*) FROM features";
    session::register_for_sql(&corrupted, sql, &options)?;
    assert_eq!(csv(&corrupted, sql, options)?, "count(*)\n12\n");
    Ok(())
}

#[test]
fn lazy_a5_relations_work_case_insensitively() -> TestResult {
    let (_dir, path) = cities_fixture()?;
    for sql in [
        "SELECT SUM(count) AS total FROM a5_band_2",
        "SELECT SUM(count) AS total FROM A5_BAND_2",
        r#"SELECT SUM(count) AS total FROM "a5_band_2""#,
    ] {
        assert_eq!(csv(&path, sql, QueryOptions::default())?, "total\n12\n");
    }

    let viewport_options = QueryOptions {
        bbox: Some([-10.0, 40.0, 10.0, 60.0]),
        res: Some(2),
        ..QueryOptions::default()
    };
    for table in [
        "a5_viewport_approx",
        "A5_VIEWPORT_APPROX",
        r#""a5_viewport_approx""#,
    ] {
        let sql = format!("SELECT COUNT(*) AS cells, SUM(count) AS estimated FROM {table}");
        assert_eq!(
            csv(&path, &sql, viewport_options.clone())?,
            "cells,estimated\n1,8\n"
        );
    }

    for table in [r#""A5_BAND_2""#, r#""A5_VIEWPORT_APPROX""#] {
        let sql = format!("SELECT * FROM {table}");
        assert!(csv(&path, &sql, viewport_options.clone()).is_err());
    }

    let sql = "SELECT * FROM a5_viewport_approx";
    session::register_for_sql(&path, sql, &QueryOptions::default())?;
    assert_eq!(
        csv(
            &path,
            "SELECT COUNT(*) FROM features",
            QueryOptions::default()
        )?,
        "count(*)\n12\n"
    );
    let error = csv(&path, sql, QueryOptions::default())
        .err()
        .ok_or("viewport without --bbox succeeded")?;
    assert!(error.to_string().contains("requires --bbox"), "{error}");

    let options = QueryOptions {
        res: Some(2),
        ..QueryOptions::default()
    };
    session::register_for_sql(&path, "SELECT COUNT(*) FROM features", &options)?;
    assert_eq!(
        csv(&path, "SELECT COUNT(*) FROM features", options.clone())?,
        "count(*)\n12\n"
    );
    let error = csv(&path, sql, options)
        .err()
        .ok_or("--res without --bbox succeeded")?;
    assert!(error.to_string().contains("--res requires --bbox"));
    Ok(())
}

#[test]
fn a5_names_outside_actual_scans_do_not_read_bands() -> TestResult {
    let (_analytics_dir, analytics) = cities_fixture()?;
    let corrupted = corrupt_band(&analytics, 2)?;
    let dir = tempfile::tempdir()?;
    let plain = dir.path().join("plain.fgb");
    std::fs::write(&plain, cities_body(false)?)?;
    let options = QueryOptions {
        bbox: Some([-10.0, 40.0, 10.0, 60.0]),
        res: Some(2),
        ..QueryOptions::default()
    };
    for sql in [
        "SELECT 'a5_viewport_approx' FROM features",
        "SELECT 1 AS a5_viewport_approx FROM features",
        "SELECT 'a5_band_2' AS label FROM features",
        "SELECT 1 AS a5_band_2 FROM features",
        "-- a5_viewport_approx\nSELECT COUNT(*) FROM features",
        "/* a5_band_2 */ SELECT COUNT(*) FROM features",
        "SELECT COUNT(*) FROM features AS a5_viewport_approx",
    ] {
        // Neither plain-FGB viewport requirements nor a corrupt band may be
        // triggered by names in literals, aliases, comments, or table aliases.
        csv(&plain, sql, options.clone())?;
        session::register_for_sql(&corrupted, sql, &options)?;
        csv(&corrupted, sql, options.clone())?;
    }

    // Without a bbox, only an actual viewport relation is an error.
    for sql in [
        "SELECT 'a5_viewport_approx' FROM features",
        "SELECT 1 AS a5_viewport_approx FROM features",
    ] {
        csv(&plain, sql, QueryOptions::default())?;
    }
    Ok(())
}

#[test]
fn lazy_nested_and_joined_a5_relations_work() -> TestResult {
    let (_dir, path) = cities_fixture()?;
    let viewport_options = QueryOptions {
        bbox: Some([-10.0, 40.0, 10.0, 60.0]),
        res: Some(2),
        ..QueryOptions::default()
    };
    for relation in ["a5_band_2", "a5_viewport_approx"] {
        let is_viewport = relation == "a5_viewport_approx";
        let options = if is_viewport {
            viewport_options.clone()
        } else {
            QueryOptions::default()
        };
        for sql in [
            format!(
                "WITH cells AS (SELECT * FROM {relation}) SELECT COUNT(*) AS total FROM cells"
            ),
            format!("SELECT COUNT(*) AS total FROM (SELECT * FROM {relation}) AS cells"),
            format!(
                "SELECT COUNT(*) AS total FROM {relation} AS a JOIN {relation} AS b ON a.cell_id = b.cell_id"
            ),
        ] {
            csv(&path, &sql, options.clone())?;
        }
    }
    Ok(())
}

#[test]
fn sql_parse_failures_are_deferred_to_datafusion() -> TestResult {
    let (_dir, path) = cities_fixture()?;
    let sql = "SELECT * FROM";
    session::register_for_sql(&path, sql, &QueryOptions::default())?;
    let error = csv(&path, sql, QueryOptions::default())
        .err()
        .ok_or("invalid SQL succeeded")?;
    let message = error.to_string();
    assert!(message.contains("ParserError"), "{message}");
    assert!(message.contains("Expected: identifier"), "{message}");
    Ok(())
}

#[test]
fn full_registration_still_loads_all_a5_tables() -> TestResult {
    let (_dir, path) = cities_fixture()?;
    let session = session::register(&path, &QueryOptions::default())?;
    assert!(session.ctx.table_exist("a5_band_2")?);
    assert!(!session.ctx.table_exist("a5_viewport_approx")?);

    let session = session::register(
        &path,
        &QueryOptions {
            bbox: Some([-10.0, 40.0, 10.0, 60.0]),
            res: Some(2),
            ..QueryOptions::default()
        },
    )?;
    assert!(session.ctx.table_exist("a5_band_2")?);
    assert!(session.ctx.table_exist("a5_viewport_approx")?);

    let corrupted = corrupt_band(&path, 2)?;
    let error = session::register(&corrupted, &QueryOptions::default())
        .err()
        .ok_or("eager registration accepted a corrupt band")?;
    assert!(
        error.to_string().contains("unsupported band grid"),
        "{error}"
    );
    Ok(())
}

#[test]
fn cte_shadowing_never_scans_a5_providers() -> TestResult {
    let dir = tempfile::tempdir()?;
    let plain = dir.path().join("plain.fgb");
    std::fs::write(&plain, cities_body(false)?)?;
    let options = QueryOptions {
        bbox: Some([-10.0, 40.0, 10.0, 60.0]),
        res: Some(2),
        ..QueryOptions::default()
    };
    let reported = "WITH a5_viewport_approx AS (SELECT * FROM features) SELECT COUNT(*) FROM a5_viewport_approx";
    assert_eq!(csv(&plain, reported, options.clone())?, "count(*)\n12\n");

    let (_analytics_dir, analytics) = cities_fixture()?;
    let corrupted = corrupt_band(&analytics, 2)?;
    for relation in ["a5_viewport_approx", "a5_band_2"] {
        let sql =
            format!("WITH {relation} AS (SELECT * FROM features) SELECT COUNT(*) FROM {relation}");
        assert_eq!(
            csv(&corrupted, &sql, options.clone())?,
            "count(*)\n12\n",
            "{sql}"
        );
    }

    let error = csv(&plain, "SELECT * FROM a5_viewport_approx", options)
        .err()
        .ok_or("plain-FGB viewport scan succeeded")?;
    assert!(error.to_string().contains("FGBA file"), "{error}");
    Ok(())
}

#[test]
fn cte_definitions_scan_actual_a5_base_relations() -> TestResult {
    let (_dir, path) = cities_fixture()?;
    assert_eq!(
        csv(
            &path,
            "WITH cells AS (SELECT * FROM a5_band_2) SELECT SUM(count) AS total FROM cells",
            QueryOptions::default(),
        )?,
        "total\n12\n"
    );
    assert_eq!(
        csv(
            &path,
            "WITH cells AS (SELECT * FROM a5_viewport_approx) SELECT COUNT(*) AS cells, SUM(count) AS estimated FROM cells",
            QueryOptions {
                bbox: Some([-10.0, 40.0, 10.0, 60.0]),
                res: Some(2),
                ..QueryOptions::default()
            },
        )?,
        "cells,estimated\n1,8\n"
    );
    Ok(())
}

#[test]
fn actual_a5_scans_surface_corruption_and_unavailable_res() -> TestResult {
    let (_dir, path) = cities_fixture()?;
    let corrupted = corrupt_band(&path, 2)?;
    let error = csv(
        &corrupted,
        "SELECT * FROM a5_band_2",
        QueryOptions::default(),
    )
    .err()
    .ok_or("corrupt band scan succeeded")?;
    assert!(
        error.to_string().contains("unsupported band grid"),
        "{error}"
    );

    let viewport_options = QueryOptions {
        bbox: Some([-10.0, 40.0, 10.0, 60.0]),
        res: Some(2),
        ..QueryOptions::default()
    };
    let error = csv(
        &corrupted,
        "SELECT * FROM a5_viewport_approx",
        viewport_options,
    )
    .err()
    .ok_or("corrupt viewport band scan succeeded")?;
    assert!(
        error.to_string().contains("unsupported band grid"),
        "{error}"
    );

    let unavailable = QueryOptions {
        bbox: Some([-10.0, 40.0, 10.0, 60.0]),
        res: Some(30),
        ..QueryOptions::default()
    };
    assert_eq!(
        csv(
            &corrupted,
            "SELECT COUNT(*) FROM features",
            unavailable.clone(),
        )?,
        "count(*)\n12\n"
    );
    let error = csv(&corrupted, "SELECT * FROM a5_viewport_approx", unavailable)
        .err()
        .ok_or("unavailable viewport resolution succeeded")?;
    assert!(
        error
            .to_string()
            .contains("no aggregate band at resolution 30"),
        "{error}"
    );
    Ok(())
}

#[test]
fn two_scans_and_plain_or_empty_fallback() -> TestResult {
    let (_dir, path) = cities_fixture()?;
    let joined = execute(
        &path,
        "SELECT COUNT(*) FROM features a JOIN features b ON a.name=b.name",
    )?;
    assert_eq!(scalar_i64(&joined.batches)?, 12);
    assert_eq!(delta(joined.before, joined.after).features_read, 24);

    let dir = tempfile::tempdir()?;
    let plain = dir.path().join("plain.fgb");
    std::fs::write(&plain, cities_body(false)?)?;
    let predicate =
        "bbox_xmin <= 10.0 AND bbox_xmax >= -10.0 AND bbox_ymin <= 60.0 AND bbox_ymax >= 40.0";
    let filtered = execute(
        &plain,
        &format!("SELECT COUNT(*) FROM features WHERE {predicate}"),
    )?;
    assert_eq!(scalar_i64(&filtered.batches)?, 8);
    assert_eq!(delta(filtered.before, filtered.after).features_read, 12);
    assert!(delta(filtered.before, filtered.after).body_bytes_read > 0);

    let empty = dir.path().join("empty.fgb");
    std::fs::write(
        &empty,
        body(&[("name", ColumnType::String, true)], &[], vec![], false)?,
    )?;
    assert_eq!(
        scalar_i64(&execute(&empty, "SELECT COUNT(*) FROM features")?.batches)?,
        0
    );
    assert_eq!(
        row_count(&execute(&empty, "SELECT name FROM features")?.batches),
        0
    );
    assert_eq!(
        scalar_i64(
            &execute(
                &empty,
                &format!("SELECT COUNT(*) FROM features WHERE {predicate}"),
            )?
            .batches
        )?,
        0
    );
    Ok(())
}

#[test]
fn exact_count_alias_limit_and_negative_guards() -> TestResult {
    let (_dir, path) = cities_fixture()?;
    let limited = execute(&path, "SELECT COUNT(1) AS exact_rows FROM features LIMIT 2")?;
    assert_eq!(scalar_i64(&limited.batches)?, 12);
    assert_eq!(
        one_batch(&limited.batches)?.schema().field(0).name(),
        "exact_rows"
    );
    assert_eq!(delta(limited.before, limited.after), ScanMetrics::default());

    let zero = execute(&path, "SELECT COUNT(*) AS exact_rows FROM features LIMIT 0")?;
    assert_eq!(row_count(&zero.batches), 0);
    assert_eq!(delta(zero.before, zero.after), ScanMetrics::default());

    for (sql, expected) in [
        ("SELECT COUNT(*) FROM features WHERE name='city1'", 1),
        ("SELECT COUNT(*) FROM features WHERE name!='city1'", 11),
        ("SELECT COUNT(name) FROM features", 12),
        ("SELECT COUNT(DISTINCT name) FROM features", 12),
    ] {
        let execution = execute(&path, sql)?;
        assert_eq!(scalar_i64(&execution.batches)?, expected, "{sql}");
        assert!(
            delta(execution.before, execution.after).features_read > 0,
            "{sql}"
        );
    }
    let grouped = execute(&path, "SELECT COUNT(*) FROM features GROUP BY name")?;
    assert_eq!(row_count(&grouped.batches), 12);
    assert!(delta(grouped.before, grouped.after).features_read > 0);
    Ok(())
}

#[test]
fn attribute_group_reads_only_name_arrow_chunks() -> TestResult {
    let (_dir, path) = cities_fixture()?;
    let grouped = execute(
        &path,
        "SELECT name, COUNT(*) AS n FROM features GROUP BY name",
    )?;
    assert_eq!(row_count(&grouped.batches), 12);
    let used = delta(grouped.before, grouped.after);
    assert_eq!(used.features_read, 12);
    assert_eq!(used.body_bytes_read, 0);
    assert_eq!(used.wkb_values_decoded, 0);

    let analytics = Reader::open_path(&path)?
        .read_body_analytics_metadata()?
        .ok_or("missing analytics")?;
    let expected_bytes = (0..analytics.chunk_count)
        .map(|chunk| {
            let (start, end) = analytics.arrow_range(0, chunk)?;
            Ok::<u64, fgba::Error>(end - start)
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .sum::<u64>();
    assert_eq!(used.analytics_bytes_read, expected_bytes);
    assert_eq!(used.ranges_opened, analytics.chunk_count);
    Ok(())
}

fn stats_body() -> TestResult<Vec<u8>> {
    body(
        &[
            ("signed", ColumnType::Long, true),
            ("unsigned", ColumnType::ULong, true),
            ("float", ColumnType::Double, true),
            ("finite", ColumnType::Double, true),
            ("single", ColumnType::Float, true),
            ("text", ColumnType::String, true),
        ],
        &[(0.0, 0.0); 5],
        vec![
            vec![
                Some(ColumnValue::Long(-5)),
                Some(ColumnValue::ULong(u64::MAX)),
                Some(ColumnValue::Double(1.5)),
                Some(ColumnValue::Double(1.0)),
                Some(ColumnValue::Float(1.25)),
                Some(ColumnValue::String("a")),
            ],
            vec![
                None,
                None,
                None,
                Some(ColumnValue::Double(2.0)),
                Some(ColumnValue::Float(-0.0)),
                None,
            ],
            vec![
                Some(ColumnValue::Long(9_007_199_254_740_993)),
                Some(ColumnValue::ULong(3)),
                Some(ColumnValue::Double(f64::NAN)),
                None,
                Some(ColumnValue::Float(f32::NAN)),
                Some(ColumnValue::String("c")),
            ],
            vec![
                Some(ColumnValue::Long(2)),
                Some(ColumnValue::ULong(4)),
                Some(ColumnValue::Double(f64::INFINITY)),
                Some(ColumnValue::Double(4.0)),
                None,
                None,
            ],
            vec![
                Some(ColumnValue::Long(7)),
                Some(ColumnValue::ULong(5)),
                Some(ColumnValue::Double(-0.0)),
                Some(ColumnValue::Double(-0.0)),
                Some(ColumnValue::Float(3.5)),
                Some(ColumnValue::String("e")),
            ],
        ],
        true,
    )
}

#[test]
fn attribute_stats_are_exact_and_do_not_read_payload() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = analytics_file(dir.path(), &stats_body()?)?;
    let execution = execute(&path, "SELECT * FROM attribute_stats ORDER BY column_index")?;
    assert_eq!(execution.before, execution.after);
    let batch = one_batch(&execution.batches)?;
    assert_eq!(batch.num_rows(), 6);
    let names = array::<StringArray>(batch, "name")?;
    let row = |name: &str| {
        (0..names.len())
            .find(|&index| names.value(index) == name)
            .ok_or_else(|| format!("missing stats row {name}"))
    };

    let signed = row("signed")?;
    assert_eq!(array::<UInt64Array>(batch, "row_count")?.value(signed), 5);
    assert_eq!(
        array::<UInt64Array>(batch, "non_null_count")?.value(signed),
        4
    );
    assert_eq!(array::<UInt64Array>(batch, "null_count")?.value(signed), 1);
    assert_eq!(array::<Int64Array>(batch, "min_i64")?.value(signed), -5);
    assert_eq!(
        array::<Int64Array>(batch, "max_i64")?.value(signed),
        9_007_199_254_740_993
    );
    assert_eq!(
        array::<StringArray>(batch, "sum_text")?.value(signed),
        "9007199254740997"
    );
    assert!(array::<BooleanArray>(batch, "sum_valid")?.value(signed));

    let unsigned = row("unsigned")?;
    assert_eq!(array::<UInt64Array>(batch, "min_u64")?.value(unsigned), 3);
    assert_eq!(
        array::<UInt64Array>(batch, "max_u64")?.value(unsigned),
        u64::MAX
    );
    assert_eq!(
        array::<StringArray>(batch, "sum_text")?.value(unsigned),
        "18446744073709551627"
    );

    let float = row("float")?;
    assert_eq!(
        array::<UInt64Array>(batch, "non_null_count")?.value(float),
        4
    );
    assert_eq!(array::<UInt64Array>(batch, "nan_count")?.value(float), 1);
    assert!(array::<Float64Array>(batch, "min_f64")?
        .value(float)
        .is_sign_negative());
    assert_eq!(
        array::<Float64Array>(batch, "max_f64")?.value(float),
        f64::INFINITY
    );
    assert!(array::<StringArray>(batch, "sum_text")?.is_null(float));
    assert!(!array::<BooleanArray>(batch, "sum_valid")?.value(float));
    assert!(array::<BooleanArray>(batch, "has_infinity")?.value(float));

    let finite = row("finite")?;
    assert_eq!(array::<StringArray>(batch, "sum_text")?.value(finite), "7");
    assert_eq!(array::<Float64Array>(batch, "max_f64")?.value(finite), 4.0);

    let text = row("text")?;
    assert_eq!(
        array::<StringArray>(batch, "value_class")?.value(text),
        "opaque"
    );
    assert_eq!(
        array::<UInt64Array>(batch, "non_null_count")?.value(text),
        3
    );
    assert_eq!(array::<UInt64Array>(batch, "null_count")?.value(text), 2);
    assert!(array::<Int64Array>(batch, "min_i64")?.is_null(text));
    assert!(array::<Int64Array>(batch, "max_i64")?.is_null(text));
    assert!(array::<UInt64Array>(batch, "min_u64")?.is_null(text));
    assert!(array::<UInt64Array>(batch, "max_u64")?.is_null(text));
    assert!(array::<Float64Array>(batch, "min_f64")?.is_null(text));
    assert!(array::<Float64Array>(batch, "max_f64")?.is_null(text));
    assert!(array::<StringArray>(batch, "sum_text")?.is_null(text));
    assert!(!array::<BooleanArray>(batch, "sum_valid")?.value(text));
    Ok(())
}

const TYPE_COLUMNS: [(&str, ColumnType); 15] = [
    ("byte", ColumnType::Byte),
    ("ubyte", ColumnType::UByte),
    ("bool", ColumnType::Bool),
    ("short", ColumnType::Short),
    ("ushort", ColumnType::UShort),
    ("int", ColumnType::Int),
    ("uint", ColumnType::UInt),
    ("long", ColumnType::Long),
    ("ulong", ColumnType::ULong),
    ("float", ColumnType::Float),
    ("double", ColumnType::Double),
    ("string", ColumnType::String),
    ("json", ColumnType::Json),
    ("datetime", ColumnType::DateTime),
    ("binary", ColumnType::Binary),
];

fn typed_body() -> TestResult<Vec<u8>> {
    let declarations = TYPE_COLUMNS
        .iter()
        .map(|(name, kind)| (*name, *kind, true))
        .collect::<Vec<_>>();
    let first = vec![
        Some(ColumnValue::Byte(-8)),
        Some(ColumnValue::UByte(8)),
        Some(ColumnValue::Bool(true)),
        Some(ColumnValue::Short(-16)),
        Some(ColumnValue::UShort(16)),
        Some(ColumnValue::Int(-32)),
        Some(ColumnValue::UInt(32)),
        Some(ColumnValue::Long(-64)),
        Some(ColumnValue::ULong(64)),
        Some(ColumnValue::Float(1.25)),
        Some(ColumnValue::Double(2.5)),
        Some(ColumnValue::String("text")),
        Some(ColumnValue::Json("{\"a\":1}")),
        Some(ColumnValue::DateTime("2026-10-25T01:02:03")),
        Some(ColumnValue::Binary(&[0, 1, 255])),
    ];
    body(
        &declarations,
        &[(0.0, 0.0); 2],
        vec![first, (0..TYPE_COLUMNS.len()).map(|_| None).collect()],
        true,
    )
}

fn assert_typed_values(batch: &RecordBatch) -> TestResult {
    macro_rules! value {
        ($name:expr, $array:ident, $expected:expr) => {
            assert_eq!(array::<$array>(batch, $name)?.value(0), $expected, $name);
        };
    }
    value!("byte", Int8Array, -8);
    value!("ubyte", UInt8Array, 8);
    value!("bool", BooleanArray, true);
    value!("short", Int16Array, -16);
    value!("ushort", UInt16Array, 16);
    value!("int", Int32Array, -32);
    value!("uint", UInt32Array, 32);
    value!("long", Int64Array, -64);
    value!("ulong", UInt64Array, 64);
    value!("float", Float32Array, 1.25);
    value!("double", Float64Array, 2.5);
    value!("string", StringArray, "text");
    value!("json", StringArray, "{\"a\":1}");
    value!("datetime", StringArray, "2026-10-25T01:02:03");
    value!("binary", BinaryArray, &[0, 1, 255][..]);
    for (name, _) in TYPE_COLUMNS {
        let index = batch.schema().index_of(name)?;
        assert!(batch.column(index).is_null(1), "{name} should be null");
    }
    Ok(())
}

#[test]
fn every_column_type_and_null_round_trips_through_both_sources() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = analytics_file(dir.path(), &typed_body()?)?;
    let properties = TYPE_COLUMNS
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(", ");
    let arrow = execute(&path, &format!("SELECT {properties} FROM features"))?;
    assert_eq!(delta(arrow.before, arrow.after).body_bytes_read, 0);
    assert_eq!(delta(arrow.before, arrow.after).wkb_values_decoded, 0);
    let arrow_batch = one_batch(&arrow.batches)?;
    assert_typed_values(arrow_batch)?;
    for (name, expected) in [
        ("byte", DataType::Int8),
        ("ubyte", DataType::UInt8),
        ("bool", DataType::Boolean),
        ("short", DataType::Int16),
        ("ushort", DataType::UInt16),
        ("int", DataType::Int32),
        ("uint", DataType::UInt32),
        ("long", DataType::Int64),
        ("ulong", DataType::UInt64),
        ("float", DataType::Float32),
        ("double", DataType::Float64),
        ("string", DataType::Utf8),
        ("json", DataType::Utf8),
        ("datetime", DataType::Utf8),
        ("binary", DataType::Binary),
    ] {
        assert_eq!(
            arrow_batch.schema().field_with_name(name)?.data_type(),
            &expected
        );
    }

    let body = execute(
        &path,
        &format!("SELECT {properties}, geom_wkb FROM features"),
    )?;
    assert!(delta(body.before, body.after).body_bytes_read > 0);
    assert_eq!(delta(body.before, body.after).wkb_values_decoded, 2);
    assert_typed_values(one_batch(&body.batches)?)?;
    Ok(())
}

#[test]
fn crs_metadata_is_preserved() -> TestResult {
    let (_dir, path) = cities_fixture()?;
    let session = session::register(&path, &QueryOptions::default())?;
    let metadata = &session.features.output_schema().schema.metadata;
    assert_eq!(
        metadata.get(CRS_CODE_METADATA).map(String::as_str),
        Some("4326")
    );
    assert_eq!(
        metadata.get(CRS_WKT_METADATA).map(String::as_str),
        Some("test CRS WKT")
    );
    Ok(())
}

#[test]
fn missing_nonnullable_and_bad_schema_errors_are_named() -> TestResult {
    let dir = tempfile::tempdir()?;
    let missing = dir.path().join("missing.fgb");
    std::fs::write(
        &missing,
        body(
            &[("required", ColumnType::String, false)],
            &[(0.0, 0.0); 2],
            vec![vec![Some(ColumnValue::String("present"))], vec![None]],
            false,
        )?,
    )?;
    let error = execute(&missing, "SELECT required FROM features")
        .err()
        .ok_or("missing non-nullable value succeeded")?
        .to_string();
    assert!(error.contains("required"), "{error}");
    assert!(error.contains("feature 1"), "{error}");

    let mut schema_cases = vec![
        ("exact", vec!["name", "name"], "duplicate property name"),
        ("case", vec!["Name", "name"], "ignoring case"),
    ];
    for reserved in [
        "geom_wkb",
        "bbox_xmin",
        "bbox_ymin",
        "bbox_xmax",
        "BBOX_YMAX",
        "geom_length",
        "geom_area",
        "geom_npoints",
    ] {
        schema_cases.push((reserved, vec![reserved], "reserved SQL geometry"));
    }
    for (tag, names, expected) in schema_cases {
        let declarations = names
            .iter()
            .map(|name| (*name, ColumnType::String, true))
            .collect::<Vec<_>>();
        let path = dir.path().join(format!("{tag}.fgb"));
        std::fs::write(
            &path,
            body(&declarations, &[(0.0, 0.0)], vec![vec![]], false)?,
        )?;
        let error = session::register(&path, &QueryOptions::default())
            .err()
            .ok_or("invalid schema succeeded")?
            .to_string();
        assert!(error.contains(expected), "{tag}: {error}");
    }
    let unknown = dir.path().join("unknown.fgb");
    std::fs::write(
        &unknown,
        body(
            &[("mystery", ColumnType(99), true)],
            &[(0.0, 0.0)],
            vec![vec![]],
            false,
        )?,
    )?;
    let error = session::register(&unknown, &QueryOptions::default())
        .err()
        .ok_or("unknown type succeeded")?
        .to_string();
    assert!(error.contains("mystery"), "{error}");
    Ok(())
}

#[test]
fn bbox_validation_reports_feature_ordinal_only_when_decoded() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("invalid-geometry.fgb");
    std::fs::write(
        &path,
        body(
            &[("name", ColumnType::String, true)],
            &[(0.0, 0.0), (f64::NAN, 1.0)],
            vec![
                vec![Some(ColumnValue::String("first"))],
                vec![Some(ColumnValue::String("second"))],
            ],
            false,
        )?,
    )?;
    let wkb = execute(&path, "SELECT geom_wkb FROM features")?;
    assert_eq!(row_count(&wkb.batches), 2);
    let error = execute(&path, "SELECT bbox_xmin FROM features")
        .err()
        .ok_or("non-finite bbox succeeded")?
        .to_string();
    assert!(error.contains("feature 1"), "{error}");
    Ok(())
}

#[test]
fn bbox_count_fast_path_and_joint_filter_stay_exact() -> TestResult {
    let (_dir, path) = cities_fixture()?;
    let predicate =
        "bbox_xmin <= 10.0 AND bbox_xmax >= -10.0 AND bbox_ymin <= 60.0 AND bbox_ymax >= 40.0";
    let count = execute(
        &path,
        &format!("SELECT COUNT(*) FROM features WHERE {predicate}"),
    )?;
    assert_eq!(scalar_i64(&count.batches)?, 8);
    let used = delta(count.before, count.after);
    assert_eq!(used.features_read, 0);
    assert_eq!(used.analytics_bytes_read, 0);
    assert_eq!(used.wkb_values_decoded, 0);
    assert_eq!(used.ranges_opened, 1);
    assert!(used.body_bytes_read > 0);

    let joint = execute(
        &path,
        &format!("SELECT COUNT(*) FROM features WHERE {predicate} AND name='city1'"),
    )?;
    assert_eq!(scalar_i64(&joint.batches)?, 1);
    assert!(delta(joint.before, joint.after).features_read > 0);
    Ok(())
}

#[test]
fn read_only_sql_and_option_validation() -> TestResult {
    let (_dir, path) = cities_fixture()?;
    assert!(csv(&path, "   ", QueryOptions::default()).is_err());
    assert!(csv(
        &path,
        "SELECT * FROM a5_viewport_approx",
        QueryOptions {
            res: Some(2),
            ..QueryOptions::default()
        },
    )
    .is_err());
    for sql in [
        "CREATE TABLE x (a BIGINT)",
        "COPY features TO 'copy.csv'",
        "SET datafusion.execution.target_partitions = 1",
    ] {
        let error = execute(&path, sql)
            .err()
            .ok_or_else(|| format!("read-only query succeeded: {sql}"))?;
        assert!(!error.to_string().is_empty());
    }
    let explain = execute(&path, "EXPLAIN SELECT * FROM features")?;
    assert!(row_count(&explain.batches) > 0);
    assert_eq!(
        one_batch(&explain.batches)?.schema().field(0).name(),
        "plan_type"
    );

    let dir = tempfile::tempdir()?;
    let plain = dir.path().join("plain.fgb");
    std::fs::write(&plain, cities_body(false)?)?;
    let error = csv(
        &plain,
        "SELECT * FROM a5_viewport_approx",
        QueryOptions {
            bbox: Some([-10.0, 40.0, 10.0, 60.0]),
            ..QueryOptions::default()
        },
    )
    .err()
    .ok_or("bbox without a band succeeded")?;
    assert!(error.to_string().contains("band"));
    Ok(())
}

#[test]
fn limit_one_cancels_body_source_after_bounded_batches() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("large.fgb");
    let rows = 100_000;
    let names = (0..rows).map(|_| "x".to_owned()).collect::<Vec<_>>();
    let points = (0..rows).map(|_| (0.0, 0.0)).collect::<Vec<_>>();
    let properties = names
        .iter()
        .map(|name| vec![Some(ColumnValue::String(name.as_str()))])
        .collect();
    std::fs::write(
        &path,
        body(
            &[("name", ColumnType::String, false)],
            &points,
            properties,
            false,
        )?,
    )?;
    let execution = execute_with(
        &path,
        "SELECT name FROM features LIMIT 1",
        QueryOptions {
            batch_rows: 16,
            ..QueryOptions::default()
        },
    )?;
    assert_eq!(row_count(&execution.batches), 1);
    let used = delta(execution.before, execution.after);
    assert!(used.features_read > 0);
    assert!(
        used.features_read <= 3 * 16,
        "source decoded {} rows instead of stopping near the limit",
        used.features_read
    );
    Ok(())
}

#[test]
fn oversized_features_still_progress_in_both_sources() -> TestResult {
    let (_dir, path) = cities_fixture()?;
    for sql in ["SELECT name FROM features", "SELECT geom_wkb FROM features"] {
        let execution = execute_with(
            &path,
            sql,
            QueryOptions {
                batch_rows: 100,
                batch_bytes: 1,
                ..QueryOptions::default()
            },
        )?;
        assert_eq!(row_count(&execution.batches), 12, "{sql}");
        assert_eq!(
            delta(execution.before, execution.after).features_read,
            12,
            "{sql}"
        );
    }
    Ok(())
}

#[test]
fn malformed_optional_analytics_falls_back_to_body() -> TestResult {
    let (dir, path) = cities_fixture()?;
    let analytics = Reader::open_path(&path)?
        .read_body_analytics_metadata()?
        .ok_or("missing analytics")?;
    let mut bytes = std::fs::read(&path)?;
    bytes[analytics.section_start as usize] = fgba::analytics::ANALYTICS_VERSION + 1;
    let malformed = dir.path().join("malformed.a.fgb");
    std::fs::write(&malformed, bytes)?;

    let execution = execute(&malformed, "SELECT name FROM features")?;
    assert_eq!(row_count(&execution.batches), 12);
    assert!(delta(execution.before, execution.after).body_bytes_read > 0);
    assert_eq!(
        delta(execution.before, execution.after).analytics_bytes_read,
        0
    );
    Ok(())
}
