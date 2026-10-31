//! Bounded eager SQL over sparse FGBA bytes, with native differential checks.

#[cfg(not(target_arch = "wasm32"))]
use fgba::{build, Reader};
use fgba_wasm::range::{SparseRange, MAX_CACHE_BYTES, MAX_RANGE_BYTES};
use fgba_wasm::sql::{query_sql, SqlLimits, SqlRequest};
use futures::executor::LocalPool;
use serde_json::{json, Value};

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test;
#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[cfg(not(target_arch = "wasm32"))]
mod fixtures {
    use super::TestResult;
    use fgba::{build_with_options, BuildOptions};
    use flatgeobuf::geozero::{ColumnValue, GeomProcessor, GeozeroGeometry, PropertyProcessor};
    use flatgeobuf::{ColumnType, FgbWriter, FgbWriterOptions, GeometryType};

    #[derive(Clone)]
    struct Line(Vec<(f64, f64)>);

    impl GeozeroGeometry for Line {
        fn process_geom<P: GeomProcessor>(
            &self,
            processor: &mut P,
        ) -> flatgeobuf::geozero::error::Result<()> {
            processor.linestring_begin(true, self.0.len(), 0)?;
            for (index, &(x, y)) in self.0.iter().enumerate() {
                processor.xy(x, y, index)?;
            }
            processor.linestring_end(true, 0)
        }
    }

    struct Rectangle {
        x: f64,
        y: f64,
        size: f64,
    }

    impl GeozeroGeometry for Rectangle {
        fn process_geom<P: GeomProcessor>(
            &self,
            processor: &mut P,
        ) -> flatgeobuf::geozero::error::Result<()> {
            let points = [
                (self.x, self.y),
                (self.x + self.size, self.y),
                (self.x + self.size, self.y + self.size),
                (self.x, self.y + self.size),
                (self.x, self.y),
            ];
            processor.polygon_begin(true, 1, 0)?;
            processor.linestring_begin(false, points.len(), 0)?;
            for (index, &(x, y)) in points.iter().enumerate() {
                processor.xy(x, y, index)?;
            }
            processor.linestring_end(false, 0)?;
            processor.polygon_end(true, 0)
        }
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

    pub(super) fn line_body(rows: usize) -> TestResult<Vec<u8>> {
        let mut writer = FgbWriter::create_with_options(
            "wasm-sql",
            GeometryType::LineString,
            FgbWriterOptions {
                write_index: true,
                ..FgbWriterOptions::default()
            },
        )?;
        writer.add_column("id", ColumnType::Long, |_, args| args.nullable = false);
        writer.add_column("name", ColumnType::String, |_, args| args.nullable = false);
        writer.add_column("payload", ColumnType::String, |_, args| {
            args.nullable = true
        });
        for row in 0..rows {
            let (x, y) = CITY_POINTS[row % CITY_POINTS.len()];
            let line = Line(vec![(x, y), (x + 0.5, y), (x + 1.0, y)]);
            let name = format!("feature-{row:04}");
            let payload = format!("payload-{row:04}-{}", "x".repeat(32));
            let mut property_error = None;
            writer.add_feature_geom(line, |feature| {
                for (column, name, value) in [
                    (0, "id", ColumnValue::Long(row as i64)),
                    (1, "name", ColumnValue::String(name.as_str())),
                    (2, "payload", ColumnValue::String(payload.as_str())),
                ] {
                    if let Err(error) = feature.property(column, name, &value) {
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

    pub(super) fn polygon_body() -> TestResult<Vec<u8>> {
        let mut writer = FgbWriter::create_with_options(
            "wasm-sql-areas",
            GeometryType::Polygon,
            FgbWriterOptions {
                write_index: true,
                ..FgbWriterOptions::default()
            },
        )?;
        writer.add_column("id", ColumnType::Long, |_, args| args.nullable = false);
        for (id, size) in [(0, 1.0), (1, 2.0)] {
            let rectangle = Rectangle {
                x: 2.0 + id as f64 * 10.0,
                y: 48.0,
                size,
            };
            let mut property_error = None;
            writer.add_feature_geom(rectangle, |feature| {
                if let Err(error) = feature.property(0, "id", &ColumnValue::Long(id)) {
                    property_error = Some(error);
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

    pub(super) fn analytics(bytes: &[u8], geometry_metrics: bool) -> TestResult<Vec<u8>> {
        let (file, _) = build_with_options(
            bytes,
            &BuildOptions {
                analytics: true,
                geometry_metrics,
                chunk_rows: 3,
            },
        )?;
        Ok(file)
    }
}

#[cfg(not(target_arch = "wasm32"))]
use fixtures::*;

fn sparse(bytes: &[u8]) -> TestResult<SparseRange> {
    let mut source = SparseRange::new(bytes.len() as u64, MAX_RANGE_BYTES, MAX_CACHE_BYTES)?;
    for (index, chunk) in bytes.chunks(MAX_RANGE_BYTES).enumerate() {
        source.insert((index * MAX_RANGE_BYTES) as u64, chunk)?;
    }
    Ok(source)
}

fn request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_owned(),
        bbox: None,
        res: None,
        limits: SqlLimits::default(),
    }
}

fn run(source: SparseRange, request: SqlRequest) -> TestResult<Value> {
    let mut pool = LocalPool::new();
    let json = pool.run_until(query_sql(source, request))?;
    Ok(serde_json::from_str(&json)?)
}

fn run_bytes(bytes: &[u8], sql: &str) -> TestResult<Value> {
    run(sparse(bytes)?, request(sql))
}

fn run_error(bytes: &[u8], sql: &str) -> TestResult<String> {
    let mut pool = LocalPool::new();
    let error = pool
        .run_until(query_sql(sparse(bytes)?, request(sql)))
        .err()
        .ok_or_else(|| format!("query unexpectedly succeeded: {sql}"))?;
    Ok(error.to_string())
}

#[cfg(not(target_arch = "wasm32"))]
fn native_csv(
    path: &std::path::Path,
    sql: &str,
    bbox: Option<[f64; 4]>,
    res: Option<u8>,
) -> TestResult<String> {
    let mut bytes = Vec::new();
    fgba_sql::query_csv(
        path,
        sql,
        fgba_sql::QueryOptions {
            bbox,
            res,
            ..fgba_sql::QueryOptions::default()
        },
        &mut bytes,
    )?;
    Ok(String::from_utf8(bytes)?)
}

#[cfg(not(target_arch = "wasm32"))]
fn assert_matches_native_csv(actual: &Value, csv: &str) -> TestResult {
    let mut lines = csv.lines();
    let columns = lines
        .next()
        .ok_or("native CSV has no header")?
        .split(',')
        .collect::<Vec<_>>();
    assert_eq!(
        actual["columns"].as_array().ok_or("columns")?.len(),
        columns.len()
    );
    for (actual, expected) in actual["columns"]
        .as_array()
        .ok_or("columns")?
        .iter()
        .zip(columns.iter())
    {
        assert_eq!(actual.as_str().ok_or("column name")?, *expected);
    }
    let expected_rows = lines
        .map(|line| line.split(',').collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let actual_rows = actual["rows"].as_array().ok_or("rows")?;
    assert_eq!(actual_rows.len(), expected_rows.len());
    for (actual_row, expected_row) in actual_rows.iter().zip(expected_rows) {
        let actual_row = actual_row.as_array().ok_or("row")?;
        assert_eq!(actual_row.len(), expected_row.len());
        for (actual, expected) in actual_row.iter().zip(expected_row) {
            match actual {
                Value::String(actual) => assert_eq!(actual, expected),
                Value::Bool(actual) => assert_eq!(actual.to_string(), expected),
                Value::Number(actual) => {
                    let actual = actual.as_f64().ok_or("JSON number")?;
                    let expected = expected.parse::<f64>()?;
                    let tolerance = expected.abs().max(1.0) * 1e-12;
                    assert!(
                        (actual - expected).abs() <= tolerance,
                        "{actual} != {expected}"
                    );
                }
                Value::Null => assert_eq!(expected, ""),
                other => return Err(format!("unsupported JSON value {other}").into()),
            }
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn select_explain_and_read_only_rejections() -> TestResult {
    let file = analytics(&line_body(12)?, true)?;
    let result = run_bytes(&file, "SELECT COUNT(*) AS exact_rows FROM features")?;
    assert_eq!(result["columns"], json!(["exact_rows"]));
    assert_eq!(result["rows"], json!([["12"]]));
    assert_eq!(result["truncated"], json!(false));

    let explain = run_bytes(&file, "EXPLAIN SELECT id FROM features")?;
    assert!(!explain["rows"].as_array().ok_or("EXPLAIN rows")?.is_empty());

    for sql in [
        "DELETE FROM features",
        "UPDATE features SET id = 1",
        "CREATE TABLE copy AS SELECT * FROM features",
        "SELECT * FROM features; SELECT * FROM features",
        "EXPLAIN DELETE FROM features",
        "EXPLAIN ANALYZE SELECT * FROM features",
        "SELECT * INTO copy FROM features",
        "SELECT * FROM unknown_source",
        "SELECT * FROM read_csv('other.csv')",
        "SELECT * FROM features(1)",
    ] {
        let error = run_error(&file, sql)?;
        assert!(!error.is_empty(), "{sql}");
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn aggregate_self_join_and_udfs_execute_without_a_tokio_runtime() -> TestResult {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let file = analytics(&line_body(12)?, true)?;
    let aggregate = run_bytes(
        &file,
        "SELECT SUM(id) AS total, COUNT(*) AS rows FROM features",
    )?;
    assert_eq!(aggregate["rows"], json!([["66", "12"]]));

    let joined = run_bytes(
        &file,
        "SELECT COUNT(*) AS rows FROM features AS a JOIN features AS b ON a.id = b.id",
    )?;
    assert_eq!(joined["rows"], json!([["12"]]));

    let udf = run_bytes(
        &file,
        "SELECT SUM(st_length(geom_wkb)) AS length, SUM(st_area(geom_wkb)) AS area, SUM(st_npoints(geom_wkb)) AS points FROM features",
    )?;
    assert_eq!(udf["rows"][0][1], json!(0.0));
    assert_eq!(udf["rows"][0][2], json!("36"));
    let length = udf["rows"][0][0].as_f64().ok_or("length")?;
    assert!((length - 12.0).abs() < 1e-12, "{length}");
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn differential_features_a5_stats_metrics_and_udfs_match_native() -> TestResult {
    let dir = tempfile::tempdir()?;
    let file = analytics(&line_body(12)?, true)?;
    let path = dir.path().join("fixture.a.fgb");
    std::fs::write(&path, &file)?;
    let viewport = [-10.0, 40.0, 10.0, 60.0];
    for (sql, bbox, res) in [
        ("SELECT id, name FROM features WHERE id >= 5 ORDER BY id", None, None),
        ("SELECT SUM(count) AS total FROM a5_band_2", None, None),
        ("SELECT COUNT(*) AS cells, SUM(count) AS estimated FROM a5_viewport_approx", Some(viewport), Some(2)),
        ("SELECT name, row_count, non_null_count, null_count, min_i64, max_i64 FROM attribute_stats WHERE name = 'id'", None, None),
        ("SELECT id, st_length(geom_wkb) AS length, st_area(geom_wkb) AS area, st_npoints(geom_wkb) AS points FROM features ORDER BY id", None, None),
        ("SELECT id, geom_length, geom_area, geom_npoints, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax FROM features ORDER BY id", None, None),
    ] {
        let actual = run(
            sparse(&file)?,
            SqlRequest {
                sql: sql.to_owned(),
                bbox,
                res,
                limits: SqlLimits::default(),
            },
        )?;
        let native = native_csv(&path, sql, bbox, res)?;
        assert_matches_native_csv(&actual, &native)?;
    }

    let polygons = analytics(&polygon_body()?, true)?;
    let polygon_path = dir.path().join("polygons.a.fgb");
    std::fs::write(&polygon_path, &polygons)?;
    for sql in [
        "SELECT id, st_area(geom_wkb) AS area FROM features ORDER BY id",
        "SELECT id, geom_area, geom_length, geom_npoints FROM features ORDER BY id",
    ] {
        let actual = run_bytes(&polygons, sql)?;
        assert_matches_native_csv(&actual, &native_csv(&polygon_path, sql, None, None)?)?;
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn bigint_scalars_are_decimal_strings_not_lossy_doubles() -> TestResult {
    let file = analytics(&line_body(12)?, true)?;
    let actual = run_bytes(
        &file,
        "SELECT 9007199254740993 AS exact_value, COUNT(*) AS exact_count FROM features",
    )?;
    assert_eq!(actual["rows"], json!([["9007199254740993", "12"]]));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn plain_no_analytics_file_uses_exact_body_conversion() -> TestResult {
    let (file, _) = build(&line_body(12)?)?;
    let actual = run_bytes(
        &file,
        "SELECT id, name, geom_wkb, geom_length, geom_area, geom_npoints FROM features WHERE id = 0",
    )?;
    assert_eq!(actual["rows"][0][0], json!("0"));
    assert_eq!(actual["rows"][0][1], json!("feature-0000"));
    assert!(actual["rows"][0][2]
        .as_str()
        .ok_or("WKB hex")?
        .starts_with("01"));
    assert_eq!(actual["rows"][0][3], json!(1.0));
    assert_eq!(actual["rows"][0][4], json!(0.0));
    assert_eq!(actual["rows"][0][5], json!(3));

    let body_len = Reader::open(sparse(&file)?)?.body_len();
    let error = run(
        sparse(&file)?,
        SqlRequest {
            limits: SqlLimits {
                max_encoded_bytes: body_len - 1,
                ..SqlLimits::default()
            },
            ..request("SELECT geom_wkb FROM features")
        },
    )
    .err()
    .ok_or("encoded budget unexpectedly succeeded")?;
    assert!(error.to_string().contains("encoded"), "{error}");
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn malformed_arrow_falls_back_to_the_exact_chunk_body_range() -> TestResult {
    let mut file = analytics(&line_body(12)?, true)?;
    let mut reader = Reader::open(sparse(&file)?)?;
    let metadata = reader
        .read_body_analytics_metadata()?
        .ok_or("analytics metadata")?;
    let (start, end) = metadata.arrow_range(0, 0)?;
    file[start as usize..end as usize].fill(0);
    let actual = run_bytes(&file, "SELECT id, name FROM features ORDER BY id")?;
    assert_eq!(actual["rows"].as_array().ok_or("rows")?.len(), 12);
    assert_eq!(actual["rows"][0], json!(["0", "feature-0000"]));
    assert_eq!(actual["rows"][11], json!(["11", "feature-0011"]));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn result_source_and_json_budgets_are_enforced() -> TestResult {
    let file = analytics(&line_body(1001)?, true)?;
    let actual = run_bytes(&file, "SELECT id FROM features ORDER BY id")?;
    assert_eq!(actual["rows"].as_array().ok_or("rows")?.len(), 1000);
    assert_eq!(actual["rows"][0], json!(["0"]));
    assert_eq!(actual["rows"][999], json!(["999"]));
    assert_eq!(actual["truncated"], json!(true));

    for (limits, sql, expected) in [
        (
            SqlLimits {
                max_source_rows: 10,
                ..SqlLimits::default()
            },
            "SELECT COUNT(*) FROM features",
            "source rows",
        ),
        (
            SqlLimits {
                max_decoded_bytes: 32,
                ..SqlLimits::default()
            },
            "SELECT payload FROM features",
            "decoded",
        ),
        (
            SqlLimits {
                max_result_json_bytes: 64,
                ..SqlLimits::default()
            },
            "SELECT id, name, payload FROM features",
            "result JSON",
        ),
    ] {
        let error = run(
            sparse(&file)?,
            SqlRequest {
                sql: sql.to_owned(),
                bbox: None,
                res: None,
                limits,
            },
        )
        .err()
        .ok_or("budget unexpectedly succeeded")?;
        assert!(error.to_string().contains(expected), "{error}");
    }
    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn cities() -> &'static [u8] {
    include_bytes!("../../../testdata/cities_12.a.fgb")
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen_test]
fn browser_select_explain_body_and_read_only_rejections() -> TestResult {
    let result = run_bytes(cities(), "SELECT COUNT(*) AS exact_rows FROM features")?;
    assert_eq!(result["columns"], json!(["exact_rows"]));
    assert_eq!(result["rows"], json!([["12"]]));
    assert_eq!(result["truncated"], json!(false));

    let explain = run_bytes(cities(), "EXPLAIN SELECT geom_wkb FROM features")?;
    assert!(!explain["rows"].as_array().ok_or("EXPLAIN rows")?.is_empty());
    let body = run_bytes(
        cities(),
        "SELECT geom_wkb, geom_length, geom_area, geom_npoints FROM features LIMIT 1",
    )?;
    assert!(body["rows"][0][0]
        .as_str()
        .ok_or("WKB hex")?
        .starts_with("01"));
    assert_eq!(body["rows"][0][1], json!(0.0));
    assert_eq!(body["rows"][0][2], json!(0.0));
    assert_eq!(body["rows"][0][3], json!(1));

    for sql in [
        "DELETE FROM features",
        "UPDATE features SET geom_npoints = 1",
        "CREATE TABLE copy AS SELECT * FROM features",
        "SELECT * FROM features; SELECT * FROM features",
        "EXPLAIN DELETE FROM features",
        "EXPLAIN ANALYZE SELECT * FROM features",
        "SELECT * INTO copy FROM features",
        "SELECT * FROM unknown_source",
        "SELECT * FROM read_csv('other.csv')",
        "SELECT * FROM features(1)",
    ] {
        let error = run_error(cities(), sql)?;
        assert!(!error.is_empty(), "{sql}");
    }
    Ok(())
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen_test]
fn browser_aggregate_self_join_udfs_and_result_cap() -> TestResult {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let joined = run_bytes(
        cities(),
        "SELECT COUNT(*) AS rows FROM features AS a JOIN features AS b ON a.geom_npoints = b.geom_npoints",
    )?;
    assert_eq!(joined["rows"], json!([["144"]]));
    let udf = run_bytes(
        cities(),
        "SELECT SUM(st_length(geom_wkb)) AS length, SUM(st_area(geom_wkb)) AS area, SUM(st_npoints(geom_wkb)) AS points FROM features",
    )?;
    assert_eq!(udf["rows"], json!([[0.0, 0.0, "12"]]));

    let capped = run_bytes(
        cities(),
        "SELECT a.geom_npoints FROM features AS a CROSS JOIN features AS b CROSS JOIN features AS c",
    )?;
    assert_eq!(capped["rows"].as_array().ok_or("rows")?.len(), 1000);
    assert_eq!(capped["truncated"], json!(true));
    Ok(())
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen_test]
fn browser_source_and_output_budgets_fail_cleanly() -> TestResult {
    for (limits, sql, expected) in [
        (
            SqlLimits {
                max_source_rows: 11,
                ..SqlLimits::default()
            },
            "SELECT COUNT(*) FROM features",
            "source rows",
        ),
        (
            SqlLimits {
                max_decoded_bytes: 1,
                ..SqlLimits::default()
            },
            "SELECT geom_wkb FROM features",
            "decoded",
        ),
        (
            SqlLimits {
                max_result_json_bytes: 64,
                ..SqlLimits::default()
            },
            "SELECT geom_wkb FROM features",
            "result JSON",
        ),
    ] {
        let error = run(
            sparse(cities())?,
            SqlRequest {
                sql: sql.to_owned(),
                bbox: None,
                res: None,
                limits,
            },
        )
        .err()
        .ok_or("budget unexpectedly succeeded")?;
        assert!(error.to_string().contains(expected), "{error}");
    }
    Ok(())
}
