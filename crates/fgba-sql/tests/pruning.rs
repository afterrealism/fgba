//! Conservative pruning and exact full-scan parity tests.

use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use datafusion::prelude::SQLOptions;
use fgba::io::SharedReader;
use fgba::{build_with_options, BuildOptions, Reader};
use fgba_sql::{query_csv, session, QueryOptions, ScanMetrics};
use flatgeobuf::geozero::{ColumnValue, GeomProcessor, GeozeroGeometry, PropertyProcessor};
use flatgeobuf::{
    ColumnType, FallibleStreamingIterator, FgbReader, FgbWriter, FgbWriterOptions, GeometryType,
};
use futures::StreamExt;
use std::io::Cursor;
use std::path::{Path, PathBuf};

// Only small test results are collected; production query_csv streams output.
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

fn body() -> TestResult<Vec<u8>> {
    let columns = [
        ("name", ColumnType::String, false),
        ("height", ColumnType::Long, true),
        ("payload", ColumnType::String, true),
        ("single", ColumnType::Float, true),
        ("double", ColumnType::Double, true),
        ("big", ColumnType::Long, true),
    ];
    let names = (0..8).map(|row| format!("city{row}")).collect::<Vec<_>>();
    let payloads = (0..8)
        .map(|row| format!("payload-{row}-{}", "x".repeat(96)))
        .collect::<Vec<_>>();
    let heights = [0, 10, 200, 300, 20, 30, 400, 500];
    let singles = [1.0f32, 1.0, 3.0, 3.0, 1.0, 1.0, 3.0, 3.0];
    let doubles = [
        f64::NAN,
        10.0,
        20.0,
        20.0,
        0.0,
        0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];
    let first_big = 9_007_199_254_740_991i64;
    let mut writer = FgbWriter::create_with_options(
        "pruning-test",
        GeometryType::Point,
        FgbWriterOptions {
            write_index: true,
            ..FgbWriterOptions::default()
        },
    )?;
    for (name, kind, nullable) in columns {
        writer.add_column(name, kind, |_, args| args.nullable = nullable);
    }
    for row in 0..8 {
        let mut property_error = None;
        writer.add_feature_geom(Point(row as f64, row as f64), |feature| {
            let values = [
                Some(ColumnValue::String(names[row].as_str())),
                Some(ColumnValue::Long(heights[row])),
                Some(ColumnValue::String(payloads[row].as_str())),
                Some(ColumnValue::Float(singles[row])),
                if (4..6).contains(&row) {
                    None
                } else {
                    Some(ColumnValue::Double(doubles[row]))
                },
                Some(ColumnValue::Long(first_big + row as i64)),
            ];
            for (column, value) in values.into_iter().enumerate() {
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

struct Fixture {
    _dir: tempfile::TempDir,
    analytics: PathBuf,
    // Same indexed body without kind 2: attribute filters take an unpruned body scan.
    reference: PathBuf,
    body_len: u64,
}

fn fixture() -> TestResult<Fixture> {
    let dir = tempfile::tempdir()?;
    let indexed_body = body()?;
    let body_len = indexed_body.len() as u64;
    let (file, _) = build_with_options(
        &indexed_body,
        &BuildOptions {
            analytics: true,
            geometry_metrics: false,
            chunk_rows: 2,
        },
    )?;
    let analytics = dir.path().join("pruning.a.fgb");
    std::fs::write(&analytics, file)?;
    let (reference_file, _) = build_with_options(&indexed_body, &BuildOptions::default())?;
    let reference = dir.path().join("reference.a.fgb");
    std::fs::write(&reference, reference_file)?;
    Ok(Fixture {
        _dir: dir,
        analytics,
        reference,
        body_len,
    })
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

fn csv(path: &Path, sql: &str) -> TestResult<String> {
    let mut output = Vec::new();
    query_csv(path, sql, QueryOptions::default(), &mut output)?;
    Ok(String::from_utf8(output)?)
}

fn assert_parity(fixture: &Fixture, sql: &str) -> TestResult<String> {
    let actual = csv(&fixture.analytics, sql)?;
    let expected = csv(&fixture.reference, sql)?;
    let mut actual_lines = actual.lines();
    let mut expected_lines = expected.lines();
    assert_eq!(
        actual_lines.next(),
        expected_lines.next(),
        "schema differs for {sql}"
    );
    let mut actual_rows = actual_lines.collect::<Vec<_>>();
    let mut expected_rows = expected_lines.collect::<Vec<_>>();
    actual_rows.sort_unstable();
    expected_rows.sort_unstable();
    assert_eq!(
        actual_rows, expected_rows,
        "unpruned full-scan parity failed for {sql}"
    );
    Ok(actual)
}

fn delta(execution: &Execution) -> ScanMetrics {
    let before = execution.before;
    let after = execution.after;
    ScanMetrics {
        features_read: after.features_read - before.features_read,
        body_bytes_read: after.body_bytes_read - before.body_bytes_read,
        analytics_bytes_read: after.analytics_bytes_read - before.analytics_bytes_read,
        ranges_opened: after.ranges_opened - before.ranges_opened,
        wkb_values_decoded: after.wkb_values_decoded - before.wkb_values_decoded,
    }
}

fn names(batches: &[RecordBatch]) -> TestResult<Vec<String>> {
    let mut names = Vec::new();
    for batch in batches {
        let index = batch.schema().index_of("name")?;
        let values = batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("name is not Utf8")?;
        names.extend(
            values
                .iter()
                .map(|value| {
                    value
                        .map(str::to_owned)
                        .ok_or_else(|| "unexpected null name".into())
                })
                .collect::<TestResult<Vec<_>>>()?,
        );
    }
    names.sort();
    Ok(names)
}

fn scalar_i64(batches: &[RecordBatch]) -> TestResult<i64> {
    if batches.len() != 1 || batches[0].num_rows() != 1 {
        return Err("expected one scalar batch".into());
    }
    Ok(batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("scalar is not Int64")?
        .value(0))
}

fn explain(path: &Path, sql: &str) -> TestResult<String> {
    let execution = execute(path, &format!("EXPLAIN {sql}"))?;
    let mut output = String::new();
    for batch in execution.batches {
        for column in 0..batch.num_columns() {
            let values = batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("EXPLAIN output is not text")?;
            for row in 0..batch.num_rows() {
                output.push_str(values.value(row));
                output.push('\n');
            }
        }
    }
    Ok(output)
}

/// Print deterministic per-query counter deltas and trailer section sizes.
fn benchmark_report(fixture: &Fixture) -> TestResult<String> {
    let joint = format!("SELECT name FROM features WHERE {BBOX} AND height > 100");
    let rtree = format!("SELECT name FROM features WHERE {BBOX}");
    let cases = [
        ("full", "SELECT name, height FROM features", 8),
        ("R-tree", rtree.as_str(), 3),
        (
            "stats",
            "SELECT name, height FROM features WHERE height > 100",
            4,
        ),
        ("joint", joint.as_str(), 1),
    ];
    let mut report = String::from(
        "scan rows delta_features_read delta_body_bytes_read delta_analytics_bytes_read delta_ranges_opened\n",
    );
    for (label, sql, expected_rows) in cases {
        assert_parity(fixture, sql)?;
        let execution = execute(&fixture.analytics, sql)?;
        let rows = execution
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>();
        assert_eq!(rows, expected_rows, "wrong result count for {label}");
        let used = delta(&execution);
        report.push_str(&format!(
            "{label} {rows} {} {} {} {}\n",
            used.features_read, used.body_bytes_read, used.analytics_bytes_read, used.ranges_opened
        ));
    }

    let reader = Reader::open_path(&fixture.analytics)?;
    for section in &reader.directory().sections {
        match section.kind {
            fgba::format::KIND_AGGREGATE_BAND => report.push_str(&format!(
                "band res={} section_bytes={}\n",
                section.meta.first().copied().unwrap_or_default(),
                section.size
            )),
            fgba::format::KIND_BODY_ANALYTICS => {
                report.push_str(&format!("stats section_bytes={}\n", section.size));
            }
            _ => {}
        }
    }
    print!("{report}");
    Ok(report)
}

#[test]
fn numeric_pruning_is_selective_and_full_scan_identical() -> TestResult {
    let fixture = fixture()?;
    for sql in [
        "SELECT name, height FROM features WHERE height > 100",
        "SELECT name, height FROM features WHERE height <= 100",
    ] {
        assert_parity(&fixture, sql)?;
    }
    let full = execute(&fixture.analytics, "SELECT name, height FROM features")?;
    let selective = execute(
        &fixture.analytics,
        "SELECT name, height FROM features WHERE height > 100",
    )?;
    assert_eq!(
        names(&selective.batches)?,
        ["city2", "city3", "city6", "city7"]
    );
    let full_used = delta(&full);
    let used = delta(&selective);
    assert_eq!(full_used.features_read, 8);
    assert_eq!(used.features_read, 4);
    assert!(used.total_bytes_read() < full_used.total_bytes_read());
    assert_eq!(used.body_bytes_read, 0);
    Ok(())
}

#[test]
fn null_nan_and_nonfinite_bounds_stay_conservative() -> TestResult {
    let fixture = fixture()?;
    for sql in [
        "SELECT name, double FROM features WHERE double IS NULL",
        "SELECT name, double FROM features WHERE double IS NOT NULL",
        "SELECT name, double FROM features WHERE double > 100",
        "SELECT name, double FROM features WHERE double = 200",
    ] {
        assert_parity(&fixture, sql)?;
    }
    let nulls = execute(
        &fixture.analytics,
        "SELECT name FROM features WHERE double IS NULL",
    )?;
    assert_eq!(names(&nulls.batches)?, ["city4", "city5"]);
    assert_eq!(delta(&nulls).features_read, 2);

    let not_null = execute(
        &fixture.analytics,
        "SELECT name FROM features WHERE double IS NOT NULL",
    )?;
    assert_eq!(delta(&not_null).features_read, 6);
    assert!(!names(&not_null.batches)?.contains(&"city4".to_owned()));

    // The NaN-containing first chunk must remain a candidate for an inequality.
    let inequality = execute(
        &fixture.analytics,
        "SELECT name FROM features WHERE double > 100",
    )?;
    assert_eq!(names(&inequality.batches)?, ["city0", "city6"]);
    assert_eq!(delta(&inequality).features_read, 4);

    // Finite equality outside [10, 10] is a specific proof, despite the NaN.
    let equality = execute(
        &fixture.analytics,
        "SELECT name FROM features WHERE double = 200",
    )?;
    assert_eq!(delta(&equality).features_read, 2);
    assert!(names(&equality.batches)?.is_empty());

    let count = execute(
        &fixture.analytics,
        "SELECT COUNT(*) FROM features WHERE double IS NULL",
    )?;
    assert_eq!(scalar_i64(&count.batches)?, 2);
    assert_eq!(delta(&count).features_read, 2);
    let all = execute(&fixture.analytics, "SELECT COUNT(*) FROM features")?;
    assert_eq!(scalar_i64(&all.batches)?, 8);
    assert_eq!(delta(&all), ScanMetrics::default());
    Ok(())
}

#[test]
fn large_integers_and_f32_boundaries_do_not_round_pruning_literals() -> TestResult {
    let fixture = fixture()?;
    for sql in [
        "SELECT name, big FROM features WHERE big = 9007199254740993",
        "SELECT name, big FROM features WHERE big > 9007199254740994",
        "SELECT name, single FROM features WHERE single = 1.00000001",
    ] {
        assert_parity(&fixture, sql)?;
    }
    let exact = execute(
        &fixture.analytics,
        "SELECT name FROM features WHERE big = 9007199254740993",
    )?;
    assert_eq!(names(&exact.batches)?, ["city2"]);
    assert_eq!(delta(&exact).features_read, 2);

    let above = execute(
        &fixture.analytics,
        "SELECT name FROM features WHERE big > 9007199254740994",
    )?;
    assert_eq!(names(&above.batches)?, ["city4", "city5", "city6", "city7"]);
    assert_eq!(delta(&above).features_read, 4);

    // Whether DataFusion compares after widening or f32 rounding, the uncertain
    // f32 boundary literal cannot eliminate any chunk.
    let boundary = execute(
        &fixture.analytics,
        "SELECT name FROM features WHERE single = 1.00000001",
    )?;
    assert_eq!(delta(&boundary).features_read, 8);
    Ok(())
}

const BBOX: &str = "bbox_xmin <= 2 AND bbox_xmax >= 0 AND bbox_ymin <= 2 AND bbox_ymax >= 0";

#[test]
fn bbox_and_joint_filters_preserve_exactness_and_residual_predicates() -> TestResult {
    let fixture = fixture()?;
    let bbox_only = format!("SELECT name FROM features WHERE {BBOX}");
    let joint = format!("SELECT name FROM features WHERE {BBOX} AND height > 100");
    let joint_geometry =
        format!("SELECT name, geom_wkb FROM features WHERE {BBOX} AND height > 100");
    for sql in [&bbox_only, &joint, &joint_geometry] {
        assert_parity(&fixture, sql)?;
    }
    assert_eq!(
        names(&execute(&fixture.analytics, &bbox_only)?.batches)?,
        ["city0", "city1", "city2"]
    );
    let joint_execution = execute(&fixture.analytics, &joint)?;
    assert_eq!(names(&joint_execution.batches)?, ["city2"]);
    let joint_used = delta(&joint_execution);
    assert!(
        joint_used.analytics_bytes_read > 0,
        "joint attribute query must use Arrow chunks"
    );
    assert_eq!(joint_used.wkb_values_decoded, 0);
    assert_eq!(joint_used.features_read, 2);
    let analytics = Reader::open_path(&fixture.analytics)?
        .read_body_analytics_metadata()?
        .ok_or("missing analytics")?;
    let mut expected_bytes = 0;
    for column in [0, 1] {
        let (start, end) = analytics.arrow_range(column, 1)?;
        expected_bytes += end - start;
    }
    assert_eq!(joint_used.analytics_bytes_read, expected_bytes);
    assert_eq!(joint_used.ranges_opened, 3);
    assert_eq!(
        names(&execute(&fixture.analytics, &joint_geometry)?.batches)?,
        ["city2"]
    );

    let bbox_plan = explain(&fixture.analytics, &bbox_only)?;
    assert!(bbox_plan.contains("full_filters"), "{bbox_plan}");
    assert!(!bbox_plan.contains("FilterExec"), "{bbox_plan}");
    let joint_plan = explain(&fixture.analytics, &joint)?;
    assert!(joint_plan.contains("FilterExec"), "{joint_plan}");
    assert!(joint_plan.contains("height"), "{joint_plan}");

    let count = execute(
        &fixture.analytics,
        &format!("SELECT COUNT(*) FROM features WHERE {BBOX}"),
    )?;
    assert_eq!(scalar_i64(&count.batches)?, 3);
    let count_used = delta(&count);
    assert_eq!(count_used.features_read, 0);
    assert_eq!(count_used.analytics_bytes_read, 0);
    assert!(count_used.body_bytes_read > 0);

    let ordinary = execute(
        &fixture.analytics,
        &format!("SELECT COUNT(*) FROM features WHERE {BBOX} AND name = 'city1'"),
    )?;
    assert_eq!(scalar_i64(&ordinary.batches)?, 1);
    assert!(delta(&ordinary).features_read > 0);

    let unsupported = execute(
        &fixture.analytics,
        "SELECT COUNT(*) FROM features WHERE name = 'city1'",
    )?;
    assert_eq!(scalar_i64(&unsupported.batches)?, 1);
    assert!(delta(&unsupported).features_read > 0);
    Ok(())
}

#[test]
fn inexact_filter_limit_is_applied_after_residual_filtering() -> TestResult {
    let fixture = fixture()?;
    let sql = "SELECT name, height FROM features WHERE height < 450 ORDER BY name DESC LIMIT 1";
    assert_parity(&fixture, sql)?;
    let execution = execute_with(
        &fixture.analytics,
        sql,
        QueryOptions {
            batch_rows: 1,
            ..QueryOptions::default()
        },
    )?;
    assert_eq!(names(&execution.batches)?, ["city6"]);
    assert!(delta(&execution).features_read >= 2);
    Ok(())
}

#[test]
fn body_ranges_cover_first_middle_last_and_merged_gaps_without_trailer_reads() -> TestResult {
    let fixture = fixture()?;
    let analytics = Reader::open_path(&fixture.analytics)?
        .read_body_analytics_metadata()?
        .ok_or("missing analytics")?;
    assert_eq!(analytics.chunk_count, 4);
    assert_eq!(analytics.chunk_offsets.last(), Some(&fixture.body_len));
    let last = analytics.body_range(3)?;
    assert_eq!(last.end, fixture.body_len);
    assert_eq!(last.end_ordinal - last.first_ordinal, 2);

    for (literal, expected) in [(0, "city0"), (20, "city4"), (500, "city7")] {
        let sql = format!("SELECT name, geom_wkb FROM features WHERE height = {literal}");
        assert_parity(&fixture, &sql)?;
        let execution = execute(&fixture.analytics, &sql)?;
        assert_eq!(names(&execution.batches)?, [expected]);
        let used = delta(&execution);
        assert_eq!(used.features_read, 2);
        assert_eq!(used.ranges_opened, 1);
        assert!(used.body_bytes_read > 0);
    }

    // Decode the final range directly and stop at its row count. Another next()
    // would consume trailer bytes rather than a FlatGeobuf feature.
    let file = std::fs::read(&fixture.analytics)?;
    let body = &file[..fixture.body_len as usize];
    let shared = SharedReader::new_at(Cursor::new(body), last.start);
    let mut features = FgbReader::open(shared.handle())?.select_all()?;
    for _ in 0..(last.end_ordinal - last.first_ordinal) {
        assert!(features.next()?.is_some());
    }
    assert_eq!(shared.position(), fixture.body_len);

    let merged_sql = "SELECT name, geom_wkb FROM features WHERE height <= 100";
    assert_parity(&fixture, merged_sql)?;
    // Exercise gap merging itself rather than independent per-partition ranges.
    let merged = execute_with(
        &fixture.analytics,
        merged_sql,
        QueryOptions {
            scan_threads: Some(1),
            ..QueryOptions::default()
        },
    )?;
    assert_eq!(
        names(&merged.batches)?,
        ["city0", "city1", "city4", "city5"]
    );
    let used = delta(&merged);
    assert_eq!(used.ranges_opened, 1);
    assert_eq!(
        used.features_read, 6,
        "the skipped middle chunk must be decoded inside the merged range"
    );
    Ok(())
}

#[test]
fn attribute_pruning_reads_only_required_arrow_chunks_and_bad_analytics_uses_body() -> TestResult {
    let fixture = fixture()?;
    let sql = "SELECT name FROM features WHERE height > 100";
    let execution = execute(&fixture.analytics, sql)?;
    let used = delta(&execution);
    assert_eq!(used.body_bytes_read, 0);
    assert_eq!(used.features_read, 4);
    let analytics = Reader::open_path(&fixture.analytics)?
        .read_body_analytics_metadata()?
        .ok_or("missing analytics")?;
    let mut expected_bytes = 0;
    for chunk in [0, 2] {
        for column in [0, 1] {
            let (start, end) = analytics.arrow_range(column, chunk)?;
            expected_bytes += end - start;
        }
    }
    assert_eq!(used.analytics_bytes_read, expected_bytes);
    assert_eq!(used.ranges_opened, 4);

    let missing = execute(&fixture.reference, sql)?;
    assert_eq!(
        names(&missing.batches)?,
        ["city2", "city3", "city6", "city7"]
    );
    assert!(delta(&missing).body_bytes_read > 0);

    let mut bytes = std::fs::read(&fixture.analytics)?;
    bytes[analytics.section_start as usize] = fgba::analytics::ANALYTICS_VERSION + 1;
    let malformed = fixture.analytics.with_file_name("malformed.a.fgb");
    std::fs::write(&malformed, bytes)?;
    assert_eq!(csv(&malformed, sql)?, csv(&fixture.reference, sql)?);
    let fallback = execute(&malformed, sql)?;
    assert!(delta(&fallback).body_bytes_read > 0);
    assert_eq!(delta(&fallback).analytics_bytes_read, 0);
    Ok(())
}

#[test]
fn pruning_benchmark_report_has_correct_counts_and_counter_deltas() -> TestResult {
    let fixture = fixture()?;
    let report = benchmark_report(&fixture)?;
    for expected in [
        "full 8 ",
        "R-tree 3 ",
        "stats 4 ",
        "joint 1 ",
        "delta_features_read",
        "delta_body_bytes_read",
        "delta_analytics_bytes_read",
        "delta_ranges_opened",
        "band res=",
        "stats section_bytes=",
    ] {
        assert!(report.contains(expected), "missing {expected:?}:\n{report}");
    }
    Ok(())
}
