//! Deterministic multi-partition scans over independent analytical chunks.

use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SQLOptions;
use fgba::{build_with_options, BuildOptions, Reader};
use fgba_sql::{session, QueryOptions, ScanMetrics};
use flatgeobuf::geozero::{ColumnValue, GeomProcessor, GeozeroGeometry, PropertyProcessor};
use flatgeobuf::{ColumnType, FgbWriter, FgbWriterOptions, GeometryType};
use futures::StreamExt;
use std::path::PathBuf;
use std::sync::Arc;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const ROW_COUNT: usize = 24;
const CHUNK_ROWS: u32 = 3;
const CHUNK_COUNT: u64 = 8;
const SURVIVING_CHUNKS: [u64; 4] = [1, 3, 5, 7];

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

struct Fixture {
    _dir: tempfile::TempDir,
    analytics: PathBuf,
    plain: PathBuf,
}

fn fixture() -> TestResult<Fixture> {
    let dir = tempfile::tempdir()?;
    let names = (0..ROW_COUNT)
        .map(|row| format!("feature-{row:02}"))
        .collect::<Vec<_>>();
    // Keep skipped body ranges above the reader's small-range merge threshold.
    let payloads = (0..ROW_COUNT)
        .map(|row| format!("{row:02}-{}", "x".repeat(70 * 1024)))
        .collect::<Vec<_>>();
    let columns = [
        ("id", ColumnType::Long, false),
        ("name", ColumnType::String, false),
        ("keep", ColumnType::Long, false),
        ("payload", ColumnType::String, false),
    ];
    let mut writer = FgbWriter::create_with_options(
        "parallel-scan-test",
        GeometryType::Point,
        FgbWriterOptions {
            write_index: true,
            ..FgbWriterOptions::default()
        },
    )?;
    for (name, kind, nullable) in columns {
        writer.add_column(name, kind, |_, args| args.nullable = nullable);
    }
    for row in 0..ROW_COUNT {
        let mut property_error = None;
        writer.add_feature_geom(Point(row as f64, row as f64), |feature| {
            let keep = if (row / CHUNK_ROWS as usize).is_multiple_of(2) {
                0
            } else {
                100
            };
            let values = [
                Some(ColumnValue::Long(row as i64)),
                Some(ColumnValue::String(names[row].as_str())),
                Some(ColumnValue::Long(keep)),
                Some(ColumnValue::String(payloads[row].as_str())),
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
    let mut body = Vec::new();
    writer.write(&mut body)?;
    let plain = dir.path().join("plain.fgb");
    std::fs::write(&plain, &body)?;
    let (file, _) = build_with_options(
        &body,
        &BuildOptions {
            analytics: true,
            geometry_metrics: false,
            chunk_rows: CHUNK_ROWS,
        },
    )?;
    let analytics = dir.path().join("parallel.a.fgb");
    std::fs::write(&analytics, file)?;
    Ok(Fixture {
        _dir: dir,
        analytics,
        plain,
    })
}

struct Execution {
    batches: Vec<RecordBatch>,
    source_partitions: Vec<usize>,
    before: ScanMetrics,
    after: ScanMetrics,
}

fn read_only() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

fn source_partitions(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
    fn visit(plan: &Arc<dyn ExecutionPlan>, output: &mut Vec<usize>) {
        if plan.name() == "StreamingTableExec" {
            output.push(plan.output_partitioning().partition_count());
        }
        for child in plan.children() {
            visit(child, output);
        }
    }

    let mut output = Vec::new();
    visit(plan, &mut output);
    output
}

fn execute_with_threads(
    path: &std::path::Path,
    sql: &str,
    threads: usize,
) -> TestResult<Execution> {
    let session = session::register(
        path,
        &QueryOptions {
            scan_threads: Some(threads),
            ..QueryOptions::default()
        },
    )?;
    let before = session.features.metrics();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (batches, source_partitions) = runtime.block_on(async {
        let df = session.ctx.sql_with_options(sql, read_only()).await?;
        let plan = df.clone().create_physical_plan().await?;
        let source_partitions = source_partitions(&plan);
        let mut stream = session.execute_dataframe(df).await?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        Ok::<_, fgba_sql::Error>((batches, source_partitions))
    })?;
    let after = session.features.metrics();
    Ok(Execution {
        batches,
        source_partitions,
        before,
        after,
    })
}

fn sorted_rows(batches: &[RecordBatch]) -> TestResult<Vec<String>> {
    let mut bytes = Vec::new();
    {
        let mut csv = arrow_csv::WriterBuilder::new()
            .with_header(false)
            .build(&mut bytes);
        for batch in batches {
            csv.write(batch)?;
        }
    }
    let text = String::from_utf8(bytes)?;
    let mut rows = text.lines().map(str::to_owned).collect::<Vec<_>>();
    rows.sort_unstable();
    Ok(rows)
}

fn ids(batches: &[RecordBatch]) -> TestResult<Vec<i64>> {
    let mut output = Vec::new();
    for batch in batches {
        let index = batch.schema().index_of("id")?;
        let values = batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id is not Int64")?;
        for row in 0..batch.num_rows() {
            output.push(values.value(row));
        }
    }
    output.sort_unstable();
    Ok(output)
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
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

fn expected_pruned_ids() -> Vec<i64> {
    (0..ROW_COUNT as i64)
        .filter(|row| !(*row as usize / CHUNK_ROWS as usize).is_multiple_of(2))
        .collect()
}

#[test]
fn columns_and_body_scans_match_single_partition_results() -> TestResult {
    let fixture = fixture()?;
    let analytics = Reader::open_path(&fixture.analytics)?
        .read_body_analytics_metadata()?
        .ok_or("missing analytics")?;
    assert_eq!(analytics.chunk_count, CHUNK_COUNT);

    for (label, sql, body_source) in [
        ("columns", "SELECT id, name FROM features", false),
        ("body", "SELECT id, name, bbox_xmin FROM features", true),
    ] {
        let single = execute_with_threads(&fixture.analytics, sql, 1)?;
        let multi = execute_with_threads(&fixture.analytics, sql, 4)?;
        assert_eq!(single.source_partitions, [1], "{label}");
        assert_eq!(multi.source_partitions, [4], "{label}");
        assert_eq!(row_count(&single.batches), ROW_COUNT, "{label}");
        assert_eq!(row_count(&multi.batches), ROW_COUNT, "{label}");
        assert_eq!(
            sorted_rows(&multi.batches)?,
            sorted_rows(&single.batches)?,
            "{label} rows differ after sorting"
        );
        assert_eq!(delta(&single).features_read, ROW_COUNT as u64);
        assert_eq!(delta(&multi).features_read, ROW_COUNT as u64);
        if body_source {
            assert!(delta(&single).body_bytes_read > 0);
            assert!(delta(&multi).body_bytes_read > 0);
            assert_eq!(delta(&single).ranges_opened, 1);
            assert_eq!(delta(&multi).ranges_opened, 4);
        } else {
            assert_eq!(delta(&single).body_bytes_read, 0);
            assert_eq!(delta(&multi).body_bytes_read, 0);
            assert!(delta(&multi).analytics_bytes_read > 0);
        }
    }
    Ok(())
}

#[test]
fn non_contiguous_pruning_reads_only_surviving_chunks() -> TestResult {
    let fixture = fixture()?;
    for (label, sql, body_source) in [
        (
            "columns",
            "SELECT id, name FROM features WHERE keep = 100",
            false,
        ),
        (
            "body",
            "SELECT id, bbox_xmin FROM features WHERE keep = 100",
            true,
        ),
    ] {
        let execution = execute_with_threads(&fixture.analytics, sql, 2)?;
        assert_eq!(execution.source_partitions, [2], "{label}");
        assert_eq!(ids(&execution.batches)?, expected_pruned_ids(), "{label}");
        let used = delta(&execution);
        assert_eq!(used.features_read, 12, "{label}");
        if body_source {
            assert!(used.body_bytes_read > 0);
            assert_eq!(used.analytics_bytes_read, 0);
            assert_eq!(used.ranges_opened, 4);
        } else {
            assert_eq!(used.body_bytes_read, 0);
            let analytics = Reader::open_path(&fixture.analytics)?
                .read_body_analytics_metadata()?
                .ok_or("missing analytics")?;
            let mut expected_bytes = 0;
            for chunk in SURVIVING_CHUNKS {
                for column in [0, 1, 2] {
                    let (start, end) = analytics.arrow_range(column, chunk)?;
                    expected_bytes += end - start;
                }
            }
            assert_eq!(used.analytics_bytes_read, expected_bytes);
            assert_eq!(used.ranges_opened, 12);
        }
    }
    Ok(())
}

#[test]
fn joint_bbox_and_zone_pruning_shares_one_rtree_search() -> TestResult {
    let fixture = fixture()?;
    let predicate = "bbox_xmin <= 100 AND bbox_xmax >= -1 AND bbox_ymin <= 100 AND bbox_ymax >= -1 AND keep = 100";
    for (label, sql, body_source) in [
        (
            "columns",
            format!("SELECT id, name FROM features WHERE {predicate}"),
            false,
        ),
        (
            "body",
            format!("SELECT id, bbox_xmin FROM features WHERE {predicate}"),
            true,
        ),
    ] {
        let execution = execute_with_threads(&fixture.analytics, &sql, 8)?;
        assert_eq!(execution.source_partitions, [4], "{label}");
        assert_eq!(ids(&execution.batches)?, expected_pruned_ids(), "{label}");
        let used = delta(&execution);
        assert_eq!(used.features_read, 12, "{label}");
        assert!(used.body_bytes_read > 0, "{label} R-tree bytes");
        if body_source {
            assert_eq!(used.analytics_bytes_read, 0);
            assert_eq!(used.ranges_opened, 5, "{label}");
        } else {
            assert!(used.analytics_bytes_read > 0);
            assert_eq!(used.ranges_opened, 13, "{label}");
        }
    }
    Ok(())
}

#[test]
fn empty_pruning_emits_no_rows_and_opens_no_sources() -> TestResult {
    let fixture = fixture()?;
    for sql in [
        "SELECT id, name FROM features WHERE keep = 999",
        "SELECT id, bbox_xmin FROM features WHERE keep = 999",
    ] {
        let execution = execute_with_threads(&fixture.analytics, sql, 8)?;
        assert_eq!(execution.source_partitions, [1], "{sql}");
        assert_eq!(row_count(&execution.batches), 0, "{sql}");
        assert_eq!(delta(&execution), ScanMetrics::default(), "{sql}");
    }

    let count = execute_with_threads(
        &fixture.analytics,
        "SELECT COUNT(*) FROM features WHERE keep = 999",
        8,
    )?;
    assert_eq!(count.source_partitions, [1]);
    assert_eq!(row_count(&count.batches), 1);
    assert_eq!(delta(&count), ScanMetrics::default());
    let batch = count.batches.first().ok_or("missing count batch")?;
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("count is not Int64")?;
    assert_eq!(values.value(0), 0);
    Ok(())
}

#[test]
fn plain_and_bbox_selections_remain_single_partition() -> TestResult {
    let fixture = fixture()?;
    let plain = execute_with_threads(&fixture.plain, "SELECT id, name FROM features", 8)?;
    assert_eq!(plain.source_partitions, [1]);
    assert_eq!(row_count(&plain.batches), ROW_COUNT);
    assert!(delta(&plain).body_bytes_read > 0);

    let predicate = "bbox_xmin <= 8 AND bbox_xmax >= 5 AND bbox_ymin <= 8 AND bbox_ymax >= 5";
    for sql in [
        format!("SELECT id, name FROM features WHERE {predicate}"),
        format!("SELECT id, bbox_xmin FROM features WHERE {predicate}"),
    ] {
        let execution = execute_with_threads(&fixture.analytics, &sql, 8)?;
        assert_eq!(execution.source_partitions, [1], "{sql}");
        assert_eq!(ids(&execution.batches)?, [5, 6, 7, 8], "{sql}");
    }
    Ok(())
}

#[test]
fn corrupt_property_arrow_chunk_falls_back_atomically_across_partitions() -> TestResult {
    let fixture = fixture()?;
    let analytics = Reader::open_path(&fixture.analytics)?
        .read_body_analytics_metadata()?
        .ok_or("missing analytics")?;
    assert!(analytics.chunk_count > 2);
    let (_, end) = analytics.arrow_range(1, 2)?;
    let mut bytes = std::fs::read(&fixture.analytics)?;
    let last = bytes
        .get_mut(end as usize - 1)
        .ok_or("property Arrow range is outside the file")?;
    assert_eq!(*last, b'1');
    *last ^= 0xff;
    let corrupted = fixture.analytics.with_file_name("corrupt-property.a.fgb");
    std::fs::write(&corrupted, bytes)?;

    let sql = "SELECT id, name FROM features";
    let clean = execute_with_threads(&fixture.analytics, sql, 4)?;
    let fallback = execute_with_threads(&corrupted, sql, 4)?;
    assert_eq!(fallback.source_partitions, [4]);
    assert_eq!(row_count(&fallback.batches), ROW_COUNT);
    // The second selected array is corrupt. Any partial bad-chunk emission or
    // whole-query retry would duplicate rows in this exact comparison.
    assert_eq!(
        sorted_rows(&fallback.batches)?,
        sorted_rows(&clean.batches)?
    );
    let clean_used = delta(&clean);
    let used = delta(&fallback);
    assert_eq!(clean_used.body_bytes_read, 0);
    assert!(used.body_bytes_read > 0);
    assert_eq!(used.features_read, ROW_COUNT as u64);
    assert_eq!(used.analytics_bytes_read, clean_used.analytics_bytes_read);
    assert_eq!(used.ranges_opened, clean_used.ranges_opened + 1);
    assert_eq!(used.wkb_values_decoded, 0);

    // Joint R-tree and zone pruning must preserve the candidate set when one
    // surviving partition falls back and the others continue using Arrow.
    let predicate = "bbox_xmin <= 100 AND bbox_xmax >= -1 AND bbox_ymin <= 100 AND bbox_ymax >= -1 AND keep = 100";
    let sql = format!("SELECT id, name FROM features WHERE {predicate}");
    let clean_candidate = execute_with_threads(&fixture.analytics, &sql, 4)?;
    let candidate = execute_with_threads(&corrupted, &sql, 4)?;
    assert_eq!(ids(&candidate.batches)?, expected_pruned_ids());
    assert_eq!(
        sorted_rows(&candidate.batches)?,
        sorted_rows(&clean_candidate.batches)?
    );
    let clean_used = delta(&clean_candidate);
    let used = delta(&candidate);
    assert!(
        clean_used.body_bytes_read > 0,
        "R-tree search must read body"
    );
    assert!(
        used.body_bytes_read > clean_used.body_bytes_read,
        "fallback must add its exact body-range reads: clean={clean_used:?}, fallback={used:?}"
    );
    assert!(used.analytics_bytes_read > 0);
    assert_eq!(used.features_read, clean_used.features_read);
    Ok(())
}
