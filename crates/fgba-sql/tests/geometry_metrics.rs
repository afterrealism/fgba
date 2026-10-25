//! SQL virtual columns, zone maps, and fallbacks over GEOMETRY_METRICS.

use datafusion::arrow::array::{
    Array, BinaryArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt32Array,
};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SQLOptions;
use fgba::format::{crc32, Directory, Footer, KIND_GEOMETRY_METRICS};
use fgba::geometry::BboxSink;
use fgba::{build_with_options, BuildOptions, Reader};
use fgba_sql::{session, QueryOptions, ScanMetrics};
use flatgeobuf::geozero::{
    ColumnValue, FeatureProperties, GeomProcessor, GeozeroGeometry, PropertyProcessor,
};
use flatgeobuf::{
    ColumnType, FallibleStreamingIterator, FgbReader, FgbWriter, FgbWriterOptions, GeometryType,
};
use futures::StreamExt;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const ROW_COUNT: usize = 24;
const CHUNK_ROWS: u32 = 3;
const CHUNK_COUNT: usize = 8;

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
    width: f64,
    height: f64,
}

impl GeozeroGeometry for Rectangle {
    fn process_geom<P: GeomProcessor>(
        &self,
        processor: &mut P,
    ) -> flatgeobuf::geozero::error::Result<()> {
        let points = [
            (self.x, self.y),
            (self.x + self.width, self.y),
            (self.x + self.width, self.y + self.height),
            (self.x, self.y + self.height),
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

#[derive(Clone, Debug)]
struct ExpectedRow {
    id: i64,
    length: f64,
    npoints: u32,
    bbox: [f64; 4],
}

struct Fixture {
    _dir: tempfile::TempDir,
    plain: PathBuf,
    metrics: PathBuf,
    kind_two: PathBuf,
    rows: Vec<ExpectedRow>,
}

fn body(write_index: bool) -> TestResult<Vec<u8>> {
    let mut writer = FgbWriter::create_with_options(
        "sql-geometry-metrics",
        GeometryType::LineString,
        FgbWriterOptions {
            write_index,
            ..FgbWriterOptions::default()
        },
    )?;
    writer.add_column("id", ColumnType::Long, |_, args| args.nullable = false);
    writer.add_column("name", ColumnType::String, |_, args| args.nullable = false);
    for row in 0..ROW_COUNT {
        let chunk = row / CHUNK_ROWS as usize;
        let vertices = if matches!(row, 4 | 22) {
            101 + chunk
        } else {
            2 + row % 4
        };
        let base_x = if chunk % 2 == 1 {
            -100.0
        } else if matches!(chunk, 0 | 4) {
            100.0
        } else {
            50.0
        };
        let start_x = base_x + (row % CHUNK_ROWS as usize) as f64 * 10.0;
        let y = row as f64;
        let step = 0.1 + row as f64 * 0.01;
        let points = (0..vertices)
            .map(|index| (start_x + index as f64 * step, y))
            .collect::<Vec<_>>();
        let name = format!("feature-{row:02}");
        let mut property_error = None;
        writer.add_feature_geom(Line(points), |feature| {
            for (column, name, value) in [
                (0, "id", ColumnValue::Long(row as i64)),
                (1, "name", ColumnValue::String(name.as_str())),
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

#[derive(Default)]
struct BodyMetricDecoder {
    previous: Option<(f64, f64)>,
    length: f64,
    npoints: u32,
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
}

impl BodyMetricDecoder {
    fn new() -> Self {
        Self {
            min_x: f64::INFINITY,
            min_y: f64::INFINITY,
            max_x: f64::NEG_INFINITY,
            max_y: f64::NEG_INFINITY,
            ..Self::default()
        }
    }
}

impl GeomProcessor for BodyMetricDecoder {
    fn linestring_begin(
        &mut self,
        _tagged: bool,
        _size: usize,
        _idx: usize,
    ) -> flatgeobuf::geozero::error::Result<()> {
        self.previous = None;
        Ok(())
    }

    fn xy(&mut self, x: f64, y: f64, _idx: usize) -> flatgeobuf::geozero::error::Result<()> {
        if let Some((previous_x, previous_y)) = self.previous {
            self.length += (x - previous_x).hypot(y - previous_y);
        }
        self.previous = Some((x, y));
        self.npoints = self.npoints.checked_add(1).ok_or_else(|| {
            flatgeobuf::geozero::error::GeozeroError::Geometry(
                "test feature has too many vertices".into(),
            )
        })?;
        self.min_x = self.min_x.min(x);
        self.min_y = self.min_y.min(y);
        self.max_x = self.max_x.max(x);
        self.max_y = self.max_y.max(y);
        Ok(())
    }
}

fn decode_body(body: &[u8]) -> TestResult<Vec<ExpectedRow>> {
    let mut features = FgbReader::open(Cursor::new(body))?.select_all()?;
    let mut rows = Vec::new();
    while let Some(feature) = features.next()? {
        let id = feature.property::<i64>("id")?;
        let mut decoder = BodyMetricDecoder::new();
        feature.process_geom(&mut decoder)?;
        rows.push(ExpectedRow {
            id,
            length: decoder.length,
            npoints: decoder.npoints,
            bbox: [decoder.min_x, decoder.min_y, decoder.max_x, decoder.max_y],
        });
    }
    Ok(rows)
}

fn build_file(dir: &Path, body: &[u8], name: &str, geometry_metrics: bool) -> TestResult<PathBuf> {
    let (file, _) = build_with_options(
        body,
        &BuildOptions {
            analytics: true,
            geometry_metrics,
            chunk_rows: CHUNK_ROWS,
        },
    )?;
    let path = dir.join(name);
    std::fs::write(&path, file)?;
    Ok(path)
}

fn fixture() -> TestResult<Fixture> {
    let dir = tempfile::tempdir()?;
    let body = body(true)?;
    let rows = decode_body(&body)?;
    assert_eq!(rows.len(), ROW_COUNT);
    let metrics = build_file(dir.path(), &body, "metrics.a.fgb", true)?;
    let kind_two = build_file(dir.path(), &body, "kind-two.a.fgb", false)?;
    let plain = dir.path().join("plain.fgb");
    std::fs::write(&plain, &body)?;
    Ok(Fixture {
        _dir: dir,
        plain,
        metrics,
        kind_two,
        rows,
    })
}

fn polygon_body() -> TestResult<Vec<u8>> {
    let mut writer = FgbWriter::create("sql-geometry-areas", GeometryType::Polygon)?;
    writer.add_column("id", ColumnType::Long, |_, args| args.nullable = false);
    for row in 0..4usize {
        let rectangle = Rectangle {
            x: row as f64 * 2.0,
            y: row as f64 * 3.0,
            width: 1.0 + row as f64,
            height: 2.0 + row as f64,
        };
        let mut property_error = None;
        writer.add_feature_geom(rectangle, |feature| {
            if let Err(error) = feature.property(0, "id", &ColumnValue::Long(row as i64)) {
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

fn decode_polygon_areas(body: &[u8]) -> TestResult<Vec<(i64, f64)>> {
    let mut features = FgbReader::open(Cursor::new(body))?.select_all()?;
    let mut rows = Vec::new();
    while let Some(feature) = features.next()? {
        let id = feature.property::<i64>("id")?;
        let mut sink = BboxSink::new();
        feature.process_geom(&mut sink)?;
        let bbox = sink.checked_bounds()?.ok_or("rectangle has no bbox")?;
        let area = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]);
        rows.push((id, area));
    }
    Ok(rows)
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

fn execute_with_threads(path: &Path, sql: &str, threads: usize) -> TestResult<Execution> {
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

fn execute(path: &Path, sql: &str) -> TestResult<Execution> {
    execute_with_threads(path, sql, 1)
}

fn plan_error(path: &Path, sql: &str) -> TestResult<String> {
    let session = session::register(path, &QueryOptions::default())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let planned = runtime.block_on(async {
        let df = session.ctx.sql_with_options(sql, read_only()).await?;
        df.create_physical_plan().await?;
        Ok::<_, fgba_sql::Error>(())
    });
    match planned {
        Ok(()) => Err(format!("query unexpectedly planned: {sql}").into()),
        Err(error) => Ok(error.to_string()),
    }
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

fn one_batch(batches: &[RecordBatch]) -> TestResult<&RecordBatch> {
    if batches.len() == 1 && batches[0].num_rows() == 1 {
        Ok(&batches[0])
    } else {
        Err("expected one scalar batch".into())
    }
}

fn combined_batch(batches: &[RecordBatch]) -> TestResult<RecordBatch> {
    let schema = batches
        .first()
        .ok_or("expected at least one result batch")?
        .schema();
    Ok(concat_batches(&schema, batches)?)
}

fn scalar_f64(batches: &[RecordBatch]) -> TestResult<f64> {
    Ok(one_batch(batches)?
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or("scalar is not Float64")?
        .value(0))
}

fn scalar_i64(batches: &[RecordBatch]) -> TestResult<i64> {
    Ok(one_batch(batches)?
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("scalar is not Int64")?
        .value(0))
}

fn ids(batches: &[RecordBatch]) -> TestResult<Vec<i64>> {
    let mut output = Vec::new();
    for batch in batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id is not Int64")?;
        output.extend((0..batch.num_rows()).map(|row| values.value(row)));
    }
    output.sort_unstable();
    Ok(output)
}

fn expected_sum(rows: &[ExpectedRow]) -> f64 {
    rows.iter().fold(0.0, |sum, row| sum + row.length)
}

fn chunks_matching(rows: &[ExpectedRow], predicate: impl Fn(&ExpectedRow) -> bool) -> Vec<u64> {
    (0..CHUNK_COUNT)
        .filter(|chunk| {
            let start = chunk * CHUNK_ROWS as usize;
            let end = (start + CHUNK_ROWS as usize).min(rows.len());
            rows[start..end].iter().any(&predicate)
        })
        .map(|chunk| chunk as u64)
        .collect()
}

fn geometry_chunk_bytes(path: &Path, metric_id: u16, chunks: &[u64]) -> TestResult<u64> {
    let metrics = Reader::open_path(path)?
        .read_geometry_metrics_metadata()?
        .ok_or("missing geometry metrics")?;
    let mut bytes = 0u64;
    for &chunk in chunks {
        let (start, end) = metrics.arrow_range(metric_id, chunk)?;
        bytes += end - start;
    }
    Ok(bytes)
}

#[test]
fn single_partition_sum_exactly_matches_independent_body_decode() -> TestResult {
    let fixture = fixture()?;
    let execution = execute(&fixture.metrics, "SELECT SUM(geom_length) FROM features")?;
    assert_eq!(execution.source_partitions, [1]);
    assert_eq!(scalar_f64(&execution.batches)?, expected_sum(&fixture.rows));
    let used = delta(&execution);
    assert_eq!(used.body_bytes_read, 0);
    assert_eq!(used.wkb_values_decoded, 0);
    assert!(used.analytics_bytes_read > 0);
    Ok(())
}

#[test]
fn npoints_filter_count_and_zone_pruning_match_body_decode() -> TestResult {
    let fixture = fixture()?;
    let expected_chunks = chunks_matching(&fixture.rows, |row| row.npoints > 100);
    assert!(expected_chunks.len() > 1);
    assert!(
        expected_chunks.windows(2).any(|pair| pair[1] - pair[0] > 1),
        "expected non-contiguous npoints chunks: {expected_chunks:?}"
    );
    let expected_count = fixture.rows.iter().filter(|row| row.npoints > 100).count() as i64;
    let execution = execute(
        &fixture.metrics,
        "SELECT COUNT(*) FROM features WHERE geom_npoints > 100",
    )?;
    assert_eq!(scalar_i64(&execution.batches)?, expected_count);
    let used = delta(&execution);
    assert_eq!(used.body_bytes_read, 0);
    assert_eq!(
        used.features_read,
        (expected_chunks.len() * CHUNK_ROWS as usize) as u64
    );
    assert_eq!(used.ranges_opened, expected_chunks.len() as u64);
    assert_eq!(
        used.analytics_bytes_read,
        geometry_chunk_bytes(&fixture.metrics, 2, &expected_chunks)?
    );
    Ok(())
}

#[test]
fn mixed_property_metric_and_stored_bbox_projection_never_reads_body() -> TestResult {
    let fixture = fixture()?;
    let execution = execute(
        &fixture.metrics,
        "SELECT id, name, geom_length, geom_area, geom_npoints, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax FROM features ORDER BY id",
    )?;
    let mut expected = fixture.rows.clone();
    expected.sort_by_key(|row| row.id);
    let mut position = 0usize;
    for batch in &execution.batches {
        assert_eq!(
            batch.schema().field_with_name("geom_length")?.data_type(),
            &DataType::Float64
        );
        assert_eq!(
            batch.schema().field_with_name("geom_area")?.data_type(),
            &DataType::Float64
        );
        assert_eq!(
            batch.schema().field_with_name("geom_npoints")?.data_type(),
            &DataType::UInt32
        );
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id is not Int64")?;
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("name is not Utf8")?;
        let lengths = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("length is not Float64")?;
        let areas = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("area is not Float64")?;
        let npoints = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or("npoints is not UInt32")?;
        let bbox = (5..9)
            .map(|column| batch.column(column).as_any().downcast_ref::<Float64Array>())
            .collect::<Option<Vec<_>>>()
            .ok_or("bbox is not Float64")?;
        for row in 0..batch.num_rows() {
            let expected = expected.get(position).ok_or("too many output rows")?;
            assert_eq!(ids.value(row), expected.id);
            assert_eq!(names.value(row), format!("feature-{:02}", expected.id));
            assert_eq!(lengths.value(row), expected.length);
            assert_eq!(areas.value(row), 0.0);
            assert_eq!(npoints.value(row), expected.npoints);
            for (coordinate, values) in bbox.iter().enumerate() {
                assert_eq!(values.value(row), expected.bbox[coordinate]);
            }
            position += 1;
        }
    }
    assert_eq!(position, ROW_COUNT);
    let used = delta(&execution);
    assert_eq!(used.body_bytes_read, 0);
    assert_eq!(used.wkb_values_decoded, 0);
    Ok(())
}

#[test]
fn area_metric_matches_independent_polygon_body_decode() -> TestResult {
    let dir = tempfile::tempdir()?;
    let body = polygon_body()?;
    let mut expected = decode_polygon_areas(&body)?;
    expected.sort_by_key(|row| row.0);
    let path = build_file(dir.path(), &body, "areas.a.fgb", true)?;
    let execution = execute(&path, "SELECT id, geom_area FROM features ORDER BY id")?;
    let mut position = 0usize;
    for batch in &execution.batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id is not Int64")?;
        let areas = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("area is not Float64")?;
        for row in 0..batch.num_rows() {
            let (expected_id, expected_area) = expected.get(position).ok_or("too many areas")?;
            assert_eq!(ids.value(row), *expected_id);
            assert!(areas.value(row) > 0.0);
            assert_eq!(areas.value(row), *expected_area);
            position += 1;
        }
    }
    assert_eq!(position, expected.len());
    assert_eq!(delta(&execution).body_bytes_read, 0);
    Ok(())
}

#[test]
fn bbox_zone_pruning_reads_only_non_contiguous_geometry_chunks() -> TestResult {
    let fixture = fixture()?;
    let expected_chunks = chunks_matching(&fixture.rows, |row| row.bbox[0] > 75.0);
    assert!(expected_chunks.len() > 1);
    assert!(
        expected_chunks.windows(2).any(|pair| pair[1] - pair[0] > 1),
        "expected non-contiguous bbox chunks: {expected_chunks:?}"
    );
    let execution = execute(
        &fixture.metrics,
        "SELECT bbox_xmin FROM features WHERE bbox_xmin > 75 ORDER BY bbox_xmin",
    )?;
    let mut expected = fixture
        .rows
        .iter()
        .filter(|row| row.bbox[0] > 75.0)
        .map(|row| row.bbox[0])
        .collect::<Vec<_>>();
    expected.sort_by(f64::total_cmp);
    let actual = execution
        .batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .into_iter()
                .flat_map(|values| values.iter().flatten())
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    let used = delta(&execution);
    assert_eq!(used.body_bytes_read, 0);
    assert_eq!(
        used.features_read,
        (expected_chunks.len() * CHUNK_ROWS as usize) as u64
    );
    assert_eq!(used.ranges_opened, expected_chunks.len() as u64);
    assert_eq!(
        used.analytics_bytes_read,
        geometry_chunk_bytes(&fixture.metrics, 3, &expected_chunks)?
    );
    Ok(())
}

#[test]
fn eight_partition_float_sum_is_within_relative_tolerance() -> TestResult {
    let fixture = fixture()?;
    let execution =
        execute_with_threads(&fixture.metrics, "SELECT SUM(geom_length) FROM features", 8)?;
    assert_eq!(execution.source_partitions, [8]);
    let expected = expected_sum(&fixture.rows);
    let actual = scalar_f64(&execution.batches)?;
    let relative = (actual - expected).abs() / expected.abs().max(1.0);
    assert!(
        relative <= 1e-9,
        "relative error {relative}: {actual} != {expected}"
    );
    assert_eq!(delta(&execution).body_bytes_read, 0);
    Ok(())
}

#[test]
fn missing_kind_three_errors_eagerly_and_preserves_body_fallbacks() -> TestResult {
    let fixture = fixture()?;
    for sql in [
        "SELECT geom_length FROM features",
        "SELECT SUM(geom_area) FROM features",
        "SELECT COUNT(*) FROM features WHERE geom_npoints > 100",
    ] {
        let error = plan_error(&fixture.kind_two, sql)?;
        assert!(error.contains("--geometry-metrics"), "{sql}: {error}");
    }

    let fallback = execute(
        &fixture.kind_two,
        "SELECT id, geom_wkb, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax FROM features WHERE id = 0",
    )?;
    assert_eq!(one_batch(&fallback.batches)?.num_rows(), 1);
    assert!(delta(&fallback).body_bytes_read > 0);
    assert!(delta(&fallback).wkb_values_decoded > 0);
    Ok(())
}

#[test]
fn star_on_plain_and_kind_two_files_matches_kind_three_outputs() -> TestResult {
    let fixture = fixture()?;
    let names = [
        "geom_length",
        "geom_area",
        "geom_npoints",
        "bbox_xmin",
        "bbox_ymin",
        "bbox_xmax",
        "bbox_ymax",
    ];
    let columnar = execute(
        &fixture.metrics,
        "SELECT geom_length, geom_area, geom_npoints, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax FROM features",
    )?;
    assert_eq!(delta(&columnar).body_bytes_read, 0);
    let columnar = combined_batch(&columnar.batches)?;
    let kind_three = execute(&fixture.metrics, "SELECT * FROM features")?;
    let expected = combined_batch(&kind_three.batches)?;
    assert_eq!(expected.num_rows(), ROW_COUNT);

    for (source, path) in [
        ("plain", &fixture.plain),
        ("kind-2-only", &fixture.kind_two),
    ] {
        let execution = execute(path, "SELECT * FROM features")?;
        let used = delta(&execution);
        assert!(used.body_bytes_read > 0, "{source}");
        assert_eq!(used.analytics_bytes_read, 0, "{source}");
        assert_eq!(used.wkb_values_decoded, ROW_COUNT as u64, "{source}");
        let actual = combined_batch(&execution.batches)?;
        assert_eq!(actual.num_rows(), ROW_COUNT, "{source}");
        for field in expected.schema().fields() {
            let name = field.name();
            assert_eq!(
                actual.column(actual.schema().index_of(name)?).to_data(),
                expected.column(expected.schema().index_of(name)?).to_data(),
                "{source}: {name} differs from the kind-3 body scan"
            );
        }
        for name in names {
            assert_eq!(
                actual.column(actual.schema().index_of(name)?).to_data(),
                columnar.column(columnar.schema().index_of(name)?).to_data(),
                "{source}: {name} differs from kind-3 columnar values"
            );
        }
        let wkb_index = actual.schema().index_of("geom_wkb")?;
        let wkb = actual
            .column(wkb_index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or("geom_wkb is not Binary")?;
        assert!(
            wkb.iter().all(|value| value.is_some()),
            "{source}: null WKB"
        );
    }
    Ok(())
}

#[test]
fn malformed_kind_three_disables_only_geometry_acceleration() -> TestResult {
    let fixture = fixture()?;
    let metrics = Reader::open_path(&fixture.metrics)?
        .read_geometry_metrics_metadata()?
        .ok_or("missing geometry metrics")?;
    let mut bytes = std::fs::read(&fixture.metrics)?;
    bytes[metrics.section_start as usize] = fgba::geom_analytics::GEOMETRY_METRICS_VERSION + 1;
    let malformed = fixture.metrics.with_file_name("malformed.a.fgb");
    std::fs::write(&malformed, bytes)?;

    let properties = execute(
        &malformed,
        "SELECT id, name FROM features WHERE id >= 18 ORDER BY id",
    )?;
    assert_eq!(
        properties
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        6
    );
    assert_eq!(delta(&properties).body_bytes_read, 0);
    assert!(delta(&properties).analytics_bytes_read > 0);

    let bbox = execute(
        &malformed,
        "SELECT id, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax FROM features WHERE id = 0",
    )?;
    assert_eq!(one_batch(&bbox.batches)?.num_rows(), 1);
    assert!(delta(&bbox).body_bytes_read > 0);
    let error = plan_error(&malformed, "SELECT geom_length FROM features")?;
    assert!(error.contains("--geometry-metrics"), "{error}");
    Ok(())
}

#[test]
fn structurally_malformed_kind_three_falls_back_to_exact_body_queries() -> TestResult {
    let fixture = fixture()?;
    let bytes = std::fs::read(&fixture.metrics)?;
    let footer = Footer::decode(&bytes[bytes.len() - 32..])?;
    let dir_start = footer.dir_offset as usize;
    let mut directory =
        Directory::decode(&bytes[dir_start..dir_start + usize::try_from(footer.dir_size)?])?;
    directory
        .sections
        .iter_mut()
        .find(|section| section.kind == KIND_GEOMETRY_METRICS)
        .ok_or("missing geometry entry")?
        .size = u64::MAX;
    let encoded = directory.encode()?;
    let footer = Footer {
        dir_offset: footer.dir_offset,
        dir_size: encoded.len() as u64,
        dir_crc32: crc32(&encoded),
    };
    let mut malformed_bytes = bytes[..dir_start].to_vec();
    malformed_bytes.extend_from_slice(&encoded);
    malformed_bytes.extend_from_slice(&footer.encode());
    let malformed = fixture.metrics.with_file_name("structural-malformed.a.fgb");
    std::fs::write(&malformed, malformed_bytes)?;

    let properties = execute(
        &malformed,
        "SELECT id, name FROM features WHERE id >= 18 ORDER BY id",
    )?;
    assert_eq!(
        properties
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        6
    );
    assert_eq!(delta(&properties).body_bytes_read, 0);
    assert!(delta(&properties).analytics_bytes_read > 0);

    let sql = "SELECT id, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax FROM features WHERE id = 7";
    let actual = execute(&malformed, sql)?;
    let expected = execute(&fixture.kind_two, sql)?;
    assert!(delta(&actual).body_bytes_read > 0);
    let actual = combined_batch(&actual.batches)?;
    let expected = combined_batch(&expected.batches)?;
    assert_eq!(actual.num_rows(), 1);
    for name in ["id", "bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax"] {
        assert_eq!(
            actual.column(actual.schema().index_of(name)?).to_data(),
            expected.column(expected.schema().index_of(name)?).to_data(),
            "{name} differs from the exact body fallback"
        );
    }
    Ok(())
}

#[test]
fn wkb_metric_mix_and_star_match_columnar_values_using_body() -> TestResult {
    let fixture = fixture()?;
    let names = [
        "geom_length",
        "geom_area",
        "geom_npoints",
        "bbox_xmin",
        "bbox_ymin",
        "bbox_xmax",
        "bbox_ymax",
    ];
    let columnar = execute(
        &fixture.metrics,
        "SELECT geom_length, geom_area, geom_npoints, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax FROM features",
    )?;
    let columnar_used = delta(&columnar);
    assert_eq!(columnar_used.body_bytes_read, 0);
    assert!(columnar_used.analytics_bytes_read > 0);
    let columnar = combined_batch(&columnar.batches)?;
    assert_eq!(columnar.num_rows(), ROW_COUNT);

    for (sql, projected_names) in [
        (
            "SELECT geom_wkb, geom_length, geom_area, geom_npoints, bbox_xmin FROM features",
            &names[..4],
        ),
        ("SELECT * FROM features", &names[..]),
    ] {
        let execution = execute(&fixture.metrics, sql)?;
        let used = delta(&execution);
        assert!(used.body_bytes_read > 0, "{sql}");
        assert_eq!(used.analytics_bytes_read, 0, "{sql}");
        assert_eq!(used.wkb_values_decoded, ROW_COUNT as u64, "{sql}");
        let actual = combined_batch(&execution.batches)?;
        assert_eq!(actual.num_rows(), ROW_COUNT, "{sql}");
        let wkb_index = actual.schema().index_of("geom_wkb")?;
        let wkb = actual
            .column(wkb_index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or("geom_wkb is not Binary")?;
        for row in 0..ROW_COUNT {
            assert!(wkb.is_valid(row), "{sql}: null WKB at row {row}");
            assert_eq!(wkb.value(row).first(), Some(&1), "{sql}: row {row}");
        }
        for &name in projected_names {
            let actual_values = actual.column(actual.schema().index_of(name)?);
            let expected_values = columnar.column(columnar.schema().index_of(name)?);
            assert_eq!(
                actual_values.to_data(),
                expected_values.to_data(),
                "{sql}: {name} differs from columnar values"
            );
        }
    }
    Ok(())
}

#[test]
fn body_mixed_metrics_are_null_for_empty_geometry() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut writer = FgbWriter::create_with_options(
        "sql-empty-geometry",
        GeometryType::LineString,
        FgbWriterOptions {
            write_index: true,
            ..FgbWriterOptions::default()
        },
    )?;
    writer.add_column("id", ColumnType::Long, |_, args| args.nullable = false);
    for (id, points) in [(0, vec![(0.0, 0.0), (3.0, 4.0)]), (1, Vec::new())] {
        let mut property_error = None;
        writer.add_feature_geom(Line(points), |feature| {
            if let Err(error) = feature.property(0, "id", &ColumnValue::Long(id)) {
                property_error = Some(error);
            }
        })?;
        if let Some(error) = property_error {
            return Err(error.into());
        }
    }
    let mut body = Vec::new();
    writer.write(&mut body)?;
    let path = build_file(dir.path(), &body, "empty-geometry.a.fgb", true)?;
    let names = ["geom_length", "geom_area", "geom_npoints", "bbox_xmin"];
    let columnar = execute(
        &path,
        "SELECT id, geom_length, geom_area, geom_npoints, bbox_xmin FROM features",
    )?;
    assert_eq!(delta(&columnar).body_bytes_read, 0);
    let columnar = combined_batch(&columnar.batches)?;
    let execution = execute(
        &path,
        "SELECT id, geom_wkb, geom_length, geom_area, geom_npoints, bbox_xmin FROM features",
    )?;
    let used = delta(&execution);
    assert!(used.body_bytes_read > 0);
    assert_eq!(used.analytics_bytes_read, 0);
    let actual = combined_batch(&execution.batches)?;
    assert_eq!(actual.num_rows(), 2);
    for name in names {
        let actual_values = actual.column(actual.schema().index_of(name)?);
        assert!(actual_values.is_valid(0), "{name} should be non-null");
        assert!(actual_values.is_null(1), "{name} should be null");
        let expected_values = columnar.column(columnar.schema().index_of(name)?);
        assert_eq!(actual_values.to_data(), expected_values.to_data(), "{name}");
    }
    Ok(())
}

#[test]
fn wkb_udfs_match_kind_three_and_independent_body_decode() -> TestResult {
    let fixture = fixture()?;
    let execution = execute(
        &fixture.kind_two,
        "SELECT id, st_length(geom_wkb) AS geom_length, st_area(geom_wkb) AS geom_area, st_npoints(geom_wkb) AS geom_npoints FROM features ORDER BY id",
    )?;
    let used = delta(&execution);
    assert!(used.body_bytes_read > 0);
    assert_eq!(used.analytics_bytes_read, 0);
    assert_eq!(used.wkb_values_decoded, ROW_COUNT as u64);
    let actual = combined_batch(&execution.batches)?;
    assert_eq!(actual.num_rows(), ROW_COUNT);

    let stored = execute(
        &fixture.metrics,
        "SELECT id, geom_length, geom_area, geom_npoints FROM features ORDER BY id",
    )?;
    assert_eq!(delta(&stored).body_bytes_read, 0);
    let stored = combined_batch(&stored.batches)?;
    for name in ["id", "geom_length", "geom_area", "geom_npoints"] {
        assert_eq!(
            actual.column(actual.schema().index_of(name)?).to_data(),
            stored.column(stored.schema().index_of(name)?).to_data(),
            "WKB UDF differs from kind-3 column {name}"
        );
    }

    let ids = actual
        .column(actual.schema().index_of("id")?)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("id is not Int64")?;
    let lengths = actual
        .column(actual.schema().index_of("geom_length")?)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or("length is not Float64")?;
    let areas = actual
        .column(actual.schema().index_of("geom_area")?)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or("area is not Float64")?;
    let npoints = actual
        .column(actual.schema().index_of("geom_npoints")?)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or("npoints is not UInt32")?;
    let mut expected_rows = fixture.rows.clone();
    expected_rows.sort_by_key(|row| row.id);
    for (index, expected) in expected_rows.iter().enumerate() {
        assert_eq!(ids.value(index), expected.id);
        assert_eq!(lengths.value(index), expected.length);
        assert_eq!(areas.value(index), 0.0);
        assert_eq!(npoints.value(index), expected.npoints);
    }

    let sum = execute(
        &fixture.kind_two,
        "SELECT SUM(st_length(geom_wkb)) FROM features",
    )?;
    assert_eq!(scalar_f64(&sum.batches)?, expected_sum(&fixture.rows));
    assert!(delta(&sum).body_bytes_read > 0);

    let nulls = execute(
        &fixture.kind_two,
        "SELECT st_length(NULL), st_area(NULL), st_npoints(NULL)",
    )?;
    let nulls = one_batch(&nulls.batches)?;
    for column in nulls.columns() {
        assert!(column.is_null(0));
    }
    Ok(())
}

#[test]
fn wkb_area_udf_matches_nonzero_polygon_body_and_kind_three() -> TestResult {
    let dir = tempfile::tempdir()?;
    let body = polygon_body()?;
    let mut expected = decode_polygon_areas(&body)?;
    expected.sort_by_key(|row| row.0);
    let without_metrics = build_file(dir.path(), &body, "udf-areas.a.fgb", false)?;
    let with_metrics = build_file(dir.path(), &body, "stored-areas.a.fgb", true)?;
    let execution = execute(
        &without_metrics,
        "SELECT id, st_area(geom_wkb) AS geom_area FROM features ORDER BY id",
    )?;
    assert!(delta(&execution).body_bytes_read > 0);
    let actual = combined_batch(&execution.batches)?;
    let ids = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("id is not Int64")?;
    let areas = actual
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or("area is not Float64")?;
    for (index, &(expected_id, expected_area)) in expected.iter().enumerate() {
        assert_eq!(ids.value(index), expected_id);
        assert!(areas.value(index) > 0.0);
        assert_eq!(areas.value(index), expected_area);
    }

    let stored = execute(
        &with_metrics,
        "SELECT id, geom_area FROM features ORDER BY id",
    )?;
    assert_eq!(delta(&stored).body_bytes_read, 0);
    let stored = combined_batch(&stored.batches)?;
    for column in 0..actual.num_columns() {
        assert_eq!(
            actual.column(column).to_data(),
            stored.column(column).to_data()
        );
    }
    Ok(())
}

#[test]
fn rtree_fast_path_and_joint_metric_filter_remain_exact() -> TestResult {
    let dir = tempfile::tempdir()?;
    let body = body(true)?;
    let rows = decode_body(&body)?;
    let path = build_file(dir.path(), &body, "indexed.a.fgb", true)?;
    let predicate =
        "bbox_xmin <= -70 AND bbox_xmax >= -110 AND bbox_ymin <= 20 AND bbox_ymax >= -1";
    let bbox_only = execute(
        &path,
        &format!("SELECT COUNT(*) FROM features WHERE {predicate}"),
    )?;
    let expected_bbox = rows
        .iter()
        .filter(|row| {
            row.bbox[0] <= -70.0
                && row.bbox[2] >= -110.0
                && row.bbox[1] <= 20.0
                && row.bbox[3] >= -1.0
        })
        .count() as i64;
    assert_eq!(scalar_i64(&bbox_only.batches)?, expected_bbox);
    let used = delta(&bbox_only);
    assert_eq!(used.features_read, 0);
    assert_eq!(used.analytics_bytes_read, 0);
    assert!(used.body_bytes_read > 0);

    let direct = execute_with_threads(
        &path,
        &format!("SELECT id FROM features WHERE {predicate}"),
        8,
    )?;
    assert_eq!(direct.source_partitions, [1]);
    let mut expected_ids = rows
        .iter()
        .filter(|row| {
            row.bbox[0] <= -70.0
                && row.bbox[2] >= -110.0
                && row.bbox[1] <= 20.0
                && row.bbox[3] >= -1.0
        })
        .map(|row| row.id)
        .collect::<Vec<_>>();
    expected_ids.sort_unstable();
    assert_eq!(ids(&direct.batches)?, expected_ids);
    assert!(delta(&direct).body_bytes_read > 0);
    assert!(delta(&direct).analytics_bytes_read > 0);

    let joint = execute(
        &path,
        &format!("SELECT id FROM features WHERE {predicate} AND geom_npoints > 100"),
    )?;
    assert_eq!(scalar_i64(&joint.batches)?, 4);
    let used = delta(&joint);
    assert!(used.body_bytes_read > 0);
    assert!(used.analytics_bytes_read > 0);
    assert_eq!(used.wkb_values_decoded, 0);
    Ok(())
}

#[test]
fn corrupt_metric_arrow_chunk_falls_back_atomically_across_partitions() -> TestResult {
    let fixture = fixture()?;
    let metrics = Reader::open_path(&fixture.metrics)?
        .read_geometry_metrics_metadata()?
        .ok_or("missing geometry metrics")?;
    let (_, end) = metrics.arrow_range(0, 3)?;
    let mut bytes = std::fs::read(&fixture.metrics)?;
    let last = bytes
        .get_mut(end as usize - 1)
        .ok_or("geometry Arrow range is outside the file")?;
    assert_eq!(*last, b'1');
    *last ^= 0xff;
    let corrupted = fixture.metrics.with_file_name("corrupt-metric.a.fgb");
    std::fs::write(&corrupted, bytes)?;

    let sql = "SELECT id, geom_length FROM features ORDER BY id";
    let clean = execute_with_threads(&fixture.metrics, sql, 4)?;
    let fallback = execute_with_threads(&corrupted, sql, 4)?;
    assert_eq!(fallback.source_partitions, [4]);
    let expected = combined_batch(&clean.batches)?;
    let actual = combined_batch(&fallback.batches)?;
    assert_eq!(actual.num_rows(), ROW_COUNT);
    for column in 0..expected.num_columns() {
        assert_eq!(
            actual.column(column).to_data(),
            expected.column(column).to_data(),
            "ordered column {column} differs after chunk fallback"
        );
    }
    let clean_used = delta(&clean);
    let used = delta(&fallback);
    assert_eq!(clean_used.body_bytes_read, 0);
    assert!(used.body_bytes_read > 0);
    assert_eq!(used.features_read, ROW_COUNT as u64);
    assert_eq!(used.analytics_bytes_read, clean_used.analytics_bytes_read);
    assert_eq!(used.ranges_opened, clean_used.ranges_opened + 1);
    assert_eq!(used.wkb_values_decoded, 0);
    Ok(())
}
