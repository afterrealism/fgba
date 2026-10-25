//! Synchronous local registration and DataFusion session execution.

use crate::plan::metadata_count_plan;
use crate::schema::OutputSchema;
use crate::stream::BatchConfig;
use crate::table::FgbaTable;
use crate::{Error, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{
    BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt16Array, UInt32Array,
    UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::{SessionConfig, SessionContext};
use fgba::{BodyAnalytics, ColumnAnalytics, ValueClass};
use std::any::Any;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Source batching and optional viewport registration options.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryOptions {
    /// Optional `[minx, miny, maxx, maxy]` viewport.
    pub bbox: Option<[f64; 4]>,
    /// A5 resolution for `a5_viewport_approx`; deepest band when omitted.
    pub res: Option<u8>,
    /// Maximum decoded rows before emitting a source batch.
    pub batch_rows: usize,
    /// Variable WKB/string/binary byte target before emitting a source batch.
    pub batch_bytes: usize,
    /// DataFusion target partitions for analytical chunk scans; CPU-based default when omitted.
    pub scan_threads: Option<usize>,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            bbox: None,
            res: None,
            batch_rows: 4096,
            batch_bytes: 8 * 1024 * 1024,
            scan_threads: None,
        }
    }
}

impl QueryOptions {
    fn validate(&self) -> Result<()> {
        self.validate_source()?;
        validate_viewport_options(self.bbox, self.res)
    }

    fn validate_source(&self) -> Result<()> {
        if self.batch_rows == 0 || self.batch_bytes == 0 {
            return Err(Error::InvalidInput(
                "batch_rows and batch_bytes must be positive".into(),
            ));
        }
        if self.scan_threads == Some(0) {
            return Err(Error::InvalidInput(
                "--scan-threads must be positive".into(),
            ));
        }
        Ok(())
    }
}

fn validate_viewport_options(bbox: Option<[f64; 4]>, res: Option<u8>) -> Result<()> {
    if res.is_some() && bbox.is_none() {
        return Err(Error::InvalidInput("--res requires --bbox".into()));
    }
    if let Some(bbox) = bbox {
        if bbox.iter().any(|value| !value.is_finite()) {
            return Err(Error::InvalidInput(
                "bbox coordinates must be finite".into(),
            ));
        }
        if bbox[0] > bbox[2] || bbox[1] > bbox[3] {
            return Err(Error::InvalidInput(
                "bbox minimums must not exceed maximums".into(),
            ));
        }
    }
    Ok(())
}

/// A registered read-only SQL session and its measurable `features` table.
pub struct FgbaSession {
    /// DataFusion catalog and SQL planner.
    pub ctx: SessionContext,
    /// Exact body table with cumulative scan metrics.
    pub features: Arc<FgbaTable>,
}

impl FgbaSession {
    /// Execute a DataFrame, using metadata only for the exact count-plan shape.
    pub async fn execute_dataframe(
        &self,
        df: DataFrame,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let plan = df.clone().into_unoptimized_plan();
        if let Some((batch, fetch)) = metadata_count_plan(&plan, &self.features).await? {
            let schema = batch.schema();
            return Ok(Box::pin(
                MemoryStream::try_new(vec![batch], schema, None)?.with_fetch(fetch),
            ));
        }
        df.execute_stream().await
    }
}

/// Register a local FlatGeobuf/FGBA path and eagerly load all available side tables.
pub fn register(path: &Path, options: &QueryOptions) -> Result<FgbaSession> {
    register_inner(path, options, true)
}

/// Register a local path with lazy A5 side tables for DataFusion to resolve.
///
/// The SQL argument is retained for API compatibility, but is deliberately not
/// parsed here. Band payloads and viewport resources are read only if a planned
/// scan actually uses their provider, so CTEs can shadow any registered name.
pub fn register_for_sql(path: &Path, _sql: &str, options: &QueryOptions) -> Result<FgbaSession> {
    register_inner(path, options, false)
}

fn register_inner(path: &Path, options: &QueryOptions, eager_a5: bool) -> Result<FgbaSession> {
    if eager_a5 {
        options.validate()?;
    } else {
        options.validate_source()?;
    }
    let output_schema = OutputSchema::open(path)?;
    let mut reader = match fgba::Reader::open_path(path) {
        Ok(reader) => Some(reader),
        Err(fgba::Error::NotFgba) => None,
        Err(error) => return Err(error.into()),
    };
    let analytics = match reader.as_mut() {
        Some(reader) => match reader.read_body_analytics_metadata() {
            Ok(analytics) => analytics,
            Err(fgba::Error::Format(error)) => {
                eprintln!("analytics metadata disabled; using the exact body source: {error}");
                None
            }
            Err(error) => return Err(error.into()),
        },
        None => None,
    };
    let geometry_metrics = match reader.as_mut() {
        Some(reader) if analytics.is_some() => match reader.read_geometry_metrics_metadata() {
            Ok(metrics) => metrics,
            Err(fgba::Error::Format(error)) => {
                eprintln!(
                    "geometry metrics disabled; preserving body and analytics sources: {error}"
                );
                None
            }
            Err(error) => return Err(error.into()),
        },
        _ => None,
    };
    let features = Arc::new(FgbaTable::new(
        path.to_path_buf(),
        output_schema,
        analytics.clone(),
        geometry_metrics.clone(),
        BatchConfig {
            rows: options.batch_rows,
            bytes: options.batch_bytes,
        },
    )?);
    if let Some(analytics) = &analytics {
        features
            .metrics
            .add_analytics_bytes(analytics.metadata_bytes_read);
    }
    if let Some(geometry_metrics) = &geometry_metrics {
        features
            .metrics
            .add_analytics_bytes(geometry_metrics.metadata_bytes_read);
    }

    let config = match options.scan_threads {
        Some(threads) => SessionConfig::new().with_target_partitions(threads),
        None => SessionConfig::new(),
    };
    let ctx = SessionContext::new_with_config(config);
    crate::udf_geom::register(&ctx);
    ctx.register_table("features", features.clone())?;
    if eager_a5 {
        let mut bands = Vec::new();
        if let Some(reader) = reader.as_mut() {
            for meta in reader.bands() {
                let band = reader.read_band(meta.res)?;
                ctx.register_table(format!("a5_band_{}", meta.res), cells_table(&band.cells)?)?;
                bands.push(meta.res);
            }
        }
        if let Some(bbox) = options.bbox {
            let reader = reader.as_mut().ok_or_else(|| {
                Error::InvalidInput("bbox requires an FGBA file with an available band".into())
            })?;
            let res = match options.res {
                Some(res) if bands.contains(&res) => res,
                Some(res) => {
                    return Err(Error::InvalidInput(format!(
                        "no aggregate band at resolution {res}"
                    )))
                }
                None => bands.into_iter().max().ok_or_else(|| {
                    Error::InvalidInput("bbox requires an available aggregate band".into())
                })?,
            };
            let cells = reader.query(bbox, res)?;
            ctx.register_table("a5_viewport_approx", cells_table(&cells)?)?;
        }
    } else {
        if let Some(reader) = reader.as_ref() {
            for meta in reader.bands() {
                ctx.register_table(
                    format!("a5_band_{}", meta.res),
                    Arc::new(A5Table::band(path, meta.res)),
                )?;
            }
        }
        ctx.register_table(
            "a5_viewport_approx",
            Arc::new(A5Table::viewport(path, options.bbox, options.res)),
        )?;
    }
    if let Some(analytics) = &analytics {
        ctx.register_table("attribute_stats", attribute_stats_table(analytics)?)?;
    }
    Ok(FgbaSession { ctx, features })
}

#[derive(Debug, Clone, Copy)]
enum A5Source {
    Band(u8),
    Viewport {
        bbox: Option<[f64; 4]>,
        res: Option<u8>,
    },
}

#[derive(Debug)]
struct A5Table {
    path: PathBuf,
    source: A5Source,
    schema: SchemaRef,
}

impl A5Table {
    fn band(path: &Path, res: u8) -> Self {
        Self {
            path: path.to_path_buf(),
            source: A5Source::Band(res),
            schema: cells_schema(),
        }
    }

    fn viewport(path: &Path, bbox: Option<[f64; 4]>, res: Option<u8>) -> Self {
        Self {
            path: path.to_path_buf(),
            source: A5Source::Viewport { bbox, res },
            schema: cells_schema(),
        }
    }

    fn load(&self) -> Result<Arc<MemTable>> {
        let cells = match self.source {
            A5Source::Band(res) => fgba::Reader::open_path(&self.path)?.read_band(res)?.cells,
            A5Source::Viewport { bbox, res } => {
                validate_viewport_options(bbox, res)?;
                let bbox = bbox.ok_or_else(|| {
                    Error::InvalidInput("a5_viewport_approx requires --bbox".into())
                })?;
                let mut reader = match fgba::Reader::open_path(&self.path) {
                    Ok(reader) => reader,
                    Err(fgba::Error::NotFgba) => {
                        return Err(Error::InvalidInput(
                            "bbox requires an FGBA file with an available band".into(),
                        ))
                    }
                    Err(error) => return Err(error.into()),
                };
                let bands = reader.bands();
                let res = match res {
                    Some(res) if bands.iter().any(|meta| meta.res == res) => res,
                    Some(res) => {
                        return Err(Error::InvalidInput(format!(
                            "no aggregate band at resolution {res}"
                        )))
                    }
                    None => bands
                        .into_iter()
                        .map(|meta| meta.res)
                        .max()
                        .ok_or_else(|| {
                            Error::InvalidInput("bbox requires an available aggregate band".into())
                        })?,
                };
                reader.query(bbox, res)?
            }
        };
        cells_table(&cells)
    }
}

#[async_trait]
impl TableProvider for A5Table {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let table = self.load()?;
        table.scan(state, projection, filters, limit).await
    }
}

fn cells_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("cell_id", DataType::UInt64, false),
        Field::new("count", DataType::UInt32, false),
    ]))
}

fn cells_table(cells: &[(u64, u32)]) -> Result<Arc<MemTable>> {
    let ids = cells.iter().map(|cell| cell.0).collect::<Vec<_>>();
    let counts = cells.iter().map(|cell| cell.1).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        cells_schema(),
        vec![
            Arc::new(UInt64Array::from(ids)),
            Arc::new(UInt32Array::from(counts)),
        ],
    )?;
    Ok(Arc::new(MemTable::try_new(
        batch.schema(),
        vec![vec![batch]],
    )?))
}

#[allow(clippy::type_complexity)]
fn attribute_stats_table(analytics: &BodyAnalytics) -> Result<Arc<MemTable>> {
    let schema = stats_schema();
    let mut column_index = Vec::new();
    let mut names = Vec::new();
    let mut classes = Vec::new();
    let mut row_count = Vec::new();
    let mut non_null_count = Vec::new();
    let mut null_count = Vec::new();
    let mut nan_count = Vec::new();
    let mut min_i64 = Vec::new();
    let mut max_i64 = Vec::new();
    let mut min_u64 = Vec::new();
    let mut max_u64 = Vec::new();
    let mut min_f64 = Vec::new();
    let mut max_f64 = Vec::new();
    let mut sum_text = Vec::new();
    let mut sum_valid = Vec::new();
    let mut has_infinity = Vec::new();
    for column in &analytics.columns {
        let stat = &column.global;
        let class = column.descriptor.value_class;
        column_index.push(column.descriptor.column_index);
        names.push(column.name.clone());
        classes.push(value_class_name(class).to_owned());
        row_count.push(analytics.row_count);
        non_null_count.push(stat.non_null_count);
        null_count.push(
            analytics
                .row_count
                .checked_sub(stat.non_null_count)
                .ok_or_else(|| {
                    Error::Format(format!("invalid null count for property {:?}", column.name))
                })?,
        );
        nan_count.push(stat.nan_count);
        let (min_signed, max_signed) = signed_bounds(column);
        let (min_unsigned, max_unsigned) = unsigned_bounds(column);
        let (min_float, max_float) = float_bounds(column);
        min_i64.push(min_signed);
        max_i64.push(max_signed);
        min_u64.push(min_unsigned);
        max_u64.push(max_unsigned);
        min_f64.push(min_float);
        max_f64.push(max_float);
        sum_text.push(stat_sum_text(column)?);
        sum_valid.push(stat.sum_valid());
        has_infinity.push(stat.has_infinity());
    }
    let arrays: Vec<Arc<dyn datafusion::arrow::array::Array>> = vec![
        Arc::new(UInt16Array::from(column_index)),
        Arc::new(StringArray::from(names)),
        Arc::new(StringArray::from(classes)),
        Arc::new(UInt64Array::from(row_count)),
        Arc::new(UInt64Array::from(non_null_count)),
        Arc::new(UInt64Array::from(null_count)),
        Arc::new(UInt64Array::from(nan_count)),
        Arc::new(Int64Array::from(min_i64)),
        Arc::new(Int64Array::from(max_i64)),
        Arc::new(UInt64Array::from(min_u64)),
        Arc::new(UInt64Array::from(max_u64)),
        Arc::new(Float64Array::from(min_f64)),
        Arc::new(Float64Array::from(max_f64)),
        Arc::new(StringArray::from(sum_text)),
        Arc::new(BooleanArray::from(sum_valid)),
        Arc::new(BooleanArray::from(has_infinity)),
    ];
    let batch = RecordBatch::try_new(schema, arrays)?;
    Ok(Arc::new(MemTable::try_new(
        batch.schema(),
        vec![vec![batch]],
    )?))
}

fn stats_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("column_index", DataType::UInt16, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value_class", DataType::Utf8, false),
        Field::new("row_count", DataType::UInt64, false),
        Field::new("non_null_count", DataType::UInt64, false),
        Field::new("null_count", DataType::UInt64, false),
        Field::new("nan_count", DataType::UInt64, false),
        Field::new("min_i64", DataType::Int64, true),
        Field::new("max_i64", DataType::Int64, true),
        Field::new("min_u64", DataType::UInt64, true),
        Field::new("max_u64", DataType::UInt64, true),
        Field::new("min_f64", DataType::Float64, true),
        Field::new("max_f64", DataType::Float64, true),
        Field::new("sum_text", DataType::Utf8, true),
        Field::new("sum_valid", DataType::Boolean, false),
        Field::new("has_infinity", DataType::Boolean, false),
    ]))
}

fn value_class_name(class: ValueClass) -> &'static str {
    match class {
        ValueClass::Opaque => "opaque",
        ValueClass::Signed => "signed",
        ValueClass::Unsigned => "unsigned",
        ValueClass::Float => "float",
    }
}

fn signed_bounds(column: &ColumnAnalytics) -> (Option<i64>, Option<i64>) {
    if column.descriptor.value_class == ValueClass::Signed && column.global.has_bounds() {
        (
            Some(i64::from_le_bytes(column.global.min_bits.to_le_bytes())),
            Some(i64::from_le_bytes(column.global.max_bits.to_le_bytes())),
        )
    } else {
        (None, None)
    }
}

fn unsigned_bounds(column: &ColumnAnalytics) -> (Option<u64>, Option<u64>) {
    if column.descriptor.value_class == ValueClass::Unsigned && column.global.has_bounds() {
        (Some(column.global.min_bits), Some(column.global.max_bits))
    } else {
        (None, None)
    }
}

fn float_bounds(column: &ColumnAnalytics) -> (Option<f64>, Option<f64>) {
    if column.descriptor.value_class == ValueClass::Float && column.global.has_bounds() {
        (
            Some(f64::from_bits(column.global.min_bits)),
            Some(f64::from_bits(column.global.max_bits)),
        )
    } else {
        (None, None)
    }
}

fn stat_sum_text(column: &ColumnAnalytics) -> Result<Option<String>> {
    let stat = &column.global;
    if !stat.sum_valid() {
        return Ok(None);
    }
    match column.descriptor.value_class {
        ValueClass::Opaque => Ok(None),
        ValueClass::Signed => Ok(Some(i128::from_le_bytes(stat.sum).to_string())),
        ValueClass::Unsigned => Ok(Some(u128::from_le_bytes(stat.sum).to_string())),
        ValueClass::Float => {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&stat.sum[..8]);
            let value = f64::from_le_bytes(bytes);
            if value.is_finite() {
                Ok(Some(value.to_string()))
            } else {
                Err(Error::Format(format!(
                    "property {:?} has a non-finite sum marked valid",
                    column.name
                )))
            }
        }
    }
}
