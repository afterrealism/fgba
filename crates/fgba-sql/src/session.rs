//! Synchronous local registration and DataFusion session execution.

use crate::memory::{attribute_stats_table, cells_schema, cells_table};
use crate::plan::metadata_count_plan;
use crate::schema::OutputSchema;
use crate::stream::BatchConfig;
use crate::table::FgbaTable;
use crate::{Error, Result};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::{SessionConfig, SessionContext};
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
