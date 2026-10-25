//! Thin DataFusion `TableProvider` over built-in streaming execution.

use crate::error::Result as CrateResult;
use crate::plan::canonical_bbox;
use crate::prune::{supports_filter, ChunkPruner};
use crate::schema::{OutputSchema, GEOMETRY_COLUMNS};
use crate::stream::{BatchConfig, FgbPartition, MetricsInner, Scan, ScanMetrics, Selection};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::utils::project_schema;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::ExecutionPlan;
use fgba::{BodyAnalytics, GeometryMetrics};
use std::any::Any;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

/// Reopenable `features` table. Planning performs no file I/O.
pub struct FgbaTable {
    pub(crate) path: PathBuf,
    pub(crate) output_schema: Arc<OutputSchema>,
    schema: SchemaRef,
    analytics: Option<Arc<BodyAnalytics>>,
    geometry_metrics: Option<Arc<GeometryMetrics>>,
    config: BatchConfig,
    pub(crate) metrics: Arc<MetricsInner>,
}

impl FgbaTable {
    pub(crate) fn new(
        path: PathBuf,
        output_schema: OutputSchema,
        analytics: Option<BodyAnalytics>,
        geometry_metrics: Option<GeometryMetrics>,
        config: BatchConfig,
    ) -> CrateResult<Self> {
        if config.rows == 0 || config.bytes == 0 {
            return Err(crate::Error::InvalidInput(
                "batch_rows and batch_bytes must be positive".into(),
            ));
        }
        let schema = Arc::clone(&output_schema.schema);
        Ok(Self {
            path,
            output_schema: Arc::new(output_schema),
            schema,
            analytics: analytics.map(Arc::new),
            geometry_metrics: geometry_metrics.map(Arc::new),
            config,
            metrics: Arc::new(MetricsInner::default()),
        })
    }

    fn supports_zone_filter(&self, expr: &Expr) -> bool {
        self.analytics.is_some()
            && supports_filter(&self.output_schema, self.geometry_metrics.is_some(), expr)
    }

    fn selection_for(&self, filters: &[&Expr]) -> Selection {
        let bbox = filters_bbox(filters).filter(|_| self.output_schema.has_spatial_index);
        let attributes = filters
            .iter()
            .filter(|expr| self.supports_zone_filter(expr))
            .map(|expr| (**expr).clone())
            .collect::<Vec<_>>();
        let zone_prune = !attributes.is_empty()
            && (bbox.is_none() || attributes.iter().any(|expr| !is_bbox_comparison(expr)));
        match (bbox, zone_prune) {
            (Some(bbox), false) => Selection::BBox(bbox),
            (bbox, true) => Selection::Prune {
                bbox,
                filters: attributes,
            },
            (None, false) => Selection::All,
        }
    }

    fn filter_support(&self, expr: &Expr, bbox: Option<[f64; 4]>) -> TableProviderFilterPushDown {
        if bbox.is_some() && is_bbox_comparison(expr) {
            // Bbox-only and joint sources both apply exact R-tree ordinals.
            TableProviderFilterPushDown::Exact
        } else if self.supports_zone_filter(expr) {
            TableProviderFilterPushDown::Inexact
        } else {
            TableProviderFilterPushDown::Unsupported
        }
    }

    /// Snapshot cumulative counters across all executions of this table.
    pub fn metrics(&self) -> ScanMetrics {
        self.metrics.snapshot()
    }

    /// Full source schema and exact body metadata.
    pub fn output_schema(&self) -> &OutputSchema {
        &self.output_schema
    }
}

impl fmt::Debug for FgbaTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FgbaTable")
            .field("path", &self.path)
            .field("features_count", &self.output_schema.features_count)
            .field("has_analytics", &self.analytics.is_some())
            .field("has_geometry_metrics", &self.geometry_metrics.is_some())
            .field("metrics", &self.metrics())
            .finish()
    }
}

fn is_bbox_comparison(expr: &Expr) -> bool {
    let Expr::BinaryExpr(binary) = expr else {
        return false;
    };
    let Expr::Column(column) = binary.left.as_ref() else {
        return false;
    };
    GEOMETRY_COLUMNS[1..5].contains(&column.name.as_str())
        && matches!(binary.right.as_ref(), Expr::Literal(_, _))
}

fn filters_bbox(filters: &[&Expr]) -> Option<[f64; 4]> {
    let mut comparisons = filters
        .iter()
        .filter(|expr| is_bbox_comparison(expr))
        .map(|expr| (**expr).clone());
    let first = comparisons.next()?;
    let conjunction = comparisons.fold(first, |left, right| left.and(right));
    canonical_bbox(&conjunction)
}

fn partition_chunks(chunks: Vec<u64>, target_partitions: usize) -> Vec<Arc<[u64]>> {
    if chunks.is_empty() {
        // DataFusion plans need at least one partition; this one owns no reads.
        return vec![Arc::from(chunks)];
    }
    let partition_count = chunks.len().min(target_partitions.max(1));
    let width = chunks.len().div_ceil(partition_count);
    chunks
        .chunks(width)
        .map(|group| Arc::from(group.to_vec()))
        .collect()
}

#[async_trait]
impl TableProvider for FgbaTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let bbox = filters_bbox(filters).filter(|_| self.output_schema.has_spatial_index);
        Ok(filters
            .iter()
            .map(|expr| self.filter_support(expr, bbox))
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projection = projection.cloned();
        let projected = project_schema(&self.schema, projection.as_ref())?;
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let selection = self.selection_for(&filter_refs);
        let inexact = matches!(&selection, Selection::Prune { .. });
        let target_partitions = state.config().target_partitions();
        let chunk_groups = match (&selection, self.analytics.as_deref()) {
            (Selection::All, Some(analytics)) => {
                partition_chunks((0..analytics.chunk_count).collect(), target_partitions)
            }
            (Selection::Prune { filters, .. }, Some(analytics)) => partition_chunks(
                ChunkPruner::new(
                    &self.output_schema,
                    analytics,
                    self.geometry_metrics.as_deref(),
                )
                .surviving_chunks(filters),
                target_partitions,
            ),
            // Plain body scans and direct R-tree selections remain indivisible.
            _ => vec![Arc::from(Vec::new())],
        };
        let scan = Scan::new(
            self.path.clone(),
            Arc::clone(&self.output_schema),
            Arc::clone(&projected),
            projection,
            self.analytics.clone(),
            self.geometry_metrics.clone(),
            selection,
            self.config,
            Arc::clone(&self.metrics),
        )?;
        let partitions = chunk_groups
            .into_iter()
            .map(|chunks| {
                Arc::new(FgbPartition::new(
                    scan.clone().with_chunks(chunks),
                    Arc::clone(&projected),
                )) as Arc<dyn PartitionStream>
            })
            .collect();
        Ok(Arc::new(StreamingTableExec::try_new(
            projected,
            partitions,
            None,
            std::iter::empty::<LexOrdering>(),
            false,
            if inexact { None } else { limit },
        )?))
    }
}
