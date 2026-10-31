//! Minimal read-only DataFusion execution over in-memory record batches.

use datafusion::arrow::array::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{execute_stream, ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};
use std::sync::Arc;

/// A read-only, single-partition DataFusion session for in-memory batches.
///
/// Execution is directly awaited and does not create a runtime or spawn work.
/// Plans that require analysis or repartitioning are rejected.
pub struct EagerSession {
    ctx: SessionContext,
}

impl EagerSession {
    /// Create a session with DataFusion's default no-spill behavior.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one record batch as a one-partition in-memory table.
    pub fn register_batch(&self, name: &str, batch: RecordBatch) -> Result<()> {
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        self.ctx.register_table(name, Arc::new(table))?;
        Ok(())
    }

    /// Plan and execute one read-only SQL statement as a single stream.
    pub async fn execute(&self, sql: &str) -> Result<SendableRecordBatchStream> {
        let options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = self.ctx.sql_with_options(sql, options).await?;
        let task_ctx = Arc::new(df.task_ctx());
        let plan = df.create_physical_plan().await?;
        reject_unsupported_plan(&plan)?;
        let partitions = plan.properties().output_partitioning().partition_count();
        if partitions != 1 {
            return Err(DataFusionError::Plan(format!(
                "EagerSession requires one output partition, got {partitions}"
            )));
        }
        execute_stream(plan, task_ctx)
    }
}

impl Default for EagerSession {
    fn default() -> Self {
        let config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);
        crate::udf_geom::register(&ctx);
        Self { ctx }
    }
}

fn reject_unsupported_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
    if plan.as_any().is::<AnalyzeExec>()
        || plan.as_any().is::<RepartitionExec>()
        || plan.as_any().is::<CoalescePartitionsExec>()
    {
        return Err(DataFusionError::Plan(format!(
            "{} is not supported by EagerSession",
            plan.name()
        )));
    }
    for child in plan.children() {
        reject_unsupported_plan(child)?;
    }
    Ok(())
}
