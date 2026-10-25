//! Streaming CSV output and the synchronous public entry point.

use crate::session::{register_for_sql, QueryOptions};
use crate::{Error, Result};
use arrow_csv::WriterBuilder;
use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::SQLOptions;
use futures::StreamExt;
use std::io::Write;
use std::path::Path;

/// Execute one read-only SQL statement and stream its result as CSV.
///
/// Registration and feature schema/statistics reads are synchronous. A5 band
/// payloads are loaded only by DataFusion scans of their lazy providers. A
/// current-thread Tokio runtime is created for planning, execution, and source
/// streaming. No result `collect` is performed.
pub fn query_csv(path: &Path, sql: &str, options: QueryOptions, writer: impl Write) -> Result<()> {
    if sql.trim().is_empty() {
        return Err(Error::InvalidInput("SQL must not be empty".into()));
    }
    let session = register_for_sql(path, sql, &options)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = session.ctx.sql_with_options(sql, sql_options).await?;
        let mut stream = session.execute_dataframe(df).await?;
        let mut csv = WriterBuilder::new().with_header(true).build(writer);
        // Establish exactly one header even when the stream has no rows.
        csv.write(&RecordBatch::new_empty(stream.schema()))?;
        while let Some(batch) = stream.next().await {
            csv.write(&batch?)?;
        }
        Ok(())
    })
}
