//! Minimal pure-Rust in-memory DataFusion execution smoke test.

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use fgba_sql::EagerSession;
use futures::{executor::LocalPool, StreamExt};
use std::sync::Arc;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn eager_session_executes_and_rejects_unsupported_statements() -> TestResult {
    let mut pool = LocalPool::new();
    pool.run_until(async {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![2, 1])),
                Arc::new(StringArray::from(vec!["two", "one"])),
            ],
        )?;
        let session = EagerSession::new();
        session.register_batch("features", batch)?;

        let mut stream = session
            .execute("SELECT * FROM features ORDER BY id")
            .await?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id is not an Int64Array")?;
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("name is not a StringArray")?;
        assert_eq!(ids.value(0), 1);
        assert_eq!(names.value(0), "one");
        assert_eq!(ids.value(1), 2);
        assert_eq!(names.value(1), "two");

        assert!(session.execute("DELETE FROM features").await.is_err());
        assert!(session
            .execute("EXPLAIN ANALYZE SELECT * FROM features")
            .await
            .is_err());
        Ok(())
    })
}
