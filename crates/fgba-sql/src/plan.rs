//! Exact logical-plan recognition for metadata-only body counts.

use crate::table::FgbaTable;
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::{ScalarValue, TableReference};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::logical_plan::{FetchType, SkipType};
use datafusion::logical_expr::{Expr, LogicalPlan, Operator};
use fgba::io::SharedReader;
use flatgeobuf::packed_r_tree::PackedRTree;
use flatgeobuf::FgbReader;
use std::io::BufReader;
use std::sync::Arc;

/// Return a one-row count batch and literal fetch only for the exact count shape.
pub async fn metadata_count_plan(
    plan: &LogicalPlan,
    features: &FgbaTable,
) -> Result<Option<(RecordBatch, Option<usize>)>> {
    let (root, fetch) = match plan {
        LogicalPlan::Limit(limit) => {
            if !matches!(limit.get_skip_type(), Ok(SkipType::Literal(0))) {
                return Ok(None);
            }
            let fetch = match limit.get_fetch_type() {
                Ok(FetchType::Literal(fetch)) => fetch,
                _ => return Ok(None),
            };
            (limit.input.as_ref(), fetch)
        }
        other => (other, None),
    };
    let LogicalPlan::Projection(projection) = root else {
        return Ok(None);
    };
    if projection.expr.len() != 1
        || !direct_output(&projection.expr[0])
        || projection.schema.fields().len() != 1
        || projection.schema.fields()[0].data_type() != &DataType::Int64
    {
        return Ok(None);
    }
    let LogicalPlan::Aggregate(aggregate) = projection.input.as_ref() else {
        return Ok(None);
    };
    if !aggregate.group_expr.is_empty() || aggregate.aggr_expr.len() != 1 {
        return Ok(None);
    }
    let Expr::AggregateFunction(function) = &aggregate.aggr_expr[0] else {
        return Ok(None);
    };
    if function.func.name() != "count"
        || function.params.distinct
        || function.params.filter.is_some()
        || function.params.order_by.is_some()
        || function.params.null_treatment.is_some()
        || !matches!(
            function.params.args.as_slice(),
            [Expr::Literal(ScalarValue::Int64(Some(1)), _)]
        )
    {
        return Ok(None);
    }
    let (scan, bbox) = match aggregate.input.as_ref() {
        LogicalPlan::TableScan(scan) => (scan, None),
        LogicalPlan::Filter(filter) => {
            let LogicalPlan::TableScan(scan) = filter.input.as_ref() else {
                return Ok(None);
            };
            let Some(bbox) = canonical_bbox(&filter.predicate) else {
                return Ok(None);
            };
            (scan, Some(bbox))
        }
        _ => return Ok(None),
    };
    if !matches!(&scan.table_name, TableReference::Bare { table } if table.as_ref() == "features")
        || scan
            .projection
            .as_ref()
            .is_some_and(|projection| !projection.is_empty())
        || !scan.filters.is_empty()
        || scan.fetch.is_some()
    {
        return Ok(None);
    }
    if bbox.is_some() && !features.output_schema.has_spatial_index {
        return Ok(None);
    }

    let count = match bbox {
        None => features.output_schema.features_count,
        Some(bbox) => bbox_count(features, bbox).await?,
    };
    let count = i64::try_from(count)
        .map_err(|_| DataFusionError::Execution("COUNT(*) does not fit Int64".into()))?;
    let field = Arc::clone(&projection.schema.fields()[0]);
    let schema = Arc::new(Schema::new_with_metadata(
        vec![field],
        projection.schema.metadata().clone(),
    ));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![count]))])?;
    Ok(Some((batch, fetch)))
}

fn direct_output(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Alias(alias) => matches!(alias.expr.as_ref(), Expr::Column(_)),
        _ => false,
    }
}

pub(crate) fn canonical_bbox(expr: &Expr) -> Option<[f64; 4]> {
    let mut comparisons = Vec::new();
    if !collect_comparisons(expr, &mut comparisons) || comparisons.len() != 4 {
        return None;
    }
    let mut bbox = [0.0; 4];
    let mut seen = [false; 4];
    for (name, operator, value) in comparisons {
        let (index, output) = match (name, operator) {
            ("bbox_xmin", Operator::LtEq) => (0, 2),
            ("bbox_xmax", Operator::GtEq) => (1, 0),
            ("bbox_ymin", Operator::LtEq) => (2, 3),
            ("bbox_ymax", Operator::GtEq) => (3, 1),
            _ => return None,
        };
        if seen[index] {
            return None;
        }
        seen[index] = true;
        bbox[output] = value;
    }
    if seen.iter().all(|seen| *seen)
        && bbox.iter().all(|value| value.is_finite())
        && bbox[0] <= bbox[2]
        && bbox[1] <= bbox[3]
    {
        Some(bbox)
    } else {
        None
    }
}

fn collect_comparisons<'a>(expr: &'a Expr, output: &mut Vec<(&'a str, Operator, f64)>) -> bool {
    let Expr::BinaryExpr(binary) = expr else {
        return false;
    };
    if binary.op == Operator::And {
        return collect_comparisons(&binary.left, output)
            && collect_comparisons(&binary.right, output);
    }
    let Expr::Column(column) = binary.left.as_ref() else {
        return false;
    };
    if column
        .relation
        .as_ref()
        .is_some_and(|relation| relation.table() != "features")
    {
        return false;
    }
    let Some(value) = numeric_literal(&binary.right) else {
        return false;
    };
    if !value.is_finite() {
        return false;
    }
    output.push((&column.name, binary.op, value));
    true
}

fn numeric_literal(expr: &Expr) -> Option<f64> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    match value {
        ScalarValue::Float64(Some(value)) => Some(*value),
        ScalarValue::Float32(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Int64(Some(value)) => exact_integer_to_f64(i128::from(*value)),
        ScalarValue::Int32(Some(value)) => exact_integer_to_f64(i128::from(*value)),
        ScalarValue::Int16(Some(value)) => exact_integer_to_f64(i128::from(*value)),
        ScalarValue::Int8(Some(value)) => exact_integer_to_f64(i128::from(*value)),
        ScalarValue::UInt64(Some(value)) => exact_unsigned_to_f64(u128::from(*value)),
        ScalarValue::UInt32(Some(value)) => exact_unsigned_to_f64(u128::from(*value)),
        ScalarValue::UInt16(Some(value)) => exact_unsigned_to_f64(u128::from(*value)),
        ScalarValue::UInt8(Some(value)) => exact_unsigned_to_f64(u128::from(*value)),
        _ => None,
    }
}

fn exact_integer_to_f64(value: i128) -> Option<f64> {
    const MAX_EXACT: i128 = 1 << 53;
    if !(-MAX_EXACT..=MAX_EXACT).contains(&value) {
        return None;
    }
    Some(value as f64)
}

fn exact_unsigned_to_f64(value: u128) -> Option<f64> {
    const MAX_EXACT: u128 = 1 << 53;
    if value > MAX_EXACT {
        return None;
    }
    Some(value as f64)
}

async fn bbox_count(features: &FgbaTable, bbox: [f64; 4]) -> Result<u64> {
    let path = features.path.clone();
    let features_count = usize::try_from(features.output_schema.features_count).map_err(|_| {
        DataFusionError::Execution("features_count does not fit this platform".into())
    })?;
    let node_size = features.output_schema.index_node_size;
    let metrics = Arc::clone(&features.metrics);
    metrics.add_range();
    let result = tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(path).map_err(|error| error.to_string())?;
        let shared = SharedReader::new(BufReader::new(file));
        let header = FgbReader::open(shared.handle()).map_err(|error| error.to_string())?;
        drop(header);
        let mut source = shared.handle();
        let matches = PackedRTree::stream_search(
            &mut source,
            features_count,
            node_size,
            bbox[0],
            bbox[1],
            bbox[2],
            bbox[3],
        )
        .map_err(|error| error.to_string())?;
        Ok::<(usize, u64), String>((matches.len(), shared.bytes_read()))
    })
    .await
    .map_err(|error| DataFusionError::Execution(format!("R-tree worker failed: {error}")))?
    .map_err(DataFusionError::Execution)?;
    metrics.add_body_bytes(result.1);
    u64::try_from(result.0)
        .map_err(|_| DataFusionError::Execution("R-tree result count does not fit u64".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};

    fn bbox_expr() -> Expr {
        col("bbox_xmin")
            .lt_eq(lit(10i64))
            .and(col("bbox_xmax").gt_eq(lit(-10i64)))
            .and(col("bbox_ymin").lt_eq(lit(60i64)))
            .and(col("bbox_ymax").gt_eq(lit(40i64)))
    }

    #[test]
    fn exact_numeric_literals_are_accepted() {
        assert_eq!(
            canonical_bbox(&bbox_expr()),
            Some([-10.0, 40.0, 10.0, 60.0])
        );
        let float32_expr = col("bbox_xmin")
            .lt_eq(lit(10f32))
            .and(col("bbox_xmax").gt_eq(lit(-10f32)))
            .and(col("bbox_ymin").lt_eq(lit(60f32)))
            .and(col("bbox_ymax").gt_eq(lit(40f32)));
        assert_eq!(
            canonical_bbox(&float32_expr),
            Some([-10.0, 40.0, 10.0, 60.0])
        );
    }

    #[test]
    fn unsafe_integer_or_nonfinite_literals_are_rejected() {
        let large = col("bbox_xmin")
            .lt_eq(lit(10i64))
            .and(col("bbox_xmax").gt_eq(lit(-10i64)))
            .and(col("bbox_ymin").lt_eq(lit(60i64)))
            .and(col("bbox_ymax").gt_eq(lit(9_007_199_254_740_993i64)));
        assert_eq!(canonical_bbox(&large), None);
        let nan = col("bbox_xmin")
            .lt_eq(lit(10i64))
            .and(col("bbox_xmax").gt_eq(lit(-10i64)))
            .and(col("bbox_ymin").lt_eq(lit(60i64)))
            .and(col("bbox_ymax").gt_eq(lit(f64::NAN)));
        assert_eq!(canonical_bbox(&nan), None);
    }
}
