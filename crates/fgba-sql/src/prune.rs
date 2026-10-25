//! Conservative numeric/null zone-map evaluation.
//!
//! A skipped chunk is a proof that no row can satisfy the predicate. Every
//! other case stays a candidate and DataFusion applies the residual predicate.

use crate::schema::{geometry_metric_id, OutputSchema, PropertySchema};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, Operator};
use fgba::{BodyAnalytics, ChunkStat, GeometryMetrics, ValueClass};
use flatgeobuf::ColumnType;

/// Whether one chunk can be eliminated without evaluating its rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Candidate {
    /// The predicate may match; residual row evaluation is required.
    Keep,
    /// No row can satisfy the predicate.
    Skip,
}

impl Candidate {
    fn and(self, other: Self) -> Self {
        if self == Self::Skip || other == Self::Skip {
            Self::Skip
        } else {
            Self::Keep
        }
    }
}

#[derive(Clone, Copy)]
enum ZoneColumn<'a> {
    Property(&'a PropertySchema),
    Metric(u16),
}

/// Evaluate loaded BODY_ANALYTICS metadata without reading feature payloads.
pub struct ChunkPruner<'a> {
    schema: &'a OutputSchema,
    analytics: &'a BodyAnalytics,
    geometry_metrics: Option<&'a GeometryMetrics>,
}

impl<'a> ChunkPruner<'a> {
    /// Create a pruner over one loaded schema and metadata table.
    pub fn new(
        schema: &'a OutputSchema,
        analytics: &'a BodyAnalytics,
        geometry_metrics: Option<&'a GeometryMetrics>,
    ) -> Self {
        Self {
            schema,
            analytics,
            geometry_metrics,
        }
    }

    /// Return chunks for which no conjunction member proves elimination.
    pub fn surviving_chunks(&self, filters: &[Expr]) -> Vec<u64> {
        (0..self.analytics.chunk_count)
            .filter(|&chunk| {
                filters
                    .iter()
                    .all(|expr| self.candidate(chunk, expr) == Candidate::Keep)
            })
            .collect()
    }

    /// Evaluate one expression against one chunk without reading its rows.
    pub fn candidate(&self, chunk_index: u64, expr: &Expr) -> Candidate {
        let Expr::BinaryExpr(binary) = expr else {
            return self.null_or_keep(chunk_index, expr);
        };
        if binary.op == Operator::And {
            return self
                .candidate(chunk_index, &binary.left)
                .and(self.candidate(chunk_index, &binary.right));
        }
        self.comparison(chunk_index, expr)
            .unwrap_or(Candidate::Keep)
    }

    fn null_or_keep(&self, chunk_index: u64, expr: &Expr) -> Candidate {
        let (value, not_null) = match expr {
            Expr::IsNull(value) => (value, false),
            Expr::IsNotNull(value) => (value, true),
            _ => return Candidate::Keep,
        };
        let Some(column) = zone_column(self.schema, self.geometry_metrics.is_some(), value) else {
            return Candidate::Keep;
        };
        let Some((_, _, stat)) = self.stat(column, chunk_index) else {
            return Candidate::Keep;
        };
        let Some(rows) = self.analytics.chunk_row_count(chunk_index) else {
            return Candidate::Keep;
        };
        if (not_null && stat.non_null_count == 0) || (!not_null && stat.non_null_count == rows) {
            Candidate::Skip
        } else {
            Candidate::Keep
        }
    }

    fn comparison(&self, chunk_index: u64, expr: &Expr) -> Option<Candidate> {
        let (column, operator, literal) =
            comparison(self.schema, self.geometry_metrics.is_some(), expr)?;
        let (class, kind, stat) = self.stat(column, chunk_index)?;
        Some(evaluate_numeric(class, kind, stat, operator, literal))
    }

    fn stat(
        &self,
        column: ZoneColumn<'_>,
        chunk_index: u64,
    ) -> Option<(ValueClass, ColumnType, &ChunkStat)> {
        let chunk_index = usize::try_from(chunk_index).ok()?;
        match column {
            ZoneColumn::Property(property) => {
                let column = self.analytics.column(property.column_index)?;
                let stat = column.chunks.get(chunk_index)?;
                Some((column.descriptor.value_class, property.kind, stat))
            }
            ZoneColumn::Metric(metric_id) => {
                let column = self.geometry_metrics?.column(metric_id)?;
                let stat = column.chunks.get(chunk_index)?;
                if metric_id == 2 {
                    Some((ValueClass::Unsigned, ColumnType::UInt, stat))
                } else {
                    Some((ValueClass::Float, ColumnType::Double, stat))
                }
            }
        }
    }
}

/// Whether DataFusion may pass this expression as an inexact zone-map filter.
pub(crate) fn supports_filter(
    schema: &OutputSchema,
    has_geometry_metrics: bool,
    expr: &Expr,
) -> bool {
    match expr {
        Expr::IsNull(value) | Expr::IsNotNull(value) => {
            zone_column(schema, has_geometry_metrics, value).is_some()
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            supports_filter(schema, has_geometry_metrics, &binary.left)
                || supports_filter(schema, has_geometry_metrics, &binary.right)
        }
        _ => comparison(schema, has_geometry_metrics, expr).is_some(),
    }
}

fn zone_column<'a>(
    schema: &'a OutputSchema,
    has_geometry_metrics: bool,
    expr: &Expr,
) -> Option<ZoneColumn<'a>> {
    let Expr::Column(column) = expr else {
        return None;
    };
    if column
        .relation
        .as_ref()
        .is_some_and(|relation| relation.table() != "features")
    {
        return None;
    }
    if let Some(property) = schema
        .properties
        .iter()
        .find(|property| property.name == column.name)
    {
        return Some(ZoneColumn::Property(property));
    }
    if has_geometry_metrics {
        return geometry_metric_id(&column.name).map(ZoneColumn::Metric);
    }
    None
}

fn comparison<'a>(
    schema: &'a OutputSchema,
    has_geometry_metrics: bool,
    expr: &'a Expr,
) -> Option<(ZoneColumn<'a>, Operator, &'a ScalarValue)> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if !matches!(
        binary.op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
    ) {
        return None;
    }
    let (column, operator, literal) =
        if let Some(column) = zone_column(schema, has_geometry_metrics, &binary.left) {
            let Expr::Literal(literal, _) = binary.right.as_ref() else {
                return None;
            };
            (column, binary.op, literal)
        } else {
            let column = zone_column(schema, has_geometry_metrics, &binary.right)?;
            let Expr::Literal(literal, _) = binary.left.as_ref() else {
                return None;
            };
            (column, flip(binary.op), literal)
        };
    if !numeric_column(column) || !numeric_literal(literal) {
        return None;
    }
    Some((column, operator, literal))
}

fn numeric_column(column: ZoneColumn<'_>) -> bool {
    match column {
        ZoneColumn::Property(property) => numeric_property(property),
        ZoneColumn::Metric(_) => true,
    }
}

fn numeric_property(property: &PropertySchema) -> bool {
    !matches!(
        property.kind,
        ColumnType::Bool
            | ColumnType::String
            | ColumnType::Json
            | ColumnType::DateTime
            | ColumnType::Binary
    )
}

fn numeric_literal(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Float16(Some(_))
            | ScalarValue::Float32(Some(_))
            | ScalarValue::Float64(Some(_))
            | ScalarValue::Decimal128(Some(_), _, _)
            | ScalarValue::Decimal256(Some(_), _, _)
            | ScalarValue::Int8(Some(_))
            | ScalarValue::Int16(Some(_))
            | ScalarValue::Int32(Some(_))
            | ScalarValue::Int64(Some(_))
            | ScalarValue::UInt8(Some(_))
            | ScalarValue::UInt16(Some(_))
            | ScalarValue::UInt32(Some(_))
            | ScalarValue::UInt64(Some(_))
    )
}

fn flip(operator: Operator) -> Operator {
    match operator {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

#[derive(Clone, Copy)]
enum Literal {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}

fn parse_literal(value: &ScalarValue) -> Option<Literal> {
    match value {
        ScalarValue::Int8(Some(value)) => Some(Literal::Signed(i64::from(*value))),
        ScalarValue::Int16(Some(value)) => Some(Literal::Signed(i64::from(*value))),
        ScalarValue::Int32(Some(value)) => Some(Literal::Signed(i64::from(*value))),
        ScalarValue::Int64(Some(value)) => Some(Literal::Signed(*value)),
        ScalarValue::UInt8(Some(value)) => Some(Literal::Unsigned(u64::from(*value))),
        ScalarValue::UInt16(Some(value)) => Some(Literal::Unsigned(u64::from(*value))),
        ScalarValue::UInt32(Some(value)) => Some(Literal::Unsigned(u64::from(*value))),
        ScalarValue::UInt64(Some(value)) => Some(Literal::Unsigned(*value)),
        ScalarValue::Float32(Some(value)) => Some(Literal::Float(f64::from(*value))),
        ScalarValue::Float64(Some(value)) => Some(Literal::Float(*value)),
        _ => None,
    }
}

fn evaluate_numeric(
    class: ValueClass,
    kind: ColumnType,
    stat: &ChunkStat,
    operator: Operator,
    literal: &ScalarValue,
) -> Candidate {
    // Every numeric comparison with NULL is NULL, never true.
    if stat.non_null_count == 0 {
        return Candidate::Skip;
    }
    let Some(literal) = parse_literal(literal) else {
        return Candidate::Keep;
    };
    match (class, literal) {
        (ValueClass::Signed, Literal::Signed(value)) => signed(stat, operator, value),
        (ValueClass::Unsigned, Literal::Unsigned(value)) => unsigned(stat, operator, value),
        (ValueClass::Float, literal) => {
            let Some(value) = exact_float_literal(kind, literal) else {
                return Candidate::Keep;
            };
            float(stat, operator, value)
        }
        (ValueClass::Unsigned, Literal::Signed(value)) if value < 0 => match operator {
            Operator::Eq | Operator::Lt | Operator::LtEq => Candidate::Skip,
            _ => Candidate::Keep,
        },
        (ValueClass::Signed, Literal::Unsigned(value)) if value > i64::MAX as u64 => match operator
        {
            Operator::Eq | Operator::Gt | Operator::GtEq => Candidate::Skip,
            _ => Candidate::Keep,
        },
        _ => Candidate::Keep,
    }
}

fn exact_float_literal(kind: ColumnType, literal: Literal) -> Option<f64> {
    const MAX_EXACT: u64 = 1 << 53;
    let value = match literal {
        Literal::Float(value) => value,
        Literal::Signed(value) if value.unsigned_abs() <= MAX_EXACT => value as f64,
        Literal::Unsigned(value) if value <= MAX_EXACT => value as f64,
        _ => return None,
    };
    if kind == ColumnType::Float {
        let narrowed = value as f32;
        if narrowed.is_nan() || f64::from(narrowed) != value {
            return None;
        }
    }
    Some(value)
}

fn signed(stat: &ChunkStat, operator: Operator, value: i64) -> Candidate {
    if !stat.has_bounds() {
        return Candidate::Keep;
    }
    compare(
        operator,
        value,
        i64::from_le_bytes(stat.min_bits.to_le_bytes()),
        i64::from_le_bytes(stat.max_bits.to_le_bytes()),
    )
}

fn unsigned(stat: &ChunkStat, operator: Operator, value: u64) -> Candidate {
    if !stat.has_bounds() {
        return Candidate::Keep;
    }
    compare(operator, value, stat.min_bits, stat.max_bits)
}

fn float(stat: &ChunkStat, operator: Operator, value: f64) -> Candidate {
    if value.is_nan() {
        return Candidate::Keep;
    }
    if !stat.has_bounds() {
        return if operator == Operator::Eq && value.is_finite() {
            Candidate::Skip
        } else {
            Candidate::Keep
        };
    }
    if stat.nan_count > 0 {
        // NaN is not NULL. Only finite equality outside the non-NaN bounds has
        // a self-contained elimination proof in this conservative pruner.
        return if operator == Operator::Eq && value.is_finite() && {
            let min = f64::from_bits(stat.min_bits);
            let max = f64::from_bits(stat.max_bits);
            value < min || value > max
        } {
            Candidate::Skip
        } else {
            Candidate::Keep
        };
    }
    compare(
        operator,
        value,
        f64::from_bits(stat.min_bits),
        f64::from_bits(stat.max_bits),
    )
}

fn compare<T: PartialOrd + PartialEq + Copy>(
    operator: Operator,
    value: T,
    min: T,
    max: T,
) -> Candidate {
    let disjoint = match operator {
        Operator::Eq => value < min || value > max,
        Operator::NotEq => min == max && max == value,
        Operator::Lt => min >= value,
        Operator::LtEq => min > value,
        Operator::Gt => max <= value,
        Operator::GtEq => max < value,
        _ => false,
    };
    if disjoint {
        Candidate::Skip
    } else {
        Candidate::Keep
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stat(min: f64, max: f64, non_null_count: u32, nan_count: u32) -> ChunkStat {
        ChunkStat {
            min_bits: min.to_bits(),
            max_bits: max.to_bits(),
            non_null_count,
            nan_count,
        }
    }

    #[test]
    fn nan_rules_require_a_specific_proof() {
        let with_nan = stat(10.0, 10.0, 2, 1);
        assert_eq!(float(&with_nan, Operator::Eq, 200.0), Candidate::Skip);
        assert_eq!(float(&with_nan, Operator::Gt, 100.0), Candidate::Keep);
        assert_eq!(float(&with_nan, Operator::NotEq, 10.0), Candidate::Keep);
        assert_eq!(float(&with_nan, Operator::Eq, f64::NAN), Candidate::Keep);

        let same = stat(10.0, 10.0, 2, 0);
        assert_eq!(float(&same, Operator::NotEq, 10.0), Candidate::Skip);
        assert_eq!(float(&same, Operator::Gt, f64::INFINITY), Candidate::Skip);
        assert_eq!(float(&same, Operator::Eq, f64::INFINITY), Candidate::Skip);
    }

    #[test]
    fn f32_and_integer_conversions_are_exact_or_keep() {
        assert_eq!(
            exact_float_literal(ColumnType::Float, Literal::Float(1.5)),
            Some(1.5)
        );
        assert_eq!(
            exact_float_literal(ColumnType::Float, Literal::Float(1.00000001)),
            None
        );
        assert_eq!(
            exact_float_literal(ColumnType::Double, Literal::Signed(1 << 53)),
            Some((1i64 << 53) as f64)
        );
        assert_eq!(
            exact_float_literal(ColumnType::Double, Literal::Signed((1 << 53) + 1)),
            None
        );
    }

    #[test]
    fn integer_not_equal_and_literal_left_normalization_are_conservative() {
        let stat = ChunkStat {
            min_bits: 10,
            max_bits: 10,
            non_null_count: 2,
            nan_count: 0,
        };
        assert_eq!(unsigned(&stat, Operator::NotEq, 10), Candidate::Skip);
        assert_eq!(unsigned(&stat, Operator::NotEq, 11), Candidate::Keep);
        assert_eq!(unsigned(&stat, Operator::Eq, 11), Candidate::Skip);
        assert_eq!(flip(Operator::Lt), Operator::Gt);
        assert_eq!(flip(Operator::GtEq), Operator::LtEq);
        assert_eq!(Candidate::Keep.and(Candidate::Skip), Candidate::Skip);
        assert_eq!(Candidate::Keep.and(Candidate::Keep), Candidate::Keep);
    }

    #[test]
    fn mixed_integer_classes_need_disjoint_proofs() {
        let unsigned_stat = ChunkStat {
            min_bits: 0,
            max_bits: 10,
            non_null_count: 2,
            nan_count: 0,
        };
        assert_eq!(
            evaluate_numeric(
                ValueClass::Unsigned,
                ColumnType::ULong,
                &unsigned_stat,
                Operator::Lt,
                &ScalarValue::Int64(Some(-1))
            ),
            Candidate::Skip
        );
        assert_eq!(
            evaluate_numeric(
                ValueClass::Unsigned,
                ColumnType::ULong,
                &unsigned_stat,
                Operator::NotEq,
                &ScalarValue::Int64(Some(-1))
            ),
            Candidate::Keep
        );
        assert_eq!(
            evaluate_numeric(
                ValueClass::Unsigned,
                ColumnType::ULong,
                &unsigned_stat,
                Operator::Eq,
                &ScalarValue::Int64(Some(1))
            ),
            Candidate::Keep
        );

        let signed_stat = ChunkStat {
            min_bits: u64::from_le_bytes((-10i64).to_le_bytes()),
            max_bits: 10,
            non_null_count: 2,
            nan_count: 0,
        };
        assert_eq!(
            evaluate_numeric(
                ValueClass::Signed,
                ColumnType::Long,
                &signed_stat,
                Operator::Gt,
                &ScalarValue::UInt64(Some(u64::MAX))
            ),
            Candidate::Skip
        );
        assert_eq!(
            evaluate_numeric(
                ValueClass::Signed,
                ColumnType::Long,
                &signed_stat,
                Operator::Lt,
                &ScalarValue::UInt64(Some(u64::MAX))
            ),
            Candidate::Keep
        );
    }
}
