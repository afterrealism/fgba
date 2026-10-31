//! Minimal read-only DataFusion execution over in-memory record batches.

use crate::{Error, Result};
use datafusion::arrow::array::{
    BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt16Array, UInt32Array,
    UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{execute_stream, ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};
use fgba::{BodyAnalytics, ColumnAnalytics, ValueClass};
use sqlparser::ast::{
    Expr, Ident, ObjectName, Query, SelectItem, SetExpr, Statement, TableFactor, Visit, Visitor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeSet;
use std::ops::ControlFlow;
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
    pub fn register_batch(&self, name: &str, batch: RecordBatch) -> DataFusionResult<()> {
        self.register_batches(name, vec![batch])
    }

    /// Record batches as one in-memory partition.
    pub fn register_batches(&self, name: &str, batches: Vec<RecordBatch>) -> DataFusionResult<()> {
        let schema = batches.first().map(|batch| batch.schema()).ok_or_else(|| {
            DataFusionError::Plan(format!("cannot register {name:?} without a schema"))
        })?;
        let table = MemTable::try_new(schema, vec![batches])?;
        self.ctx.register_table(name, Arc::new(table))?;
        Ok(())
    }

    /// Plan and execute one read-only SQL statement as a single stream.
    pub async fn execute(&self, sql: &str) -> DataFusionResult<SendableRecordBatchStream> {
        validate_sql(sql).map_err(|error| DataFusionError::Plan(error.to_string()))?;
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

fn reject_unsupported_plan(plan: &Arc<dyn ExecutionPlan>) -> DataFusionResult<()> {
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

/// One materializable browser SQL source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SqlSource {
    /// Exact FlatGeobuf features.
    Features,
    /// One raw kind-1 aggregate band.
    A5Band(u8),
    /// The explicitly approximate viewport relation.
    A5ViewportApprox,
    /// Exact kind-2 global property statistics.
    AttributeStats,
}

/// One column reference retained for conservative source projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlColumn {
    /// Identifier text without qualification.
    pub name: String,
    /// Whether SQL quoting requires an exact case-sensitive match.
    pub quoted: bool,
}

/// Parsed read-only SQL and the sources it actually references.
#[derive(Debug, Clone)]
pub struct ValidatedSql {
    /// Base relations, excluding CTE names and aliases.
    pub sources: BTreeSet<SqlSource>,
    /// Column identifiers used to over-approximate a safe feature projection.
    pub columns: Vec<SqlColumn>,
    /// Whether any SELECT list contains a wildcard, requiring exact body fallback.
    pub wildcard: bool,
}

/// Validate exactly one SELECT or plain EXPLAIN and inventory its AST relations.
pub fn validate_sql(sql: &str) -> Result<ValidatedSql> {
    if sql.trim().is_empty() {
        return Err(Error::InvalidInput("SQL must not be empty".into()));
    }
    let statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|error| Error::InvalidInput(format!("cannot parse SQL: {error}")))?;
    let statement = match statements.as_slice() {
        [statement] => statement,
        _ => {
            return Err(Error::InvalidInput(
                "exactly one SQL statement is required".into(),
            ))
        }
    };
    match statement {
        Statement::Query(_) => {}
        Statement::Explain {
            analyze,
            statement,
            options,
            ..
        } => {
            let utility_analyze = options.as_ref().is_some_and(|options| {
                options
                    .iter()
                    .any(|option| option.name.value.eq_ignore_ascii_case("analyze"))
            });
            if *analyze || utility_analyze {
                return Err(Error::InvalidInput(
                    "EXPLAIN ANALYZE is not read-only eager SQL".into(),
                ));
            }
            if !matches!(statement.as_ref(), Statement::Query(_)) {
                return Err(Error::InvalidInput(
                    "EXPLAIN requires one read-only SELECT".into(),
                ));
            }
        }
        _ => {
            return Err(Error::InvalidInput(
                "only one SELECT or read-only EXPLAIN is allowed".into(),
            ))
        }
    }

    let mut analysis = SqlAnalysis::default();
    match statement.visit(&mut analysis) {
        ControlFlow::Continue(()) => Ok(ValidatedSql {
            sources: analysis.sources,
            columns: analysis.columns,
            wildcard: analysis.wildcard,
        }),
        ControlFlow::Break(error) => Err(Error::InvalidInput(error)),
    }
}

#[derive(Default)]
struct SqlAnalysis {
    sources: BTreeSet<SqlSource>,
    ctes: BTreeSet<String>,
    columns: Vec<SqlColumn>,
    wildcard: bool,
}

impl SqlAnalysis {
    fn select_body(&mut self, body: &SetExpr) -> ControlFlow<String> {
        match body {
            SetExpr::Select(select) => {
                if select.into.is_some() {
                    return ControlFlow::Break("SELECT INTO is not read-only SQL".into());
                }
                if select.projection.iter().any(|item| {
                    matches!(
                        item,
                        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
                    )
                }) {
                    self.wildcard = true;
                }
                ControlFlow::Continue(())
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.select_body(left)?;
                self.select_body(right)
            }
            SetExpr::Query(_) => ControlFlow::Continue(()),
            _ => ControlFlow::Break("only SELECT query bodies are allowed".into()),
        }
    }

    fn relation(&mut self, relation: &ObjectName) -> ControlFlow<String> {
        let [part] = relation.0.as_slice() else {
            return ControlFlow::Break(format!(
                "qualified or empty relation {relation} is not allowed"
            ));
        };
        let Some(ident) = part.as_ident() else {
            return ControlFlow::Break(format!("relation {relation} is not allowed"));
        };
        let name = normalized_ident(ident);
        if self.ctes.contains(&name) {
            return ControlFlow::Continue(());
        }
        let source = match name.as_str() {
            "features" => SqlSource::Features,
            "a5_viewport_approx" => SqlSource::A5ViewportApprox,
            "attribute_stats" => SqlSource::AttributeStats,
            _ => {
                let Some(res) = name.strip_prefix("a5_band_") else {
                    return ControlFlow::Break(format!("unknown SQL source {relation}"));
                };
                if res.is_empty()
                    || !res.bytes().all(|byte| byte.is_ascii_digit())
                    || res.starts_with('0') && res.len() > 1
                {
                    return ControlFlow::Break(format!("unknown SQL source {relation}"));
                }
                let Ok(res) = res.parse::<u8>() else {
                    return ControlFlow::Break(format!("unknown SQL source {relation}"));
                };
                if res > 30 || res.to_string() != name[8..] {
                    return ControlFlow::Break(format!("unknown SQL source {relation}"));
                }
                SqlSource::A5Band(res)
            }
        };
        self.sources.insert(source);
        ControlFlow::Continue(())
    }

    fn column(&mut self, ident: &Ident) {
        self.columns.push(SqlColumn {
            name: ident.value.clone(),
            quoted: ident.quote_style.is_some(),
        });
    }
}

impl Visitor for SqlAnalysis {
    type Break = String;

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if !query.locks.is_empty() {
            return ControlFlow::Break("SELECT locking clauses are not read-only SQL".into());
        }
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.ctes.insert(normalized_ident(&cte.alias.name));
            }
        }
        self.select_body(&query.body)
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
        match factor {
            TableFactor::Table { args: Some(_), .. }
            | TableFactor::TableFunction { .. }
            | TableFactor::Function { .. }
            | TableFactor::UNNEST { .. }
            | TableFactor::JsonTable { .. }
            | TableFactor::OpenJsonTable { .. } => ControlFlow::Break(
                "external table functions and table-valued arguments are not allowed".into(),
            ),
            _ => ControlFlow::Continue(()),
        }
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        self.relation(relation)
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Identifier(ident) => self.column(ident),
            Expr::CompoundIdentifier(idents) => {
                if let Some(ident) = idents.last() {
                    self.column(ident);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn normalized_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

pub(crate) fn cells_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("cell_id", DataType::UInt64, false),
        Field::new("count", DataType::UInt32, false),
    ]))
}

/// Build a one-partition A5 cell table shared by native and eager sessions.
pub fn cells_table(cells: &[(u64, u32)]) -> Result<Arc<MemTable>> {
    let batch = cells_batch(cells)?;
    Ok(Arc::new(MemTable::try_new(
        batch.schema(),
        vec![vec![batch]],
    )?))
}

/// Build the bounded A5 cell values without registering a provider.
pub fn cells_batch(cells: &[(u64, u32)]) -> Result<RecordBatch> {
    let ids = cells.iter().map(|cell| cell.0).collect::<Vec<_>>();
    let counts = cells.iter().map(|cell| cell.1).collect::<Vec<_>>();
    Ok(RecordBatch::try_new(
        cells_schema(),
        vec![
            Arc::new(UInt64Array::from(ids)),
            Arc::new(UInt32Array::from(counts)),
        ],
    )?)
}

/// Build the shared global-statistics table.
#[allow(clippy::type_complexity)]
pub fn attribute_stats_table(analytics: &BodyAnalytics) -> Result<Arc<MemTable>> {
    let batch = attribute_stats_batch(analytics)?;
    Ok(Arc::new(MemTable::try_new(
        batch.schema(),
        vec![vec![batch]],
    )?))
}

/// Build global-statistics values without reading Arrow payloads.
#[allow(clippy::type_complexity)]
pub fn attribute_stats_batch(analytics: &BodyAnalytics) -> Result<RecordBatch> {
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
    Ok(batch)
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
