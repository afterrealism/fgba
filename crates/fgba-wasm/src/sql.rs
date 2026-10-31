//! Bounded eager FGBA source materialization for browser SQL.

pub use crate::arrow::SqlLimits;
use crate::arrow::{execute_json, Budget};
use crate::range::SparseRange;
use crate::reader::find_miss;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use fgba::geometry::BboxSink;
use fgba::io::SharedReader;
use fgba::{feature_metrics, BodyAnalytics, BodyRange, GeometryMetrics, Reader};
use fgba_sql::batch::{
    decode_arrow_chunk, make_batch, plan_source_mode, FeatureBatchBuilder, SourceMode,
};
use fgba_sql::memory::{attribute_stats_batch, cells_batch, validate_sql, SqlColumn, SqlSource};
use fgba_sql::{EagerSession, Error, OutputSchema, Result};
use flatgeobuf::geozero::{ColumnValue, FeatureProperties, PropertyProcessor};
use flatgeobuf::{FallibleStreamingIterator, FgbReader};
use geozero::{GeozeroGeometry, ToWkb};
use std::collections::BTreeSet;
use std::io::{Seek, SeekFrom};
use std::sync::Arc;

const BODY_BATCH_ROWS: usize = 4096;
const BODY_BATCH_BYTES: usize = 8 * 1024 * 1024;

/// One bounded SQL request over the already opened sparse object.
#[derive(Debug, Clone)]
pub struct SqlRequest {
    /// Exactly one SELECT or read-only EXPLAIN statement.
    pub sql: String,
    /// Optional viewport for `a5_viewport_approx`.
    pub bbox: Option<[f64; 4]>,
    /// Optional A5 resolution for the viewport relation.
    pub res: Option<u8>,
    /// Source and output limits, clamped to the hard caps.
    pub limits: SqlLimits,
}

/// Materialize only the request's referenced relations and execute DataFusion
/// directly over one-partition in-memory batches.
pub async fn query_sql(source: SparseRange, request: SqlRequest) -> Result<String> {
    request.limits.validate()?;
    validate_viewport(request.bbox, request.res)?;
    let validated = validate_sql(&request.sql)?;
    let session = EagerSession::new();
    let mut budget = Budget::new(request.limits);

    let needs_feature_metadata = validated.sources.contains(&SqlSource::Features)
        || validated.sources.contains(&SqlSource::AttributeStats);
    let context = if needs_feature_metadata {
        Some(FeatureContext::open(&source, &mut budget)?)
    } else {
        None
    };

    for source_name in &validated.sources {
        match *source_name {
            SqlSource::Features => {
                let context = context
                    .as_ref()
                    .ok_or_else(|| Error::Format("feature metadata was not opened".into()))?;
                let batches = materialize_features(
                    &source,
                    context,
                    &validated.columns,
                    validated.wildcard,
                    &mut budget,
                )?;
                session.register_batches("features", batches)?;
            }
            SqlSource::A5Band(res) => {
                let batch = materialize_band(&source, res, &mut budget)?;
                session.register_batch(&format!("a5_band_{res}"), batch)?;
            }
            SqlSource::A5ViewportApprox => {
                let batch = materialize_viewport(&source, request.bbox, request.res, &mut budget)?;
                session.register_batch("a5_viewport_approx", batch)?;
            }
            SqlSource::AttributeStats => {
                let context = context
                    .as_ref()
                    .ok_or_else(|| Error::Format("feature metadata was not opened".into()))?;
                let analytics = context.analytics.as_ref().ok_or_else(|| {
                    Error::InvalidInput(
                        "attribute_stats requires valid BODY_ANALYTICS metadata".into(),
                    )
                })?;
                budget.reserve_rows(analytics.columns.len() as u64)?;
                let batch = attribute_stats_batch(analytics)?;
                let mut batches = Vec::new();
                budget.retain(&mut batches, batch)?;
                session.register_batches("attribute_stats", batches)?;
            }
        }
    }
    execute_json(&session, &request.sql, request.limits).await
}

fn validate_viewport(bbox: Option<[f64; 4]>, res: Option<u8>) -> Result<()> {
    if res.is_some_and(|res| res > 30) {
        return Err(Error::InvalidInput("res must be in 0..=30".into()));
    }
    if let Some(bbox) = bbox {
        if bbox.iter().any(|value| !value.is_finite()) || bbox[0] > bbox[2] || bbox[1] > bbox[3] {
            return Err(Error::InvalidInput(
                "bbox must be finite and ordered as minX,minY,maxX,maxY".into(),
            ));
        }
    }
    Ok(())
}

fn open_reader(source: &SparseRange) -> Result<Reader<SparseRange>> {
    let mut source = source.clone();
    source.seek(SeekFrom::Start(0))?;
    Ok(Reader::open(source)?)
}

struct FeatureContext {
    output: Arc<OutputSchema>,
    body_len: u64,
    index_start: u64,
    analytics: Option<BodyAnalytics>,
    geometry_metrics: Option<GeometryMetrics>,
}

impl FeatureContext {
    fn open(source: &SparseRange, budget: &mut Budget) -> Result<Self> {
        let mut body = source.clone();
        body.seek(SeekFrom::Start(0))?;
        let fgb = FgbReader::open(&mut body)?;
        let output = Arc::new(OutputSchema::from_header(fgb.header())?);
        drop(fgb);
        let index_start = body.stream_position()?;

        let mut reader = open_reader(source)?;
        let body_len = reader.body_len();
        let analytics = match reader.read_body_analytics_metadata() {
            Ok(analytics) => analytics,
            Err(fgba::Error::Format(_)) => None,
            Err(error) => return Err(error.into()),
        };
        let geometry_metrics = if analytics.is_some() {
            match reader.read_geometry_metrics_metadata() {
                Ok(metrics) => metrics,
                Err(fgba::Error::Format(_)) => None,
                Err(error) => return Err(error.into()),
            }
        } else {
            None
        };
        if let Some(analytics) = &analytics {
            budget.reserve_encoded(analytics.metadata_bytes_read)?;
        }
        if let Some(metrics) = &geometry_metrics {
            budget.reserve_encoded(metrics.metadata_bytes_read)?;
        }
        Ok(Self {
            output,
            body_len,
            index_start,
            analytics,
            geometry_metrics,
        })
    }
}

fn materialize_features(
    source: &SparseRange,
    context: &FeatureContext,
    columns: &[SqlColumn],
    wildcard: bool,
    budget: &mut Budget,
) -> Result<Vec<RecordBatch>> {
    let selected = projection(&context.output, columns, wildcard);
    let projected_schema = Arc::new(context.output.schema.project(&selected)?);
    let mut batches = Vec::new();
    if selected.is_empty() {
        budget.reserve_rows(context.output.features_count)?;
        let rows = usize::try_from(context.output.features_count)
            .map_err(|_| Error::Format("feature count does not fit this platform".into()))?;
        let batch = make_batch(projected_schema, Vec::new(), rows)?;
        budget.retain(&mut batches, batch)?;
        return Ok(batches);
    }

    // Browser SQL deliberately falls back to exact body metrics when kind 3 is
    // absent. Native planning keeps its historical eager-error behavior.
    let mode = plan_source_mode(
        &context.output,
        &selected,
        context.analytics.is_some(),
        context.geometry_metrics.is_some(),
        true,
    )?;
    match mode {
        SourceMode::Columns => {
            materialize_columns(source, context, &selected, projected_schema, budget)
        }
        SourceMode::Body => {
            if let Some(analytics) = &context.analytics {
                for chunk in 0..analytics.chunk_count {
                    let range = analytics.body_range(chunk)?;
                    materialize_body(
                        source,
                        context,
                        &selected,
                        Arc::clone(&projected_schema),
                        Some(range),
                        budget,
                        &mut batches,
                    )?;
                }
            } else {
                materialize_body(
                    source,
                    context,
                    &selected,
                    projected_schema,
                    None,
                    budget,
                    &mut batches,
                )?;
            }
            Ok(batches)
        }
    }
}

fn projection(output: &OutputSchema, columns: &[SqlColumn], wildcard: bool) -> Vec<usize> {
    if wildcard {
        return (0..output.schema.fields().len()).collect();
    }
    let mut selected = BTreeSet::new();
    for column in columns {
        for (index, field) in output.schema.fields().iter().enumerate() {
            let matches = if column.quoted {
                field.name() == &column.name
            } else {
                field.name().eq_ignore_ascii_case(&column.name)
            };
            if matches {
                selected.insert(index);
            }
        }
    }
    selected.into_iter().collect()
}

fn materialize_columns(
    source: &SparseRange,
    context: &FeatureContext,
    selected: &[usize],
    projected_schema: SchemaRef,
    budget: &mut Budget,
) -> Result<Vec<RecordBatch>> {
    let analytics = context
        .analytics
        .as_ref()
        .ok_or_else(|| Error::Format("column source requires analytics".into()))?;
    let mut reader = open_reader(source)?;
    let mut batches = Vec::new();
    for chunk_index in 0..analytics.chunk_count {
        let mut attempt = budget.clone();
        let arrow = (|| {
            let rows = analytics
                .chunk_row_count(chunk_index)
                .ok_or_else(|| Error::Format(format!("invalid analytics chunk {chunk_index}")))?
                as usize;
            attempt.reserve_rows(rows as u64)?;
            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(selected.len());
            for (position, &output_index) in selected.iter().enumerate() {
                let (chunk, context_name, stored_name) = if let Some(property) =
                    context.output.properties.get(output_index)
                {
                    let (start, end) = analytics.arrow_range(property.column_index, chunk_index)?;
                    attempt.reserve_encoded(end - start)?;
                    let chunk = reader.read_arrow_chunk_with_metadata(
                        analytics,
                        property.column_index,
                        chunk_index,
                    )?;
                    (
                        chunk,
                        format!(
                            "analytics column {} ({:?}), chunk {chunk_index}",
                            property.column_index, property.name
                        ),
                        property.name.as_str(),
                    )
                } else if let Some(metric_id) = context.output.metric_id(output_index) {
                    let metrics = context
                        .geometry_metrics
                        .as_ref()
                        .ok_or_else(|| Error::Format("geometry metrics are absent".into()))?;
                    let (start, end) = metrics.arrow_range(metric_id, chunk_index)?;
                    attempt.reserve_encoded(end - start)?;
                    let chunk = reader.read_geometry_arrow_chunk_with_metadata(
                        metrics,
                        metric_id,
                        chunk_index,
                    )?;
                    let name = context
                        .output
                        .schema
                        .fields()
                        .get(output_index)
                        .map(|field| field.name().as_str())
                        .unwrap_or("unknown");
                    (
                        chunk,
                        format!("geometry metric {metric_id} ({name:?}), chunk {chunk_index}"),
                        fgba::geom_analytics::metric_name(metric_id).ok_or_else(|| {
                            Error::Format("geometry metric name is absent".into())
                        })?,
                    )
                } else {
                    return Err(Error::Format(format!(
                        "invalid column projection {output_index}"
                    )));
                };
                arrays.push(decode_arrow_chunk(
                    chunk.bytes,
                    &context_name,
                    stored_name,
                    rows,
                    Arc::clone(&projected_schema),
                    position,
                )?);
            }
            make_batch(Arc::clone(&projected_schema), arrays, rows)
        })();
        match arrow {
            Ok(batch) => {
                attempt.retain(&mut batches, batch)?;
                *budget = attempt;
            }
            Err(error) => {
                if find_miss(&error).is_some() || error.to_string().contains("budget") {
                    return Err(error);
                }
                let range = analytics.body_range(chunk_index)?;
                materialize_body(
                    source,
                    context,
                    selected,
                    Arc::clone(&projected_schema),
                    Some(range),
                    budget,
                    &mut batches,
                )?;
            }
        }
    }
    Ok(batches)
}

#[allow(clippy::too_many_arguments)]
fn materialize_body(
    source: &SparseRange,
    context: &FeatureContext,
    selected: &[usize],
    projected_schema: SchemaRef,
    range: Option<BodyRange>,
    budget: &mut Budget,
    batches: &mut Vec<RecordBatch>,
) -> Result<()> {
    let expected_rows = match range {
        Some(range) => range.end_ordinal - range.first_ordinal,
        None => context.output.features_count,
    };
    budget.reserve_rows(expected_rows)?;
    let encoded = match range {
        Some(range) => context
            .index_start
            .checked_add(range.end - range.start)
            .ok_or_else(|| Error::Format("body range budget overflows".into()))?,
        None => context.body_len,
    };
    // This preflight also bounds FlatGeobuf's per-feature allocation by the
    // complete range that is about to be decoded.
    budget.reserve_encoded(encoded)?;

    let mut source = source.clone();
    source.seek(SeekFrom::Start(0))?;
    let shared = match range {
        Some(range) => SharedReader::new_at(source, range.start),
        None => SharedReader::new(source),
    };
    let fgb = FgbReader::open(shared.handle())?;
    let mut features = fgb.select_all()?;
    let needs_wkb = selected.iter().any(|&index| context.output.is_wkb(index));
    let needs_bbox = selected.iter().any(|&index| context.output.is_bbox(index));
    let needs_metrics = selected
        .iter()
        .any(|&index| context.output.is_virtual_metric(index));
    let mut builder = FeatureBatchBuilder::new(
        Arc::clone(&context.output),
        Arc::clone(&projected_schema),
        selected,
    )?;
    let first_ordinal = range.map_or(0, |range| range.first_ordinal);
    let mut decoded = 0_u64;
    while decoded < expected_rows {
        let feature = features.next()?.ok_or_else(|| {
            Error::Format("FlatGeobuf body ended before the expected row range".into())
        })?;
        let ordinal = first_ordinal + decoded;
        builder.begin_row();
        if builder.has_projected_properties() {
            feature
                .process_properties(&mut BodyProperties {
                    builder: &mut builder,
                    ordinal,
                })
                .map_err(|error| {
                    Error::Format(format!(
                        "cannot decode properties at feature {ordinal}: {error}"
                    ))
                })?;
        }
        let has_geometry = feature.geometry().is_some();
        let wkb = if needs_wkb && has_geometry {
            Some(feature.to_wkb(context.output.dimensions).map_err(|error| {
                Error::Format(format!("cannot convert WKB at feature {ordinal}: {error}"))
            })?)
        } else {
            None
        };
        let metrics = if needs_metrics {
            feature_metrics(feature).map_err(|error| {
                Error::Format(format!(
                    "cannot decode geometry metrics at feature {ordinal}: {error}"
                ))
            })?
        } else {
            None
        };
        let bbox = if !needs_bbox {
            None
        } else if needs_metrics {
            metrics.map(|metrics| metrics.bbox)
        } else if has_geometry {
            let mut sink = BboxSink::new();
            feature.process_geom(&mut sink).map_err(|error| {
                Error::Format(format!("cannot decode bbox at feature {ordinal}: {error}"))
            })?;
            sink.checked_bounds().map_err(|error| {
                Error::Format(format!("invalid bbox at feature {ordinal}: {error}"))
            })?
        } else {
            None
        };
        builder.end_row(ordinal)?;
        builder.set_geometry(wkb.as_deref(), bbox, metrics)?;
        decoded += 1;
        if builder.rows >= BODY_BATCH_ROWS || builder.current_bytes >= BODY_BATCH_BYTES {
            let batch = builder.finish()?;
            budget.retain(batches, batch)?;
            builder = FeatureBatchBuilder::new(
                Arc::clone(&context.output),
                Arc::clone(&projected_schema),
                selected,
            )?;
        }
    }
    if builder.rows > 0 {
        budget.retain(batches, builder.finish()?)?;
    }
    if decoded != expected_rows {
        return Err(Error::Format(format!(
            "decoded {decoded} features, expected {expected_rows}"
        )));
    }
    Ok(())
}

struct BodyProperties<'a> {
    builder: &'a mut FeatureBatchBuilder,
    ordinal: u64,
}

impl PropertyProcessor for BodyProperties<'_> {
    fn property(
        &mut self,
        index: usize,
        name: &str,
        value: &ColumnValue<'_>,
    ) -> geozero::error::Result<bool> {
        self.builder
            .set_property(index, name, value)
            .map_err(|error| {
                geozero::error::GeozeroError::Property(format!("feature {}: {error}", self.ordinal))
            })?;
        Ok(false)
    }
}

fn materialize_band(source: &SparseRange, res: u8, budget: &mut Budget) -> Result<RecordBatch> {
    let mut reader = open_reader(source)?;
    let meta = reader
        .bands()
        .into_iter()
        .find(|meta| meta.res == res)
        .ok_or_else(|| Error::InvalidInput(format!("no aggregate band at resolution {res}")))?;
    budget.reserve_encoded(meta.size)?;
    let band = reader.read_band(res)?;
    budget.reserve_rows(band.cells.len() as u64)?;
    let batch = cells_batch(&band.cells)?;
    budget.reserve_decoded(batch.get_array_memory_size() as u64)?;
    Ok(batch)
}

fn materialize_viewport(
    source: &SparseRange,
    bbox: Option<[f64; 4]>,
    res: Option<u8>,
    budget: &mut Budget,
) -> Result<RecordBatch> {
    let bbox = bbox.ok_or_else(|| {
        Error::InvalidInput("a5_viewport_approx requires bbox for this query".into())
    })?;
    let mut reader = open_reader(source)?;
    let bands = reader.bands();
    let res = match res {
        Some(res) if bands.iter().any(|meta| meta.res == res) => res,
        Some(res) => {
            return Err(Error::InvalidInput(format!(
                "no aggregate band at resolution {res}"
            )))
        }
        None => bands.iter().map(|meta| meta.res).max().ok_or_else(|| {
            Error::InvalidInput("bbox requires an available aggregate band".into())
        })?,
    };
    let size = bands
        .iter()
        .find(|meta| meta.res == res)
        .map(|meta| meta.size)
        .ok_or_else(|| Error::InvalidInput(format!("no aggregate band at resolution {res}")))?;
    budget.reserve_encoded(size)?;
    let cells = reader.query(bbox, res)?;
    budget.reserve_rows(cells.len() as u64)?;
    let batch = cells_batch(&cells)?;
    budget.reserve_decoded(batch.get_array_memory_size() as u64)?;
    Ok(batch)
}
