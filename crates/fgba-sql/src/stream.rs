//! Bounded body and analytical partition streams over kind-2 and kind-3 chunks.

use crate::error::{Error, Result};
use crate::schema::{OutputSchema, PropertySchema};
use arrow_ipc::reader::FileReader;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, RecordBatch, RecordBatchOptions, StringArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use fgba::geometry::BboxSink;
use fgba::io::SharedReader;
use fgba::{feature_metrics, BodyAnalytics, BodyRange, FeatureMetrics, GeometryMetrics, Reader};
use flatgeobuf::geozero::{ColumnValue, FeatureProperties, PropertyProcessor};
use flatgeobuf::packed_r_tree::PackedRTree;
use flatgeobuf::{ColumnType, FallibleStreamingIterator, FgbReader};
use geozero::{GeozeroGeometry, ToWkb};
use std::collections::HashSet;
use std::fmt;
use std::io::{BufReader, Cursor};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

// Keep cancellation read-ahead bounded: with one queued batch, a blocked
// `blocking_send` leaves at most one further batch to decode before the close
// is observed.
const CHANNEL_CAPACITY: usize = 1;
const RANGE_GAP_BYTES: u64 = 64 * 1024;

/// Cumulative I/O and decode counters for one registered `features` table.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ScanMetrics {
    /// Body features or Arrow rows decoded by executions.
    pub features_read: u64,
    /// Bytes read through FlatGeobuf body sources, including header/index reads.
    pub body_bytes_read: u64,
    /// Kind-2/kind-3 metadata and Arrow payload bytes read.
    pub analytics_bytes_read: u64,
    /// Independent body/Arrow sources or ranges opened.
    pub ranges_opened: u64,
    /// Geometries converted to WKB.
    pub wkb_values_decoded: u64,
}

impl ScanMetrics {
    /// Body plus kind-2/kind-3 bytes. Kind-1 band bytes are deliberately separate.
    pub fn total_bytes_read(&self) -> u64 {
        self.body_bytes_read + self.analytics_bytes_read
    }
}

#[derive(Default)]
pub(crate) struct MetricsInner {
    features_read: AtomicU64,
    body_bytes_read: AtomicU64,
    analytics_bytes_read: AtomicU64,
    ranges_opened: AtomicU64,
    wkb_values_decoded: AtomicU64,
}

impl MetricsInner {
    pub(crate) fn snapshot(&self) -> ScanMetrics {
        ScanMetrics {
            features_read: self.features_read.load(Ordering::Relaxed),
            body_bytes_read: self.body_bytes_read.load(Ordering::Relaxed),
            analytics_bytes_read: self.analytics_bytes_read.load(Ordering::Relaxed),
            ranges_opened: self.ranges_opened.load(Ordering::Relaxed),
            wkb_values_decoded: self.wkb_values_decoded.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn add_features(&self, rows: u64) {
        self.features_read.fetch_add(rows, Ordering::Relaxed);
    }

    pub(crate) fn add_body_bytes(&self, bytes: u64) {
        self.body_bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn add_analytics_bytes(&self, bytes: u64) {
        self.analytics_bytes_read
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn add_range(&self) {
        self.ranges_opened.fetch_add(1, Ordering::Relaxed);
    }

    fn add_wkb(&self) {
        self.wkb_values_decoded.fetch_add(1, Ordering::Relaxed);
    }
}

/// Per-source output bounds.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BatchConfig {
    pub(crate) rows: usize,
    pub(crate) bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceMode {
    Body,
    Columns,
}

fn plan_source_mode(
    output_schema: &OutputSchema,
    selected: &[usize],
    has_analytics: bool,
    has_geometry_metrics: bool,
    wildcard: bool,
) -> Result<SourceMode> {
    let needs_metrics = selected
        .iter()
        .any(|&index| output_schema.is_virtual_metric(index));
    if needs_metrics && !has_geometry_metrics && !wildcard {
        return Err(Error::InvalidInput(
            "geom_length, geom_area, and geom_npoints require valid GEOMETRY_METRICS metadata; rebuild with --geometry-metrics"
                .into(),
        ));
    }
    if has_analytics
        && selected.iter().all(|&index| {
            !output_schema.is_geometry(index)
                || (has_geometry_metrics && output_schema.metric_id(index).is_some())
        })
    {
        Ok(SourceMode::Columns)
    } else {
        Ok(SourceMode::Body)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Selection {
    All,
    BBox([f64; 4]),
    Prune {
        bbox: Option<[f64; 4]>,
        filters: Vec<Expr>,
    },
}

#[derive(Clone, Copy)]
enum BodySource {
    All,
    BBox([f64; 4]),
    Range(BodyRange),
}

#[derive(Debug, Clone, Copy)]
struct RTreeCandidate {
    offset: u64,
    ordinal: u64,
}

struct SpatialChunks {
    chunks: HashSet<u64>,
    ordinals: HashSet<u64>,
}

/// Cloneable execution description. No cursor is cloned or shared.
#[derive(Clone)]
pub(crate) struct Scan {
    path: PathBuf,
    output_schema: Arc<OutputSchema>,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    analytics: Option<Arc<BodyAnalytics>>,
    geometry_metrics: Option<Arc<GeometryMetrics>>,
    source_mode: SourceMode,
    selection: Selection,
    chunks: Arc<[u64]>,
    // Joint bbox partitions share one lazy R-tree search, never a source cursor.
    spatial: Arc<OnceLock<std::result::Result<SpatialChunks, String>>>,
    config: BatchConfig,
    metrics: Arc<MetricsInner>,
}

impl Scan {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        path: PathBuf,
        output_schema: Arc<OutputSchema>,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        analytics: Option<Arc<BodyAnalytics>>,
        geometry_metrics: Option<Arc<GeometryMetrics>>,
        selection: Selection,
        config: BatchConfig,
        metrics: Arc<MetricsInner>,
    ) -> Result<Self> {
        // DataFusion can expand `SELECT *` to a complete ordered projection.
        let wildcard = projection.as_deref().is_none_or(|indices| {
            indices
                .iter()
                .copied()
                .eq(0..output_schema.schema.fields().len())
        });
        let selected = projection
            .clone()
            .unwrap_or_else(|| (0..output_schema.schema.fields().len()).collect::<Vec<_>>());
        let source_mode = plan_source_mode(
            &output_schema,
            &selected,
            analytics.is_some(),
            geometry_metrics.is_some(),
            wildcard,
        )?;
        Ok(Self {
            path,
            output_schema,
            projected_schema,
            projection,
            analytics,
            geometry_metrics,
            source_mode,
            selection,
            chunks: Arc::from(Vec::new()),
            spatial: Arc::new(OnceLock::new()),
            config,
            metrics,
        })
    }

    pub(crate) fn with_chunks(mut self, chunks: Arc<[u64]>) -> Self {
        self.chunks = chunks;
        self
    }

    fn selected(&self) -> Vec<usize> {
        self.projection
            .clone()
            .unwrap_or_else(|| (0..self.output_schema.schema.fields().len()).collect::<Vec<_>>())
    }

    /// Run one owned source, returning `Ok` early when `emit` reports cancellation.
    pub(crate) fn run(&mut self, mut emit: impl FnMut(RecordBatch) -> bool) -> Result<()> {
        let selected = self.selected();
        match self.source_mode {
            SourceMode::Body => self.run_body(&selected, &mut emit),
            SourceMode::Columns => self.run_columns(&selected, &mut emit),
        }
    }

    fn run_body(
        &self,
        selected: &[usize],
        emit: &mut impl FnMut(RecordBatch) -> bool,
    ) -> Result<()> {
        match &self.selection {
            Selection::All => {
                if self.analytics.is_some() {
                    self.run_body_chunks(selected, &self.chunks, None, emit)?;
                } else {
                    self.run_body_source(selected, BodySource::All, None, emit)?;
                }
            }
            Selection::BBox(bbox) => {
                self.run_body_source(selected, BodySource::BBox(*bbox), None, emit)?;
            }
            Selection::Prune { bbox, .. } => {
                let (chunks, candidates) = self.pruned_chunks(*bbox)?;
                self.run_body_chunks(selected, &chunks, candidates, emit)?;
            }
        }
        Ok(())
    }

    fn run_body_chunks(
        &self,
        selected: &[usize],
        chunks: &[u64],
        candidates: Option<&HashSet<u64>>,
        emit: &mut impl FnMut(RecordBatch) -> bool,
    ) -> Result<()> {
        for range in self.body_ranges(chunks)? {
            if self.run_body_source(selected, BodySource::Range(range), candidates, emit)? {
                break;
            }
        }
        Ok(())
    }

    fn run_body_source(
        &self,
        selected: &[usize],
        source: BodySource,
        candidates: Option<&HashSet<u64>>,
        emit: &mut impl FnMut(RecordBatch) -> bool,
    ) -> Result<bool> {
        let file = std::fs::File::open(&self.path)?;
        self.metrics.add_range();
        let shared = match source {
            BodySource::Range(range) => SharedReader::new_at(BufReader::new(file), range.start),
            _ => SharedReader::new(BufReader::new(file)),
        };
        let result = (|| {
            let reader = FgbReader::open(shared.handle())?;
            let mut features = match source {
                BodySource::BBox(bbox) => reader.select_bbox(bbox[0], bbox[1], bbox[2], bbox[3])?,
                _ => reader.select_all()?,
            };
            let needs_wkb = selected
                .iter()
                .any(|&index| self.output_schema.is_wkb(index));
            let needs_bbox = selected
                .iter()
                .any(|&index| self.output_schema.is_bbox(index));
            let needs_metrics = selected
                .iter()
                .any(|&index| self.output_schema.is_virtual_metric(index));
            let mut builder = FeatureBatchBuilder::new(
                Arc::clone(&self.output_schema),
                Arc::clone(&self.projected_schema),
                selected,
            )?;
            let (row_limit, first_ordinal) = match source {
                BodySource::Range(range) => (
                    Some(range.end_ordinal - range.first_ordinal),
                    range.first_ordinal,
                ),
                _ => (None, 0),
            };
            let mut decoded = 0u64;
            loop {
                if row_limit.is_some_and(|limit| decoded == limit) {
                    break;
                }
                let next = features.next();
                let Some(feature) = next? else {
                    if row_limit.is_some() {
                        return Err(Error::Format(
                            "FlatGeobuf body ended before the selected range".into(),
                        ));
                    }
                    break;
                };
                let ordinal = first_ordinal + decoded;
                self.metrics.add_features(1);
                if candidates.is_some_and(|candidates| !candidates.contains(&ordinal)) {
                    decoded += 1;
                    continue;
                }
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
                    Some(
                        feature
                            .to_wkb(self.output_schema.dimensions)
                            .map_err(|error| {
                                Error::Format(format!(
                                    "cannot convert WKB at feature {ordinal}: {error}"
                                ))
                            })?,
                    )
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
                if wkb.is_some() {
                    self.metrics.add_wkb();
                }
                decoded += 1;
                if builder.rows >= self.config.rows || builder.current_bytes >= self.config.bytes {
                    let batch = builder.finish()?;
                    if emit(batch) {
                        return Ok(true);
                    }
                    builder = FeatureBatchBuilder::new(
                        Arc::clone(&self.output_schema),
                        Arc::clone(&self.projected_schema),
                        selected,
                    )?;
                }
            }
            if builder.rows > 0 && emit(builder.finish()?) {
                return Ok(true);
            }
            Ok(false)
        })();
        self.metrics.add_body_bytes(shared.bytes_read());
        result
    }

    fn analytics(&self) -> Result<&BodyAnalytics> {
        self.analytics
            .as_deref()
            .ok_or_else(|| Error::Format("pruning selected without analytics".into()))
    }

    fn geometry_metrics(&self) -> Result<&GeometryMetrics> {
        self.geometry_metrics.as_deref().ok_or_else(|| {
            Error::Format("geometry columns selected without --geometry-metrics metadata".into())
        })
    }

    fn search_rtree(&self, bbox: [f64; 4]) -> Result<Vec<RTreeCandidate>> {
        if !self.output_schema.has_spatial_index {
            return Err(Error::Format(
                "bbox selection requires a nonempty spatial index".into(),
            ));
        }
        let features_count = usize::try_from(self.output_schema.features_count)
            .map_err(|_| Error::Format("features_count does not fit this platform".into()))?;
        let file = std::fs::File::open(&self.path)?;
        self.metrics.add_range();
        let shared = SharedReader::new(BufReader::new(file));
        let result = (|| {
            let header = FgbReader::open(shared.handle())?;
            drop(header);
            let mut source = shared.handle();
            let matches = PackedRTree::stream_search(
                &mut source,
                features_count,
                self.output_schema.index_node_size,
                bbox[0],
                bbox[1],
                bbox[2],
                bbox[3],
            )?;
            let data_start = shared.position();
            matches
                .into_iter()
                .map(|item| {
                    let ordinal = u64::try_from(item.index)
                        .map_err(|_| Error::Format("R-tree ordinal does not fit u64".into()))?;
                    let offset = u64::try_from(item.offset)
                        .map_err(|_| Error::Format("R-tree offset does not fit u64".into()))?;
                    let offset = data_start
                        .checked_add(offset)
                        .ok_or_else(|| Error::Format("R-tree absolute offset overflows".into()))?;
                    Ok(RTreeCandidate { offset, ordinal })
                })
                .collect::<Result<Vec<_>>>()
        })();
        self.metrics.add_body_bytes(shared.bytes_read());
        result
    }

    fn candidate_chunk(&self, candidate: RTreeCandidate) -> Result<u64> {
        let analytics = self.analytics()?;
        if candidate.ordinal >= analytics.row_count {
            return Err(Error::Format(
                "R-tree candidate ordinal is outside the body".into(),
            ));
        }
        let chunk_index = candidate.ordinal / u64::from(analytics.chunk_rows);
        let range = analytics.body_range(chunk_index)?;
        if candidate.ordinal < range.first_ordinal
            || candidate.ordinal >= range.end_ordinal
            || candidate.offset < range.start
            || candidate.offset >= range.end
        {
            return Err(Error::Format(format!(
                "R-tree candidate {} does not align with analytics chunk {chunk_index}",
                candidate.ordinal
            )));
        }
        Ok(chunk_index)
    }

    fn load_spatial(&self, bbox: [f64; 4]) -> Result<SpatialChunks> {
        let mut chunks = HashSet::new();
        let mut ordinals = HashSet::new();
        for candidate in self.search_rtree(bbox)? {
            chunks.insert(self.candidate_chunk(candidate)?);
            ordinals.insert(candidate.ordinal);
        }
        Ok(SpatialChunks { chunks, ordinals })
    }

    fn spatial_chunks(&self, bbox: [f64; 4]) -> Result<&SpatialChunks> {
        match self
            .spatial
            .get_or_init(|| self.load_spatial(bbox).map_err(|error| error.to_string()))
        {
            Ok(spatial) => Ok(spatial),
            Err(error) => Err(Error::Format(error.clone())),
        }
    }

    fn pruned_chunks(&self, bbox: Option<[f64; 4]>) -> Result<(Vec<u64>, Option<&HashSet<u64>>)> {
        self.analytics()?;
        let Some(bbox) = bbox else {
            return Ok((self.chunks.to_vec(), None));
        };
        let spatial = self.spatial_chunks(bbox)?;
        let chunks = self
            .chunks
            .iter()
            .filter(|chunk| spatial.chunks.contains(chunk))
            .copied()
            .collect();
        Ok((chunks, Some(&spatial.ordinals)))
    }

    fn body_ranges(&self, chunks: &[u64]) -> Result<Vec<BodyRange>> {
        let analytics = self.analytics()?;
        let mut ranges: Vec<BodyRange> = Vec::new();
        for &chunk in chunks {
            let next = analytics.body_range(chunk)?;
            if let Some(previous) = ranges.last_mut() {
                if next.start < previous.end {
                    return Err(Error::Format("analytics body ranges overlap".into()));
                }
                if next.start - previous.end <= RANGE_GAP_BYTES {
                    previous.end = next.end;
                    previous.end_ordinal = next.end_ordinal;
                    continue;
                }
            }
            ranges.push(next);
        }
        Ok(ranges)
    }

    fn run_columns(
        &self,
        selected: &[usize],
        emit: &mut impl FnMut(RecordBatch) -> bool,
    ) -> Result<()> {
        match &self.selection {
            Selection::All => self.run_column_chunks(selected, &self.chunks, None, emit),
            Selection::BBox(bbox) => {
                let mut spatial = HashSet::new();
                let mut ordinals = HashSet::new();
                for candidate in self.search_rtree(*bbox)? {
                    spatial.insert(self.candidate_chunk(candidate)?);
                    ordinals.insert(candidate.ordinal);
                }
                let chunks = (0..self.analytics()?.chunk_count)
                    .filter(|chunk| spatial.contains(chunk))
                    .collect::<Vec<_>>();
                self.run_column_chunks(selected, &chunks, Some(&ordinals), emit)
            }
            Selection::Prune { bbox, .. } => {
                let (chunks, candidates) = self.pruned_chunks(*bbox)?;
                self.run_column_chunks(selected, &chunks, candidates, emit)
            }
        }
    }

    fn run_column_chunks(
        &self,
        selected: &[usize],
        chunks: &[u64],
        candidates: Option<&HashSet<u64>>,
        emit: &mut impl FnMut(RecordBatch) -> bool,
    ) -> Result<()> {
        let analytics = self.analytics()?;
        let mut reader = None;
        for &chunk_index in chunks {
            let rows = analytics
                .chunk_row_count(chunk_index)
                .ok_or_else(|| Error::Format(format!("invalid analytics chunk {chunk_index}")))?
                as usize;
            let arrays = 'arrow: {
                let mut arrays = Vec::with_capacity(selected.len());
                if !selected.is_empty() {
                    if reader.is_none() {
                        reader = Some(Reader::open_path(&self.path)?);
                    }
                    let reader = reader
                        .as_mut()
                        .ok_or_else(|| Error::Format("Arrow reader was not opened".into()))?;
                    for &output_index in selected {
                        let (chunk, context, stored_name) = if let Some(property) =
                            self.output_schema.properties.get(output_index)
                        {
                            let chunk = match reader.read_arrow_chunk_with_metadata(
                                analytics,
                                property.column_index,
                                chunk_index,
                            ) {
                                Ok(chunk) => chunk,
                                Err(error) => break 'arrow Err(Error::from(error)),
                            };
                            let context = format!(
                                "analytics column {} ({:?}), chunk {chunk_index}",
                                property.column_index, property.name
                            );
                            (chunk, context, property.name.as_str())
                        } else if let Some(metric_id) = self.output_schema.metric_id(output_index) {
                            let chunk = match reader.read_geometry_arrow_chunk_with_metadata(
                                self.geometry_metrics()?,
                                metric_id,
                                chunk_index,
                            ) {
                                Ok(chunk) => chunk,
                                Err(error) => break 'arrow Err(Error::from(error)),
                            };
                            let name = self
                                .output_schema
                                .schema
                                .fields()
                                .get(output_index)
                                .map(|field| field.name().as_str())
                                .unwrap_or("unknown");
                            let context = format!(
                                "geometry metric {metric_id} ({name:?}), chunk {chunk_index}"
                            );
                            let stored_name = fgba::geom_analytics::metric_name(metric_id)
                                .ok_or_else(|| {
                                    Error::Format("geometry metric name is absent".into())
                                })?;
                            (chunk, context, stored_name)
                        } else {
                            return Err(Error::Format(format!(
                                "invalid column projection {output_index}"
                            )));
                        };
                        self.metrics.add_range();
                        self.metrics.add_analytics_bytes(chunk.bytes.len() as u64);
                        let array = match decode_arrow_chunk(
                            chunk.bytes,
                            &context,
                            stored_name,
                            rows,
                            Arc::clone(&self.projected_schema),
                            arrays.len(),
                        ) {
                            Ok(array) => array,
                            Err(error) => break 'arrow Err(error),
                        };
                        arrays.push(array);
                    }
                }
                Ok(arrays)
            };
            let mut arrays = match arrays {
                Ok(arrays) => arrays,
                Err(error) => {
                    eprintln!(
                        "analytics Arrow chunk {chunk_index} failed; using exact body range: {error}"
                    );
                    let range = analytics.body_range(chunk_index)?;
                    if self.run_body_source(selected, BodySource::Range(range), candidates, emit)? {
                        return Ok(());
                    }
                    continue;
                }
            };
            let output_rows = if let Some(candidates) = candidates {
                let first_ordinal = chunk_index
                    .checked_mul(u64::from(analytics.chunk_rows))
                    .ok_or_else(|| Error::Format("chunk ordinal overflow".into()))?;
                let indices = (0..rows as u64)
                    .filter(|index| candidates.contains(&(first_ordinal + index)))
                    .collect::<Vec<_>>();
                let indices = UInt64Array::from(indices);
                arrays = arrays
                    .iter()
                    .map(|array| take(array, &indices, None))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                indices.len()
            } else {
                rows
            };
            let mut row_bytes = vec![0usize; output_rows];
            for array in &arrays {
                add_variable_bytes(array, &mut row_bytes);
            }
            self.metrics.add_features(rows as u64);
            let batch = make_batch(Arc::clone(&self.projected_schema), arrays, output_rows)?;
            if emit_sliced(batch, &row_bytes, self.config, emit) {
                return Ok(());
            }
        }
        Ok(())
    }
}

fn add_variable_bytes(array: &ArrayRef, row_bytes: &mut [usize]) {
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        for (row, bytes) in row_bytes.iter_mut().enumerate() {
            if !array.is_null(row) {
                *bytes += array.value_length(row) as usize;
            }
        }
    } else if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        for (row, bytes) in row_bytes.iter_mut().enumerate() {
            if !array.is_null(row) {
                *bytes += array.value_length(row) as usize;
            }
        }
    }
}

fn emit_sliced(
    batch: RecordBatch,
    row_bytes: &[usize],
    config: BatchConfig,
    emit: &mut impl FnMut(RecordBatch) -> bool,
) -> bool {
    let mut start = 0usize;
    let mut bytes = 0usize;
    for row in 0..batch.num_rows() {
        bytes += row_bytes.get(row).copied().unwrap_or(0);
        let length = row - start + 1;
        if length >= config.rows || bytes >= config.bytes {
            if emit(batch.slice(start, length)) {
                return true;
            }
            start = row + 1;
            bytes = 0;
        }
    }
    start < batch.num_rows() && emit(batch.slice(start, batch.num_rows() - start))
}

fn decode_arrow_chunk(
    bytes: Vec<u8>,
    context: &str,
    stored_name: &str,
    expected_rows: usize,
    projected_schema: SchemaRef,
    output_position: usize,
) -> Result<ArrayRef> {
    let reader = FileReader::try_new(Cursor::new(bytes), None)
        .map_err(|error| Error::Format(format!("{context}: malformed Arrow IPC: {error}")))?;
    if reader.schema().fields().len() != 1 || reader.num_batches() != 1 {
        return Err(Error::Format(format!(
            "{context}: expected one field and one RecordBatch"
        )));
    }
    let schema = reader.schema();
    let field = &schema.fields()[0];
    let projected_field = projected_schema
        .fields()
        .get(output_position)
        .ok_or_else(|| Error::Format(format!("{context}: projected field is missing")))?;
    if field.name() != stored_name
        || field.data_type() != projected_field.data_type()
        || field.is_nullable() != projected_field.is_nullable()
    {
        return Err(Error::Format(format!(
            "{context}: Arrow schema disagrees with the declared SQL schema"
        )));
    }
    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| Error::Format(format!("{context}: cannot decode Arrow IPC: {error}")))?;
    if batches.len() != 1 || batches[0].num_columns() != 1 {
        return Err(Error::Format(format!(
            "{context}: expected exactly one single-column RecordBatch"
        )));
    }
    let batch = &batches[0];
    if batch.num_rows() != expected_rows {
        return Err(Error::Format(format!(
            "{context}: Arrow rows {} != expected {expected_rows}",
            batch.num_rows()
        )));
    }
    if batch.column(0).data_type() != projected_field.data_type() {
        return Err(Error::Format(format!(
            "{context}: Arrow array type disagrees with the SQL schema"
        )));
    }
    Ok(Arc::clone(batch.column(0)))
}

fn make_batch(schema: SchemaRef, columns: Vec<ArrayRef>, rows: usize) -> Result<RecordBatch> {
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(Into::into)
}

/// One DataFusion partition backed by a fresh blocking source per execution.
pub struct FgbPartition {
    scan: Scan,
    projected_schema: SchemaRef,
}

impl FgbPartition {
    pub(crate) fn new(scan: Scan, projected_schema: SchemaRef) -> Self {
        Self {
            scan,
            projected_schema,
        }
    }
}

impl fmt::Debug for FgbPartition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FgbPartition")
            .field("path", &self.scan.path)
            .field("source", &self.source_mode_for_debug())
            .finish_non_exhaustive()
    }
}

impl FgbPartition {
    fn source_mode_for_debug(&self) -> SourceMode {
        self.scan.source_mode
    }
}

impl PartitionStream for FgbPartition {
    fn schema(&self) -> &SchemaRef {
        &self.projected_schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = RecordBatchReceiverStreamBuilder::new(
            Arc::clone(&self.projected_schema),
            CHANNEL_CAPACITY,
        );
        let tx = builder.tx();
        let mut scan = self.scan.clone();
        builder.spawn_blocking(move || {
            scan.run(|batch| tx.blocking_send(Ok(batch)).is_err())
                .map_err(DataFusionError::from)
        });
        builder.build()
    }
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

#[derive(Clone)]
enum OwnedValue {
    Byte(i8),
    UByte(u8),
    Bool(bool),
    Short(i16),
    UShort(u16),
    Int(i32),
    UInt(u32),
    Long(i64),
    ULong(u64),
    Float(f32),
    Double(f64),
    String(String),
    Json(String),
    DateTime(String),
    Binary(Vec<u8>),
}

impl OwnedValue {
    fn from_column(property: &PropertySchema, value: &ColumnValue<'_>) -> Result<Self> {
        let converted = match (property.kind, value) {
            (ColumnType::Byte, ColumnValue::Byte(value)) => Self::Byte(*value),
            (ColumnType::UByte, ColumnValue::UByte(value)) => Self::UByte(*value),
            (ColumnType::Bool, ColumnValue::Bool(value)) => Self::Bool(*value),
            (ColumnType::Short, ColumnValue::Short(value)) => Self::Short(*value),
            (ColumnType::UShort, ColumnValue::UShort(value)) => Self::UShort(*value),
            (ColumnType::Int, ColumnValue::Int(value)) => Self::Int(*value),
            (ColumnType::UInt, ColumnValue::UInt(value)) => Self::UInt(*value),
            (ColumnType::Long, ColumnValue::Long(value)) => Self::Long(*value),
            (ColumnType::ULong, ColumnValue::ULong(value)) => Self::ULong(*value),
            (ColumnType::Float, ColumnValue::Float(value)) => Self::Float(*value),
            (ColumnType::Double, ColumnValue::Double(value)) => Self::Double(*value),
            (ColumnType::String, ColumnValue::String(value)) => Self::String((*value).to_owned()),
            (ColumnType::Json, ColumnValue::Json(value)) => Self::Json((*value).to_owned()),
            (ColumnType::DateTime, ColumnValue::DateTime(value)) => {
                Self::DateTime((*value).to_owned())
            }
            (ColumnType::Binary, ColumnValue::Binary(value)) => Self::Binary(value.to_vec()),
            _ => {
                return Err(Error::Format(format!(
                    "property {:?} has a runtime/header type mismatch",
                    property.name
                )))
            }
        };
        Ok(converted)
    }
}

/// Owned vectors for every supported Arrow physical type.
enum ColumnValues {
    Int8(Vec<Option<i8>>),
    UInt8(Vec<Option<u8>>),
    Boolean(Vec<Option<bool>>),
    Int16(Vec<Option<i16>>),
    UInt16(Vec<Option<u16>>),
    Int32(Vec<Option<i32>>),
    UInt32(Vec<Option<u32>>),
    Int64(Vec<Option<i64>>),
    UInt64(Vec<Option<u64>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    Utf8(Vec<Option<String>>),
    Binary(Vec<Option<Vec<u8>>>),
}

impl ColumnValues {
    fn new(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
            DataType::Int8 => Self::Int8(Vec::new()),
            DataType::UInt8 => Self::UInt8(Vec::new()),
            DataType::Boolean => Self::Boolean(Vec::new()),
            DataType::Int16 => Self::Int16(Vec::new()),
            DataType::UInt16 => Self::UInt16(Vec::new()),
            DataType::Int32 => Self::Int32(Vec::new()),
            DataType::UInt32 => Self::UInt32(Vec::new()),
            DataType::Int64 => Self::Int64(Vec::new()),
            DataType::UInt64 => Self::UInt64(Vec::new()),
            DataType::Float32 => Self::Float32(Vec::new()),
            DataType::Float64 => Self::Float64(Vec::new()),
            DataType::Utf8 => Self::Utf8(Vec::new()),
            DataType::Binary => Self::Binary(Vec::new()),
            other => {
                return Err(Error::Format(format!(
                    "unsupported Arrow output type {other}"
                )))
            }
        })
    }

    fn append_null(&mut self) {
        match self {
            Self::Int8(values) => values.push(None),
            Self::UInt8(values) => values.push(None),
            Self::Boolean(values) => values.push(None),
            Self::Int16(values) => values.push(None),
            Self::UInt16(values) => values.push(None),
            Self::Int32(values) => values.push(None),
            Self::UInt32(values) => values.push(None),
            Self::Int64(values) => values.push(None),
            Self::UInt64(values) => values.push(None),
            Self::Float32(values) => values.push(None),
            Self::Float64(values) => values.push(None),
            Self::Utf8(values) => values.push(None),
            Self::Binary(values) => values.push(None),
        }
    }

    fn is_null(&self, row: usize) -> bool {
        macro_rules! null {
            ($values:expr) => {
                $values.get(row).map_or(true, Option::is_none)
            };
        }
        match self {
            Self::Int8(values) => null!(values),
            Self::UInt8(values) => null!(values),
            Self::Boolean(values) => null!(values),
            Self::Int16(values) => null!(values),
            Self::UInt16(values) => null!(values),
            Self::Int32(values) => null!(values),
            Self::UInt32(values) => null!(values),
            Self::Int64(values) => null!(values),
            Self::UInt64(values) => null!(values),
            Self::Float32(values) => null!(values),
            Self::Float64(values) => null!(values),
            Self::Utf8(values) => null!(values),
            Self::Binary(values) => null!(values),
        }
    }

    fn set(&mut self, row: usize, value: OwnedValue, name: &str) -> Result<usize> {
        macro_rules! assign {
            ($values:expr, $value:expr) => {{
                let slot = $values.get_mut(row).ok_or_else(|| {
                    Error::Format(format!("column {name:?} row {row} is out of range"))
                })?;
                *slot = Some($value);
                Ok(0usize)
            }};
        }
        match (self, value) {
            (Self::Int8(values), OwnedValue::Byte(value)) => assign!(values, value),
            (Self::UInt8(values), OwnedValue::UByte(value)) => assign!(values, value),
            (Self::Boolean(values), OwnedValue::Bool(value)) => assign!(values, value),
            (Self::Int16(values), OwnedValue::Short(value)) => assign!(values, value),
            (Self::UInt16(values), OwnedValue::UShort(value)) => assign!(values, value),
            (Self::Int32(values), OwnedValue::Int(value)) => assign!(values, value),
            (Self::UInt32(values), OwnedValue::UInt(value)) => assign!(values, value),
            (Self::Int64(values), OwnedValue::Long(value)) => assign!(values, value),
            (Self::UInt64(values), OwnedValue::ULong(value)) => assign!(values, value),
            (Self::Float32(values), OwnedValue::Float(value)) => assign!(values, value),
            (Self::Float64(values), OwnedValue::Double(value)) => assign!(values, value),
            (Self::Utf8(values), OwnedValue::String(value))
            | (Self::Utf8(values), OwnedValue::Json(value))
            | (Self::Utf8(values), OwnedValue::DateTime(value)) => {
                let bytes = value.len();
                let slot = values.get_mut(row).ok_or_else(|| {
                    Error::Format(format!("column {name:?} row {row} is out of range"))
                })?;
                *slot = Some(value);
                Ok(bytes)
            }
            (Self::Binary(values), OwnedValue::Binary(value)) => {
                let bytes = value.len();
                let slot = values.get_mut(row).ok_or_else(|| {
                    Error::Format(format!("column {name:?} row {row} is out of range"))
                })?;
                *slot = Some(value);
                Ok(bytes)
            }
            (this, _) => {
                let actual = match this {
                    Self::Int8(_) => "Int8",
                    Self::UInt8(_) => "UInt8",
                    Self::Boolean(_) => "Boolean",
                    Self::Int16(_) => "Int16",
                    Self::UInt16(_) => "UInt16",
                    Self::Int32(_) => "Int32",
                    Self::UInt32(_) => "UInt32",
                    Self::Int64(_) => "Int64",
                    Self::UInt64(_) => "UInt64",
                    Self::Float32(_) => "Float32",
                    Self::Float64(_) => "Float64",
                    Self::Utf8(_) => "Utf8",
                    Self::Binary(_) => "Binary",
                };
                Err(Error::Format(format!(
                    "column {name:?} has a runtime/header type mismatch (expected {actual})"
                )))
            }
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Int8(values) => Arc::new(Int8Array::from(values)),
            Self::UInt8(values) => Arc::new(UInt8Array::from(values)),
            Self::Boolean(values) => Arc::new(BooleanArray::from(values)),
            Self::Int16(values) => Arc::new(Int16Array::from(values)),
            Self::UInt16(values) => Arc::new(UInt16Array::from(values)),
            Self::Int32(values) => Arc::new(Int32Array::from(values)),
            Self::UInt32(values) => Arc::new(UInt32Array::from(values)),
            Self::Int64(values) => Arc::new(Int64Array::from(values)),
            Self::UInt64(values) => Arc::new(UInt64Array::from(values)),
            Self::Float32(values) => Arc::new(Float32Array::from(values)),
            Self::Float64(values) => Arc::new(Float64Array::from(values)),
            Self::Utf8(values) => Arc::new(StringArray::from(values)),
            Self::Binary(values) => {
                let values = values
                    .iter()
                    .map(|value| value.as_deref())
                    .collect::<Vec<_>>();
                Arc::new(BinaryArray::from(values))
            }
        }
    }
}

/// One bounded output batch with sparse property projection.
struct FeatureBatchBuilder {
    output_schema: Arc<OutputSchema>,
    projected_schema: SchemaRef,
    selected: Vec<usize>,
    property_positions: Vec<Vec<usize>>,
    columns: Vec<ColumnValues>,
    seen_properties: HashSet<usize>,
    rows: usize,
    current_bytes: usize,
}

impl FeatureBatchBuilder {
    fn new(
        output_schema: Arc<OutputSchema>,
        projected_schema: SchemaRef,
        selected: &[usize],
    ) -> Result<Self> {
        let columns = projected_schema
            .fields()
            .iter()
            .map(|field| ColumnValues::new(field.data_type()))
            .collect::<Result<Vec<_>>>()?;
        let mut property_positions = vec![Vec::new(); output_schema.properties.len()];
        for (position, &output_index) in selected.iter().enumerate() {
            if let Some(positions) = property_positions.get_mut(output_index) {
                positions.push(position);
            }
        }
        Ok(Self {
            output_schema,
            projected_schema,
            selected: selected.to_vec(),
            property_positions,
            columns,
            seen_properties: HashSet::new(),
            rows: 0,
            current_bytes: 0,
        })
    }

    fn has_projected_properties(&self) -> bool {
        self.property_positions
            .iter()
            .any(|positions| !positions.is_empty())
    }

    fn begin_row(&mut self) {
        for column in &mut self.columns {
            column.append_null();
        }
        self.seen_properties.clear();
        self.rows += 1;
    }

    fn set_property(&mut self, index: usize, name: &str, value: &ColumnValue<'_>) -> Result<()> {
        let property = self.output_schema.properties.get(index).ok_or_else(|| {
            Error::Format(format!("property index {index} is outside the header"))
        })?;
        if property.name != name {
            return Err(Error::Format(format!(
                "property index {index} is {name:?}, expected {:?}",
                property.name
            )));
        }
        if !self.seen_properties.insert(index) {
            return Err(Error::Format(format!(
                "property {name:?} occurs more than once in one feature"
            )));
        }
        let positions = self.property_positions.get(index).ok_or_else(|| {
            Error::Format(format!("property index {index} has no projection map"))
        })?;
        if positions.is_empty() {
            return Ok(());
        }
        let value = OwnedValue::from_column(property, value)?;
        let row = self.rows.saturating_sub(1);
        for &position in positions {
            let bytes = self
                .columns
                .get_mut(position)
                .ok_or_else(|| Error::Format(format!("property {name:?} has no output storage")))?
                .set(row, value.clone(), name)?;
            self.current_bytes += bytes;
        }
        Ok(())
    }

    fn end_row(&self, ordinal: u64) -> Result<()> {
        let row = self.rows.saturating_sub(1);
        for &output_index in &self.selected {
            let Some(property) = self.output_schema.properties.get(output_index) else {
                continue;
            };
            if property.nullable {
                continue;
            }
            for &position in &self.property_positions[output_index] {
                if self.columns[position].is_null(row) {
                    return Err(Error::Format(format!(
                        "missing non-nullable property {:?} at feature {ordinal}",
                        property.name
                    )));
                }
            }
        }
        Ok(())
    }

    fn set_geometry(
        &mut self,
        wkb: Option<&[u8]>,
        bbox: Option<[f64; 4]>,
        metrics: Option<FeatureMetrics>,
    ) -> Result<()> {
        let row = self.rows.saturating_sub(1);
        let property_count = self.output_schema.properties.len();
        for (position, &output_index) in self.selected.iter().enumerate() {
            if output_index < property_count {
                continue;
            }
            let geometry_index = output_index - property_count;
            let (name, value) = match geometry_index {
                0 => (
                    "geom_wkb",
                    wkb.map(|bytes| OwnedValue::Binary(bytes.to_vec())),
                ),
                1..=4 => (
                    crate::schema::GEOMETRY_COLUMNS[geometry_index],
                    bbox.map(|bounds| OwnedValue::Double(bounds[geometry_index - 1])),
                ),
                5 => (
                    "geom_length",
                    metrics.map(|metrics| OwnedValue::Double(metrics.length)),
                ),
                6 => (
                    "geom_area",
                    metrics.map(|metrics| OwnedValue::Double(metrics.area)),
                ),
                7 => (
                    "geom_npoints",
                    metrics.map(|metrics| OwnedValue::UInt(metrics.npoints)),
                ),
                _ => {
                    return Err(Error::Format(format!(
                        "invalid geometry projection {output_index}"
                    )))
                }
            };
            if let Some(value) = value {
                let bytes = self.columns[position].set(row, value, name)?;
                self.current_bytes += bytes;
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<RecordBatch> {
        let columns = self.columns.into_iter().map(ColumnValues::finish).collect();
        make_batch(self.projected_schema, columns, self.rows)
    }
}
