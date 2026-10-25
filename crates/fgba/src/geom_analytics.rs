//! GEOMETRY_METRICS (directory kind 3) encoding and validated metadata.
//!
//! The seven columns reuse BODY_ANALYTICS column builders, statistics, Arrow
//! chunks, and fixed-record codecs. Only the smaller §6B descriptor/layout is
//! specific to this module.

use crate::analytics::{
    chunk_count, encode_global, get_u16, get_u32, get_u64, put_u16, put_u32, put_u64, read_blob,
    remaining_rows, take, validate_chunk, validate_global, write_part, BuiltColumn, ColumnBuilder,
    ColumnSchema, Value, ANALYTICS_VERSION, CHUNK_STAT_SIZE, GLOBAL_STAT_SIZE, HAS_ARROW_IPC,
    MAX_ARROW_CHUNK_BYTES, MAX_METADATA_BYTES, SECTION_HEADER_SIZE,
};
use crate::error::{Error, Result};
use crate::format::SectionEntry;
use crate::geom_metrics::FeatureMetrics;
use crate::{BodyAnalytics, ChunkStat, GlobalStat, ValueClass};
use arrow_schema::DataType;
use flatgeobuf::ColumnType;
use std::io::{Read, Seek, Write};

/// GEOMETRY_METRICS encoding version.
pub const GEOMETRY_METRICS_VERSION: u8 = ANALYTICS_VERSION;
/// Number of required geometry metric columns.
pub const GEOMETRY_METRIC_COUNT: usize = 7;
/// Fixed metric descriptor size.
pub const METRIC_DESC_SIZE: usize = 24;
/// `MetricDesc::arrow_type` value for Float64.
pub const ARROW_FLOAT64: u8 = 0;
/// `MetricDesc::arrow_type` value for UInt32.
pub const ARROW_UINT32: u8 = 1;
/// Metric field names in on-disk metric-ID order.
pub const GEOMETRY_METRIC_NAMES: [&str; GEOMETRY_METRIC_COUNT] = [
    "length",
    "area",
    "npoints",
    "bbox_xmin",
    "bbox_ymin",
    "bbox_xmax",
    "bbox_ymax",
];

/// Return the required nullable Arrow field name for a metric ID.
pub fn metric_name(metric_id: u16) -> Option<&'static str> {
    GEOMETRY_METRIC_NAMES.get(metric_id as usize).copied()
}

/// Return the required Arrow physical type for a metric ID.
pub fn metric_data_type(metric_id: u16) -> Result<DataType> {
    match metric_id {
        0 | 1 | 3..=6 => Ok(DataType::Float64),
        2 => Ok(DataType::UInt32),
        _ => Err(Error::Format(format!(
            "geometry metric ID {metric_id} is out of range"
        ))),
    }
}

fn metric_arrow_type(metric_id: u16) -> Result<u8> {
    match metric_id {
        0 | 1 | 3..=6 => Ok(ARROW_FLOAT64),
        2 => Ok(ARROW_UINT32),
        _ => Err(Error::Format(format!(
            "geometry metric ID {metric_id} is out of range"
        ))),
    }
}

fn metric_class(metric_id: u16) -> Result<ValueClass> {
    match metric_id {
        0 | 1 | 3..=6 => Ok(ValueClass::Float),
        2 => Ok(ValueClass::Unsigned),
        _ => Err(Error::Format(format!(
            "geometry metric ID {metric_id} is out of range"
        ))),
    }
}

/// The fixed 24-byte GEOMETRY_METRICS descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricDesc {
    /// Metric identity in `0..=6`.
    pub metric_id: u16,
    /// [`ARROW_FLOAT64`] or [`ARROW_UINT32`].
    pub arrow_type: u8,
    /// `HAS_ARROW_IPC`; all other bits are reserved zero.
    pub flags: u32,
    /// Metric stream offset relative to the section start.
    pub stream_offset: u64,
    /// Complete stream size, including statistics, offsets, and Arrow payloads.
    pub stream_size: u64,
}

/// One metric's descriptor and validated non-payload metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricColumn {
    /// Fixed section descriptor.
    pub descriptor: MetricDesc,
    /// Global statistics.
    pub global: GlobalStat,
    /// One statistic record per shared chunk.
    pub chunks: Vec<ChunkStat>,
    /// Absolute Arrow chunk offsets, with one terminal offset.
    pub arrow_offsets: Vec<u64>,
}

/// Fully validated GEOMETRY_METRICS metadata, without Arrow payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeometryMetrics {
    /// Absolute section start.
    pub section_start: u64,
    /// Directory-declared section size.
    pub section_size: u64,
    /// Shared rows per chunk, equal to BODY_ANALYTICS.
    pub chunk_rows: u32,
    /// Body feature count, equal to FGB `features_count`.
    pub row_count: u64,
    /// Shared chunk count, equal to BODY_ANALYTICS.
    pub chunk_count: u64,
    /// Seven metric columns in ascending metric-ID order.
    pub columns: [MetricColumn; GEOMETRY_METRIC_COUNT],
    /// Actual fixed metadata bytes read, excluding Arrow payloads.
    pub metadata_bytes_read: u64,
}

impl GeometryMetrics {
    /// Find a metric column by its stable metric ID.
    pub fn column(&self, metric_id: u16) -> Option<&MetricColumn> {
        self.columns
            .iter()
            .find(|column| column.descriptor.metric_id == metric_id)
    }

    /// Number of body rows in one shared chunk.
    pub fn chunk_row_count(&self, chunk_index: u64) -> Option<u32> {
        if chunk_index >= self.chunk_count {
            return None;
        }
        let first = chunk_index.checked_mul(u64::from(self.chunk_rows))?;
        let remaining = self.row_count.checked_sub(first)?;
        Some(remaining.min(u64::from(self.chunk_rows)) as u32)
    }

    /// Return the exact absolute byte range of one metric Arrow IPC chunk.
    pub fn arrow_range(&self, metric_id: u16, chunk_index: u64) -> Result<(u64, u64)> {
        let column = self
            .column(metric_id)
            .ok_or_else(|| Error::Format(format!("geometry metric {metric_id} does not exist")))?;
        if chunk_index >= self.chunk_count {
            return Err(Error::Format(format!(
                "geometry chunk {chunk_index} is out of range for metric {metric_id}"
            )));
        }
        let index = usize::try_from(chunk_index)
            .map_err(|_| Error::Format("geometry chunk index does not fit this platform".into()))?;
        let start = *column
            .arrow_offsets
            .get(index)
            .ok_or_else(|| Error::Format("geometry Arrow offsets are truncated".into()))?;
        let next = index
            .checked_add(1)
            .ok_or_else(|| Error::Format("geometry Arrow chunk index overflows".into()))?;
        let end = *column
            .arrow_offsets
            .get(next)
            .ok_or_else(|| Error::Format("geometry Arrow offsets are truncated".into()))?;
        let len = end
            .checked_sub(start)
            .ok_or_else(|| Error::Format("geometry Arrow offsets are not increasing".into()))?;
        if len == 0 || len > MAX_ARROW_CHUNK_BYTES {
            return Err(Error::Format(format!(
                "geometry Arrow chunk {metric_id}/{chunk_index} has invalid size {len}"
            )));
        }
        Ok((start, end))
    }
}

pub(crate) struct GeometryEncoder {
    builders: Vec<ColumnBuilder>,
}

impl GeometryEncoder {
    pub(crate) fn new(chunk_rows: u32, spooled: bool) -> Result<Self> {
        let builders = (0..GEOMETRY_METRIC_COUNT)
            .map(|index| {
                let metric_id = u16::try_from(index)
                    .map_err(|_| Error::Format("geometry metric ID does not fit u16".into()))?;
                let schema = ColumnSchema {
                    index: metric_id,
                    name: metric_name(metric_id)
                        .ok_or_else(|| Error::Format("geometry metric name is absent".into()))?
                        .to_owned(),
                    kind: if metric_id == 2 {
                        ColumnType::UInt
                    } else {
                        ColumnType::Double
                    },
                    nullable: true,
                };
                if spooled {
                    ColumnBuilder::new_spooled(schema, chunk_rows)
                } else {
                    ColumnBuilder::new(schema, chunk_rows)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { builders })
    }

    pub(crate) fn push(&mut self, metrics: Option<FeatureMetrics>) -> Result<()> {
        let values: [Option<Value>; GEOMETRY_METRIC_COUNT] = match metrics {
            Some(metrics) => [
                Some(Value::Double(metrics.length)),
                Some(Value::Double(metrics.area)),
                Some(Value::UInt(metrics.npoints)),
                Some(Value::Double(metrics.bbox[0])),
                Some(Value::Double(metrics.bbox[1])),
                Some(Value::Double(metrics.bbox[2])),
                Some(Value::Double(metrics.bbox[3])),
            ],
            None => std::array::from_fn(|_| None),
        };
        for (builder, value) in self.builders.iter_mut().zip(values) {
            builder.push(value)?;
        }
        Ok(())
    }

    pub(crate) fn finish(
        self,
        section_start: u64,
        chunk_rows: u32,
        row_count: u64,
    ) -> Result<EncodedGeometry> {
        let chunks = chunk_count(row_count, chunk_rows)?;
        let expected_chunks = usize::try_from(chunks)
            .map_err(|_| Error::Format("geometry chunk count does not fit this platform".into()))?;
        let columns = self
            .builders
            .into_iter()
            .map(|builder| builder.finish(expected_chunks))
            .collect::<Result<Vec<_>>>()?;
        EncodedGeometry::new(section_start, chunk_rows, row_count, chunks, columns)
    }
}

pub(crate) struct EncodedGeometry {
    section_start: u64,
    chunk_rows: u32,
    row_count: u64,
    chunk_count: u64,
    first_stream: u64,
    column_metadata_size: u64,
    descriptors: Vec<MetricDesc>,
    arrow_offsets: Vec<Vec<u64>>,
    size: u64,
    columns: Vec<BuiltColumn>,
}

impl EncodedGeometry {
    fn new(
        section_start: u64,
        chunk_rows: u32,
        row_count: u64,
        chunk_count: u64,
        columns: Vec<BuiltColumn>,
    ) -> Result<Self> {
        if columns.len() != GEOMETRY_METRIC_COUNT {
            return Err(Error::Format(
                "geometry metrics require exactly seven columns".into(),
            ));
        }
        let offset_array_size = 8u64
            .checked_mul(
                chunk_count
                    .checked_add(1)
                    .ok_or_else(|| Error::Format("geometry offset count overflows".into()))?,
            )
            .ok_or_else(|| Error::Format("geometry offset array size overflows".into()))?;
        let chunk_stats_size = (CHUNK_STAT_SIZE as u64)
            .checked_mul(chunk_count)
            .ok_or_else(|| Error::Format("geometry chunk stats overflow".into()))?;
        let column_metadata_size = (GLOBAL_STAT_SIZE as u64)
            .checked_add(chunk_stats_size)
            .and_then(|size| size.checked_add(offset_array_size))
            .ok_or_else(|| Error::Format("geometry column metadata size overflows".into()))?;
        let first_stream = (SECTION_HEADER_SIZE as u64)
            .checked_add((METRIC_DESC_SIZE as u64) * GEOMETRY_METRIC_COUNT as u64)
            .ok_or_else(|| Error::Format("geometry descriptor size overflows".into()))?;
        let fixed_metadata_size = column_metadata_size
            .checked_mul(GEOMETRY_METRIC_COUNT as u64)
            .and_then(|size| first_stream.checked_add(size))
            .ok_or_else(|| Error::Format("geometry fixed metadata size overflows".into()))?;
        if fixed_metadata_size > MAX_METADATA_BYTES {
            return Err(Error::Format(
                "geometry fixed metadata exceeds the 16 MiB cap".into(),
            ));
        }

        let mut stream_offset = first_stream;
        let mut descriptors = Vec::new();
        let mut arrow_offsets = Vec::new();
        for (index, column) in columns.iter().enumerate() {
            let metric_id = u16::try_from(index)
                .map_err(|_| Error::Format("geometry metric ID does not fit u16".into()))?;
            if column.schema.index != metric_id {
                return Err(Error::Format(
                    "geometry metric columns are not in metric-ID order".into(),
                ));
            }
            let arrow_bytes = column.arrow_chunks.arrow_bytes()?;
            let stream_size = column_metadata_size
                .checked_add(arrow_bytes)
                .ok_or_else(|| Error::Format("geometry stream size overflows".into()))?;
            let payload_start = section_start
                .checked_add(stream_offset)
                .and_then(|offset| offset.checked_add(column_metadata_size))
                .ok_or_else(|| Error::Format("geometry Arrow payload offset overflows".into()))?;
            let mut offsets = Vec::new();
            offsets
                .try_reserve(column.arrow_chunks.len().saturating_add(1))
                .map_err(|_| {
                    Error::Format("geometry Arrow-offset allocation is too large".into())
                })?;
            let mut offset = payload_start;
            offsets.push(offset);
            for index in 0..column.arrow_chunks.len() {
                offset = offset
                    .checked_add(column.arrow_chunks.chunk_len(index)?)
                    .ok_or_else(|| Error::Format("geometry Arrow chunk offset overflows".into()))?;
                offsets.push(offset);
            }
            let stream_end = section_start
                .checked_add(stream_offset)
                .and_then(|start| start.checked_add(stream_size))
                .ok_or_else(|| Error::Format("geometry stream end overflows".into()))?;
            if offsets.last() != Some(&stream_end) {
                return Err(Error::Format(
                    "geometry terminal Arrow offset does not equal stream end".into(),
                ));
            }
            descriptors.push(MetricDesc {
                metric_id,
                arrow_type: metric_arrow_type(metric_id)?,
                flags: HAS_ARROW_IPC,
                stream_offset,
                stream_size,
            });
            arrow_offsets.push(offsets);
            stream_offset = stream_offset
                .checked_add(stream_size)
                .ok_or_else(|| Error::Format("geometry section size overflows".into()))?;
        }
        Ok(Self {
            section_start,
            chunk_rows,
            row_count,
            chunk_count,
            first_stream,
            column_metadata_size,
            descriptors,
            arrow_offsets,
            size: stream_offset,
            columns,
        })
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn write_to(&mut self, out: &mut dyn Write) -> Result<u64> {
        let max_metadata = self.first_stream.max(self.column_metadata_size);
        let metadata_capacity = usize::try_from(max_metadata)
            .map_err(|_| Error::Format("geometry metadata does not fit this platform".into()))?;
        let mut metadata = Vec::new();
        metadata
            .try_reserve(metadata_capacity)
            .map_err(|_| Error::Format("geometry metadata allocation is too large".into()))?;
        metadata.extend_from_slice(&[GEOMETRY_METRICS_VERSION, 0, 0, 0]);
        put_u32(&mut metadata, self.chunk_rows);
        put_u32(&mut metadata, GEOMETRY_METRIC_COUNT as u32);
        put_u32(&mut metadata, 0);
        put_u64(&mut metadata, self.row_count);
        put_u64(&mut metadata, self.chunk_count);
        for descriptor in &self.descriptors {
            put_u16(&mut metadata, descriptor.metric_id);
            metadata.push(descriptor.arrow_type);
            metadata.push(0);
            put_u32(&mut metadata, descriptor.flags);
            put_u64(&mut metadata, descriptor.stream_offset);
            put_u64(&mut metadata, descriptor.stream_size);
        }
        let mut written = 0u64;
        write_part(out, &metadata, &mut written)?;
        for ((column, descriptor), offsets) in self
            .columns
            .iter_mut()
            .zip(&self.descriptors)
            .zip(&self.arrow_offsets)
        {
            metadata.clear();
            encode_global(&mut metadata, &column.global);
            for stat in &column.chunks {
                put_u64(&mut metadata, stat.min_bits);
                put_u64(&mut metadata, stat.max_bits);
                put_u32(&mut metadata, stat.non_null_count);
                put_u32(&mut metadata, stat.nan_count);
            }
            for &offset in offsets {
                put_u64(&mut metadata, offset);
            }
            write_part(out, &metadata, &mut written)?;
            let expected = self
                .section_start
                .checked_add(descriptor.stream_offset)
                .and_then(|offset| offset.checked_add(descriptor.stream_size))
                .ok_or_else(|| Error::Format("geometry encoded stream end overflows".into()))?;
            let payload = column.arrow_chunks.write_to(out)?;
            if payload != descriptor.stream_size - self.column_metadata_size {
                return Err(Error::Format(
                    "geometry encoded Arrow size disagrees with descriptor".into(),
                ));
            }
            written = written
                .checked_add(payload)
                .ok_or_else(|| Error::Format("geometry encoded section end overflows".into()))?;
            let actual = self
                .section_start
                .checked_add(written)
                .ok_or_else(|| Error::Format("geometry encoded section end overflows".into()))?;
            if actual != expected {
                return Err(Error::Format(
                    "geometry encoded stream size disagrees with descriptor".into(),
                ));
            }
        }
        if written != self.size {
            return Err(Error::Format(
                "geometry encoded section size disagrees with layout".into(),
            ));
        }
        Ok(written)
    }

    pub(crate) fn into_bytes(mut self) -> Result<Vec<u8>> {
        let total_size = usize::try_from(self.size)
            .map_err(|_| Error::Format("geometry section does not fit this platform".into()))?;
        let mut out = Vec::new();
        out.try_reserve(total_size)
            .map_err(|_| Error::Format("geometry section allocation is too large".into()))?;
        let written = self.write_to(&mut out)?;
        if written != out.len() as u64 {
            return Err(Error::Format(
                "geometry encoded byte count disagrees with output".into(),
            ));
        }
        Ok(out)
    }
}

/// Read and validate kind-3 fixed metadata against an already validated kind 2.
pub(crate) fn read_metadata<R: Read + Seek>(
    inner: &mut R,
    entry: &SectionEntry,
    analytics: &BodyAnalytics,
) -> Result<GeometryMetrics> {
    if entry.flags != 0 || !entry.meta.is_empty() {
        return Err(Error::Format(
            "geometry directory flags and meta_len must be zero".into(),
        ));
    }
    let first_stream = (SECTION_HEADER_SIZE as u64)
        .checked_add((METRIC_DESC_SIZE as u64) * GEOMETRY_METRIC_COUNT as u64)
        .ok_or_else(|| Error::Format("geometry descriptor size overflows".into()))?;
    if entry.size < first_stream {
        return Err(Error::Format(
            "geometry section header/descriptors are truncated".into(),
        ));
    }
    let mut remaining = MAX_METADATA_BYTES;
    let header = read_blob(
        inner,
        entry.offset,
        SECTION_HEADER_SIZE as u64,
        &mut remaining,
        "geometry section header",
    )?;
    let mut at = 0;
    if take(&header, &mut at, 1, "version")?[0] != GEOMETRY_METRICS_VERSION
        || take(&header, &mut at, 1, "flags")?[0] != 0
        || get_u16(&header, &mut at, "reserved")? != 0
    {
        return Err(Error::Format(
            "geometry section version/flags/reserved fields are invalid".into(),
        ));
    }
    let chunk_rows = get_u32(&header, &mut at, "chunk_rows")?;
    let column_count = get_u32(&header, &mut at, "column_count")?;
    if get_u32(&header, &mut at, "reserved")? != 0 {
        return Err(Error::Format(
            "geometry header reserved bytes are nonzero".into(),
        ));
    }
    let row_count = get_u64(&header, &mut at, "row_count")?;
    let chunks = get_u64(&header, &mut at, "chunk_count")?;
    if column_count as usize != GEOMETRY_METRIC_COUNT
        || chunk_rows == 0
        || chunk_rows != analytics.chunk_rows
        || row_count != analytics.row_count
        || chunks != analytics.chunk_count
        || chunks != chunk_count(row_count, chunk_rows)?
    {
        return Err(Error::Format(
            "geometry counts/chunk grid disagree with BODY_ANALYTICS or FGB".into(),
        ));
    }

    let offset_array_size = 8u64
        .checked_mul(
            chunks
                .checked_add(1)
                .ok_or_else(|| Error::Format("geometry offset count overflows".into()))?,
        )
        .ok_or_else(|| Error::Format("geometry offset array size overflows".into()))?;
    let chunk_stats_size = (CHUNK_STAT_SIZE as u64)
        .checked_mul(chunks)
        .ok_or_else(|| Error::Format("geometry chunk stats overflow".into()))?;
    let column_metadata_size = (GLOBAL_STAT_SIZE as u64)
        .checked_add(chunk_stats_size)
        .and_then(|size| size.checked_add(offset_array_size))
        .ok_or_else(|| Error::Format("geometry column metadata size overflows".into()))?;
    let fixed_metadata_size = column_metadata_size
        .checked_mul(GEOMETRY_METRIC_COUNT as u64)
        .and_then(|size| first_stream.checked_add(size))
        .ok_or_else(|| Error::Format("geometry fixed metadata size overflows".into()))?;
    if fixed_metadata_size > entry.size || fixed_metadata_size > MAX_METADATA_BYTES {
        return Err(Error::Format(
            "geometry fixed metadata exceeds the section or 16 MiB cap".into(),
        ));
    }
    let descriptors_at = entry
        .offset
        .checked_add(SECTION_HEADER_SIZE as u64)
        .ok_or_else(|| Error::Format("geometry descriptor location overflows".into()))?;
    let descriptor_bytes = read_blob(
        inner,
        descriptors_at,
        (METRIC_DESC_SIZE as u64) * GEOMETRY_METRIC_COUNT as u64,
        &mut remaining,
        "geometry descriptors",
    )?;
    let mut descriptors = Vec::new();
    let mut expected_stream = first_stream;
    at = 0;
    for index in 0..GEOMETRY_METRIC_COUNT {
        let metric_id = get_u16(&descriptor_bytes, &mut at, "metric_id")?;
        let arrow_type = take(&descriptor_bytes, &mut at, 1, "arrow_type")?[0];
        let reserved = take(&descriptor_bytes, &mut at, 1, "reserved")?[0];
        let flags = get_u32(&descriptor_bytes, &mut at, "flags")?;
        let stream_offset = get_u64(&descriptor_bytes, &mut at, "stream_offset")?;
        let stream_size = get_u64(&descriptor_bytes, &mut at, "stream_size")?;
        if metric_id as usize != index
            || arrow_type != metric_arrow_type(metric_id)?
            || reserved != 0
            || flags != HAS_ARROW_IPC
            || stream_offset != expected_stream
            || stream_size < column_metadata_size
        {
            return Err(Error::Format(format!(
                "geometry metric {metric_id} descriptor offsets/type/flags are invalid"
            )));
        }
        expected_stream = stream_offset
            .checked_add(stream_size)
            .ok_or_else(|| Error::Format("geometry stream end overflows".into()))?;
        if expected_stream > entry.size {
            return Err(Error::Format(format!(
                "geometry metric {metric_id} stream lies outside the section"
            )));
        }
        descriptors.push(MetricDesc {
            metric_id,
            arrow_type,
            flags,
            stream_offset,
            stream_size,
        });
    }
    if expected_stream != entry.size {
        return Err(Error::Format(
            "geometry section size does not equal its exact stream layout".into(),
        ));
    }

    let mut columns = Vec::new();
    for descriptor in descriptors {
        let class = metric_class(descriptor.metric_id)?;
        let stream_start = entry
            .offset
            .checked_add(descriptor.stream_offset)
            .ok_or_else(|| Error::Format("geometry stream start overflows".into()))?;
        let metadata = read_blob(
            inner,
            stream_start,
            column_metadata_size,
            &mut remaining,
            "geometry statistics/Arrow offsets",
        )?;
        at = 0;
        let global = GlobalStat {
            non_null_count: get_u64(&metadata, &mut at, "global non_null_count")?,
            nan_count: get_u64(&metadata, &mut at, "global nan_count")?,
            min_bits: get_u64(&metadata, &mut at, "global min")?,
            max_bits: get_u64(&metadata, &mut at, "global max")?,
            sum: {
                let mut sum = [0u8; 16];
                sum.copy_from_slice(take(&metadata, &mut at, 16, "global sum")?);
                sum
            },
            flags: get_u32(&metadata, &mut at, "global flags")?,
            reserved: get_u32(&metadata, &mut at, "global reserved")?,
        };
        validate_global(&global, class, row_count)?;
        let mut chunk_stats = Vec::new();
        let mut non_null_sum = 0u64;
        let mut nan_sum = 0u64;
        for chunk_index in 0..chunks {
            let stat = ChunkStat {
                min_bits: get_u64(&metadata, &mut at, "chunk min")?,
                max_bits: get_u64(&metadata, &mut at, "chunk max")?,
                non_null_count: get_u32(&metadata, &mut at, "chunk non_null_count")?,
                nan_count: get_u32(&metadata, &mut at, "chunk nan_count")?,
            };
            validate_chunk(
                &stat,
                class,
                remaining_rows(row_count, chunk_rows, chunk_index)?,
            )?;
            non_null_sum = non_null_sum
                .checked_add(u64::from(stat.non_null_count))
                .ok_or_else(|| Error::Format("geometry chunk count sum overflows".into()))?;
            nan_sum = nan_sum
                .checked_add(u64::from(stat.nan_count))
                .ok_or_else(|| Error::Format("geometry chunk NaN sum overflows".into()))?;
            chunk_stats.push(stat);
        }
        if non_null_sum != global.non_null_count || nan_sum != global.nan_count {
            return Err(Error::Format(format!(
                "geometry metric {} global/chunk counts disagree",
                descriptor.metric_id
            )));
        }
        let mut arrow_offsets = Vec::new();
        for _ in 0..=chunks {
            arrow_offsets.push(get_u64(&metadata, &mut at, "Arrow offset")?);
        }
        let payload_start = stream_start
            .checked_add(column_metadata_size)
            .ok_or_else(|| Error::Format("geometry Arrow payload start overflows".into()))?;
        let stream_end = stream_start
            .checked_add(descriptor.stream_size)
            .ok_or_else(|| Error::Format("geometry stream end overflows".into()))?;
        if arrow_offsets.first() != Some(&payload_start)
            || arrow_offsets.last() != Some(&stream_end)
            || !arrow_offsets.windows(2).all(|pair| pair[0] < pair[1])
        {
            return Err(Error::Format(format!(
                "geometry metric {} Arrow offsets are invalid",
                descriptor.metric_id
            )));
        }
        for pair in arrow_offsets.windows(2) {
            let len = pair[1] - pair[0];
            if len == 0 || len > MAX_ARROW_CHUNK_BYTES {
                return Err(Error::Format(format!(
                    "geometry metric {} has an oversized or empty Arrow chunk",
                    descriptor.metric_id
                )));
            }
        }
        columns.push(MetricColumn {
            descriptor,
            global,
            chunks: chunk_stats,
            arrow_offsets,
        });
    }
    let first = columns
        .first()
        .ok_or_else(|| Error::Format("geometry metrics contain no columns".into()))?;
    if columns.iter().any(|column| {
        column.global.non_null_count != first.global.non_null_count
            || column
                .chunks
                .iter()
                .zip(&first.chunks)
                .any(|(left, right)| left.non_null_count != right.non_null_count)
    }) {
        return Err(Error::Format(
            "geometry metric null counts disagree across columns".into(),
        ));
    }
    let columns: [MetricColumn; GEOMETRY_METRIC_COUNT] = columns
        .try_into()
        .map_err(|_| Error::Format("geometry metrics require exactly seven columns".into()))?;
    Ok(GeometryMetrics {
        section_start: entry.offset,
        section_size: entry.size,
        chunk_rows,
        row_count,
        chunk_count: chunks,
        columns,
        metadata_bytes_read: MAX_METADATA_BYTES - remaining,
    })
}
