//! Optional BODY_ANALYTICS (directory kind 2) encoding and decoding.
//!
//! Arrow IPC is used only for independent single-column chunks. Statistics,
//! offsets, and descriptors use the fixed little-endian records in SPEC.md.

use crate::error::{Error, Result};
use crate::format::SectionEntry;
use crate::io::{copy_exact, SharedReader};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Schema};
use flatgeobuf::geozero::{ColumnValue, FeatureProperties, PropertyProcessor};
use flatgeobuf::packed_r_tree::{NodeItem, PackedRTree};
use flatgeobuf::{ColumnType, FallibleStreamingIterator, FgbReader, Header};
use std::io::{BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::sync::Arc;

#[cfg(test)]
use std::cell::Cell;

/// BODY_ANALYTICS encoding version.
pub const ANALYTICS_VERSION: u8 = 1;
/// Fixed section-header size.
pub const SECTION_HEADER_SIZE: usize = 32;
/// Fixed column-descriptor size.
pub const COLUMN_DESC_SIZE: usize = 40;
/// Fixed global-statistic size.
pub const GLOBAL_STAT_SIZE: usize = 56;
/// Fixed per-chunk-statistic size.
pub const CHUNK_STAT_SIZE: usize = 24;
/// Maximum combined metadata bytes read from one section.
pub const MAX_METADATA_BYTES: u64 = 16 * 1024 * 1024;
/// Maximum size of one independently encoded Arrow IPC chunk.
pub const MAX_ARROW_CHUNK_BYTES: u64 = 64 * 1024 * 1024;
/// Column descriptor flag indicating independent Arrow IPC chunks.
pub const HAS_ARROW_IPC: u32 = 1;
/// Global statistic flag indicating an exact, usable sum.
pub const SUM_VALID: u32 = 1;
/// Global statistic flag indicating at least one infinite float value.
pub const HAS_INFINITY: u32 = 2;

/// Statistical representation of one FlatGeobuf property type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueClass {
    /// Exact values/null counts only; no bounds or sum.
    Opaque = 0,
    /// Signed integers widened to i64 for statistics.
    Signed = 1,
    /// Unsigned integers widened to u64 for statistics.
    Unsigned = 2,
    /// Float values, with f32 losslessly promoted to f64 for statistics.
    Float = 3,
}

impl ValueClass {
    /// Decode the on-disk value-class byte.
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Opaque),
            1 => Ok(Self::Signed),
            2 => Ok(Self::Unsigned),
            3 => Ok(Self::Float),
            _ => Err(Error::Format(format!(
                "analytics: unsupported value class {value}"
            ))),
        }
    }

    /// Return the required class for an exact FlatGeobuf `ColumnType` byte.
    pub fn from_fgb_type(fgb_type: u8) -> Result<Self> {
        match ColumnType(fgb_type) {
            ColumnType::Bool
            | ColumnType::String
            | ColumnType::Json
            | ColumnType::DateTime
            | ColumnType::Binary => Ok(Self::Opaque),
            ColumnType::Byte | ColumnType::Short | ColumnType::Int | ColumnType::Long => {
                Ok(Self::Signed)
            }
            ColumnType::UByte | ColumnType::UShort | ColumnType::UInt | ColumnType::ULong => {
                Ok(Self::Unsigned)
            }
            ColumnType::Float | ColumnType::Double => Ok(Self::Float),
            _ => Err(Error::Format(format!(
                "analytics: unsupported FlatGeobuf column type {fgb_type}"
            ))),
        }
    }
}

/// Return the Arrow physical type used for a FlatGeobuf `ColumnType` byte.
pub fn arrow_data_type(fgb_type: u8) -> Result<DataType> {
    match ColumnType(fgb_type) {
        ColumnType::Byte => Ok(DataType::Int8),
        ColumnType::UByte => Ok(DataType::UInt8),
        ColumnType::Bool => Ok(DataType::Boolean),
        ColumnType::Short => Ok(DataType::Int16),
        ColumnType::UShort => Ok(DataType::UInt16),
        ColumnType::Int => Ok(DataType::Int32),
        ColumnType::UInt => Ok(DataType::UInt32),
        ColumnType::Long => Ok(DataType::Int64),
        ColumnType::ULong => Ok(DataType::UInt64),
        ColumnType::Float => Ok(DataType::Float32),
        ColumnType::Double => Ok(DataType::Float64),
        ColumnType::String | ColumnType::Json | ColumnType::DateTime => Ok(DataType::Utf8),
        ColumnType::Binary => Ok(DataType::Binary),
        _ => Err(Error::Format(format!(
            "analytics: unsupported FlatGeobuf column type {fgb_type}"
        ))),
    }
}

/// The fixed 40-byte column descriptor (excluding its variable streams).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnDesc {
    /// FlatGeobuf header column index.
    pub column_index: u16,
    /// Exact FlatGeobuf `ColumnType` byte.
    pub fgb_type: u8,
    /// Statistical value class.
    pub value_class: ValueClass,
    /// `HAS_ARROW_IPC`; all other bits are reserved zero.
    pub flags: u32,
    /// Column stream offset relative to the section start.
    pub stream_offset: u64,
    /// Complete column stream size, including Arrow payloads.
    pub stream_size: u64,
    /// Absolute-offset array location relative to the section start.
    pub arrow_offsets_offset: u64,
    /// Sum of independent Arrow IPC chunk sizes.
    pub arrow_bytes: u64,
}

/// Exact global statistics for one property column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalStat {
    /// Non-null values, including NaN and infinities.
    pub non_null_count: u64,
    /// NaN values (zero outside float columns).
    pub nan_count: u64,
    /// i64/u64 value bits or f64 bits; zero when no bounds exist.
    pub min_bits: u64,
    /// i64/u64 value bits or f64 bits; zero when no bounds exist.
    pub max_bits: u64,
    /// i128/u128 little-endian sum, or f64 in the first eight bytes.
    pub sum: [u8; 16],
    /// `SUM_VALID` and `HAS_INFINITY`; all other bits reserved zero.
    pub flags: u32,
    /// Reserved zero.
    pub reserved: u32,
}

impl GlobalStat {
    /// Whether numeric bounds exist (`non_null_count > nan_count`).
    pub fn has_bounds(&self) -> bool {
        self.non_null_count > self.nan_count
    }

    /// Whether the stored sum passed the class-specific validity rules.
    pub fn sum_valid(&self) -> bool {
        self.flags & SUM_VALID != 0
    }

    /// Whether at least one non-null float value was infinite.
    pub fn has_infinity(&self) -> bool {
        self.flags & HAS_INFINITY != 0
    }
}

/// Numeric/null zone map for one fixed-row chunk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkStat {
    /// Minimum value bits, or zero when no bounds exist.
    pub min_bits: u64,
    /// Maximum value bits, or zero when no bounds exist.
    pub max_bits: u64,
    /// Non-null values, including NaN and infinities.
    pub non_null_count: u32,
    /// NaN values (zero outside float columns).
    pub nan_count: u32,
}

impl ChunkStat {
    /// Whether numeric bounds exist (`non_null_count > nan_count`).
    pub fn has_bounds(&self) -> bool {
        self.non_null_count > self.nan_count
    }
}

/// A body byte range and its half-open feature ordinal range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyRange {
    /// Absolute offset of the first feature length prefix.
    pub start: u64,
    /// Absolute end offset (the next chunk or `body_len`).
    pub end: u64,
    /// First feature ordinal in the range.
    pub first_ordinal: u64,
    /// Exclusive feature ordinal after the range.
    pub end_ordinal: u64,
}

/// One column's descriptor and all of its non-payload metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnAnalytics {
    /// Property name from the FlatGeobuf header.
    pub name: String,
    /// Nullability from the FlatGeobuf header.
    pub nullable: bool,
    /// Fixed section descriptor.
    pub descriptor: ColumnDesc,
    /// Global statistics.
    pub global: GlobalStat,
    /// One statistic record per chunk.
    pub chunks: Vec<ChunkStat>,
    /// Absolute Arrow chunk offsets, with one terminal offset.
    pub arrow_offsets: Vec<u64>,
}

/// Fully validated, bounded BODY_ANALYTICS metadata (without Arrow payloads).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyAnalytics {
    /// Absolute section start.
    pub section_start: u64,
    /// Directory-declared section size.
    pub section_size: u64,
    /// Fixed rows per chunk (except the final chunk).
    pub chunk_rows: u32,
    /// Number of property columns.
    pub column_count: u32,
    /// Body feature count.
    pub row_count: u64,
    /// Number of chunks.
    pub chunk_count: u64,
    /// Shared absolute body offsets, with terminal `body_len`.
    pub chunk_offsets: Vec<u64>,
    /// Property columns in ascending FlatGeobuf column-index order.
    pub columns: Vec<ColumnAnalytics>,
    /// Actual section metadata bytes read, excluding Arrow payloads and the FGB header.
    pub metadata_bytes_read: u64,
}

impl BodyAnalytics {
    /// Find one column by its FlatGeobuf header index.
    pub fn column(&self, column_index: u16) -> Option<&ColumnAnalytics> {
        self.columns
            .iter()
            .find(|column| column.descriptor.column_index == column_index)
    }

    /// Number of body rows in one chunk.
    pub fn chunk_row_count(&self, chunk_index: u64) -> Option<u32> {
        if chunk_index >= self.chunk_count {
            return None;
        }
        let first = chunk_index.checked_mul(u64::from(self.chunk_rows))?;
        let remaining = self.row_count.checked_sub(first)?;
        Some(remaining.min(u64::from(self.chunk_rows)) as u32)
    }

    /// Absolute body range and ordinal range for one chunk.
    pub fn body_range(&self, chunk_index: u64) -> Result<BodyRange> {
        let index = usize::try_from(chunk_index).map_err(|_| {
            Error::Format("analytics chunk index does not fit this platform".into())
        })?;
        if chunk_index >= self.chunk_count {
            return Err(Error::Format(format!(
                "analytics chunk {chunk_index} is out of range"
            )));
        }
        let next = index
            .checked_add(1)
            .ok_or_else(|| Error::Format("analytics chunk index overflow".into()))?;
        let start = *self
            .chunk_offsets
            .get(index)
            .ok_or_else(|| Error::Format("analytics body offsets are truncated".into()))?;
        let end = *self
            .chunk_offsets
            .get(next)
            .ok_or_else(|| Error::Format("analytics body offsets are truncated".into()))?;
        let first_ordinal = chunk_index
            .checked_mul(u64::from(self.chunk_rows))
            .ok_or_else(|| Error::Format("analytics chunk ordinal overflow".into()))?;
        let end_ordinal = first_ordinal
            .checked_add(u64::from(self.chunk_row_count(chunk_index).ok_or_else(
                || Error::Format("analytics chunk is out of range".into()),
            )?))
            .ok_or_else(|| Error::Format("analytics chunk ordinal overflow".into()))?;
        Ok(BodyRange {
            start,
            end,
            first_ordinal,
            end_ordinal,
        })
    }

    /// Return the exact absolute byte range of one Arrow IPC chunk.
    pub fn arrow_range(&self, column_index: u16, chunk_index: u64) -> Result<(u64, u64)> {
        let column = self.column(column_index).ok_or_else(|| {
            Error::Format(format!("analytics column {column_index} does not exist"))
        })?;
        if chunk_index >= self.chunk_count {
            return Err(Error::Format(format!(
                "analytics chunk {chunk_index} is out of range for column {column_index}"
            )));
        }
        let index = usize::try_from(chunk_index).map_err(|_| {
            Error::Format("analytics chunk index does not fit this platform".into())
        })?;
        let start = *column
            .arrow_offsets
            .get(index)
            .ok_or_else(|| Error::Format("analytics Arrow offsets are truncated".into()))?;
        let next = index
            .checked_add(1)
            .ok_or_else(|| Error::Format("analytics Arrow chunk index overflows".into()))?;
        let end = *column
            .arrow_offsets
            .get(next)
            .ok_or_else(|| Error::Format("analytics Arrow offsets are truncated".into()))?;
        let len = end
            .checked_sub(start)
            .ok_or_else(|| Error::Format("analytics Arrow offsets are not increasing".into()))?;
        if len == 0 || len > MAX_ARROW_CHUNK_BYTES {
            return Err(Error::Format(format!(
                "analytics Arrow chunk {column_index}/{chunk_index} has invalid size {len}"
            )));
        }
        Ok((start, end))
    }
}

/// Exact bytes and identity of one independent Arrow IPC chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrowChunk {
    /// FlatGeobuf header column index.
    pub column_index: u16,
    /// Zero-based chunk index.
    pub chunk_index: u64,
    /// One complete Arrow IPC file containing exactly one RecordBatch.
    pub bytes: Vec<u8>,
}

#[derive(Clone)]
pub(crate) struct ColumnSchema {
    pub(crate) index: u16,
    pub(crate) name: String,
    pub(crate) kind: ColumnType,
    pub(crate) nullable: bool,
}

struct BodyInfo {
    row_count: u64,
    columns: Vec<ColumnSchema>,
    data_start: u64,
    leaf_start: u64,
}

fn body_info(header: Header<'_>, index_start: u64, body_len: u64) -> Result<BodyInfo> {
    let row_count = header.features_count();
    if row_count == 0 {
        return Err(Error::Format(
            "analytics requires a positive FlatGeobuf features_count".into(),
        ));
    }
    if header.index_node_size() < 2 {
        return Err(Error::Format(
            "analytics requires an index node size of at least 2".into(),
        ));
    }
    // This also bounds the upstream index-size calculation before allocation.
    if row_count > body_len / 40 {
        return Err(Error::Format(
            "analytics feature count exceeds the available body bytes".into(),
        ));
    }
    let rows = usize::try_from(row_count)
        .map_err(|_| Error::Format("analytics row count does not fit this platform".into()))?;
    let index_size = PackedRTree::index_size(rows, header.index_node_size());
    let index_size = u64::try_from(index_size)
        .map_err(|_| Error::Format("analytics index size does not fit u64".into()))?;
    let data_start = index_start
        .checked_add(index_size)
        .ok_or_else(|| Error::Format("analytics data start overflows".into()))?;
    let leaf_bytes = row_count
        .checked_mul(40)
        .ok_or_else(|| Error::Format("analytics leaf size overflows".into()))?;
    let leaf_start = data_start
        .checked_sub(leaf_bytes)
        .ok_or_else(|| Error::Format("analytics leaf start underflows".into()))?;
    if data_start > body_len || leaf_start < index_start {
        return Err(Error::Format(
            "analytics index/data offsets lie outside the body".into(),
        ));
    }
    let mut columns = Vec::new();
    if let Some(header_columns) = header.columns() {
        if header_columns.len() > usize::from(u16::MAX) + 1 {
            return Err(Error::Format(
                "analytics has too many FlatGeobuf property columns".into(),
            ));
        }
        columns.reserve(header_columns.len());
        for index in 0..header_columns.len() {
            let column = header_columns.get(index);
            ValueClass::from_fgb_type(column.type_().0)?;
            columns.push(ColumnSchema {
                index: u16::try_from(index)
                    .map_err(|_| Error::Format("analytics column index does not fit u16".into()))?,
                name: column.name().to_owned(),
                kind: column.type_(),
                nullable: column.nullable(),
            });
        }
    }
    Ok(BodyInfo {
        row_count,
        columns,
        data_start,
        leaf_start,
    })
}

pub(crate) fn chunk_count(row_count: u64, chunk_rows: u32) -> Result<u64> {
    if chunk_rows == 0 {
        return Err(Error::Format(
            "analytics chunk_rows must be positive".into(),
        ));
    }
    let rows = u64::from(chunk_rows);
    row_count
        .checked_div(rows)
        .and_then(|whole| whole.checked_add(u64::from(!row_count.is_multiple_of(rows))))
        .ok_or_else(|| Error::Format("analytics chunk count overflows".into()))
}

#[derive(Debug)]
pub(crate) enum Value {
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

impl Value {
    fn from_column(value: &ColumnValue<'_>) -> Self {
        match value {
            ColumnValue::Byte(v) => Self::Byte(*v),
            ColumnValue::UByte(v) => Self::UByte(*v),
            ColumnValue::Bool(v) => Self::Bool(*v),
            ColumnValue::Short(v) => Self::Short(*v),
            ColumnValue::UShort(v) => Self::UShort(*v),
            ColumnValue::Int(v) => Self::Int(*v),
            ColumnValue::UInt(v) => Self::UInt(*v),
            ColumnValue::Long(v) => Self::Long(*v),
            ColumnValue::ULong(v) => Self::ULong(*v),
            ColumnValue::Float(v) => Self::Float(*v),
            ColumnValue::Double(v) => Self::Double(*v),
            ColumnValue::String(v) => Self::String((*v).to_owned()),
            ColumnValue::Json(v) => Self::Json((*v).to_owned()),
            ColumnValue::DateTime(v) => Self::DateTime((*v).to_owned()),
            ColumnValue::Binary(v) => Self::Binary(v.to_vec()),
        }
    }

    fn signed(&self) -> Result<i64> {
        match self {
            Self::Byte(v) => Ok(i64::from(*v)),
            Self::Short(v) => Ok(i64::from(*v)),
            Self::Int(v) => Ok(i64::from(*v)),
            Self::Long(v) => Ok(*v),
            _ => Err(Error::Format("analytics signed value type mismatch".into())),
        }
    }

    fn unsigned(&self) -> Result<u64> {
        match self {
            Self::UByte(v) => Ok(u64::from(*v)),
            Self::UShort(v) => Ok(u64::from(*v)),
            Self::UInt(v) => Ok(u64::from(*v)),
            Self::ULong(v) => Ok(*v),
            _ => Err(Error::Format(
                "analytics unsigned value type mismatch".into(),
            )),
        }
    }

    fn float(&self) -> Result<f64> {
        match self {
            Self::Float(v) => Ok(f64::from(*v)),
            Self::Double(v) => Ok(*v),
            _ => Err(Error::Format("analytics float value type mismatch".into())),
        }
    }

    fn matches_type(&self, kind: ColumnType) -> bool {
        matches!(
            (self, kind),
            (Self::Byte(_), ColumnType::Byte)
                | (Self::UByte(_), ColumnType::UByte)
                | (Self::Bool(_), ColumnType::Bool)
                | (Self::Short(_), ColumnType::Short)
                | (Self::UShort(_), ColumnType::UShort)
                | (Self::Int(_), ColumnType::Int)
                | (Self::UInt(_), ColumnType::UInt)
                | (Self::Long(_), ColumnType::Long)
                | (Self::ULong(_), ColumnType::ULong)
                | (Self::Float(_), ColumnType::Float)
                | (Self::Double(_), ColumnType::Double)
                | (Self::String(_), ColumnType::String)
                | (Self::Json(_), ColumnType::Json)
                | (Self::DateTime(_), ColumnType::DateTime)
                | (Self::Binary(_), ColumnType::Binary)
        )
    }

    fn canonicalize_nan(&mut self) {
        match self {
            Self::Float(v) if v.is_nan() => *v = f32::NAN,
            Self::Double(v) if v.is_nan() => *v = f64::NAN,
            _ => {}
        }
    }
}

struct StatsAccumulator {
    global: GlobalStat,
    chunk: ChunkStat,
    global_has_bound: bool,
    chunk_has_bound: bool,
    signed_sum: i128,
    unsigned_sum: u128,
    float_sum: f64,
    float_sum_valid: bool,
    has_infinity: bool,
}

impl StatsAccumulator {
    fn new() -> Self {
        Self {
            global: GlobalStat {
                non_null_count: 0,
                nan_count: 0,
                min_bits: 0,
                max_bits: 0,
                sum: [0; 16],
                flags: 0,
                reserved: 0,
            },
            chunk: ChunkStat::default(),
            global_has_bound: false,
            chunk_has_bound: false,
            signed_sum: 0,
            unsigned_sum: 0,
            float_sum: 0.0,
            float_sum_valid: true,
            has_infinity: false,
        }
    }

    fn update(&mut self, class: ValueClass, value: Option<&Value>) -> Result<()> {
        let Some(value) = value else {
            return Ok(());
        };
        self.global.non_null_count = self
            .global
            .non_null_count
            .checked_add(1)
            .ok_or_else(|| Error::Format("analytics non-null count overflows".into()))?;
        self.chunk.non_null_count = self
            .chunk
            .non_null_count
            .checked_add(1)
            .ok_or_else(|| Error::Format("analytics chunk non-null count overflows".into()))?;
        match class {
            ValueClass::Opaque => {}
            ValueClass::Signed => {
                let value = value.signed()?;
                self.signed_sum = self
                    .signed_sum
                    .checked_add(i128::from(value))
                    .ok_or_else(|| Error::Format("analytics signed sum overflows".into()))?;
                Self::update_signed_bounds(
                    &mut self.global.min_bits,
                    &mut self.global.max_bits,
                    &mut self.global_has_bound,
                    value,
                );
                Self::update_signed_bounds(
                    &mut self.chunk.min_bits,
                    &mut self.chunk.max_bits,
                    &mut self.chunk_has_bound,
                    value,
                );
            }
            ValueClass::Unsigned => {
                let value = value.unsigned()?;
                self.unsigned_sum = self
                    .unsigned_sum
                    .checked_add(u128::from(value))
                    .ok_or_else(|| Error::Format("analytics unsigned sum overflows".into()))?;
                Self::update_unsigned_bounds(
                    &mut self.global.min_bits,
                    &mut self.global.max_bits,
                    &mut self.global_has_bound,
                    value,
                );
                Self::update_unsigned_bounds(
                    &mut self.chunk.min_bits,
                    &mut self.chunk.max_bits,
                    &mut self.chunk_has_bound,
                    value,
                );
            }
            ValueClass::Float => {
                let value = value.float()?;
                if value.is_nan() {
                    self.global.nan_count = self
                        .global
                        .nan_count
                        .checked_add(1)
                        .ok_or_else(|| Error::Format("analytics NaN count overflows".into()))?;
                    self.chunk.nan_count =
                        self.chunk.nan_count.checked_add(1).ok_or_else(|| {
                            Error::Format("analytics chunk NaN count overflows".into())
                        })?;
                } else {
                    Self::update_float_bounds(
                        &mut self.global.min_bits,
                        &mut self.global.max_bits,
                        &mut self.global_has_bound,
                        value,
                    );
                    Self::update_float_bounds(
                        &mut self.chunk.min_bits,
                        &mut self.chunk.max_bits,
                        &mut self.chunk_has_bound,
                        value,
                    );
                }
                if value.is_infinite() {
                    self.has_infinity = true;
                }
                if !value.is_finite() {
                    self.float_sum_valid = false;
                } else if self.float_sum_valid {
                    self.float_sum += value;
                    if !self.float_sum.is_finite() {
                        self.float_sum_valid = false;
                    }
                }
            }
        }
        Ok(())
    }

    fn update_signed_bounds(min: &mut u64, max: &mut u64, present: &mut bool, value: i64) {
        if !*present {
            *min = u64::from_le_bytes(value.to_le_bytes());
            *max = u64::from_le_bytes(value.to_le_bytes());
            *present = true;
            return;
        }
        let current_min = i64::from_le_bytes(min.to_le_bytes());
        let current_max = i64::from_le_bytes(max.to_le_bytes());
        if value < current_min {
            *min = u64::from_le_bytes(value.to_le_bytes());
        }
        if value > current_max {
            *max = u64::from_le_bytes(value.to_le_bytes());
        }
    }

    fn update_unsigned_bounds(min: &mut u64, max: &mut u64, present: &mut bool, value: u64) {
        if !*present {
            *min = value;
            *max = value;
            *present = true;
            return;
        }
        *min = (*min).min(value);
        *max = (*max).max(value);
    }

    fn update_float_bounds(min: &mut u64, max: &mut u64, present: &mut bool, value: f64) {
        if !*present {
            *min = value.to_bits();
            *max = value.to_bits();
            *present = true;
            return;
        }
        if value < f64::from_bits(*min) {
            *min = value.to_bits();
        }
        if value > f64::from_bits(*max) {
            *max = value.to_bits();
        }
    }

    fn canonicalize_chunk(&mut self, class: ValueClass) {
        if class == ValueClass::Float && self.chunk_has_bound {
            if f64::from_bits(self.chunk.min_bits) == 0.0 {
                self.chunk.min_bits = (-0.0f64).to_bits();
            }
            if f64::from_bits(self.chunk.max_bits) == 0.0 {
                self.chunk.max_bits = 0.0f64.to_bits();
            }
        }
    }

    fn finish_chunk(&mut self, class: ValueClass) -> ChunkStat {
        self.canonicalize_chunk(class);
        self.chunk_has_bound = false;
        std::mem::take(&mut self.chunk)
    }

    fn finish_global(mut self, class: ValueClass) -> GlobalStat {
        match class {
            ValueClass::Opaque => {
                self.global.nan_count = 0;
                self.global.min_bits = 0;
                self.global.max_bits = 0;
                self.global.sum = [0; 16];
                self.global.flags = 0;
            }
            ValueClass::Signed => {
                self.global.sum = self.signed_sum.to_le_bytes();
                self.global.flags = SUM_VALID;
            }
            ValueClass::Unsigned => {
                self.global.sum = self.unsigned_sum.to_le_bytes();
                self.global.flags = SUM_VALID;
            }
            ValueClass::Float => {
                if self.global_has_bound {
                    if f64::from_bits(self.global.min_bits) == 0.0 {
                        self.global.min_bits = (-0.0f64).to_bits();
                    }
                    if f64::from_bits(self.global.max_bits) == 0.0 {
                        self.global.max_bits = 0.0f64.to_bits();
                    }
                }
                if self.float_sum_valid {
                    self.global.sum[..8].copy_from_slice(&self.float_sum.to_le_bytes());
                    self.global.flags = SUM_VALID;
                } else {
                    self.global.sum = [0; 16];
                }
                if self.has_infinity {
                    self.global.flags |= HAS_INFINITY;
                }
            }
        }
        self.global
    }
}

struct CappedWriter<W: Write> {
    inner: W,
    written: u64,
    context: String,
}

impl<W: Write> CappedWriter<W> {
    fn new(inner: W, column_index: u16, chunk_index: usize) -> Self {
        Self {
            inner,
            written: 0,
            context: format!("analytics Arrow chunk {column_index}/{chunk_index}"),
        }
    }

    fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for CappedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let incoming = buf.len() as u64;
        if self
            .written
            .checked_add(incoming)
            .is_none_or(|size| size > MAX_ARROW_CHUNK_BYTES)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{} exceeds the 64 MiB cap", self.context),
            ));
        }
        let written = self.inner.write(buf)?;
        self.written += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub(crate) struct Spool {
    file: std::fs::File,
    lengths: Vec<u64>,
}

#[cfg(test)]
thread_local! {
    static LIVE_SPOOLS: Cell<usize> = const { Cell::new(0) };
    static SPOOLS_CREATED: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn live_spool_count() -> usize {
    LIVE_SPOOLS.with(Cell::get)
}

#[cfg(test)]
pub(crate) fn spool_create_count() -> usize {
    SPOOLS_CREATED.with(Cell::get)
}

impl Spool {
    fn new() -> Result<Self> {
        let file = tempfile::tempfile()?;
        #[cfg(test)]
        LIVE_SPOOLS.with(|count| count.set(count.get() + 1));
        #[cfg(test)]
        SPOOLS_CREATED.with(|count| count.set(count.get() + 1));
        Ok(Self {
            file,
            lengths: Vec::new(),
        })
    }
}

impl Drop for Spool {
    fn drop(&mut self) {
        #[cfg(test)]
        LIVE_SPOOLS.with(|count| count.set(count.get().saturating_sub(1)));
    }
}

pub(crate) enum ArrowChunks {
    Memory(Vec<Vec<u8>>),
    Spool(Spool),
}

impl ArrowChunks {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Memory(chunks) => chunks.len(),
            Self::Spool(spool) => spool.lengths.len(),
        }
    }

    pub(crate) fn chunk_len(&self, index: usize) -> Result<u64> {
        match self {
            Self::Memory(chunks) => chunks
                .get(index)
                .map(|bytes| bytes.len() as u64)
                .ok_or_else(|| Error::Format("analytics Arrow chunk is missing".into())),
            Self::Spool(spool) => spool
                .lengths
                .get(index)
                .copied()
                .ok_or_else(|| Error::Format("analytics spooled Arrow chunk is missing".into())),
        }
    }

    pub(crate) fn arrow_bytes(&self) -> Result<u64> {
        (0..self.len()).try_fold(0u64, |sum, index| {
            sum.checked_add(self.chunk_len(index)?)
                .ok_or_else(|| Error::Format("analytics Arrow byte count overflows".into()))
        })
    }

    pub(crate) fn write_to(&mut self, out: &mut dyn Write) -> Result<u64> {
        match self {
            Self::Memory(chunks) => chunks.iter().try_fold(0u64, |sum, bytes| {
                out.write_all(bytes)?;
                sum.checked_add(bytes.len() as u64)
                    .ok_or_else(|| Error::Format("analytics Arrow byte count overflows".into()))
            }),
            Self::Spool(spool) => {
                spool.file.seek(SeekFrom::Start(0))?;
                let mut total = 0u64;
                for &len in &spool.lengths {
                    copy_exact(&mut spool.file, len, out, "spooled Arrow chunk")?;
                    total = total.checked_add(len).ok_or_else(|| {
                        Error::Format("analytics Arrow byte count overflows".into())
                    })?;
                }
                Ok(total)
            }
        }
    }
}

pub(crate) struct BuiltColumn {
    pub(crate) schema: ColumnSchema,
    pub(crate) global: GlobalStat,
    pub(crate) chunks: Vec<ChunkStat>,
    pub(crate) arrow_chunks: ArrowChunks,
}

pub(crate) struct ColumnBuilder {
    schema: ColumnSchema,
    class: ValueClass,
    chunk_rows: u32,
    stats: StatsAccumulator,
    values: Vec<Option<Value>>,
    chunks: Vec<ChunkStat>,
    arrow_chunks: ArrowChunks,
}

impl ColumnBuilder {
    pub(crate) fn new(schema: ColumnSchema, chunk_rows: u32) -> Result<Self> {
        Self::with_storage(schema, chunk_rows, false)
    }

    pub(crate) fn new_spooled(schema: ColumnSchema, chunk_rows: u32) -> Result<Self> {
        Self::with_storage(schema, chunk_rows, true)
    }

    fn with_storage(schema: ColumnSchema, chunk_rows: u32, spooled: bool) -> Result<Self> {
        let class = ValueClass::from_fgb_type(schema.kind.0)?;
        let arrow_chunks = if spooled {
            ArrowChunks::Spool(Spool::new()?)
        } else {
            ArrowChunks::Memory(Vec::new())
        };
        Ok(Self {
            class,
            schema,
            chunk_rows,
            stats: StatsAccumulator::new(),
            values: Vec::new(),
            chunks: Vec::new(),
            arrow_chunks,
        })
    }

    pub(crate) fn push(&mut self, mut value: Option<Value>) -> Result<()> {
        if let Some(value) = &value {
            if !value.matches_type(self.schema.kind) {
                return Err(Error::Format(format!(
                    "analytics property {:?} has a runtime type mismatch",
                    self.schema.name
                )));
            }
        }
        if let Some(value) = &mut value {
            value.canonicalize_nan();
        }
        self.stats.update(self.class, value.as_ref())?;
        self.values.push(value);
        if self.values.len() == self.chunk_rows as usize {
            self.finish_chunk()?;
        }
        Ok(())
    }

    fn finish_chunk(&mut self) -> Result<()> {
        if self.values.is_empty() {
            return Ok(());
        }
        let column_index = self.schema.index;
        let chunk_index = self.arrow_chunks.len();
        let array = make_array(&self.schema, &self.values)?;
        let field = Field::new(
            &self.schema.name,
            array.data_type().clone(),
            self.schema.nullable,
        );
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![array]).map_err(|error| {
            Error::Format(format!(
                "analytics Arrow batch {column_index}/{chunk_index}: {error}"
            ))
        })?;
        match &mut self.arrow_chunks {
            ArrowChunks::Memory(chunks) => {
                let buffer = CappedWriter::new(Vec::new(), column_index, chunk_index);
                let buffer = encode_arrow_chunk(buffer, &batch, column_index, chunk_index)?;
                let bytes = buffer.into_inner();
                validate_arrow_len(column_index, chunk_index, bytes.len() as u64)?;
                chunks.push(bytes);
            }
            ArrowChunks::Spool(spool) => {
                let buffer =
                    CappedWriter::new(BufWriter::new(&mut spool.file), column_index, chunk_index);
                let buffer = encode_arrow_chunk(buffer, &batch, column_index, chunk_index)?;
                let len = buffer.written;
                buffer.into_inner().flush()?;
                validate_arrow_len(column_index, chunk_index, len)?;
                spool.lengths.push(len);
            }
        }
        self.values.clear();
        let stat = self.stats.finish_chunk(self.class);
        self.chunks.push(stat);
        Ok(())
    }

    pub(crate) fn finish(mut self, expected_chunks: usize) -> Result<BuiltColumn> {
        self.finish_chunk()?;
        if self.arrow_chunks.len() != expected_chunks {
            return Err(Error::Format(format!(
                "analytics property {:?} produced {} chunks, expected {expected_chunks}",
                self.schema.name,
                self.arrow_chunks.len()
            )));
        }
        Ok(BuiltColumn {
            global: self.stats.finish_global(self.class),
            schema: self.schema,
            chunks: self.chunks,
            arrow_chunks: self.arrow_chunks,
        })
    }
}

fn encode_arrow_chunk<W: Write>(
    buffer: W,
    batch: &RecordBatch,
    column_index: u16,
    chunk_index: usize,
) -> Result<W> {
    let mut writer = FileWriter::try_new(buffer, batch.schema().as_ref()).map_err(|error| {
        Error::Format(format!(
            "analytics Arrow writer {column_index}/{chunk_index}: {error}"
        ))
    })?;
    writer.write(batch).map_err(|error| {
        Error::Format(format!(
            "analytics Arrow chunk {column_index}/{chunk_index}: {error}"
        ))
    })?;
    writer.finish().map_err(|error| {
        Error::Format(format!(
            "analytics Arrow finish {column_index}/{chunk_index}: {error}"
        ))
    })?;
    writer.into_inner().map_err(|error| {
        Error::Format(format!(
            "analytics Arrow inner {column_index}/{chunk_index}: {error}"
        ))
    })
}

fn validate_arrow_len(column_index: u16, chunk_index: usize, len: u64) -> Result<()> {
    if len == 0 || len > MAX_ARROW_CHUNK_BYTES {
        return Err(Error::Format(format!(
            "analytics Arrow chunk {column_index}/{chunk_index} has invalid size {len}"
        )));
    }
    Ok(())
}

fn map_values<T>(
    schema: &ColumnSchema,
    values: &[Option<Value>],
    mut extract: impl FnMut(&Value) -> Option<T>,
) -> Result<Vec<Option<T>>> {
    values
        .iter()
        .map(|value| match value {
            None => Ok(None),
            Some(value) => extract(value).map(Some).ok_or_else(|| {
                Error::Format(format!(
                    "analytics property {:?} has an Arrow type mismatch",
                    schema.name
                ))
            }),
        })
        .collect()
}

macro_rules! primitive_array {
    ($schema:expr, $values:expr, $variant:ident, $array:ident) => {{
        let values = map_values($schema, $values, |value| match value {
            Value::$variant(v) => Some(*v),
            _ => None,
        })?;
        Arc::new($array::from(values)) as ArrayRef
    }};
}

fn make_array(schema: &ColumnSchema, values: &[Option<Value>]) -> Result<ArrayRef> {
    Ok(match schema.kind {
        ColumnType::Byte => primitive_array!(schema, values, Byte, Int8Array),
        ColumnType::UByte => primitive_array!(schema, values, UByte, UInt8Array),
        ColumnType::Bool => primitive_array!(schema, values, Bool, BooleanArray),
        ColumnType::Short => primitive_array!(schema, values, Short, Int16Array),
        ColumnType::UShort => primitive_array!(schema, values, UShort, UInt16Array),
        ColumnType::Int => primitive_array!(schema, values, Int, Int32Array),
        ColumnType::UInt => primitive_array!(schema, values, UInt, UInt32Array),
        ColumnType::Long => primitive_array!(schema, values, Long, Int64Array),
        ColumnType::ULong => primitive_array!(schema, values, ULong, UInt64Array),
        ColumnType::Float => primitive_array!(schema, values, Float, Float32Array),
        ColumnType::Double => primitive_array!(schema, values, Double, Float64Array),
        ColumnType::String | ColumnType::Json | ColumnType::DateTime => {
            let values = values
                .iter()
                .map(|value| match value {
                    None => Ok(None),
                    Some(Value::String(v)) | Some(Value::Json(v)) | Some(Value::DateTime(v)) => {
                        Ok(Some(v.as_str()))
                    }
                    Some(_) => Err(Error::Format(format!(
                        "analytics property {:?} has an Arrow string type mismatch",
                        schema.name
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StringArray::from(values))
        }
        ColumnType::Binary => {
            let values = values
                .iter()
                .map(|value| match value {
                    None => Ok(None),
                    Some(Value::Binary(v)) => Ok(Some(v.as_slice())),
                    Some(_) => Err(Error::Format(format!(
                        "analytics property {:?} has an Arrow binary type mismatch",
                        schema.name
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(BinaryArray::from(values))
        }
        _ => {
            return Err(Error::Format(format!(
                "analytics property {:?} has unsupported type {:?}",
                schema.name, schema.kind
            )));
        }
    })
}

struct RowProperties<'a> {
    columns: &'a [ColumnSchema],
    values: &'a mut [Option<Value>],
}

impl PropertyProcessor for RowProperties<'_> {
    fn property(
        &mut self,
        index: usize,
        name: &str,
        value: &ColumnValue,
    ) -> flatgeobuf::geozero::error::Result<bool> {
        let Some(column) = self.columns.get(index) else {
            return Err(flatgeobuf::geozero::error::GeozeroError::Property(
                "analytics property index is out of range".into(),
            ));
        };
        if column.name != name {
            return Err(flatgeobuf::geozero::error::GeozeroError::Property(format!(
                "analytics property index {index} has name {name:?}, expected {:?}",
                column.name
            )));
        }
        let slot = self.values.get_mut(index).ok_or_else(|| {
            flatgeobuf::geozero::error::GeozeroError::Property(
                "analytics property row storage is out of range".into(),
            )
        })?;
        if slot.is_some() {
            return Err(flatgeobuf::geozero::error::GeozeroError::Property(format!(
                "analytics property {name:?} occurs more than once in one feature"
            )));
        }
        *slot = Some(Value::from_column(value));
        Ok(false)
    }
}

/// Encoded kind-2 bytes, optional kind-3 bytes, and the property column count.
pub(crate) type EncodedSections = (Vec<u8>, Option<Vec<u8>>, u32);

pub(crate) struct BuiltSections {
    pub(crate) analytics: EncodedAnalytics,
    pub(crate) geometry: Option<crate::geom_analytics::EncodedGeometry>,
    pub(crate) column_count: u32,
}

/// Encode optional sections after all body rows and leaf offsets are verified.
pub(crate) fn encode_section(
    body: &[u8],
    section_start: u64,
    chunk_rows: u32,
    geometry_metrics: bool,
) -> Result<Option<EncodedSections>> {
    let body_len = u64::try_from(body.len())
        .map_err(|_| Error::Format("analytics body size does not fit u64".into()))?;
    encode_section_reader(
        SharedReader::new(Cursor::new(body)),
        body_len,
        section_start,
        chunk_rows,
        geometry_metrics,
    )
}

/// Seekable driver shared by slice and streaming builds. Only one chunk of
/// R-tree leaves and property rows is buffered; the body is never materialized.
pub(crate) fn encode_section_reader<R: Read + Seek>(
    shared: SharedReader<R>,
    body_len: u64,
    section_start: u64,
    chunk_rows: u32,
    geometry_metrics: bool,
) -> Result<Option<EncodedSections>> {
    let Some(sections) = encode_section_data(
        shared,
        body_len,
        section_start,
        chunk_rows,
        geometry_metrics,
        false,
    )?
    else {
        return Ok(None);
    };
    let BuiltSections {
        analytics,
        geometry,
        column_count,
    } = sections;
    let geometry = geometry
        .map(crate::geom_analytics::EncodedGeometry::into_bytes)
        .transpose()?;
    Ok(Some((analytics.into_bytes()?, geometry, column_count)))
}

pub(crate) fn encode_section_data<R: Read + Seek>(
    shared: SharedReader<R>,
    body_len: u64,
    section_start: u64,
    chunk_rows: u32,
    geometry_metrics: bool,
    spooled: bool,
) -> Result<Option<BuiltSections>> {
    shared.handle().seek(SeekFrom::Start(0))?;
    let fgb = FgbReader::open(shared.handle())
        .map_err(|error| Error::Format(format!("analytics cannot open body: {error}")))?;
    let info = body_info(fgb.header(), shared.position(), body_len)?;
    if info.columns.is_empty() {
        return Ok(None);
    }
    let column_count = u32::try_from(info.columns.len())
        .map_err(|_| Error::Format("analytics column count does not fit u32".into()))?;
    let chunks = chunk_count(info.row_count, chunk_rows)?;
    let chunk_count_usize = usize::try_from(chunks)
        .map_err(|_| Error::Format("analytics chunk count does not fit this platform".into()))?;
    let mut builders = info
        .columns
        .iter()
        .cloned()
        .map(|schema| {
            if spooled {
                ColumnBuilder::new_spooled(schema, chunk_rows)
            } else {
                ColumnBuilder::new(schema, chunk_rows)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let mut geometry = if geometry_metrics {
        Some(crate::geom_analytics::GeometryEncoder::new(
            chunk_rows, spooled,
        )?)
    } else {
        None
    };
    let mut iter = fgb
        .select_all()
        .map_err(|error| Error::Format(format!("analytics cannot iterate body: {error}")))?;
    let mut leaf = shared.handle();
    let mut chunk_leaves = Vec::new();
    let mut chunk_offsets = Vec::new();
    chunk_offsets
        .try_reserve(chunk_count_usize.saturating_add(1))
        .map_err(|_| Error::Format("analytics body-offset allocation is too large".into()))?;
    let mut current_feature_offset = info.data_start;
    let first_prefix_end = info
        .data_start
        .checked_add(4)
        .ok_or_else(|| Error::Format("analytics first feature prefix overflows".into()))?;
    if shared.position() != first_prefix_end {
        return Err(Error::Format(
            "analytics iterator did not consume exactly the first feature prefix".into(),
        ));
    }
    let mut row_index = 0u64;
    while let Some(feature) = iter
        .next()
        .map_err(|error| Error::Format(format!("analytics cannot read body feature: {error}")))?
    {
        if row_index.is_multiple_of(u64::from(chunk_rows)) {
            chunk_offsets.push(current_feature_offset);
            // The iterator has already decoded this chunk's first feature.
            // Buffer only this chunk's leaves, then restore the cursor for the
            // next feature; its buffered properties remain valid meanwhile.
            let resume_offset = shared.position();
            let remaining = info.row_count.checked_sub(row_index).ok_or_else(|| {
                Error::Format("analytics row offset exceeds the declared row count".into())
            })?;
            let rows = usize::try_from(remaining.min(u64::from(chunk_rows))).map_err(|_| {
                Error::Format("analytics chunk rows do not fit this platform".into())
            })?;
            let leaf_offset = row_index
                .checked_mul(40)
                .and_then(|offset| info.leaf_start.checked_add(offset))
                .ok_or_else(|| Error::Format("analytics leaf offset overflows".into()))?;
            leaf.seek(SeekFrom::Start(leaf_offset))?;
            chunk_leaves.clear();
            chunk_leaves
                .try_reserve(rows)
                .map_err(|_| Error::Format("analytics leaf allocation is too large".into()))?;
            for _ in 0..rows {
                chunk_leaves.push(NodeItem::from_reader(&mut leaf).map_err(|error| {
                    Error::Format(format!("analytics cannot read R-tree leaf: {error}"))
                })?);
            }
            leaf.seek(SeekFrom::Start(resume_offset))?;
        }
        let chunk_row = usize::try_from(row_index % u64::from(chunk_rows))
            .map_err(|_| Error::Format("analytics chunk row does not fit this platform".into()))?;
        let node = chunk_leaves
            .get(chunk_row)
            .ok_or_else(|| Error::Format("analytics R-tree leaf chunk is incomplete".into()))?;
        let expected = info
            .data_start
            .checked_add(node.offset)
            .ok_or_else(|| Error::Format("analytics leaf offset overflows".into()))?;
        if expected != current_feature_offset {
            return Err(Error::Format(format!(
                "analytics R-tree leaf {row_index} points to {expected}, body feature starts at {current_feature_offset}"
            )));
        }
        let mut values = (0..info.columns.len()).map(|_| None).collect::<Vec<_>>();
        feature
            .process_properties(&mut RowProperties {
                columns: &info.columns,
                values: &mut values,
            })
            .map_err(|error| {
                Error::Format(format!(
                    "analytics cannot read properties at row {row_index}: {error}"
                ))
            })?;
        for (builder, value) in builders.iter_mut().zip(values) {
            builder.push(value)?;
        }
        if let Some(encoder) = geometry.as_mut() {
            let metrics = crate::geom_metrics::feature_metrics(feature).map_err(|error| {
                Error::Format(format!("geometry metrics at body row {row_index}: {error}"))
            })?;
            encoder.push(metrics)?;
        }
        row_index += 1;
        current_feature_offset = shared.position();
    }
    if row_index != info.row_count {
        return Err(Error::Format(format!(
            "analytics read {row_index} body rows, expected {}",
            info.row_count
        )));
    }
    if current_feature_offset != body_len {
        return Err(Error::Format(format!(
            "analytics terminal body offset {current_feature_offset} != body length {body_len}"
        )));
    }
    chunk_offsets.push(body_len);
    let expected_offset_count = chunk_count_usize
        .checked_add(1)
        .ok_or_else(|| Error::Format("analytics body-offset count overflows".into()))?;
    if chunk_offsets.len() != expected_offset_count
        || chunk_offsets.first() != Some(&info.data_start)
        || !chunk_offsets.windows(2).all(|pair| pair[0] < pair[1])
    {
        return Err(Error::Format(
            "analytics body offsets are not complete and strictly increasing".into(),
        ));
    }
    let built = builders
        .into_iter()
        .map(|builder| builder.finish(chunk_count_usize))
        .collect::<Result<Vec<_>>>()?;
    let analytics = EncodedAnalytics::new(
        section_start,
        chunk_rows,
        column_count,
        info.row_count,
        chunks,
        chunk_offsets,
        built,
    )?;
    let geometry = match geometry {
        Some(encoder) => {
            let geometry_start = section_start
                .checked_add(analytics.size())
                .ok_or_else(|| Error::Format("geometry section offset overflows".into()))?;
            Some(encoder.finish(geometry_start, chunk_rows, info.row_count)?)
        }
        None => None,
    };
    Ok(Some(BuiltSections {
        analytics,
        geometry,
        column_count,
    }))
}

struct AnalyticsLayout {
    chunk_rows: u32,
    column_count: u32,
    row_count: u64,
    chunk_count: u64,
    chunk_offsets: Vec<u64>,
    first_stream: u64,
    column_metadata_size: u64,
    descriptors: Vec<ColumnDesc>,
    arrow_offsets: Vec<Vec<u64>>,
    size: u64,
}

pub(crate) struct EncodedAnalytics {
    section_start: u64,
    layout: AnalyticsLayout,
    columns: Vec<BuiltColumn>,
}

impl EncodedAnalytics {
    fn new(
        section_start: u64,
        chunk_rows: u32,
        column_count: u32,
        row_count: u64,
        chunk_count: u64,
        chunk_offsets: Vec<u64>,
        columns: Vec<BuiltColumn>,
    ) -> Result<Self> {
        let c = chunk_count;
        let shared_size = 8u64
            .checked_mul(
                c.checked_add(1).ok_or_else(|| {
                    Error::Format("analytics shared offset count overflows".into())
                })?,
            )
            .ok_or_else(|| Error::Format("analytics shared offsets overflow".into()))?;
        let first_stream = (SECTION_HEADER_SIZE as u64)
            .checked_add(
                (COLUMN_DESC_SIZE as u64)
                    .checked_mul(u64::from(column_count))
                    .ok_or_else(|| Error::Format("analytics descriptors overflow".into()))?,
            )
            .and_then(|size| size.checked_add(shared_size))
            .ok_or_else(|| Error::Format("analytics section header size overflows".into()))?;
        let chunk_stats_size = (CHUNK_STAT_SIZE as u64)
            .checked_mul(c)
            .ok_or_else(|| Error::Format("analytics chunk stats overflow".into()))?;
        let column_metadata_size = (GLOBAL_STAT_SIZE as u64)
            .checked_add(chunk_stats_size)
            .and_then(|size| size.checked_add(shared_size))
            .ok_or_else(|| Error::Format("analytics column metadata size overflows".into()))?;
        let fixed_metadata_size = column_metadata_size
            .checked_mul(u64::from(column_count))
            .and_then(|size| first_stream.checked_add(size))
            .ok_or_else(|| Error::Format("analytics fixed metadata size overflows".into()))?;
        if fixed_metadata_size > MAX_METADATA_BYTES {
            return Err(Error::Format(
                "analytics fixed metadata exceeds the 16 MiB cap".into(),
            ));
        }
        let mut stream_offset = first_stream;
        let mut descriptors = Vec::new();
        let mut arrow_offsets = Vec::new();
        for column in &columns {
            let column_index = column.schema.index;
            let arrow_bytes = column.arrow_chunks.arrow_bytes()?;
            let stream_size = column_metadata_size
                .checked_add(arrow_bytes)
                .ok_or_else(|| Error::Format("analytics stream size overflows".into()))?;
            let arrow_offsets_offset = stream_offset
                .checked_add(GLOBAL_STAT_SIZE as u64)
                .and_then(|offset| offset.checked_add(chunk_stats_size))
                .ok_or_else(|| Error::Format("analytics Arrow-offset location overflows".into()))?;
            let payload_start = section_start
                .checked_add(stream_offset)
                .and_then(|offset| offset.checked_add(column_metadata_size))
                .ok_or_else(|| Error::Format("analytics Arrow payload offset overflows".into()))?;
            let mut offsets = Vec::new();
            offsets
                .try_reserve(column.arrow_chunks.len().saturating_add(1))
                .map_err(|_| {
                    Error::Format("analytics Arrow-offset allocation is too large".into())
                })?;
            let mut offset = payload_start;
            offsets.push(offset);
            for index in 0..column.arrow_chunks.len() {
                offset = offset
                    .checked_add(column.arrow_chunks.chunk_len(index)?)
                    .ok_or_else(|| {
                        Error::Format("analytics Arrow chunk offset overflows".into())
                    })?;
                offsets.push(offset);
            }
            let stream_end = section_start
                .checked_add(stream_offset)
                .and_then(|start| start.checked_add(stream_size))
                .ok_or_else(|| Error::Format("analytics stream end overflows".into()))?;
            if offsets.last() != Some(&stream_end) {
                return Err(Error::Format(
                    "analytics terminal Arrow offset does not equal stream end".into(),
                ));
            }
            descriptors.push(ColumnDesc {
                column_index,
                fgb_type: column.schema.kind.0,
                value_class: ValueClass::from_fgb_type(column.schema.kind.0)?,
                flags: HAS_ARROW_IPC,
                stream_offset,
                stream_size,
                arrow_offsets_offset,
                arrow_bytes,
            });
            arrow_offsets.push(offsets);
            stream_offset = stream_offset
                .checked_add(stream_size)
                .ok_or_else(|| Error::Format("analytics section size overflows".into()))?;
        }
        Ok(Self {
            section_start,
            layout: AnalyticsLayout {
                chunk_rows,
                column_count,
                row_count,
                chunk_count,
                chunk_offsets,
                first_stream,
                column_metadata_size,
                descriptors,
                arrow_offsets,
                size: stream_offset,
            },
            columns,
        })
    }

    pub(crate) fn size(&self) -> u64 {
        self.layout.size
    }

    pub(crate) fn write_to(&mut self, out: &mut dyn Write) -> Result<u64> {
        let max_metadata = self
            .layout
            .first_stream
            .max(self.layout.column_metadata_size);
        let metadata_capacity = usize::try_from(max_metadata)
            .map_err(|_| Error::Format("analytics metadata does not fit this platform".into()))?;
        let mut metadata = Vec::new();
        metadata
            .try_reserve(metadata_capacity)
            .map_err(|_| Error::Format("analytics metadata allocation is too large".into()))?;
        metadata.extend_from_slice(&[ANALYTICS_VERSION, 0, 0, 0]);
        put_u32(&mut metadata, self.layout.chunk_rows);
        put_u32(&mut metadata, self.layout.column_count);
        put_u32(&mut metadata, 0);
        put_u64(&mut metadata, self.layout.row_count);
        put_u64(&mut metadata, self.layout.chunk_count);
        for desc in &self.layout.descriptors {
            put_u16(&mut metadata, desc.column_index);
            metadata.push(desc.fgb_type);
            metadata.push(desc.value_class as u8);
            put_u32(&mut metadata, desc.flags);
            put_u64(&mut metadata, desc.stream_offset);
            put_u64(&mut metadata, desc.stream_size);
            put_u64(&mut metadata, desc.arrow_offsets_offset);
            put_u64(&mut metadata, desc.arrow_bytes);
        }
        for &offset in &self.layout.chunk_offsets {
            put_u64(&mut metadata, offset);
        }
        let mut written = 0u64;
        write_part(out, &metadata, &mut written)?;
        for ((column, desc), offsets) in self
            .columns
            .iter_mut()
            .zip(&self.layout.descriptors)
            .zip(&self.layout.arrow_offsets)
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
                .checked_add(desc.stream_offset)
                .and_then(|offset| offset.checked_add(desc.stream_size))
                .ok_or_else(|| Error::Format("analytics encoded stream end overflows".into()))?;
            let payload = column.arrow_chunks.write_to(out)?;
            if payload != desc.arrow_bytes {
                return Err(Error::Format(
                    "analytics encoded Arrow size disagrees with descriptor".into(),
                ));
            }
            written = written
                .checked_add(payload)
                .ok_or_else(|| Error::Format("analytics encoded section end overflows".into()))?;
            let actual = self
                .section_start
                .checked_add(written)
                .ok_or_else(|| Error::Format("analytics encoded section end overflows".into()))?;
            if actual != expected {
                return Err(Error::Format(
                    "analytics encoded stream size disagrees with descriptor".into(),
                ));
            }
        }
        if written != self.layout.size {
            return Err(Error::Format(
                "analytics encoded section size disagrees with layout".into(),
            ));
        }
        Ok(written)
    }

    pub(crate) fn into_bytes(mut self) -> Result<Vec<u8>> {
        let total_size = usize::try_from(self.size())
            .map_err(|_| Error::Format("analytics section does not fit this platform".into()))?;
        let mut out = Vec::new();
        out.try_reserve(total_size)
            .map_err(|_| Error::Format("analytics section allocation is too large".into()))?;
        let written = self.write_to(&mut out)?;
        if written != out.len() as u64 {
            return Err(Error::Format(
                "analytics encoded byte count disagrees with output".into(),
            ));
        }
        Ok(out)
    }
}

pub(crate) fn write_part(out: &mut dyn Write, bytes: &[u8], written: &mut u64) -> Result<()> {
    out.write_all(bytes)?;
    *written = written
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| Error::Format("analytics encoded byte count overflows".into()))?;
    Ok(())
}

pub(crate) fn encode_global(out: &mut Vec<u8>, stat: &GlobalStat) {
    put_u64(out, stat.non_null_count);
    put_u64(out, stat.nan_count);
    put_u64(out, stat.min_bits);
    put_u64(out, stat.max_bits);
    out.extend_from_slice(&stat.sum);
    put_u32(out, stat.flags);
    put_u32(out, stat.reserved);
}

pub(crate) fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn take<'a>(
    bytes: &'a [u8],
    at: &mut usize,
    len: usize,
    what: &str,
) -> Result<&'a [u8]> {
    let end = at
        .checked_add(len)
        .ok_or_else(|| Error::Format(format!("analytics {what} offset overflows")))?;
    let value = bytes
        .get(*at..end)
        .ok_or_else(|| Error::Format(format!("analytics metadata is truncated at {what}")))?;
    *at = end;
    Ok(value)
}

pub(crate) fn get_u16(bytes: &[u8], at: &mut usize, what: &str) -> Result<u16> {
    let value = take(bytes, at, 2, what)?;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

pub(crate) fn get_u32(bytes: &[u8], at: &mut usize, what: &str) -> Result<u32> {
    let value = take(bytes, at, 4, what)?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

pub(crate) fn get_u64(bytes: &[u8], at: &mut usize, what: &str) -> Result<u64> {
    let value = take(bytes, at, 8, what)?;
    Ok(u64::from_le_bytes([
        value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
    ]))
}

pub(crate) fn read_blob<R: Read + Seek>(
    inner: &mut R,
    offset: u64,
    len: u64,
    remaining: &mut u64,
    what: &str,
) -> Result<Vec<u8>> {
    if len > *remaining || len > MAX_METADATA_BYTES {
        return Err(Error::Format(format!(
            "analytics {what} exceeds the 16 MiB metadata cap"
        )));
    }
    let len = usize::try_from(len)
        .map_err(|_| Error::Format(format!("analytics {what} does not fit this platform")))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| Error::Format(format!("analytics {what} allocation is too large")))?;
    bytes.resize(len, 0);
    inner.seek(SeekFrom::Start(offset))?;
    inner.read_exact(&mut bytes).map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::Format(format!("analytics {what} is truncated: {error}"))
        } else {
            Error::Io(error)
        }
    })?;
    *remaining -= len as u64;
    Ok(bytes)
}

/// Read and validate only section metadata, never Arrow payloads.
pub(crate) fn read_metadata<R: Read + Seek>(
    inner: &mut R,
    entry: &SectionEntry,
    body_len: u64,
) -> Result<BodyAnalytics> {
    if entry.flags != 0 || !entry.meta.is_empty() {
        return Err(Error::Format(
            "analytics directory flags and meta_len must be zero".into(),
        ));
    }
    if entry.size < SECTION_HEADER_SIZE as u64 {
        return Err(Error::Format(
            "analytics section header is truncated".into(),
        ));
    }
    inner.seek(SeekFrom::Start(0))?;
    let info = {
        let shared = SharedReader::new(&mut *inner);
        let fgb = FgbReader::open(shared.handle())
            .map_err(|error| Error::Format(format!("analytics cannot open body: {error}")))?;
        body_info(fgb.header(), shared.position(), body_len)?
    };
    let mut remaining = MAX_METADATA_BYTES;
    let header = read_blob(
        inner,
        entry.offset,
        SECTION_HEADER_SIZE as u64,
        &mut remaining,
        "section header",
    )?;
    let mut at = 0;
    if take(&header, &mut at, 1, "version")?[0] != ANALYTICS_VERSION
        || take(&header, &mut at, 1, "flags")?[0] != 0
        || get_u16(&header, &mut at, "reserved")? != 0
    {
        return Err(Error::Format(
            "analytics section version/flags/reserved fields are invalid".into(),
        ));
    }
    let chunk_rows = get_u32(&header, &mut at, "chunk_rows")?;
    let column_count = get_u32(&header, &mut at, "column_count")?;
    if get_u32(&header, &mut at, "reserved")? != 0 {
        return Err(Error::Format(
            "analytics header reserved bytes are nonzero".into(),
        ));
    }
    let row_count = get_u64(&header, &mut at, "row_count")?;
    let chunks = get_u64(&header, &mut at, "chunk_count")?;
    if chunk_rows == 0 || column_count == 0 || row_count != info.row_count {
        return Err(Error::Format(
            "analytics chunk_rows/column_count/row_count are invalid".into(),
        ));
    }
    if column_count as usize != info.columns.len() || chunks != chunk_count(row_count, chunk_rows)?
    {
        return Err(Error::Format(
            "analytics column/chunk counts disagree with the body".into(),
        ));
    }
    let c = chunks;
    let offset_array_size = 8u64
        .checked_mul(
            c.checked_add(1)
                .ok_or_else(|| Error::Format("analytics offset count overflows".into()))?,
        )
        .ok_or_else(|| Error::Format("analytics offset array size overflows".into()))?;
    let descriptors_size = (COLUMN_DESC_SIZE as u64)
        .checked_mul(u64::from(column_count))
        .ok_or_else(|| Error::Format("analytics descriptor size overflows".into()))?;
    let body_offsets_at = entry
        .offset
        .checked_add(SECTION_HEADER_SIZE as u64)
        .and_then(|offset| offset.checked_add(descriptors_size))
        .ok_or_else(|| Error::Format("analytics body-offset location overflows".into()))?;
    let first_stream = (SECTION_HEADER_SIZE as u64)
        .checked_add(descriptors_size)
        .and_then(|size| size.checked_add(offset_array_size))
        .ok_or_else(|| Error::Format("analytics first stream offset overflows".into()))?;
    let chunk_stats_size = (CHUNK_STAT_SIZE as u64)
        .checked_mul(c)
        .ok_or_else(|| Error::Format("analytics chunk stats overflow".into()))?;
    let column_metadata_size = (GLOBAL_STAT_SIZE as u64)
        .checked_add(chunk_stats_size)
        .and_then(|size| size.checked_add(offset_array_size))
        .ok_or_else(|| Error::Format("analytics column metadata size overflows".into()))?;
    let all_column_metadata = column_metadata_size
        .checked_mul(u64::from(column_count))
        .ok_or_else(|| Error::Format("analytics column metadata total overflows".into()))?;
    let fixed_metadata_size = first_stream
        .checked_add(all_column_metadata)
        .ok_or_else(|| Error::Format("analytics fixed metadata size overflows".into()))?;
    if fixed_metadata_size > entry.size || fixed_metadata_size > MAX_METADATA_BYTES {
        return Err(Error::Format(
            "analytics fixed metadata exceeds the section or 16 MiB cap".into(),
        ));
    }
    let descriptors_at = entry
        .offset
        .checked_add(SECTION_HEADER_SIZE as u64)
        .ok_or_else(|| Error::Format("analytics descriptor location overflows".into()))?;
    let descriptor_bytes = read_blob(
        inner,
        descriptors_at,
        descriptors_size,
        &mut remaining,
        "column descriptors",
    )?;
    let mut descriptors = Vec::new();
    let mut expected_stream = first_stream;
    at = 0;
    for index in 0..column_count as usize {
        let column_index = get_u16(&descriptor_bytes, &mut at, "column_index")?;
        if column_index as usize != index {
            return Err(Error::Format(
                "analytics column indices are missing, duplicate, or unsorted".into(),
            ));
        }
        let fgb_type = take(&descriptor_bytes, &mut at, 1, "fgb_type")?[0];
        let class_byte = take(&descriptor_bytes, &mut at, 1, "value_class")?[0];
        let flags = get_u32(&descriptor_bytes, &mut at, "flags")?;
        let stream_offset = get_u64(&descriptor_bytes, &mut at, "stream_offset")?;
        let stream_size = get_u64(&descriptor_bytes, &mut at, "stream_size")?;
        let arrow_offsets_offset = get_u64(&descriptor_bytes, &mut at, "arrow_offsets_offset")?;
        let arrow_bytes = get_u64(&descriptor_bytes, &mut at, "arrow_bytes")?;
        let schema = &info.columns[index];
        let value_class = ValueClass::from_u8(class_byte)?;
        if fgb_type != schema.kind.0 || value_class != ValueClass::from_fgb_type(fgb_type)? {
            return Err(Error::Format(format!(
                "analytics column {column_index} type/class disagrees with the body: descriptor type={fgb_type} class={class_byte}, body type={} expected class={}",
                schema.kind.0,
                ValueClass::from_fgb_type(fgb_type)? as u8
            )));
        }
        let expected_stream_size = column_metadata_size
            .checked_add(arrow_bytes)
            .ok_or_else(|| Error::Format("analytics stream size overflows".into()))?;
        let expected_arrow_offsets = stream_offset
            .checked_add(GLOBAL_STAT_SIZE as u64)
            .and_then(|offset| offset.checked_add(chunk_stats_size))
            .ok_or_else(|| Error::Format("analytics Arrow-offset location overflows".into()))?;
        if flags != HAS_ARROW_IPC
            || stream_offset != expected_stream
            || stream_size != expected_stream_size
            || arrow_offsets_offset != expected_arrow_offsets
        {
            return Err(Error::Format(format!(
                "analytics column {column_index} descriptor offsets/flags are invalid"
            )));
        }
        expected_stream = stream_offset
            .checked_add(stream_size)
            .ok_or_else(|| Error::Format("analytics stream end overflows".into()))?;
        if expected_stream > entry.size {
            return Err(Error::Format(format!(
                "analytics column {column_index} stream lies outside the section"
            )));
        }
        descriptors.push(ColumnDesc {
            column_index,
            fgb_type,
            value_class,
            flags,
            stream_offset,
            stream_size,
            arrow_offsets_offset,
            arrow_bytes,
        });
    }
    if expected_stream != entry.size {
        return Err(Error::Format(
            "analytics section size does not equal its exact stream layout".into(),
        ));
    }
    let body_offset_bytes = read_blob(
        inner,
        body_offsets_at,
        offset_array_size,
        &mut remaining,
        "body offsets",
    )?;
    let mut chunk_offsets = Vec::new();
    at = 0;
    for _ in 0..=c {
        chunk_offsets.push(get_u64(&body_offset_bytes, &mut at, "body offset")?);
    }
    if chunk_offsets.first() != Some(&info.data_start)
        || chunk_offsets.last() != Some(&body_len)
        || !chunk_offsets.windows(2).all(|pair| pair[0] < pair[1])
    {
        return Err(Error::Format(
            "analytics body offsets are not strictly increasing inside the body".into(),
        ));
    }
    let mut columns = Vec::new();
    for (schema, descriptor) in info.columns.into_iter().zip(descriptors) {
        let stream_start = entry
            .offset
            .checked_add(descriptor.stream_offset)
            .ok_or_else(|| Error::Format("analytics stream start overflows".into()))?;
        let metadata = read_blob(
            inner,
            stream_start,
            column_metadata_size,
            &mut remaining,
            "column statistics/Arrow offsets",
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
        validate_global(&global, descriptor.value_class, row_count)?;
        let mut chunk_stats = Vec::new();
        let mut non_null_sum = 0u64;
        let mut nan_sum = 0u64;
        for chunk_index in 0..c {
            let stat = ChunkStat {
                min_bits: get_u64(&metadata, &mut at, "chunk min")?,
                max_bits: get_u64(&metadata, &mut at, "chunk max")?,
                non_null_count: get_u32(&metadata, &mut at, "chunk non_null_count")?,
                nan_count: get_u32(&metadata, &mut at, "chunk nan_count")?,
            };
            let rows = remaining_rows(row_count, chunk_rows, chunk_index)?;
            validate_chunk(&stat, descriptor.value_class, rows)?;
            non_null_sum = non_null_sum
                .checked_add(u64::from(stat.non_null_count))
                .ok_or_else(|| Error::Format("analytics chunk count sum overflows".into()))?;
            nan_sum = nan_sum
                .checked_add(u64::from(stat.nan_count))
                .ok_or_else(|| Error::Format("analytics chunk NaN sum overflows".into()))?;
            chunk_stats.push(stat);
        }
        if non_null_sum != global.non_null_count || nan_sum != global.nan_count {
            return Err(Error::Format(format!(
                "analytics column {} global/chunk counts disagree",
                descriptor.column_index
            )));
        }
        let mut arrow_offsets = Vec::new();
        for _ in 0..=c {
            arrow_offsets.push(get_u64(&metadata, &mut at, "Arrow offset")?);
        }
        let payload_start = stream_start
            .checked_add(column_metadata_size)
            .ok_or_else(|| Error::Format("analytics Arrow payload start overflows".into()))?;
        let stream_end = stream_start
            .checked_add(descriptor.stream_size)
            .ok_or_else(|| Error::Format("analytics stream end overflows".into()))?;
        if arrow_offsets.first() != Some(&payload_start)
            || arrow_offsets.last() != Some(&stream_end)
            || !arrow_offsets.windows(2).all(|pair| pair[0] < pair[1])
        {
            return Err(Error::Format(format!(
                "analytics column {} Arrow offsets are invalid",
                descriptor.column_index
            )));
        }
        let arrow_bytes = arrow_offsets.windows(2).try_fold(0u64, |sum, pair| {
            let len = pair[1] - pair[0];
            if len == 0 || len > MAX_ARROW_CHUNK_BYTES {
                return Err(Error::Format(format!(
                    "analytics column {} has an oversized or empty Arrow chunk",
                    descriptor.column_index
                )));
            }
            sum.checked_add(len)
                .ok_or_else(|| Error::Format("analytics Arrow bytes overflow".into()))
        })?;
        if arrow_bytes != descriptor.arrow_bytes {
            return Err(Error::Format(format!(
                "analytics column {} Arrow byte count disagrees with descriptor",
                descriptor.column_index
            )));
        }
        columns.push(ColumnAnalytics {
            name: schema.name,
            nullable: schema.nullable,
            descriptor,
            global,
            chunks: chunk_stats,
            arrow_offsets,
        });
    }
    let metadata_bytes_read = MAX_METADATA_BYTES - remaining;
    Ok(BodyAnalytics {
        section_start: entry.offset,
        section_size: entry.size,
        chunk_rows,
        column_count,
        row_count,
        chunk_count: chunks,
        chunk_offsets,
        columns,
        metadata_bytes_read,
    })
}

pub(crate) fn remaining_rows(row_count: u64, chunk_rows: u32, chunk_index: u64) -> Result<u32> {
    let first = chunk_index
        .checked_mul(u64::from(chunk_rows))
        .ok_or_else(|| Error::Format("analytics chunk row offset overflows".into()))?;
    let remaining = row_count
        .checked_sub(first)
        .ok_or_else(|| Error::Format("analytics chunk row offset is out of range".into()))?;
    let rows = remaining.min(u64::from(chunk_rows));
    if rows == 0 {
        return Err(Error::Format("analytics contains an empty chunk".into()));
    }
    Ok(rows as u32)
}

pub(crate) fn validate_global(stat: &GlobalStat, class: ValueClass, rows: u64) -> Result<()> {
    if stat.non_null_count > rows
        || stat.nan_count > stat.non_null_count
        || stat.reserved != 0
        || stat.flags & !(SUM_VALID | HAS_INFINITY) != 0
    {
        return Err(Error::Format(
            "analytics global counts/flags are invalid".into(),
        ));
    }
    if class == ValueClass::Opaque {
        if stat.nan_count != 0
            || stat.min_bits != 0
            || stat.max_bits != 0
            || stat.sum != [0; 16]
            || stat.flags != 0
        {
            return Err(Error::Format(
                "analytics opaque global statistics must be empty".into(),
            ));
        }
        return Ok(());
    }
    if class != ValueClass::Float && (stat.nan_count != 0 || stat.flags & HAS_INFINITY != 0) {
        return Err(Error::Format(
            "analytics integer global float flags/counts are invalid".into(),
        ));
    }
    if class != ValueClass::Float && !stat.sum_valid() {
        return Err(Error::Format(
            "analytics integer global sum is not marked valid".into(),
        ));
    }
    if class == ValueClass::Float
        && stat.sum_valid()
        && (stat.nan_count != 0 || stat.has_infinity())
    {
        return Err(Error::Format(
            "analytics float sum is valid despite NaN/infinity".into(),
        ));
    }
    if !stat.has_bounds() && (stat.min_bits != 0 || stat.max_bits != 0) {
        return Err(Error::Format(
            "analytics global bounds exist without a non-NaN value".into(),
        ));
    }
    if stat.has_bounds() {
        validate_bounds(stat.min_bits, stat.max_bits, class)?;
    }
    if !stat.sum_valid() && stat.sum != [0; 16] {
        return Err(Error::Format(
            "analytics invalid sum must have zero storage".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_chunk(stat: &ChunkStat, class: ValueClass, rows: u32) -> Result<()> {
    if stat.non_null_count > rows || stat.nan_count > stat.non_null_count {
        return Err(Error::Format("analytics chunk counts are invalid".into()));
    }
    if class == ValueClass::Opaque {
        if stat.nan_count != 0 || stat.min_bits != 0 || stat.max_bits != 0 {
            return Err(Error::Format(
                "analytics opaque chunk statistics must be empty".into(),
            ));
        }
        return Ok(());
    }
    if class != ValueClass::Float && stat.nan_count != 0 {
        return Err(Error::Format("analytics integer chunk has NaNs".into()));
    }
    if !stat.has_bounds() && (stat.min_bits != 0 || stat.max_bits != 0) {
        return Err(Error::Format(
            "analytics chunk bounds exist without a non-NaN value".into(),
        ));
    }
    if stat.has_bounds() {
        validate_bounds(stat.min_bits, stat.max_bits, class)?;
    }
    Ok(())
}

fn validate_bounds(min_bits: u64, max_bits: u64, class: ValueClass) -> Result<()> {
    let valid = match class {
        ValueClass::Opaque => min_bits == 0 && max_bits == 0,
        ValueClass::Signed => {
            i64::from_le_bytes(min_bits.to_le_bytes()) <= i64::from_le_bytes(max_bits.to_le_bytes())
        }
        ValueClass::Unsigned => min_bits <= max_bits,
        ValueClass::Float => {
            let min = f64::from_bits(min_bits);
            let max = f64::from_bits(max_bits);
            !min.is_nan()
                && !max.is_nan()
                && min <= max
                && (min != 0.0 || min.is_sign_negative())
                && (max != 0.0 || max.is_sign_positive())
        }
    };
    if valid {
        Ok(())
    } else {
        Err(Error::Format("analytics numeric bounds are invalid".into()))
    }
}
