//! Pure Arrow planning, chunk decoding, and exact feature batch construction.
//!
//! These helpers have no path, runtime, or partition dependencies so native
//! streams and bounded browser materialization share identical semantics.

use crate::error::{Error, Result};
use crate::schema::{OutputSchema, PropertySchema};
use arrow_ipc::reader::FileReader;
use datafusion::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, RecordBatch, RecordBatchOptions, StringArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use fgba::FeatureMetrics;
use flatgeobuf::geozero::ColumnValue;
use flatgeobuf::ColumnType;
use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;

/// Exact FlatGeobuf body decoding or independently range-read Arrow columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceMode {
    /// Decode selected values from FlatGeobuf features.
    Body,
    /// Decode selected values from kind-2/kind-3 Arrow IPC chunks.
    Columns,
}

/// Choose the exact source for one projection without performing any I/O.
pub fn plan_source_mode(
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

/// Decode and validate one independent single-column Arrow IPC chunk.
pub fn decode_arrow_chunk(
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

/// Build a batch, including a row-count-only zero-column batch.
pub fn make_batch(schema: SchemaRef, columns: Vec<ArrayRef>, rows: usize) -> Result<RecordBatch> {
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(Into::into)
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
pub struct FeatureBatchBuilder {
    output_schema: Arc<OutputSchema>,
    projected_schema: SchemaRef,
    selected: Vec<usize>,
    property_positions: Vec<Vec<usize>>,
    columns: Vec<ColumnValues>,
    seen_properties: HashSet<usize>,
    /// Rows accumulated in this batch.
    pub rows: usize,
    /// Variable-width value bytes accumulated in this batch.
    pub current_bytes: usize,
}

impl FeatureBatchBuilder {
    /// Create a builder for selected source-column indices.
    pub fn new(
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

    /// Whether any FlatGeobuf property must be decoded for this projection.
    pub fn has_projected_properties(&self) -> bool {
        self.property_positions
            .iter()
            .any(|positions| !positions.is_empty())
    }

    /// Append a null-initialized row.
    pub fn begin_row(&mut self) {
        for column in &mut self.columns {
            column.append_null();
        }
        self.seen_properties.clear();
        self.rows += 1;
    }

    /// Copy one FlatGeobuf property into every selected occurrence.
    pub fn set_property(
        &mut self,
        index: usize,
        name: &str,
        value: &ColumnValue<'_>,
    ) -> Result<()> {
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

    /// Validate required properties after one row is complete.
    pub fn end_row(&self, ordinal: u64) -> Result<()> {
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

    /// Fill selected WKB, bbox, and virtual geometry-metric values.
    pub fn set_geometry(
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

    /// Finish this batch without concatenating with later source chunks.
    pub fn finish(self) -> Result<RecordBatch> {
        let columns = self.columns.into_iter().map(ColumnValues::finish).collect();
        make_batch(self.projected_schema, columns, self.rows)
    }
}
