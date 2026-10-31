//! Bounded collectors and JSON-safe Arrow result encoding.

use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use fgba_sql::{EagerSession, Error, Result};
use futures::StreamExt;
use serde_json::{Number, Value};

/// Maximum encoded source payload retained by one query.
pub const MAX_ENCODED_BYTES: u64 = 256 * 1024 * 1024;
/// Maximum decoded source arrays retained by one query.
pub const MAX_DECODED_BYTES: u64 = 512 * 1024 * 1024;
/// Maximum number of source rows across all referenced tables.
pub const MAX_SOURCE_ROWS: u64 = 1_000_000;
/// Maximum number of JSON result rows.
pub const MAX_RESULT_ROWS: usize = 1_000;
/// Maximum serialized result size.
pub const MAX_RESULT_JSON_BYTES: usize = 8 * 1024 * 1024;

/// Caller-tunable limits, always clamped to the hard browser caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct SqlLimits {
    /// Maximum encoded source bytes materialized by the query.
    pub max_encoded_bytes: u64,
    /// Maximum decoded Arrow bytes retained by the query.
    pub max_decoded_bytes: u64,
    /// Maximum source rows materialized by the query.
    pub max_source_rows: u64,
    /// Maximum rows returned to JavaScript.
    pub max_result_rows: usize,
    /// Maximum serialized JSON bytes returned to JavaScript.
    pub max_result_json_bytes: usize,
}

impl Default for SqlLimits {
    fn default() -> Self {
        Self {
            max_encoded_bytes: MAX_ENCODED_BYTES,
            max_decoded_bytes: MAX_DECODED_BYTES,
            max_source_rows: MAX_SOURCE_ROWS,
            max_result_rows: MAX_RESULT_ROWS,
            max_result_json_bytes: MAX_RESULT_JSON_BYTES,
        }
    }
}

impl SqlLimits {
    /// Reject zero, overflowing, or above-hard-cap limits before any source work.
    pub fn validate(&self) -> Result<()> {
        for (value, cap, name) in [
            (self.max_encoded_bytes, MAX_ENCODED_BYTES, "maxEncodedBytes"),
            (self.max_decoded_bytes, MAX_DECODED_BYTES, "maxDecodedBytes"),
            (self.max_source_rows, MAX_SOURCE_ROWS, "maxSourceRows"),
        ] {
            if value == 0 || value > cap {
                return Err(Error::InvalidInput(format!("{name} must be in 1..={cap}")));
            }
        }
        for (value, cap, name) in [
            (self.max_result_rows, MAX_RESULT_ROWS, "maxResultRows"),
            (
                self.max_result_json_bytes,
                MAX_RESULT_JSON_BYTES,
                "maxResultJsonBytes",
            ),
        ] {
            if value == 0 || value > cap {
                return Err(Error::InvalidInput(format!("{name} must be in 1..={cap}")));
            }
        }
        Ok(())
    }
}

/// Query-wide source budgets. Reservations happen before source allocation when
/// the format exposes a byte or row count, and before retaining each decoded batch.
#[derive(Debug, Clone)]
pub(crate) struct Budget {
    encoded: u64,
    decoded: u64,
    rows: u64,
    limits: SqlLimits,
}

impl Budget {
    pub(crate) fn new(limits: SqlLimits) -> Self {
        Self {
            encoded: 0,
            decoded: 0,
            rows: 0,
            limits,
        }
    }

    pub(crate) fn reserve_encoded(&mut self, bytes: u64) -> Result<()> {
        let next = self
            .encoded
            .checked_add(bytes)
            .ok_or_else(|| Error::InvalidInput("encoded byte budget overflows".into()))?;
        if next > self.limits.max_encoded_bytes {
            return Err(Error::InvalidInput(format!(
                "encoded byte budget exceeded: {next} > {}",
                self.limits.max_encoded_bytes
            )));
        }
        self.encoded = next;
        Ok(())
    }

    pub(crate) fn reserve_decoded(&mut self, bytes: u64) -> Result<()> {
        let next = self
            .decoded
            .checked_add(bytes)
            .ok_or_else(|| Error::InvalidInput("decoded byte budget overflows".into()))?;
        if next > self.limits.max_decoded_bytes {
            return Err(Error::InvalidInput(format!(
                "decoded byte budget exceeded: {next} > {}",
                self.limits.max_decoded_bytes
            )));
        }
        self.decoded = next;
        Ok(())
    }

    pub(crate) fn reserve_rows(&mut self, rows: u64) -> Result<()> {
        let next = self
            .rows
            .checked_add(rows)
            .ok_or_else(|| Error::InvalidInput("source rows budget overflows".into()))?;
        if next > self.limits.max_source_rows {
            return Err(Error::InvalidInput(format!(
                "source rows budget exceeded: {next} > {}",
                self.limits.max_source_rows
            )));
        }
        self.rows = next;
        Ok(())
    }

    pub(crate) fn retain(
        &mut self,
        batches: &mut Vec<RecordBatch>,
        batch: RecordBatch,
    ) -> Result<()> {
        self.reserve_decoded(batch.get_array_memory_size() as u64)?;
        batches.push(batch);
        Ok(())
    }
}

/// Execute an in-memory plan and encode at most the configured result rows.
pub(crate) async fn execute_json(
    session: &EagerSession,
    sql: &str,
    limits: SqlLimits,
) -> Result<String> {
    let mut stream = session.execute(sql).await?;
    let mut encoder = ResultEncoder::new(stream.schema(), limits)?;
    let mut truncated = false;
    'batches: while let Some(batch) = stream.next().await {
        let batch = batch?;
        for row in 0..batch.num_rows() {
            if encoder.rows == limits.max_result_rows {
                truncated = true;
                break 'batches;
            }
            encoder.append_row(&batch, row)?;
        }
    }
    encoder.finish(truncated)
}

struct ResultEncoder {
    json: String,
    rows: usize,
    limits: SqlLimits,
}

impl ResultEncoder {
    fn new(schema: SchemaRef, limits: SqlLimits) -> Result<Self> {
        let mut this = Self {
            json: String::new(),
            rows: 0,
            limits,
        };
        this.append("{\"columns\":[")?;
        for (index, field) in schema.fields().iter().enumerate() {
            if index > 0 {
                this.append(",")?;
            }
            this.value(&Value::String(field.name().clone()))?;
        }
        this.append("],\"rows\":[")?;
        Ok(this)
    }

    fn append_row(&mut self, batch: &RecordBatch, row: usize) -> Result<()> {
        if self.rows > 0 {
            self.append(",")?;
        }
        self.append("[")?;
        for (column, array) in batch.columns().iter().enumerate() {
            if column > 0 {
                self.append(",")?;
            }
            self.preflight_value(array.as_ref(), row)?;
            let value = arrow_value(array.as_ref(), row)?;
            self.value(&value)?;
        }
        self.append("]")?;
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| Error::InvalidInput("result row count overflows".into()))?;
        Ok(())
    }

    fn preflight_value(&self, array: &dyn Array, row: usize) -> Result<()> {
        if array.is_null(row) {
            return self.reserve(4);
        }
        macro_rules! string_len {
            ($kind:ty, $name:literal) => {{
                let values = array
                    .as_any()
                    .downcast_ref::<$kind>()
                    .ok_or_else(|| Error::Format(concat!($name, " array type mismatch").into()))?;
                let length = usize::try_from(values.value_length(row))
                    .map_err(|_| Error::InvalidInput("result JSON size overflows".into()))?;
                self.reserve(checked_scalar_len(length, 1, 2)?)?;
                json_string_len(values.value(row))?
            }};
        }
        macro_rules! binary_len {
            ($kind:ty, $name:literal) => {{
                let values = array
                    .as_any()
                    .downcast_ref::<$kind>()
                    .ok_or_else(|| Error::Format(concat!($name, " array type mismatch").into()))?;
                let length = usize::try_from(values.value_length(row))
                    .map_err(|_| Error::InvalidInput("result JSON size overflows".into()))?;
                checked_scalar_len(length, 2, 2)?
            }};
        }
        let encoded = match array.data_type() {
            DataType::Int8 => 4,
            DataType::Int16 => 6,
            DataType::Int32 => 11,
            DataType::Int64 | DataType::UInt64 => 22,
            DataType::UInt8 => 3,
            DataType::UInt16 => 5,
            DataType::UInt32 => 10,
            DataType::Float32 | DataType::Float64 => 32,
            DataType::Boolean => 5,
            DataType::Utf8 => string_len!(StringArray, "Utf8"),
            DataType::LargeUtf8 => string_len!(LargeStringArray, "LargeUtf8"),
            DataType::Binary => binary_len!(BinaryArray, "Binary"),
            DataType::LargeBinary => binary_len!(LargeBinaryArray, "LargeBinary"),
            DataType::Null => 4,
            other => {
                return Err(Error::InvalidInput(format!(
                    "Arrow result type {other} is not supported by JSON output"
                )))
            }
        };
        self.reserve(encoded)
    }

    fn value(&mut self, value: &Value) -> Result<()> {
        let encoded = serde_json::to_string(value)
            .map_err(|error| Error::InvalidInput(format!("cannot encode result JSON: {error}")))?;
        self.append(&encoded)
    }

    fn append(&mut self, text: &str) -> Result<()> {
        self.reserve(text.len())?;
        self.json.push_str(text);
        Ok(())
    }

    fn reserve(&self, len: usize) -> Result<()> {
        let next = self
            .json
            .len()
            .checked_add(len)
            .and_then(|size| size.checked_add(32))
            .ok_or_else(|| Error::InvalidInput("result JSON size overflows".into()))?;
        if next > self.limits.max_result_json_bytes {
            return Err(Error::InvalidInput(format!(
                "result JSON budget exceeded: {next} > {}",
                self.limits.max_result_json_bytes
            )));
        }
        Ok(())
    }

    fn finish(mut self, truncated: bool) -> Result<String> {
        self.append("],\"truncated\":")?;
        self.append(if truncated { "true" } else { "false" })?;
        self.append("}")?;
        Ok(self.json)
    }
}

fn checked_scalar_len(length: usize, multiplier: usize, extra: usize) -> Result<usize> {
    length
        .checked_mul(multiplier)
        .and_then(|size| size.checked_add(extra))
        .ok_or_else(|| Error::InvalidInput("result JSON size overflows".into()))
}

fn json_string_len(value: &str) -> Result<usize> {
    value.bytes().try_fold(2_usize, |length, byte| {
        let encoded = match byte {
            b'"' | b'\\' => 2,
            b'\x08' | b'\t' | b'\n' | b'\x0c' | b'\r' => 2,
            b'\x00'..=b'\x1f' => 6,
            _ => 1,
        };
        length
            .checked_add(encoded)
            .ok_or_else(|| Error::InvalidInput("result JSON size overflows".into()))
    })
}

fn arrow_value(array: &dyn Array, row: usize) -> Result<Value> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    macro_rules! number {
        ($kind:ty) => {{
            let values = array.as_any().downcast_ref::<$kind>().ok_or_else(|| {
                Error::Format(format!(
                    "Arrow array type mismatch for {}",
                    array.data_type()
                ))
            })?;
            Ok(Value::Number(Number::from(values.value(row))))
        }};
    }
    macro_rules! decimal {
        ($kind:ty) => {{
            let values = array.as_any().downcast_ref::<$kind>().ok_or_else(|| {
                Error::Format(format!(
                    "Arrow array type mismatch for {}",
                    array.data_type()
                ))
            })?;
            Ok(Value::String(values.value(row).to_string()))
        }};
    }
    match array.data_type() {
        DataType::Int8 => number!(Int8Array),
        DataType::Int16 => number!(Int16Array),
        DataType::Int32 => number!(Int32Array),
        DataType::Int64 => decimal!(Int64Array),
        DataType::UInt8 => number!(UInt8Array),
        DataType::UInt16 => number!(UInt16Array),
        DataType::UInt32 => number!(UInt32Array),
        DataType::UInt64 => decimal!(UInt64Array),
        DataType::Float32 => {
            let values = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| Error::Format("Float32 array type mismatch".into()))?;
            Ok(float_value(f64::from(values.value(row))))
        }
        DataType::Float64 => {
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| Error::Format("Float64 array type mismatch".into()))?;
            Ok(float_value(values.value(row)))
        }
        DataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| Error::Format("Boolean array type mismatch".into()))?;
            Ok(Value::Bool(values.value(row)))
        }
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Format("Utf8 array type mismatch".into()))?;
            Ok(Value::String(values.value(row).to_owned()))
        }
        DataType::LargeUtf8 => {
            let values = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| Error::Format("LargeUtf8 array type mismatch".into()))?;
            Ok(Value::String(values.value(row).to_owned()))
        }
        DataType::Binary => {
            let values = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| Error::Format("Binary array type mismatch".into()))?;
            Ok(Value::String(hex(values.value(row))))
        }
        DataType::LargeBinary => {
            let values = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| Error::Format("LargeBinary array type mismatch".into()))?;
            Ok(Value::String(hex(values.value(row))))
        }
        DataType::Null => Ok(Value::Null),
        other => Err(Error::InvalidInput(format!(
            "Arrow result type {other} is not supported by JSON output"
        ))),
    }
}

fn float_value(value: f64) -> Value {
    Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or_else(|| Value::String(value.to_string()))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for &byte in bytes {
        output.push(DIGITS[usize::from(byte >> 4)] as char);
        output.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn oversized_decoded_reservation_fails_before_any_allocation() {
        let mut budget = Budget::new(SqlLimits::default());
        assert!(budget.reserve_decoded(MAX_DECODED_BYTES + 1).is_err());
        assert_eq!(budget.decoded, 0);
    }

    #[test]
    fn single_oversized_scalar_fails_preflight_without_growing_result_buffer() -> TestResult {
        let array = StringArray::from(vec!["x".repeat(MAX_RESULT_JSON_BYTES + 1)]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)])?;
        let mut encoder = ResultEncoder::new(schema, SqlLimits::default())?;

        let error = encoder
            .append_row(&batch, 0)
            .err()
            .ok_or("oversized scalar unexpectedly succeeded")?;
        assert!(error.to_string().contains("result JSON budget exceeded"));
        assert_eq!(encoder.rows, 0);
        assert!(encoder.json.capacity() < MAX_RESULT_JSON_BYTES);
        Ok(())
    }

    #[test]
    fn string_preflight_length_matches_serde_json() -> TestResult {
        for value in ["plain", "\"\\\u{0}\u{8}\t\n\u{c}\r\u{1f}", "é🙂"] {
            assert_eq!(json_string_len(value)?, serde_json::to_string(value)?.len());
        }
        Ok(())
    }
}
