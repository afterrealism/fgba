//! Vectorized planar geometry metrics over standard Well-Known Binary.

use crate::{Error, Result};
use datafusion::arrow::array::{
    new_null_array, Array, ArrayRef, BinaryArray, Float64Array, UInt32Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use fgba::MetricsSink;
use std::any::Any;
use std::sync::Arc;

const MAX_WKB_DEPTH: usize = 64;

#[derive(Clone, Copy, Debug)]
enum Metric {
    Length,
    Area,
    Npoints,
}

impl Metric {
    fn name(self) -> &'static str {
        match self {
            Self::Length => "st_length",
            Self::Area => "st_area",
            Self::Npoints => "st_npoints",
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Length | Self::Area => DataType::Float64,
            Self::Npoints => DataType::UInt32,
        }
    }
}

#[derive(Debug)]
struct GeometryUdf {
    metric: Metric,
    signature: Signature,
}

impl GeometryUdf {
    fn new(metric: Metric) -> Self {
        Self {
            metric,
            signature: Signature::uniform(
                1,
                vec![DataType::Binary, DataType::Null],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for GeometryUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.metric.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        match arg_types {
            [DataType::Binary] | [DataType::Null] => Ok(self.metric.data_type()),
            _ => Err(DataFusionError::Plan(format!(
                "{} expects one Binary argument, got {arg_types:?}",
                self.name()
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let input = match args.as_slice() {
            [input] => input,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "{} received {} arguments instead of one",
                    self.name(),
                    args.len()
                )))
            }
        };
        if input.data_type() == &DataType::Null {
            return Ok(ColumnarValue::Array(new_null_array(
                &self.metric.data_type(),
                input.len(),
            )));
        }
        let input = input
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{} expects a BinaryArray, got {}",
                    self.name(),
                    input.data_type()
                ))
            })?;
        evaluate(input, self.metric).map(ColumnarValue::Array)
    }
}

pub(crate) fn register(ctx: &SessionContext) {
    for metric in [Metric::Length, Metric::Area, Metric::Npoints] {
        ctx.register_udf(ScalarUDF::from(GeometryUdf::new(metric)));
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct WkbMetrics {
    length: f64,
    area: f64,
    npoints: u32,
}

fn evaluate(input: &BinaryArray, metric: Metric) -> DataFusionResult<ArrayRef> {
    let mut metrics = Vec::with_capacity(input.len());
    for row in 0..input.len() {
        if input.is_null(row) {
            metrics.push(None);
            continue;
        }
        let value = wkb_metrics(input.value(row)).map_err(|error| {
            DataFusionError::Execution(format!(
                "{} could not parse WKB at row {row}: {error}",
                metric.name()
            ))
        })?;
        metrics.push(Some(value));
    }
    let output: ArrayRef = match metric {
        Metric::Length => Arc::new(Float64Array::from(
            metrics
                .iter()
                .map(|value| value.as_ref().map(|value| value.length))
                .collect::<Vec<_>>(),
        )),
        Metric::Area => Arc::new(Float64Array::from(
            metrics
                .iter()
                .map(|value| value.as_ref().map(|value| value.area))
                .collect::<Vec<_>>(),
        )),
        Metric::Npoints => Arc::new(UInt32Array::from(
            metrics
                .iter()
                .map(|value| value.as_ref().map(|value| value.npoints))
                .collect::<Vec<_>>(),
        )),
    };
    Ok(output)
}

#[derive(Clone, Copy)]
enum Endian {
    Little,
    Big,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GeometryKind {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
}

#[derive(Clone, Copy)]
struct GeometryHeader {
    endian: Endian,
    kind: GeometryKind,
    coordinate_values: usize,
}

struct WkbCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> WkbCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn error(&self, message: impl Into<String>) -> Error {
        Error::Format(format!(
            "invalid WKB at byte {}: {}",
            self.position,
            message.into()
        ))
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn ensure(&self, bytes: usize) -> Result<()> {
        let end = self
            .position
            .checked_add(bytes)
            .ok_or_else(|| self.error("byte offset overflows usize"))?;
        if end > self.bytes.len() {
            return Err(self.error(format!(
                "truncated input needs {bytes} bytes, only {} remain",
                self.remaining()
            )));
        }
        Ok(())
    }

    fn ensure_count(&self, count: usize, minimum_bytes: usize) -> Result<()> {
        let bytes = count
            .checked_mul(minimum_bytes)
            .ok_or_else(|| self.error("declared count byte size overflows usize"))?;
        self.ensure(bytes)
    }

    fn take(&mut self, bytes: usize) -> Result<&'a [u8]> {
        self.ensure(bytes)?;
        let end = self
            .position
            .checked_add(bytes)
            .ok_or_else(|| self.error("byte offset overflows usize"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| self.error("byte range is outside the input"))?;
        self.position = end;
        Ok(value)
    }

    fn advance(&mut self, bytes: usize) -> Result<()> {
        self.take(bytes).map(|_| ())
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| self.error("cannot read fixed-size value"))
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u32(&mut self, endian: Endian) -> Result<u32> {
        let bytes = self.read_array::<4>()?;
        Ok(match endian {
            Endian::Little => u32::from_le_bytes(bytes),
            Endian::Big => u32::from_be_bytes(bytes),
        })
    }

    fn read_f64(&mut self, endian: Endian) -> Result<f64> {
        let bytes = self.read_array::<8>()?;
        Ok(match endian {
            Endian::Little => f64::from_le_bytes(bytes),
            Endian::Big => f64::from_be_bytes(bytes),
        })
    }

    fn read_count(&mut self, endian: Endian) -> Result<usize> {
        usize::try_from(self.read_u32(endian)?)
            .map_err(|_| self.error("declared count does not fit this platform"))
    }

    fn read_header(&mut self) -> Result<GeometryHeader> {
        let endian = match self.read_u8()? {
            0 => Endian::Big,
            1 => Endian::Little,
            other => return Err(self.error(format!("invalid byte-order marker {other}"))),
        };
        let type_id = self.read_u32(endian)?;
        let dimensions = type_id / 1000;
        if dimensions > 3 {
            return Err(self.error(format!("unsupported WKB type encoding {type_id}")));
        }
        let kind = match type_id % 1000 {
            1 => GeometryKind::Point,
            2 => GeometryKind::LineString,
            3 => GeometryKind::Polygon,
            4 => GeometryKind::MultiPoint,
            5 => GeometryKind::MultiLineString,
            6 => GeometryKind::MultiPolygon,
            7 => GeometryKind::GeometryCollection,
            _ => return Err(self.error(format!("unsupported WKB geometry type {type_id}"))),
        };
        let coordinate_values = 2
            + usize::from(dimensions == 1 || dimensions == 3)
            + usize::from(dimensions == 2 || dimensions == 3);
        Ok(GeometryHeader {
            endian,
            kind,
            coordinate_values,
        })
    }
}

fn read_children(
    cursor: &mut WkbCursor<'_>,
    sink: &mut MetricsSink,
    depth: usize,
    count: usize,
    expected: Option<GeometryKind>,
) -> Result<()> {
    cursor.ensure_count(count, 5)?;
    let depth = depth
        .checked_add(1)
        .ok_or_else(|| cursor.error("geometry nesting depth overflows usize"))?;
    for _ in 0..count {
        read_geometry(cursor, sink, depth, expected)?;
    }
    Ok(())
}

fn read_coordinates(
    cursor: &mut WkbCursor<'_>,
    sink: &mut MetricsSink,
    header: GeometryHeader,
    count: usize,
) -> Result<()> {
    let coordinate_bytes = header
        .coordinate_values
        .checked_mul(8)
        .ok_or_else(|| cursor.error("coordinate width overflows usize"))?;
    cursor.ensure_count(count, coordinate_bytes)?;
    let extra_bytes = (header.coordinate_values - 2)
        .checked_mul(8)
        .ok_or_else(|| cursor.error("dimensional coordinate width overflows usize"))?;
    for _ in 0..count {
        let x = cursor.read_f64(header.endian)?;
        let y = cursor.read_f64(header.endian)?;
        cursor.advance(extra_bytes)?;
        sink.push_xy(x, y)?;
    }
    Ok(())
}

fn read_line(
    cursor: &mut WkbCursor<'_>,
    sink: &mut MetricsSink,
    header: GeometryHeader,
    ring: bool,
) -> Result<()> {
    let count = cursor.read_count(header.endian)?;
    sink.begin_line(ring)?;
    read_coordinates(cursor, sink, header, count)?;
    sink.end_line()?;
    Ok(())
}

fn read_polygon(
    cursor: &mut WkbCursor<'_>,
    sink: &mut MetricsSink,
    header: GeometryHeader,
) -> Result<()> {
    let rings = cursor.read_count(header.endian)?;
    cursor.ensure_count(rings, 4)?;
    sink.begin_polygon()?;
    for _ in 0..rings {
        read_line(cursor, sink, header, true)?;
    }
    sink.end_polygon()?;
    Ok(())
}

fn read_point(
    cursor: &mut WkbCursor<'_>,
    sink: &mut MetricsSink,
    header: GeometryHeader,
) -> Result<()> {
    let coordinate_bytes = header
        .coordinate_values
        .checked_mul(8)
        .ok_or_else(|| cursor.error("coordinate width overflows usize"))?;
    let extra_bytes = (header.coordinate_values - 2)
        .checked_mul(8)
        .ok_or_else(|| cursor.error("dimensional coordinate width overflows usize"))?;
    cursor.ensure_count(1, coordinate_bytes)?;
    let x = cursor.read_f64(header.endian)?;
    let y = cursor.read_f64(header.endian)?;
    cursor.advance(extra_bytes)?;
    if !(x.is_nan() && y.is_nan()) {
        sink.push_xy(x, y)?;
    }
    Ok(())
}

fn read_geometry(
    cursor: &mut WkbCursor<'_>,
    sink: &mut MetricsSink,
    depth: usize,
    expected: Option<GeometryKind>,
) -> Result<()> {
    if depth >= MAX_WKB_DEPTH {
        return Err(cursor.error(format!("geometry nesting exceeds {MAX_WKB_DEPTH} headers")));
    }
    let header = cursor.read_header()?;
    if expected.is_some_and(|expected| expected != header.kind) {
        return Err(cursor.error(format!("expected {expected:?}, found {:?}", header.kind)));
    }
    match header.kind {
        GeometryKind::Point => read_point(cursor, sink, header),
        GeometryKind::LineString => read_line(cursor, sink, header, false),
        GeometryKind::Polygon => read_polygon(cursor, sink, header),
        GeometryKind::MultiPoint => {
            let count = cursor.read_count(header.endian)?;
            read_children(cursor, sink, depth, count, Some(GeometryKind::Point))
        }
        GeometryKind::MultiLineString => {
            let count = cursor.read_count(header.endian)?;
            read_children(cursor, sink, depth, count, Some(GeometryKind::LineString))
        }
        GeometryKind::MultiPolygon => {
            let count = cursor.read_count(header.endian)?;
            read_children(cursor, sink, depth, count, Some(GeometryKind::Polygon))
        }
        GeometryKind::GeometryCollection => {
            let count = cursor.read_count(header.endian)?;
            read_children(cursor, sink, depth, count, None)
        }
    }
}

fn wkb_metrics(bytes: &[u8]) -> Result<WkbMetrics> {
    let mut cursor = WkbCursor::new(bytes);
    let mut sink = MetricsSink::new();
    read_geometry(&mut cursor, &mut sink, 0, None)?;
    if cursor.remaining() != 0 {
        return Err(cursor.error(format!(
            "{} trailing bytes follow the geometry",
            cursor.remaining()
        )));
    }
    let metrics = sink.finish()?;
    Ok(match metrics {
        Some(metrics) => WkbMetrics {
            length: metrics.length,
            area: metrics.area,
            npoints: metrics.npoints,
        },
        None => WkbMetrics {
            length: 0.0,
            area: 0.0,
            npoints: 0,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::{evaluate, wkb_metrics, Metric};
    use datafusion::arrow::array::{Array, BinaryArray, Float64Array, UInt32Array};
    use datafusion::error::DataFusionError;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    #[derive(Clone, Copy)]
    enum Endian {
        Little,
        Big,
    }

    fn push_u32(output: &mut Vec<u8>, value: u32, endian: Endian) {
        let bytes = match endian {
            Endian::Little => value.to_le_bytes(),
            Endian::Big => value.to_be_bytes(),
        };
        output.extend_from_slice(&bytes);
    }

    fn push_f64(output: &mut Vec<u8>, value: f64, endian: Endian) {
        let bytes = match endian {
            Endian::Little => value.to_le_bytes(),
            Endian::Big => value.to_be_bytes(),
        };
        output.extend_from_slice(&bytes);
    }

    fn header(output: &mut Vec<u8>, kind: u32, dimensions: u32, endian: Endian) {
        output.push(match endian {
            Endian::Little => 1,
            Endian::Big => 0,
        });
        push_u32(output, kind + dimensions * 1000, endian);
    }

    fn push_coordinates(
        output: &mut Vec<u8>,
        points: &[(f64, f64)],
        dimensions: u32,
        endian: Endian,
    ) {
        push_u32(output, points.len() as u32, endian);
        for &(x, y) in points {
            push_f64(output, x, endian);
            push_f64(output, y, endian);
            let extra_values = u32::from(dimensions == 1 || dimensions == 3)
                + u32::from(dimensions == 2 || dimensions == 3);
            for extra in 0..extra_values {
                push_f64(output, 100.0 + f64::from(extra), endian);
            }
        }
    }

    fn line(endian: Endian, dimensions: u32, points: &[(f64, f64)]) -> Vec<u8> {
        let mut output = Vec::new();
        header(&mut output, 2, dimensions, endian);
        push_coordinates(&mut output, points, dimensions, endian);
        output
    }

    fn polygon(endian: Endian, dimensions: u32, rings: &[&[(f64, f64)]]) -> Vec<u8> {
        let mut output = Vec::new();
        header(&mut output, 3, dimensions, endian);
        push_u32(&mut output, rings.len() as u32, endian);
        for ring in rings {
            push_coordinates(&mut output, ring, dimensions, endian);
        }
        output
    }

    fn nested(endian: Endian, kind: u32, children: &[Vec<u8>]) -> Vec<u8> {
        let mut output = Vec::new();
        header(&mut output, kind, 0, endian);
        push_u32(&mut output, children.len() as u32, endian);
        for child in children {
            output.extend_from_slice(child);
        }
        output
    }

    fn multipolygon_fixture(endian: Endian, reverse: bool) -> Vec<u8> {
        let mut shell = vec![(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)];
        let mut hole = vec![(1.0, 1.0), (1.0, 2.0), (2.0, 2.0), (2.0, 1.0), (1.0, 1.0)];
        let mut unit = vec![
            (10.0, 0.0),
            (11.0, 0.0),
            (11.0, 1.0),
            (10.0, 1.0),
            (10.0, 0.0),
        ];
        if reverse {
            shell.reverse();
            hole.reverse();
            unit.reverse();
        }
        nested(
            endian,
            6,
            &[
                polygon(endian, 0, &[&shell, &hole]),
                polygon(endian, 0, &[&unit]),
            ],
        )
    }

    #[test]
    fn specified_line_metrics_work_in_both_endians() -> TestResult {
        for endian in [Endian::Little, Endian::Big] {
            let metrics = wkb_metrics(&line(endian, 0, &[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]))?;
            assert_eq!(metrics.length, 2.0);
            assert_eq!(metrics.area, 0.0);
            assert_eq!(metrics.npoints, 3);
        }
        Ok(())
    }

    #[test]
    fn unit_polygon_matches_kind_three_math() -> TestResult {
        let square = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)];
        let metrics = wkb_metrics(&polygon(Endian::Little, 0, &[&square]))?;
        assert_eq!(metrics.length, 4.0);
        assert_eq!(metrics.area, 1.0);
        assert_eq!(metrics.npoints, 5);
        Ok(())
    }

    #[test]
    fn multipolygon_holes_ignore_ring_orientation() -> TestResult {
        for endian in [Endian::Little, Endian::Big] {
            for reverse in [false, true] {
                let metrics = wkb_metrics(&multipolygon_fixture(endian, reverse))?;
                assert_eq!(metrics.length, 24.0);
                assert_eq!(metrics.area, 16.0);
                assert_eq!(metrics.npoints, 15);
            }
        }
        Ok(())
    }

    #[test]
    fn nested_and_empty_collections_are_non_null_metrics() -> TestResult {
        let line = line(Endian::Little, 0, &[(0.0, 0.0), (2.0, 0.0)]);
        let collection = nested(
            Endian::Little,
            7,
            &[line, multipolygon_fixture(Endian::Big, false)],
        );
        let metrics = wkb_metrics(&collection)?;
        assert_eq!(metrics.length, 26.0);
        assert_eq!(metrics.area, 16.0);
        assert_eq!(metrics.npoints, 17);

        let empty = nested(Endian::Big, 7, &[]);
        let metrics = wkb_metrics(&empty)?;
        assert_eq!(metrics.length, 0.0);
        assert_eq!(metrics.area, 0.0);
        assert_eq!(metrics.npoints, 0);
        Ok(())
    }

    #[test]
    fn iso_z_m_and_zm_coordinates_use_only_xy_for_planar_metrics() -> TestResult {
        for dimensions in 1..=3 {
            let metrics =
                wkb_metrics(&line(Endian::Little, dimensions, &[(0.0, 0.0), (3.0, 4.0)]))?;
            assert_eq!(metrics.length, 5.0);
            assert_eq!(metrics.area, 0.0);
            assert_eq!(metrics.npoints, 2);
        }
        Ok(())
    }

    #[test]
    fn truncation_hostile_counts_and_trailing_bytes_return_errors() -> TestResult {
        let valid = multipolygon_fixture(Endian::Little, false);
        for end in 0..valid.len() {
            assert!(
                wkb_metrics(&valid[..end]).is_err(),
                "truncated prefix of length {end} succeeded"
            );
        }
        let mut trailing = valid;
        trailing.push(0);
        assert!(wkb_metrics(&trailing).is_err());

        for kind in [2, 4, 5, 6, 7] {
            let mut hostile = Vec::new();
            header(&mut hostile, kind, 0, Endian::Little);
            push_u32(&mut hostile, u32::MAX, Endian::Little);
            assert!(wkb_metrics(&hostile).is_err(), "hostile kind {kind}");
        }
        let mut polygon_hostile = Vec::new();
        header(&mut polygon_hostile, 3, 0, Endian::Big);
        push_u32(&mut polygon_hostile, u32::MAX, Endian::Big);
        assert!(wkb_metrics(&polygon_hostile).is_err());

        assert!(wkb_metrics(&[2, 1, 0, 0, 0]).is_err());
        let mut unsupported = Vec::new();
        header(&mut unsupported, 8, 0, Endian::Little);
        assert!(wkb_metrics(&unsupported).is_err());
        Ok(())
    }

    #[test]
    fn collection_nesting_is_bounded() {
        let mut bytes = nested(Endian::Little, 7, &[]);
        for _ in 0..64 {
            bytes = nested(Endian::Little, 7, &[bytes]);
        }
        assert!(wkb_metrics(&bytes).is_err());
    }

    #[test]
    fn binary_array_evaluation_preserves_nulls_and_empty_collection_values() -> TestResult {
        let line = line(Endian::Little, 0, &[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]);
        let empty = nested(Endian::Little, 7, &[]);
        let input = BinaryArray::from(vec![Some(line.as_slice()), None, Some(empty.as_slice())]);
        let lengths = evaluate(&input, Metric::Length)?;
        let lengths = lengths
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("st_length did not return Float64")?;
        assert_eq!(lengths.value(0), 2.0);
        assert!(lengths.is_null(1));
        assert!(lengths.is_valid(2));
        assert_eq!(lengths.value(2), 0.0);

        let areas = evaluate(&input, Metric::Area)?;
        let areas = areas
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("st_area did not return Float64")?;
        assert_eq!(areas.value(0), 0.0);
        assert!(areas.is_null(1));
        assert!(areas.is_valid(2));
        assert_eq!(areas.value(2), 0.0);

        let npoints = evaluate(&input, Metric::Npoints)?;
        let npoints = npoints
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or("st_npoints did not return UInt32")?;
        assert_eq!(npoints.value(0), 3);
        assert!(npoints.is_null(1));
        assert!(npoints.is_valid(2));
        assert_eq!(npoints.value(2), 0);

        let malformed = BinaryArray::from(vec![Some(&[1, 2, 0][..])]);
        let error = evaluate(&malformed, Metric::Area)
            .err()
            .ok_or("malformed WKB unexpectedly evaluated")?;
        assert!(matches!(error, DataFusionError::Execution(_)), "{error}");
        Ok(())
    }
}
