//! `fgba convert`: read GeoJSON, CSV/TSV, Shapefile or FlatGeobuf into a
//! FlatGeobuf body (spatial index, WGS84), then append the FGBA trailer.
//!
//! Feature properties are preserved with deterministic schemas, nullability
//! and lossless value types. GeoParquet/GeoPackage inputs remain unsupported
//! and fail with a clear "not yet" error.

use flatgeobuf::{ColumnType, FgbCrs, FgbWriter, FgbWriterOptions, GeometryType};
use geo_types::{Coord, Geometry, LineString, MultiLineString, MultiPoint, Point, Polygon};
use geozero::{ColumnValue, GeozeroGeometry, PropertyProcessor};
use std::collections::{btree_map::Entry, BTreeMap};
use std::path::Path;
use std::time::Instant;

/// One source feature with owned geometry and deterministically ordered properties.
struct Feature {
    geom: Geometry<f64>,
    properties: BTreeMap<String, PropertyValue>,
}

/// Features read from the source plus the count of skipped features.
type Features = (Vec<Feature>, usize);

/// An owned FlatGeobuf property value; `Null` is omitted from feature data.
#[allow(dead_code)] // Complete the ColumnValue mapping even where no reader emits a variant.
enum PropertyValue {
    Null,
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

impl PropertyValue {
    fn column_type(&self) -> Option<ColumnType> {
        Some(match self {
            Self::Null => return None,
            Self::Byte(_) => ColumnType::Byte,
            Self::UByte(_) => ColumnType::UByte,
            Self::Bool(_) => ColumnType::Bool,
            Self::Short(_) => ColumnType::Short,
            Self::UShort(_) => ColumnType::UShort,
            Self::Int(_) => ColumnType::Int,
            Self::UInt(_) => ColumnType::UInt,
            Self::Long(_) => ColumnType::Long,
            Self::ULong(_) => ColumnType::ULong,
            Self::Float(_) => ColumnType::Float,
            Self::Double(_) => ColumnType::Double,
            Self::String(_) => ColumnType::String,
            Self::Json(_) => ColumnType::Json,
            Self::DateTime(_) => ColumnType::DateTime,
            Self::Binary(_) => ColumnType::Binary,
        })
    }

    fn as_column_value(&self) -> Option<ColumnValue<'_>> {
        Some(match self {
            Self::Null => return None,
            Self::Byte(v) => ColumnValue::Byte(*v),
            Self::UByte(v) => ColumnValue::UByte(*v),
            Self::Bool(v) => ColumnValue::Bool(*v),
            Self::Short(v) => ColumnValue::Short(*v),
            Self::UShort(v) => ColumnValue::UShort(*v),
            Self::Int(v) => ColumnValue::Int(*v),
            Self::UInt(v) => ColumnValue::UInt(*v),
            Self::Long(v) => ColumnValue::Long(*v),
            Self::ULong(v) => ColumnValue::ULong(*v),
            Self::Float(v) => ColumnValue::Float(*v),
            Self::Double(v) => ColumnValue::Double(*v),
            Self::String(v) => ColumnValue::String(v),
            Self::Json(v) => ColumnValue::Json(v),
            Self::DateTime(v) => ColumnValue::DateTime(v),
            Self::Binary(v) => ColumnValue::Binary(v),
        })
    }

    /// Encode at the inferred column type, including lossless widening.
    fn as_column_value_for(&self, target: ColumnType) -> Result<Option<ColumnValue<'_>>, String> {
        let value = match (self, target) {
            (Self::Null, _) => None,
            (Self::Byte(v), ColumnType::Long) => Some(ColumnValue::Long(i64::from(*v))),
            (Self::Short(v), ColumnType::Long) => Some(ColumnValue::Long(i64::from(*v))),
            (Self::Int(v), ColumnType::Long) => Some(ColumnValue::Long(i64::from(*v))),
            (Self::UByte(v), ColumnType::ULong) => Some(ColumnValue::ULong(u64::from(*v))),
            (Self::UShort(v), ColumnType::ULong) => Some(ColumnValue::ULong(u64::from(*v))),
            (Self::UInt(v), ColumnType::ULong) => Some(ColumnValue::ULong(u64::from(*v))),
            (Self::Float(v), ColumnType::Double) => Some(ColumnValue::Double(f64::from(*v))),
            _ if self.column_type() == Some(target) => self.as_column_value(),
            _ => {
                return Err(format!(
                    "cannot encode {:?} as {target:?}",
                    self.column_type()
                ));
            }
        };
        Ok(value)
    }

    fn check_encoded_size(&self, name: &str) -> Result<(), String> {
        let len = match self {
            Self::String(v) | Self::Json(v) | Self::DateTime(v) => v.len(),
            Self::Binary(v) => v.len(),
            _ => return Ok(()),
        };
        if len > u32::MAX as usize {
            return Err(format!(
                "property {name:?}: encoded value is {len} bytes, exceeding u32::MAX"
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct ColumnSchema {
    kind: Option<ColumnType>,
    nullable: bool,
}

impl ColumnSchema {
    fn kind_or_string(self) -> ColumnType {
        self.kind.unwrap_or(ColumnType::String)
    }
}

const RESERVED_COLUMNS: [&str; 5] = [
    "geom_wkb",
    "bbox_xmin",
    "bbox_ymin",
    "bbox_xmax",
    "bbox_ymax",
];

fn is_signed_integer(kind: ColumnType) -> bool {
    matches!(
        kind,
        ColumnType::Byte | ColumnType::Short | ColumnType::Int | ColumnType::Long
    )
}

fn is_unsigned_integer(kind: ColumnType) -> bool {
    matches!(
        kind,
        ColumnType::UByte | ColumnType::UShort | ColumnType::UInt | ColumnType::ULong
    )
}

fn merge_column_types(
    name: &str,
    left: ColumnType,
    right: ColumnType,
) -> Result<ColumnType, String> {
    if left == right {
        Ok(left)
    } else if is_signed_integer(left) && is_signed_integer(right) {
        Ok(ColumnType::Long)
    } else if is_unsigned_integer(left) && is_unsigned_integer(right) {
        Ok(ColumnType::ULong)
    } else if matches!(left, ColumnType::Float | ColumnType::Double)
        && matches!(right, ColumnType::Float | ColumnType::Double)
    {
        Ok(ColumnType::Double)
    } else {
        Err(format!(
            "property {name:?}: type conflict between {left:?} and {right:?}"
        ))
    }
}

/// Infer the complete schema before writing any features.
fn infer_schema(features: &[Feature]) -> Result<BTreeMap<String, ColumnSchema>, String> {
    let mut schema: BTreeMap<String, ColumnSchema> = BTreeMap::new();
    let mut folded_names = BTreeMap::new();
    for (feature_index, feature) in features.iter().enumerate() {
        for (name, column) in &mut schema {
            if !feature.properties.contains_key(name) {
                column.nullable = true;
            }
        }
        for (name, value) in &feature.properties {
            if RESERVED_COLUMNS
                .iter()
                .any(|reserved| name.eq_ignore_ascii_case(reserved))
            {
                return Err(format!(
                    "property {name:?}: reserved FlatGeobuf SQL column name"
                ));
            }
            if let Some(previous) = folded_names.insert(name.to_ascii_lowercase(), name.clone()) {
                if previous != *name {
                    return Err(format!(
                        "duplicate property names ignoring case: {previous:?} and {name:?}"
                    ));
                }
            }
            value.check_encoded_size(name)?;
            let kind = value.column_type();
            let explicit_null = kind.is_none();
            match schema.entry(name.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(ColumnSchema {
                        kind,
                        nullable: feature_index > 0 || explicit_null,
                    });
                }
                Entry::Occupied(entry) => {
                    let column = entry.into_mut();
                    column.nullable |= explicit_null;
                    column.kind = match (column.kind, kind) {
                        (None, next) => next,
                        (current, None) => current,
                        (Some(left), Some(right)) => Some(merge_column_types(name, left, right)?),
                    };
                }
            }
        }
    }
    if schema.len() > usize::from(u16::MAX) + 1 {
        return Err(format!(
            "schema has {} properties, exceeding the FlatGeobuf u16 property-index limit",
            schema.len()
        ));
    }
    Ok(schema)
}

/// Convert `input` to the FGBA file `out`, dispatching on the extension.
pub(crate) fn run(input: &Path, out: &Path, options: &fgba::BuildOptions) -> Result<(), String> {
    let label = input.display().to_string();
    let ext = input
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    match ext.as_str() {
        "fgb" => convert_fgb(input, out, options),
        "geojson" | "json" => {
            let geoms = read_geojson(input)?;
            convert_geoms(&label, out, geoms, options)
        }
        "csv" => convert_geoms(&label, out, read_csv(input, b',')?, options),
        "tsv" => convert_geoms(&label, out, read_csv(input, b'\t')?, options),
        "shp" => convert_geoms(&label, out, read_shp(input)?, options),
        "parquet" | "geoparquet" => Err(format!(
            "GeoParquet input is not implemented yet (v0.1): {label}"
        )),
        "gpkg" => Err(format!(
            "GeoPackage input is not implemented yet (v0.1): {label}"
        )),
        _ => Err(format!(
            "{label}: cannot detect input format (expected .geojson/.json, .csv/.tsv, .shp or .fgb)"
        )),
    }
}

/// `convert` on a FlatGeobuf input: keep the body verbatim, (re)build the
/// trailer — the same path as `fgba build`.
fn convert_fgb(input: &Path, out: &Path, options: &fgba::BuildOptions) -> Result<(), String> {
    let label = input.display().to_string();
    let bytes = std::fs::read(input).map_err(|e| format!("{label}: {e}"))?;
    let (body, had_trailer) = crate::strip_existing_trailer(bytes)?;
    if had_trailer {
        eprintln!("{label}: input already has an FGBA trailer; rebuilding from its body");
    }
    let t0 = Instant::now();
    let (report, total) = crate::build_to_file_at(&body, out, options)?;
    println!("converted {label} → {}", out.display());
    crate::print_report(&report, total, t0.elapsed());
    Ok(())
}

/// Write collected features as a FlatGeobuf body, then build the trailer.
fn convert_geoms(
    label: &str,
    out: &Path,
    (features, skipped): Features,
    options: &fgba::BuildOptions,
) -> Result<(), String> {
    if features.is_empty() {
        return Err(format!("{label}: no usable geometries"));
    }
    if skipped > 0 {
        eprintln!("{label}: skipped {skipped} feature(s) with empty or unusable geometry");
    }
    let count = features.len();
    let name = Path::new(label)
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "convert".to_string());
    let t0 = Instant::now();
    let body = write_body(&name, features)?;
    let (report, total) = crate::build_to_file_at(&body, out, options)?;
    println!(
        "converted {label} → {} ({count} features, {skipped} skipped)",
        out.display()
    );
    crate::print_report(&report, total, t0.elapsed());
    Ok(())
}

/// Write features with a predeclared schema, spatial index and WGS84 CRS.
fn write_body(name: &str, features: Vec<Feature>) -> Result<Vec<u8>, String> {
    let schema = infer_schema(&features)?;
    let options = FgbWriterOptions {
        crs: FgbCrs {
            code: 4326,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut writer = FgbWriter::create_with_options(name, GeometryType::Unknown, options)
        .map_err(|e| format!("cannot create FlatGeobuf writer: {e}"))?;
    for (name, column) in &schema {
        writer.add_column(name, column.kind_or_string(), |_, args| {
            args.nullable = column.nullable;
        });
    }
    for Feature { geom, properties } in features {
        let mut property_error = None;
        writer
            .add_feature_geom(geom, |feat| {
                for (index, (name, column)) in schema.iter().enumerate() {
                    let Some(value) = properties.get(name) else {
                        continue;
                    };
                    let value = match value.as_column_value_for(column.kind_or_string()) {
                        Ok(value) => value,
                        Err(error) => {
                            property_error = Some(format!("property {name:?}: {error}"));
                            return;
                        }
                    };
                    let Some(value) = value else {
                        continue;
                    };
                    if let Err(error) = feat.property(index, name, &value) {
                        property_error = Some(format!("cannot write property {name:?}: {error}"));
                        return;
                    }
                }
            })
            .map_err(|e| format!("cannot write feature: {e}"))?;
        if let Some(error) = property_error {
            return Err(error);
        }
    }
    let mut body = Vec::new();
    writer
        .write(&mut body)
        .map_err(|e| format!("cannot write FlatGeobuf body: {e}"))?;
    Ok(body)
}

// ---------- GeoJSON ----------

fn read_geojson(path: &Path) -> Result<Features, String> {
    let label = path.display().to_string();
    let text = std::fs::read_to_string(path).map_err(|e| format!("{label}: {e}"))?;
    let gj = text
        .parse::<geojson::GeoJson>()
        .map_err(|e| format!("{label}: invalid GeoJSON: {e}"))?;
    let mut features = Vec::new();
    let mut skipped = 0usize;
    let mut push_feature = |feature: &geojson::Feature| -> Result<(), String> {
        match geojson_feature(feature)? {
            Some(feature) => features.push(feature),
            None => skipped += 1,
        }
        Ok(())
    };
    match &gj {
        geojson::GeoJson::FeatureCollection(fc) => {
            for feature in &fc.features {
                push_feature(feature)?;
            }
        }
        geojson::GeoJson::Feature(feature) => push_feature(feature)?,
        geojson::GeoJson::Geometry(g) => match Geometry::try_from(&g.value) {
            Ok(geom) => features.push(Feature {
                geom,
                properties: BTreeMap::new(),
            }),
            Err(_) => skipped += 1,
        },
    }
    Ok((features, skipped))
}

fn geojson_feature(feature: &geojson::Feature) -> Result<Option<Feature>, String> {
    let Some(geometry) = &feature.geometry else {
        return Ok(None);
    };
    let Ok(geom) = Geometry::try_from(&geometry.value) else {
        return Ok(None);
    };
    let mut properties = BTreeMap::new();
    if let Some(values) = &feature.properties {
        for (name, value) in values {
            properties.insert(name.clone(), geojson_property(name, value)?);
        }
    }
    Ok(Some(Feature { geom, properties }))
}

fn geojson_property(name: &str, value: &geojson::JsonValue) -> Result<PropertyValue, String> {
    use geojson::JsonValue;
    match value {
        JsonValue::Null => Ok(PropertyValue::Null),
        JsonValue::Bool(v) => Ok(PropertyValue::Bool(*v)),
        JsonValue::Number(v) if v.is_i64() => v
            .as_i64()
            .map(PropertyValue::Long)
            .ok_or_else(|| format!("property {name:?}: invalid signed 64-bit integer")),
        JsonValue::Number(v) if v.is_u64() => v
            .as_u64()
            .map(PropertyValue::ULong)
            .ok_or_else(|| format!("property {name:?}: invalid unsigned 64-bit integer")),
        JsonValue::Number(v) if v.is_f64() => v
            .as_f64()
            .map(PropertyValue::Double)
            .ok_or_else(|| format!("property {name:?}: invalid double")),
        JsonValue::Number(_) => Err(format!("property {name:?}: unsupported JSON number")),
        JsonValue::String(v) => Ok(PropertyValue::String(v.clone())),
        JsonValue::Array(_) | JsonValue::Object(_) => Ok(PropertyValue::Json(value.to_string())),
    }
}

// ---------- CSV / TSV ----------

/// Column names recognized as holding WKT geometries, x or y coordinates
/// (compared case-insensitively).
const WKT_COLUMNS: [&str; 4] = ["wkt", "geom", "geometry", "the_geom"];
const X_COLUMNS: [&str; 6] = ["x", "lon", "lng", "long", "longitude", "easting"];
const Y_COLUMNS: [&str; 4] = ["y", "lat", "latitude", "northing"];

fn read_csv(path: &Path, delimiter: u8) -> Result<Features, String> {
    let label = path.display().to_string();
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .trim(csv::Trim::All)
        .from_path(path)
        .map_err(|e| format!("{label}: {e}"))?;
    let headers = reader
        .headers()
        .map_err(|e| format!("{label}: cannot read header row: {e}"))?
        .clone();
    validate_csv_headers(&headers).map_err(|e| format!("{label}: {e}"))?;
    let find = |names: &[&str]| {
        headers
            .iter()
            .position(|h| names.contains(&h.to_ascii_lowercase().as_str()))
    };
    let wkt_col = find(&WKT_COLUMNS);
    let xy_cols = find(&X_COLUMNS).zip(find(&Y_COLUMNS));
    if wkt_col.is_none() && xy_cols.is_none() {
        return Err(format!(
            "{label}: no geometry column found (looked for WKT: {WKT_COLUMNS:?}; \
             x/y: {X_COLUMNS:?} + {Y_COLUMNS:?}); header is {headers:?}"
        ));
    }

    let mut features = Vec::new();
    let mut skipped = 0usize;
    for (row, record) in reader.records().enumerate() {
        let record = record.map_err(|e| format!("{label}: row {}: {e}", row + 2))?;
        let parsed = match (wkt_col, xy_cols) {
            (Some(wkt), _) => record.get(wkt).map(parse_wkt).transpose()?.flatten(),
            (None, Some((x, y))) => parse_xy(record.get(x), record.get(y)),
            (None, None) => None, // ruled out above
        };
        match parsed {
            Some(geom) => {
                let properties = csv_properties(&headers, &record, wkt_col, xy_cols)
                    .map_err(|e| format!("{label}: row {}: {e}", row + 2))?;
                features.push(Feature { geom, properties });
            }
            None => skipped += 1,
        }
    }
    Ok((features, skipped))
}

fn validate_csv_headers(headers: &csv::StringRecord) -> Result<(), String> {
    let mut names = BTreeMap::new();
    for name in headers {
        if let Some(previous) = names.insert(name.to_ascii_lowercase(), name.to_owned()) {
            return Err(format!(
                "duplicate property names {previous:?} and {name:?} (case-insensitive)"
            ));
        }
    }
    Ok(())
}

fn csv_properties(
    headers: &csv::StringRecord,
    record: &csv::StringRecord,
    wkt_col: Option<usize>,
    xy_cols: Option<(usize, usize)>,
) -> Result<BTreeMap<String, PropertyValue>, String> {
    let mut properties = BTreeMap::new();
    for (index, name) in headers.iter().enumerate() {
        if Some(index) == wkt_col {
            continue;
        }
        let text = record.get(index).unwrap_or_default();
        let value = if xy_cols.is_some_and(|(x, y)| index == x || index == y) {
            let value = text
                .parse::<f64>()
                .map_err(|e| format!("property {name:?}: invalid double {text:?}: {e}"))?;
            if !value.is_finite() {
                return Err(format!(
                    "property {name:?}: coordinate must be finite, got {text:?}"
                ));
            }
            PropertyValue::Double(value)
        } else {
            PropertyValue::String(text.to_owned())
        };
        properties.insert(name.to_owned(), value);
    }
    Ok(properties)
}

/// Parse one WKT cell into a geometry; empty cells are skipped features.
fn parse_wkt(s: &str) -> Result<Option<Geometry<f64>>, String> {
    if s.is_empty() {
        return Ok(None);
    }
    let mut writer = geozero::geo_types::GeoWriter::new();
    geozero::wkt::Wkt(s)
        .process_geom(&mut writer)
        .map_err(|e| format!("bad WKT {s:?}: {e}"))?;
    Ok(writer.take_geometry())
}

/// Parse an x/y column pair into a point; blank or non-numeric cells skip
/// the feature.
fn parse_xy(x: Option<&str>, y: Option<&str>) -> Option<Geometry<f64>> {
    let x = x?.parse::<f64>().ok()?;
    let y = y?.parse::<f64>().ok()?;
    (x.is_finite() && y.is_finite()).then(|| Geometry::Point(Point::new(x, y)))
}

// ---------- Shapefile ----------

/// Anything with planar x/y accessors (shapefile Point, PointM, PointZ).
trait Xy {
    fn xy(&self) -> (f64, f64);
}

impl Xy for shapefile::Point {
    fn xy(&self) -> (f64, f64) {
        (self.x, self.y)
    }
}

impl Xy for shapefile::PointM {
    fn xy(&self) -> (f64, f64) {
        (self.x, self.y)
    }
}

impl Xy for shapefile::PointZ {
    fn xy(&self) -> (f64, f64) {
        (self.x, self.y)
    }
}

fn read_shp(path: &Path) -> Result<Features, String> {
    let label = path.display().to_string();
    let mut features = Vec::new();
    let mut skipped = 0usize;
    if path.with_extension("dbf").exists() {
        let mut reader = shapefile::Reader::from_path(path).map_err(|e| format!("{label}: {e}"))?;
        for (i, item) in reader.iter_shapes_and_records().enumerate() {
            let (shape, record) = item.map_err(|e| format!("{label}: shape {}: {e}", i + 1))?;
            match shp_geom(&shape) {
                Some(geom) => features.push(Feature {
                    geom,
                    properties: dbf_properties(record),
                }),
                None => skipped += 1,
            }
        }
    } else {
        // ShapeReader reads geometry only; a missing .dbf sidecar is fine.
        let mut reader =
            shapefile::ShapeReader::from_path(path).map_err(|e| format!("{label}: {e}"))?;
        for (i, shape) in reader.iter_shapes().enumerate() {
            let shape = shape.map_err(|e| format!("{label}: shape {}: {e}", i + 1))?;
            match shp_geom(&shape) {
                Some(geom) => features.push(Feature {
                    geom,
                    properties: BTreeMap::new(),
                }),
                None => skipped += 1,
            }
        }
    }
    Ok((features, skipped))
}

fn dbf_properties(record: shapefile::dbase::Record) -> BTreeMap<String, PropertyValue> {
    record
        .into_iter()
        .map(|(name, value)| (name, dbf_property(value)))
        .collect()
}

fn dbf_property(value: shapefile::dbase::FieldValue) -> PropertyValue {
    use shapefile::dbase::FieldValue;
    match value {
        FieldValue::Character(Some(v)) | FieldValue::Memo(v) => PropertyValue::String(v),
        FieldValue::Character(None) => PropertyValue::Null,
        FieldValue::Numeric(Some(v)) | FieldValue::Currency(v) | FieldValue::Double(v) => {
            PropertyValue::Double(v)
        }
        FieldValue::Numeric(None) => PropertyValue::Null,
        FieldValue::Float(Some(v)) => PropertyValue::Float(v),
        FieldValue::Float(None) => PropertyValue::Null,
        FieldValue::Integer(v) => PropertyValue::Int(v),
        FieldValue::Logical(Some(v)) => PropertyValue::Bool(v),
        FieldValue::Logical(None) => PropertyValue::Null,
        FieldValue::Date(Some(v)) => {
            PropertyValue::DateTime(format!("{:04}-{:02}-{:02}", v.year(), v.month(), v.day()))
        }
        FieldValue::Date(None) => PropertyValue::Null,
        FieldValue::DateTime(v) => {
            let date = v.date();
            let time = v.time();
            PropertyValue::DateTime(format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
                date.year(),
                date.month(),
                date.day(),
                time.hours(),
                time.minutes(),
                time.seconds()
            ))
        }
    }
}

/// Convert one shapefile record to a geo-types geometry (z/m dropped).
/// `NullShape` and `Multipatch` come out as `None` (skipped).
fn shp_geom(shape: &shapefile::Shape) -> Option<Geometry<f64>> {
    use shapefile::Shape;
    match shape {
        Shape::NullShape | Shape::Multipatch(_) => None,
        Shape::Point(p) => Some(Geometry::Point(Point::new(p.x, p.y))),
        Shape::PointM(p) => Some(Geometry::Point(Point::new(p.x, p.y))),
        Shape::PointZ(p) => Some(Geometry::Point(Point::new(p.x, p.y))),
        Shape::Multipoint(mp) => Some(multipoint(mp.points())),
        Shape::MultipointM(mp) => Some(multipoint(mp.points())),
        Shape::MultipointZ(mp) => Some(multipoint(mp.points())),
        Shape::Polyline(pl) => Some(polyline(pl.parts())),
        Shape::PolylineM(pl) => Some(polyline(pl.parts())),
        Shape::PolylineZ(pl) => Some(polyline(pl.parts())),
        Shape::Polygon(pg) => polygon(pg.rings()),
        Shape::PolygonM(pg) => polygon(pg.rings()),
        Shape::PolygonZ(pg) => polygon(pg.rings()),
    }
}

fn linestring<P: Xy>(points: &[P]) -> LineString<f64> {
    points
        .iter()
        .map(|p| {
            let (x, y) = p.xy();
            Coord { x, y }
        })
        .collect()
}

fn multipoint<P: Xy>(points: &[P]) -> Geometry<f64> {
    Geometry::MultiPoint(MultiPoint(
        points
            .iter()
            .map(|p| {
                let (x, y) = p.xy();
                Point::new(x, y)
            })
            .collect(),
    ))
}

fn polyline<P: Xy>(parts: &[Vec<P>]) -> Geometry<f64> {
    let mut lines: Vec<LineString<f64>> = parts.iter().map(|p| linestring(p)).collect();
    if lines.len() == 1 {
        Geometry::LineString(lines.remove(0))
    } else {
        Geometry::MultiLineString(MultiLineString(lines))
    }
}

/// Rings become polygons: a single outer ring takes every hole; with
/// several outer rings each hole goes to the outer ring containing its
/// first point (falling back to the first outer ring).
fn polygon<P: Xy>(rings: &[shapefile::PolygonRing<P>]) -> Option<Geometry<f64>> {
    let mut outers: Vec<LineString<f64>> = Vec::new();
    let mut inners: Vec<LineString<f64>> = Vec::new();
    for ring in rings {
        match ring {
            shapefile::PolygonRing::Outer(r) => outers.push(linestring(r)),
            shapefile::PolygonRing::Inner(r) => inners.push(linestring(r)),
        }
    }
    match outers.len() {
        0 => None,
        1 => Some(Geometry::Polygon(Polygon::new(outers.remove(0), inners))),
        _ => {
            let mut polys: Vec<Polygon<f64>> = outers
                .into_iter()
                .map(|outer| Polygon::new(outer, Vec::new()))
                .collect();
            for hole in inners {
                let target = hole
                    .0
                    .first()
                    .and_then(|c| polys.iter().position(|p| point_in_ring(*c, p.exterior())))
                    .unwrap_or(0);
                if let Some(poly) = polys.get_mut(target) {
                    poly.interiors_push(hole);
                }
            }
            Some(Geometry::MultiPolygon(geo_types::MultiPolygon(polys)))
        }
    }
}

/// Ray-casting point-in-ring test on the ring's x/y plane.
fn point_in_ring(c: Coord<f64>, ring: &LineString<f64>) -> bool {
    let mut inside = false;
    let pts = &ring.0;
    for i in 0..pts.len() {
        let a = pts[i];
        let b = pts[(i + 1) % pts.len()];
        let crosses = (a.y > c.y) != (b.y > c.y);
        if crosses {
            let at_x = a.x + (c.y - a.y) * (b.x - a.x) / (b.y - a.y);
            if c.x < at_x {
                inside = !inside;
            }
        }
    }
    inside
}
