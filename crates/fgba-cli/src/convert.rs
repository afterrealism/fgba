//! `fgba convert`: read GeoJSON, CSV/TSV, Shapefile or FlatGeobuf into a
//! FlatGeobuf body (spatial index, WGS84), then append the FGBA trailer.
//!
//! v0.1 carries geometries only: attributes are not converted (the body
//! holds the geometry plus no columns), and GeoParquet/GeoPackage inputs
//! fail with a clear "not yet" error.

use flatgeobuf::{FgbCrs, FgbWriter, FgbWriterOptions, GeometryType};
use geo_types::{Coord, Geometry, LineString, MultiLineString, MultiPoint, Point, Polygon};
use geozero::GeozeroGeometry;
use std::path::Path;
use std::time::Instant;

/// Geometries read from the source plus the count of skipped features.
type Geoms = (Vec<Geometry<f64>>, usize);

/// Convert `input` to the FGBA file `out`, dispatching on the extension.
pub(crate) fn run(input: &Path, out: &Path) -> Result<(), String> {
    let label = input.display().to_string();
    let ext = input
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    match ext.as_str() {
        "fgb" => convert_fgb(input, out),
        "geojson" | "json" => {
            let geoms = read_geojson(input)?;
            convert_geoms(&label, out, geoms)
        }
        "csv" => convert_geoms(&label, out, read_csv(input, b',')?),
        "tsv" => convert_geoms(&label, out, read_csv(input, b'\t')?),
        "shp" => convert_geoms(&label, out, read_shp(input)?),
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
fn convert_fgb(input: &Path, out: &Path) -> Result<(), String> {
    let label = input.display().to_string();
    let bytes = std::fs::read(input).map_err(|e| format!("{label}: {e}"))?;
    let (body, had_trailer) = crate::strip_existing_trailer(bytes)?;
    if had_trailer {
        eprintln!("{label}: input already has an FGBA trailer; rebuilding from its body");
    }
    let t0 = Instant::now();
    let (report, total) = crate::build_to_file_at(&body, out)?;
    println!("converted {label} → {}", out.display());
    crate::print_report(&report, total, t0.elapsed());
    Ok(())
}

/// Write collected geometries as a FlatGeobuf body, then build the trailer.
fn convert_geoms(label: &str, out: &Path, (geoms, skipped): Geoms) -> Result<(), String> {
    if geoms.is_empty() {
        return Err(format!("{label}: no usable geometries"));
    }
    if skipped > 0 {
        eprintln!("{label}: skipped {skipped} feature(s) with empty or unusable geometry");
    }
    let count = geoms.len();
    let name = Path::new(label)
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "convert".to_string());
    let t0 = Instant::now();
    let body = write_body(&name, geoms)?;
    let (report, total) = crate::build_to_file_at(&body, out)?;
    println!(
        "converted {label} → {} ({count} features, {skipped} skipped)",
        out.display()
    );
    crate::print_report(&report, total, t0.elapsed());
    Ok(())
}

/// Write geometries as a FlatGeobuf body: spatial index on, CRS WGS84
/// (EPSG:4326), geometry type detected from the features.
fn write_body(name: &str, geoms: Vec<Geometry<f64>>) -> Result<Vec<u8>, String> {
    let options = FgbWriterOptions {
        crs: FgbCrs {
            code: 4326,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut writer = FgbWriter::create_with_options(name, GeometryType::Unknown, options)
        .map_err(|e| format!("cannot create FlatGeobuf writer: {e}"))?;
    for geom in geoms {
        writer
            .add_feature_geom(geom, |_feat| {})
            .map_err(|e| format!("cannot write feature: {e}"))?;
    }
    let mut body = Vec::new();
    writer
        .write(&mut body)
        .map_err(|e| format!("cannot write FlatGeobuf body: {e}"))?;
    Ok(body)
}

// ---------- GeoJSON ----------

fn read_geojson(path: &Path) -> Result<Geoms, String> {
    let label = path.display().to_string();
    let text = std::fs::read_to_string(path).map_err(|e| format!("{label}: {e}"))?;
    let gj = text
        .parse::<geojson::GeoJson>()
        .map_err(|e| format!("{label}: invalid GeoJSON: {e}"))?;
    let mut geoms = Vec::new();
    let mut skipped = 0usize;
    let push = |g: &geojson::Geometry, geoms: &mut Vec<Geometry<f64>>, skipped: &mut usize| {
        match Geometry::try_from(&g.value) {
            Ok(g) => geoms.push(g),
            Err(_) => *skipped += 1,
        }
    };
    match &gj {
        geojson::GeoJson::FeatureCollection(fc) => {
            for feature in &fc.features {
                match &feature.geometry {
                    Some(g) => push(g, &mut geoms, &mut skipped),
                    None => skipped += 1,
                }
            }
        }
        geojson::GeoJson::Feature(feature) => match &feature.geometry {
            Some(g) => push(g, &mut geoms, &mut skipped),
            None => skipped += 1,
        },
        geojson::GeoJson::Geometry(g) => push(g, &mut geoms, &mut skipped),
    }
    Ok((geoms, skipped))
}

// ---------- CSV / TSV ----------

/// Column names recognized as holding WKT geometries, x or y coordinates
/// (compared case-insensitively).
const WKT_COLUMNS: [&str; 4] = ["wkt", "geom", "geometry", "the_geom"];
const X_COLUMNS: [&str; 6] = ["x", "lon", "lng", "long", "longitude", "easting"];
const Y_COLUMNS: [&str; 4] = ["y", "lat", "latitude", "northing"];

fn read_csv(path: &Path, delimiter: u8) -> Result<Geoms, String> {
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

    let mut geoms = Vec::new();
    let mut skipped = 0usize;
    for (row, record) in reader.records().enumerate() {
        let record = record.map_err(|e| format!("{label}: row {}: {e}", row + 2))?;
        let parsed = match (wkt_col, xy_cols) {
            (Some(wkt), _) => record.get(wkt).map(parse_wkt).transpose()?.flatten(),
            (None, Some((x, y))) => parse_xy(record.get(x), record.get(y)),
            (None, None) => None, // ruled out above
        };
        match parsed {
            Some(g) => geoms.push(g),
            None => skipped += 1,
        }
    }
    Ok((geoms, skipped))
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

fn read_shp(path: &Path) -> Result<Geoms, String> {
    let label = path.display().to_string();
    // ShapeReader reads geometry only; a missing .dbf sidecar is fine.
    let mut reader = shapefile::ShapeReader::from_path(path)
        .map_err(|e| format!("{label}: {e}"))?;
    let mut geoms = Vec::new();
    let mut skipped = 0usize;
    for (i, shape) in reader.iter_shapes().enumerate() {
        let shape = shape.map_err(|e| format!("{label}: shape {}: {e}", i + 1))?;
        match shp_geom(&shape) {
            Some(g) => geoms.push(g),
            None => skipped += 1,
        }
    }
    Ok((geoms, skipped))
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
                    .and_then(|c| {
                        polys
                            .iter()
                            .position(|p| point_in_ring(*c, p.exterior()))
                    })
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
