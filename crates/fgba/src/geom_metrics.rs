//! One-pass planar geometry metrics for the GEOMETRY_METRICS writer.

use crate::error::{Error, Result};
use flatgeobuf::geozero::{error::GeozeroError, GeomProcessor, GeozeroGeometry};

/// Canonical planar metrics for one non-empty FlatGeobuf feature geometry.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FeatureMetrics {
    /// Total planar line and ring length.
    pub length: f64,
    /// Total unsigned planar polygon area.
    pub area: f64,
    /// Number of geometry vertices.
    pub npoints: u32,
    /// Geometry bounds as `[xmin, ymin, xmax, ymax]`.
    pub bbox: [f64; 4],
}

#[derive(Clone, Copy, PartialEq)]
struct Coord {
    x: f64,
    y: f64,
}

#[derive(Default)]
struct LineState {
    first: Option<Coord>,
    previous: Option<Coord>,
    shift: Option<Coord>,
    twice_area: f64,
    vertices: u32,
    ring: bool,
}

#[derive(Default)]
struct PolygonState {
    area: f64,
    rings: usize,
}

/// Shared streaming accumulator for planar segment, shoelace, and vertex metrics.
///
/// The same implementation backs FlatGeobuf/geozero features and SQL WKB UDFs.
pub struct MetricsSink {
    length: f64,
    area: f64,
    npoints: u32,
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
    line: Option<LineState>,
    polygon: Option<PolygonState>,
}

impl Default for MetricsSink {
    fn default() -> Self {
        Self {
            length: 0.0,
            area: 0.0,
            npoints: 0,
            min_x: f64::INFINITY,
            min_y: f64::INFINITY,
            max_x: f64::NEG_INFINITY,
            max_y: f64::NEG_INFINITY,
            line: None,
            polygon: None,
        }
    }
}

impl MetricsSink {
    /// Construct an empty accumulator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin one line or polygon ring.
    pub fn begin_line(&mut self, ring: bool) -> Result<()> {
        if self.line.is_some() {
            return Err(Error::Format("nested linestring events are invalid".into()));
        }
        self.line = Some(LineState {
            ring,
            ..LineState::default()
        });
        Ok(())
    }

    /// End the current line or ring and commit its ring area when applicable.
    pub fn end_line(&mut self) -> Result<()> {
        let line = self
            .line
            .take()
            .ok_or_else(|| Error::Format("linestring ended before it began".into()))?;
        if !line.ring {
            return Ok(());
        }
        let polygon = self
            .polygon
            .as_mut()
            .ok_or_else(|| Error::Format("polygon ring has no polygon context".into()))?;
        let closed = line.vertices >= 3 && line.first == line.previous;
        let ring_area = if closed {
            line.twice_area.abs() / 2.0
        } else {
            0.0
        };
        polygon.area = if polygon.rings == 0 {
            ring_area
        } else {
            polygon.area - ring_area
        };
        polygon.rings = polygon
            .rings
            .checked_add(1)
            .ok_or_else(|| Error::Format("polygon ring count overflows usize".into()))?;
        Ok(())
    }

    /// Begin one polygon whose rings are supplied as lines with `ring = true`.
    pub fn begin_polygon(&mut self) -> Result<()> {
        if self.polygon.is_some() {
            return Err(Error::Format("nested polygon events are invalid".into()));
        }
        self.polygon = Some(PolygonState::default());
        Ok(())
    }

    /// End the current polygon and commit its orientation-independent area.
    pub fn end_polygon(&mut self) -> Result<()> {
        let polygon = self
            .polygon
            .take()
            .ok_or_else(|| Error::Format("polygon ended before it began".into()))?;
        self.area += polygon.area.abs();
        Ok(())
    }

    /// Add one finite XY coordinate, updating vertices, bounds, and open lines.
    pub fn push_xy(&mut self, x: f64, y: f64) -> Result<()> {
        if !x.is_finite() || !y.is_finite() {
            return Err(Error::Format(
                "geometry metrics require finite coordinates".into(),
            ));
        }
        self.npoints = self
            .npoints
            .checked_add(1)
            .ok_or_else(|| Error::Format("geometry vertex count exceeds u32::MAX".into()))?;
        self.min_x = self.min_x.min(x);
        self.min_y = self.min_y.min(y);
        self.max_x = self.max_x.max(x);
        self.max_y = self.max_y.max(y);
        let current = Coord { x, y };
        if let Some(line) = self.line.as_mut() {
            if let Some(previous) = line.previous {
                self.length += (x - previous.x).hypot(y - previous.y);
                if line.ring {
                    let shift = line.shift.ok_or_else(|| {
                        Error::Format("polygon ring has no shift coordinate".into())
                    })?;
                    line.twice_area += (previous.x - shift.x) * (y - shift.y)
                        - (x - shift.x) * (previous.y - shift.y);
                }
            } else {
                line.first = Some(current);
                line.shift = Some(current);
            }
            line.previous = Some(current);
            line.vertices = line
                .vertices
                .checked_add(1)
                .ok_or_else(|| Error::Format("line vertex count exceeds u32::MAX".into()))?;
        }
        Ok(())
    }

    /// Finish the geometry, returning `None` when it contains no XY coordinates.
    pub fn finish(self) -> Result<Option<FeatureMetrics>> {
        if self.line.is_some() || self.polygon.is_some() {
            return Err(Error::Format(
                "geometry metrics ended inside a geometry part".into(),
            ));
        }
        if self.npoints == 0 {
            return Ok(None);
        }
        Ok(Some(FeatureMetrics {
            length: self.length,
            area: self.area,
            npoints: self.npoints,
            bbox: [self.min_x, self.min_y, self.max_x, self.max_y],
        }))
    }
}

fn into_geozero(error: Error) -> GeozeroError {
    GeozeroError::Geometry(error.to_string())
}

impl GeomProcessor for MetricsSink {
    fn xy(&mut self, x: f64, y: f64, _idx: usize) -> flatgeobuf::geozero::error::Result<()> {
        self.push_xy(x, y).map_err(into_geozero)
    }

    fn empty_point(&mut self, _idx: usize) -> flatgeobuf::geozero::error::Result<()> {
        Ok(())
    }

    fn linestring_begin(
        &mut self,
        _tagged: bool,
        _size: usize,
        _idx: usize,
    ) -> flatgeobuf::geozero::error::Result<()> {
        self.begin_line(self.polygon.is_some())
            .map_err(into_geozero)
    }

    fn linestring_end(
        &mut self,
        _tagged: bool,
        _idx: usize,
    ) -> flatgeobuf::geozero::error::Result<()> {
        self.end_line().map_err(into_geozero)
    }

    fn circularstring_begin(
        &mut self,
        _size: usize,
        _idx: usize,
    ) -> flatgeobuf::geozero::error::Result<()> {
        self.begin_line(self.polygon.is_some())
            .map_err(into_geozero)
    }

    fn circularstring_end(&mut self, _idx: usize) -> flatgeobuf::geozero::error::Result<()> {
        self.end_line().map_err(into_geozero)
    }

    fn polygon_begin(
        &mut self,
        _tagged: bool,
        _size: usize,
        _idx: usize,
    ) -> flatgeobuf::geozero::error::Result<()> {
        self.begin_polygon().map_err(into_geozero)
    }

    fn polygon_end(
        &mut self,
        _tagged: bool,
        _idx: usize,
    ) -> flatgeobuf::geozero::error::Result<()> {
        self.end_polygon().map_err(into_geozero)
    }

    fn curvepolygon_begin(
        &mut self,
        _size: usize,
        _idx: usize,
    ) -> flatgeobuf::geozero::error::Result<()> {
        self.begin_polygon().map_err(into_geozero)
    }

    fn curvepolygon_end(&mut self, _idx: usize) -> flatgeobuf::geozero::error::Result<()> {
        self.end_polygon().map_err(into_geozero)
    }

    fn triangle_begin(
        &mut self,
        _tagged: bool,
        _size: usize,
        _idx: usize,
    ) -> flatgeobuf::geozero::error::Result<()> {
        self.begin_polygon().map_err(into_geozero)
    }

    fn triangle_end(
        &mut self,
        _tagged: bool,
        _idx: usize,
    ) -> flatgeobuf::geozero::error::Result<()> {
        self.end_polygon().map_err(into_geozero)
    }
}

pub(crate) fn process_geometry<G: GeozeroGeometry>(geometry: &G) -> Result<Option<FeatureMetrics>> {
    let mut sink = MetricsSink::new();
    geometry
        .process_geom(&mut sink)
        .map_err(|error| Error::Format(format!("cannot compute geometry metrics: {error}")))?;
    sink.finish()
}

/// Compute the canonical metrics stored in GEOMETRY_METRICS for one feature.
///
/// Returns `Ok(None)` for an absent geometry or a geometry without coordinates.
pub fn feature_metrics(feature: &flatgeobuf::FgbFeature) -> Result<Option<FeatureMetrics>> {
    let Some(geometry) = feature.geometry() else {
        return Ok(None);
    };
    let no_xy = geometry.xy().is_none_or(|xy| xy.is_empty());
    let no_parts = geometry.parts().is_none_or(|parts| parts.is_empty());
    if no_xy && no_parts {
        return Ok(None);
    }
    process_geometry(feature)
}

#[cfg(test)]
mod tests {
    use super::{process_geometry, MetricsSink};
    use geo::algorithm::line_measures::{Euclidean, Length};
    use geo::{Area, Geometry, LineString, Polygon};
    use geozero::GeomProcessor;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn ring(points: &[(f64, f64)]) -> LineString<f64> {
        LineString::from(points.to_vec())
    }

    fn polygon(exterior: &[(f64, f64)], holes: &[&[(f64, f64)]]) -> Polygon<f64> {
        Polygon::new(
            ring(exterior),
            holes.iter().map(|hole| ring(hole)).collect(),
        )
    }

    fn geo_length(geometry: &Geometry<f64>) -> f64 {
        fn polygon_length(polygon: &Polygon<f64>) -> f64 {
            std::iter::once(polygon.exterior())
                .chain(polygon.interiors())
                .map(|ring| Euclidean.length(ring))
                .sum()
        }
        match geometry {
            Geometry::Point(_) | Geometry::MultiPoint(_) => 0.0,
            Geometry::LineString(line) => Euclidean.length(line),
            Geometry::MultiLineString(lines) => {
                lines.iter().map(|line| Euclidean.length(line)).sum()
            }
            Geometry::Polygon(polygon) => polygon_length(polygon),
            Geometry::MultiPolygon(polygons) => polygons.iter().map(polygon_length).sum(),
            Geometry::GeometryCollection(collection) => collection.iter().map(geo_length).sum(),
            _ => 0.0,
        }
    }

    fn geo_npoints(geometry: &Geometry<f64>) -> u32 {
        fn polygon_npoints(polygon: &Polygon<f64>) -> u32 {
            std::iter::once(polygon.exterior())
                .chain(polygon.interiors())
                .map(|ring| ring.0.len() as u32)
                .sum()
        }
        match geometry {
            Geometry::Point(_) => 1,
            Geometry::MultiPoint(points) => points.0.len() as u32,
            Geometry::LineString(line) => line.0.len() as u32,
            Geometry::MultiLineString(lines) => lines.iter().map(|line| line.0.len() as u32).sum(),
            Geometry::Polygon(polygon) => polygon_npoints(polygon),
            Geometry::MultiPolygon(polygons) => polygons.iter().map(polygon_npoints).sum(),
            Geometry::GeometryCollection(collection) => collection.iter().map(geo_npoints).sum(),
            _ => 0,
        }
    }

    fn assert_matches_geo(geometry: &Geometry<f64>, bbox: [f64; 4]) -> TestResult {
        let metrics = process_geometry(geometry)?.ok_or("geometry unexpectedly empty")?;
        assert!((metrics.length - geo_length(geometry)).abs() <= 1e-9);
        assert!((metrics.area - geometry.unsigned_area()).abs() <= 1e-9);
        assert_eq!(metrics.npoints, geo_npoints(geometry));
        assert_eq!(metrics.bbox, bbox);
        Ok(())
    }

    #[test]
    fn two_point_line_matches_geo() -> TestResult {
        let line = Geometry::LineString(ring(&[(0.0, 0.0), (3.0, 0.0)]));
        assert_matches_geo(&line, [0.0, 0.0, 3.0, 0.0])?;
        let metrics = process_geometry(&line)?.ok_or("missing line metrics")?;
        assert_eq!(metrics.length, 3.0);
        assert_eq!(metrics.area, 0.0);
        assert_eq!(metrics.npoints, 2);
        Ok(())
    }

    #[test]
    fn unit_square_matches_plan_and_geo() -> TestResult {
        let square = Geometry::Polygon(polygon(
            &[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)],
            &[],
        ));
        assert_matches_geo(&square, [0.0, 0.0, 1.0, 1.0])?;
        let metrics = process_geometry(&square)?.ok_or("missing square metrics")?;
        assert_eq!(metrics.length, 4.0);
        assert_eq!(metrics.area, 1.0);
        assert_eq!(metrics.npoints, 5);
        Ok(())
    }

    #[test]
    fn multipolygon_holes_and_collection_match_geo() -> TestResult {
        let shell = [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)];
        let hole = [(1.0, 1.0), (1.0, 2.0), (2.0, 2.0), (2.0, 1.0), (1.0, 1.0)];
        let unit = [
            (10.0, 0.0),
            (11.0, 0.0),
            (11.0, 1.0),
            (10.0, 1.0),
            (10.0, 0.0),
        ];
        let multi = Geometry::MultiPolygon(geo::MultiPolygon(vec![
            polygon(&shell, &[&hole]),
            polygon(&unit, &[]),
        ]));
        assert_matches_geo(&multi, [0.0, 0.0, 11.0, 4.0])?;
        let metrics = process_geometry(&multi)?.ok_or("missing multipolygon metrics")?;
        assert_eq!(metrics.length, 24.0);
        assert_eq!(metrics.area, 16.0);
        assert_eq!(metrics.npoints, 15);

        let collection = Geometry::GeometryCollection(geo::GeometryCollection(vec![
            Geometry::Point(geo::Point::new(-2.0, 5.0)),
            Geometry::LineString(ring(&[(-2.0, 5.0), (-1.0, 5.0)])),
            Geometry::Polygon(polygon(&unit, &[])),
        ]));
        assert_matches_geo(&collection, [-2.0, 0.0, 11.0, 5.0])
    }

    #[test]
    fn empty_geometries_have_no_metrics_row() -> TestResult {
        let empty_collection = Geometry::GeometryCollection(geo::GeometryCollection(vec![]));
        assert!(process_geometry(&empty_collection)?.is_none());
        let empty_line = Geometry::LineString(LineString::new(Vec::new()));
        assert!(process_geometry(&empty_line)?.is_none());
        Ok(())
    }

    #[test]
    fn vertex_count_is_checked_and_non_finite_coordinates_are_rejected() {
        let mut sink = MetricsSink::new();
        sink.npoints = u32::MAX;
        assert!(sink.xy(0.0, 0.0, 0).is_err());
        assert!(sink.xy(f64::NAN, 0.0, 1).is_err());
        assert!(sink.xy(0.0, f64::INFINITY, 1).is_err());
    }
}
