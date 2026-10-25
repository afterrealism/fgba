//! Geometry helpers shared by FGBA readers and writers.

use crate::error::{Error, Result};
use flatgeobuf::geozero::{error::Result as GeozeroResult, GeomProcessor};

/// Geometry processor collecting the bounding box of one feature.
///
/// Non-finite coordinates are ignored when constructing the bounding box and
/// midpoint; [`BboxSink::checked_bounds`] can reject them instead. A feature
/// whose geometry yields no finite coordinate ends with `count == 0`.
pub struct BboxSink {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
    count: u64,
    non_finite: bool,
}

impl BboxSink {
    /// Create an empty bounding-box sink.
    pub fn new() -> Self {
        BboxSink {
            min_x: f64::INFINITY,
            min_y: f64::INFINITY,
            max_x: f64::NEG_INFINITY,
            max_y: f64::NEG_INFINITY,
            count: 0,
            non_finite: false,
        }
    }

    /// The bounding-box midpoint, or `None` when no finite coordinate was seen.
    /// Written overflow-free (`min + (max - min) / 2`) for hostile input.
    pub fn midpoint(&self) -> Option<(f64, f64)> {
        if self.count == 0 {
            return None;
        }
        Some((
            self.min_x + (self.max_x - self.min_x) / 2.0,
            self.min_y + (self.max_y - self.min_y) / 2.0,
        ))
    }

    /// Return the exact bounds, or `None` when no finite coordinate was seen.
    ///
    /// Returns [`Error::Format`] if any non-finite coordinate was encountered,
    /// even when finite coordinates were also present.
    pub fn checked_bounds(&self) -> Result<Option<[f64; 4]>> {
        if self.non_finite {
            return Err(Error::Format(
                "geometry contains non-finite coordinates".to_string(),
            ));
        }
        if self.count == 0 {
            return Ok(None);
        }
        Ok(Some([self.min_x, self.min_y, self.max_x, self.max_y]))
    }
}

impl Default for BboxSink {
    fn default() -> Self {
        Self::new()
    }
}

impl GeomProcessor for BboxSink {
    fn xy(&mut self, x: f64, y: f64, _idx: usize) -> GeozeroResult<()> {
        if x.is_finite() && y.is_finite() {
            self.min_x = self.min_x.min(x);
            self.min_y = self.min_y.min(y);
            self.max_x = self.max_x.max(x);
            self.max_y = self.max_y.max(y);
            self.count += 1;
        } else {
            self.non_finite = true;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bbox_sink_tracks_min_max() {
        let mut sink = BboxSink::new();
        assert_eq!(sink.midpoint(), None);
        let coords = [(2.0, 48.0), (4.0, 52.0), (3.0, 49.0)];
        for (i, &(x, y)) in coords.iter().enumerate() {
            sink.xy(x, y, i).unwrap_or(());
        }
        assert_eq!(sink.midpoint(), Some((3.0, 50.0)));
        // Non-finite coordinates are ignored, never poisoning the bounds.
        sink.xy(f64::NAN, f64::INFINITY, 3).unwrap_or(());
        assert_eq!(sink.midpoint(), Some((3.0, 50.0)));
    }

    #[test]
    fn checked_bounds_returns_none_for_empty_geometry() {
        let sink = BboxSink::new();
        assert_eq!(sink.checked_bounds().ok(), Some(None));
    }

    #[test]
    fn checked_bounds_returns_exact_finite_bounds() {
        let mut sink = BboxSink::new();
        sink.xy(-2.5, 10.0, 0).unwrap_or(());
        sink.xy(4.5, -6.0, 1).unwrap_or(());
        sink.xy(1.0, 2.0, 2).unwrap_or(());
        assert_eq!(
            sink.checked_bounds().ok(),
            Some(Some([-2.5, -6.0, 4.5, 10.0]))
        );
    }

    #[test]
    fn checked_bounds_rejects_nan() {
        let mut sink = BboxSink::new();
        sink.xy(f64::NAN, 0.0, 0).unwrap_or(());
        assert!(matches!(sink.checked_bounds(), Err(Error::Format(_))));
    }

    #[test]
    fn checked_bounds_rejects_positive_infinity() {
        let mut sink = BboxSink::new();
        sink.xy(0.0, f64::INFINITY, 0).unwrap_or(());
        assert!(matches!(sink.checked_bounds(), Err(Error::Format(_))));
    }

    #[test]
    fn checked_bounds_rejects_negative_infinity() {
        let mut sink = BboxSink::new();
        sink.xy(f64::NEG_INFINITY, 0.0, 0).unwrap_or(());
        assert!(matches!(sink.checked_bounds(), Err(Error::Format(_))));
    }
}
