//! Retryable FGBA operations over the sparse source.

use crate::range::{would_block, SparseRange, WouldBlock, MAX_CACHE_BYTES, MAX_RANGE_BYTES};
use fgba::{BandMeta, Reader};
use flatgeobuf::packed_r_tree::PackedRTree;
use flatgeobuf::{FallibleStreamingIterator, FgbReader};
use geozero::geojson::GeoJsonWriter;
use geozero::{FeatureAccess, FeatureProcessor};
use serde_json::{json, Value};
use std::error::Error as StdError;
use std::io::{Cursor, Error as IoError, Read, Seek, SeekFrom};
use wasm_bindgen::prelude::*;

/// Hard upper bound for exact feature output.
pub const MAX_FEATURES: u32 = 50_000;

fn set_field(object: &js_sys::Object, key: &str, value: &JsValue) -> Result<(), JsValue> {
    js_sys::Reflect::set(object, &JsValue::from_str(key), value).map(|_| ())
}

fn miss_value(miss: WouldBlock) -> JsValue {
    let object = js_sys::Object::new();
    for (key, value) in [
        ("name", JsValue::from_str("WouldBlock")),
        ("code", JsValue::from_str("FGBA_WOULD_BLOCK")),
        ("offset", JsValue::from_str(&miss.offset.to_string())),
        ("len", JsValue::from_str(&miss.len.to_string())),
    ] {
        if let Err(error) = set_field(&object, key, &value) {
            return error;
        }
    }
    object.into()
}

fn find_miss(mut error: &(dyn StdError + 'static)) -> Option<WouldBlock> {
    loop {
        if let Some(flatgeobuf::Error::IO(io_error)) = error.downcast_ref::<flatgeobuf::Error>() {
            if let Some(miss) = would_block(io_error) {
                return Some(miss);
            }
        }
        if let Some(io_error) = error.downcast_ref::<IoError>() {
            if let Some(miss) = would_block(io_error) {
                return Some(miss);
            }
        }
        error = error.source()?;
    }
}

fn dataset_error<E>(context: &str, error: E) -> JsValue
where
    E: StdError + 'static,
{
    if let Some(miss) = find_miss(&error) {
        return miss_value(miss);
    }
    JsValue::from_str(&format!("{context}: {error}"))
}

fn parse_u64(value: &str, label: &str) -> Result<u64, JsValue> {
    value
        .parse::<u64>()
        .map_err(|_| JsValue::from_str(&format!("invalid {label}")))
}

fn parse_bbox(bbox: Vec<f64>) -> Result<[f64; 4], JsValue> {
    let values: [f64; 4] = bbox
        .try_into()
        .map_err(|_| JsValue::from_str("bbox must contain four numbers"))?;
    if values.iter().any(|value| !value.is_finite())
        || values[0] > values[2]
        || values[1] > values[3]
    {
        return Err(JsValue::from_str(
            "bbox must be finite and ordered as minX,minY,maxX,maxY",
        ));
    }
    Ok(values)
}

#[derive(Debug, Clone, Copy)]
struct SelectedFeature {
    offset: u64,
    len: u64,
}

/// A forward-only FlatGeobuf stream containing the original header followed by
/// only the R-tree-selected features. `select_all` skips the index with one
/// `SeekFrom::Current`; that single seek is a no-op because this stream has no
/// index. It avoids FlatGeobuf 6.0.1's default-node-size-only `select_bbox`.
struct SelectedFeatureStream {
    header: Cursor<Vec<u8>>,
    source: SparseRange,
    features: Vec<SelectedFeature>,
    next_feature: usize,
    current: Option<(u64, u64)>,
    position: u64,
    ignore_index_skip: bool,
}

impl Read for SelectedFeatureStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.header.position() < self.header.get_ref().len() as u64 {
            let count = self.header.read(buffer)?;
            self.position += count as u64;
            return Ok(count);
        }
        if self.current.is_none() {
            let Some(feature) = self.features.get(self.next_feature).copied() else {
                return Ok(0);
            };
            self.source.seek(SeekFrom::Start(feature.offset))?;
            self.current = Some((feature.offset, feature.len));
            self.next_feature += 1;
        }
        let Some((_, remaining)) = self.current else {
            return Ok(0);
        };
        let wanted = buffer
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let count = self.source.read(&mut buffer[..wanted])?;
        if count == 0 {
            return Err(IoError::new(
                std::io::ErrorKind::UnexpectedEof,
                "selected FlatGeobuf feature is truncated",
            ));
        }
        self.position += count as u64;
        let remaining = remaining - count as u64;
        self.current = (remaining > 0).then_some((0, remaining));
        Ok(count)
    }
}

impl Seek for SelectedFeatureStream {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        match position {
            SeekFrom::Current(0) => Ok(self.position),
            SeekFrom::Current(_) if self.ignore_index_skip => {
                self.ignore_index_skip = false;
                Ok(self.position)
            }
            _ => Err(IoError::new(
                std::io::ErrorKind::Unsupported,
                "selected FlatGeobuf stream is forward-only",
            )),
        }
    }
}

fn prefetch_selected_features(
    source: &mut SparseRange,
    bbox: [f64; 4],
    limit: u32,
    body_len: u64,
) -> Result<SelectedFeatureStream, JsValue> {
    let fgb = FgbReader::open(&mut *source)
        .map_err(|error| dataset_error("open FlatGeobuf body", error))?;
    let header = fgb.header();
    let feature_count = header.features_count();
    let index_node_size = header.index_node_size();
    if feature_count == 0 || index_node_size == 0 {
        return Err(dataset_error(
            "select FlatGeobuf features",
            flatgeobuf::Error::NoIndex,
        ));
    }
    let feature_count = usize::try_from(feature_count)
        .map_err(|_| JsValue::from_str("FlatGeobuf feature count does not fit this platform"))?;
    let index_size = PackedRTree::index_size(feature_count, index_node_size) as u64;
    drop(fgb);
    let index_start = source
        .stream_position()
        .map_err(|error| dataset_error("locate FlatGeobuf index", error))?;
    let data_start = index_start
        .checked_add(index_size)
        .ok_or_else(|| JsValue::from_str("FlatGeobuf feature offset overflows"))?;
    if data_start > body_len {
        return Err(JsValue::from_str(
            "FlatGeobuf header and index extend beyond the body",
        ));
    }

    let header_len = usize::try_from(index_start)
        .map_err(|_| JsValue::from_str("FlatGeobuf header does not fit this platform"))?;
    let mut header_bytes = vec![0_u8; header_len];
    let mut header_source = source.clone();
    header_source
        .seek(SeekFrom::Start(0))
        .and_then(|_| header_source.read_exact(&mut header_bytes))
        .map_err(|error| dataset_error("cache FlatGeobuf header", error))?;
    let hits = PackedRTree::stream_search(
        &mut *source,
        feature_count,
        index_node_size,
        bbox[0],
        bbox[1],
        bbox[2],
        bbox[3],
    )
    .map_err(|error| dataset_error("search FlatGeobuf index", error))?;

    let mut features = Vec::with_capacity(hits.len().min(limit as usize));
    let mut prefix = [0_u8; 4];
    for hit in hits.iter().take(limit as usize) {
        let feature_offset = data_start
            .checked_add(hit.offset as u64)
            .ok_or_else(|| JsValue::from_str("FlatGeobuf feature offset overflows"))?;
        source
            .seek(SeekFrom::Start(feature_offset))
            .map_err(|error| dataset_error("seek to selected FlatGeobuf feature", error))?;
        source
            .read_exact(&mut prefix)
            .map_err(|error| dataset_error("read selected FlatGeobuf feature prefix", error))?;
        let feature_size = u64::from(u32::from_le_bytes(prefix));
        let payload_start = feature_offset
            .checked_add(4)
            .ok_or_else(|| JsValue::from_str("FlatGeobuf feature payload offset overflows"))?;
        let feature_end = payload_start
            .checked_add(feature_size)
            .ok_or_else(|| JsValue::from_str("FlatGeobuf feature end overflows"))?;
        if feature_end > body_len {
            return Err(JsValue::from_str(
                "FlatGeobuf feature extends beyond the body",
            ));
        }
        let copied = std::io::copy(&mut (&mut *source).take(feature_size), &mut std::io::sink())
            .map_err(|error| dataset_error("cache selected FlatGeobuf feature", error))?;
        if copied != feature_size {
            return Err(JsValue::from_str("FlatGeobuf feature is truncated"));
        }
        features.push(SelectedFeature {
            offset: feature_offset,
            len: 4 + feature_size,
        });
    }
    Ok(SelectedFeatureStream {
        header: Cursor::new(header_bytes),
        source: source.clone(),
        features,
        next_feature: 0,
        current: None,
        position: 0,
        ignore_index_skip: true,
    })
}

/// Synchronous WASM half of the async JS `FgbaDataset` wrapper. Every method
/// is retryable: a cache miss throws a plain JS object named `WouldBlock`.
#[wasm_bindgen]
pub struct SparseDataset {
    source: SparseRange,
    bands: Option<Vec<BandMeta>>,
    closed: bool,
}

#[wasm_bindgen]
impl SparseDataset {
    /// Create a source after JS has learned its size with a `bytes=0-0` probe.
    #[wasm_bindgen(constructor)]
    pub fn new(
        total: String,
        max_range_bytes: usize,
        max_cache_bytes: usize,
    ) -> Result<SparseDataset, JsValue> {
        if max_range_bytes > MAX_RANGE_BYTES || max_cache_bytes > MAX_CACHE_BYTES {
            return Err(JsValue::from_str(
                "sparse range limits exceed the hard caps",
            ));
        }
        let total = parse_u64(&total, "object size")?;
        let source = SparseRange::new(total, max_range_bytes, max_cache_bytes)
            .map_err(|error| dataset_error("create sparse source", error))?;
        Ok(Self {
            source,
            bands: None,
            closed: false,
        })
    }

    /// Decode and retain footer/directory band metadata.
    pub fn open(&mut self) -> Result<(), JsValue> {
        if self.closed {
            return Err(JsValue::from_str("dataset is closed"));
        }
        if self.bands.is_some() {
            return Ok(());
        }
        let reader =
            Reader::open(self.source.clone()).map_err(|error| dataset_error("open FGBA", error))?;
        self.bands = Some(reader.bands());
        Ok(())
    }

    /// Insert bytes fetched for a prior `WouldBlock` miss.
    #[wasm_bindgen(js_name = supplyRange)]
    pub fn supply_range(&mut self, offset: String, bytes: &[u8]) -> Result<(), JsValue> {
        if self.closed {
            return Err(JsValue::from_str("dataset is closed"));
        }
        let offset = parse_u64(&offset, "range offset")?;
        self.source
            .insert(offset, bytes)
            .map_err(|error| dataset_error("insert sparse range", error))
    }

    /// Return band metadata as JSON. Offsets and sizes are decimal strings.
    pub fn bands(&self) -> Result<String, JsValue> {
        if self.closed {
            return Err(JsValue::from_str("dataset is closed"));
        }
        let bands = self
            .bands
            .as_ref()
            .ok_or_else(|| JsValue::from_str("dataset is not open"))?;
        let values: Vec<Value> = bands
            .iter()
            .map(|band| {
                json!({
                    "res": band.res,
                    "offset": band.offset.to_string(),
                    "size": band.size.to_string(),
                })
            })
            .collect();
        serde_json::to_string(&values).map_err(|error| dataset_error("encode bands", error))
    }

    /// Query an A5 band and return GeoJSON with a decimal `estimatedCount`.
    #[wasm_bindgen(js_name = queryA5)]
    pub fn query_a5(&mut self, bbox: Vec<f64>, res: Option<u8>) -> Result<String, JsValue> {
        let bbox = parse_bbox(bbox)?;
        let bands = self
            .bands
            .as_ref()
            .ok_or_else(|| JsValue::from_str("dataset is not open"))?;
        let res = match res {
            Some(res) => res,
            None => bands
                .iter()
                .map(|band| band.res)
                .max()
                .ok_or_else(|| JsValue::from_str("dataset has no aggregate bands"))?,
        };
        let mut reader = Reader::open(self.source.clone())
            .map_err(|error| dataset_error("reopen FGBA", error))?;
        let hits = reader
            .query(bbox, res)
            .map_err(|error| dataset_error("query A5 band", error))?;
        let estimated = hits
            .iter()
            .fold(0_u128, |total, (_, count)| total + u128::from(*count));
        let mut features = Vec::with_capacity(hits.len());
        for (cell, count) in hits {
            let boundary = a5::cell_to_boundary(cell, None)
                .map_err(|error| JsValue::from_str(&format!("A5 boundary: {error}")))?;
            let ring: Vec<Value> = boundary
                .iter()
                .map(|point| json!([point.longitude(), point.latitude()]))
                .collect();
            let id = cell.to_string();
            features.push(json!({
                "type": "Feature",
                "id": id,
                "properties": { "cell": id, "count": count },
                "geometry": { "type": "Polygon", "coordinates": [ring] },
            }));
        }
        serde_json::to_string(&json!({
            "type": "FeatureCollection",
            "estimatedCount": estimated.to_string(),
            "features": features,
        }))
        .map_err(|error| dataset_error("encode A5 GeoJSON", error))
    }

    /// Select exact FlatGeobuf features and encode a bounded GeoJSON collection.
    #[wasm_bindgen(js_name = queryFeatures)]
    pub fn query_features(&mut self, bbox: Vec<f64>, limit: u32) -> Result<String, JsValue> {
        let bbox = parse_bbox(bbox)?;
        if self.bands.is_none() {
            return Err(JsValue::from_str("dataset is not open"));
        }
        if limit > MAX_FEATURES {
            return Err(JsValue::from_str("feature limit exceeds 50000"));
        }
        if limit == 0 {
            return Ok("{\"type\":\"FeatureCollection\",\"features\":[]}".to_string());
        }
        let mut reader = Reader::open(self.source.clone())
            .map_err(|error| dataset_error("reopen FGBA", error))?;
        let body_len = reader.body_len();
        let body = reader
            .body()
            .map_err(|error| dataset_error("seek to FlatGeobuf body", error))?;
        let stream = prefetch_selected_features(body, bbox, limit, body_len)?;
        let fgb = FgbReader::open(stream)
            .map_err(|error| dataset_error("open selected FlatGeobuf features", error))?;
        let mut selected = fgb
            .select_all()
            .map_err(|error| dataset_error("read selected FlatGeobuf features", error))?;

        let mut bytes = Vec::new();
        {
            let mut writer = GeoJsonWriter::new(&mut bytes);
            writer
                .dataset_begin(None)
                .map_err(|error| dataset_error("begin GeoJSON", error))?;
            let mut index = 0_u64;
            while index < u64::from(limit) {
                let feature = match selected.next() {
                    Ok(Some(feature)) => feature,
                    Ok(None) => break,
                    Err(error) => return Err(dataset_error("read FlatGeobuf feature", error)),
                };
                feature
                    .process(&mut writer, index)
                    .map_err(|error| dataset_error("encode FlatGeobuf feature", error))?;
                index += 1;
            }
            writer
                .dataset_end()
                .map_err(|error| dataset_error("finish GeoJSON", error))?;
        }
        String::from_utf8(bytes).map_err(|error| dataset_error("GeoJSON is not UTF-8", error))
    }

    /// Drop all metadata and cached bytes. Further operations are rejected.
    pub fn close(&mut self) {
        self.bands = None;
        self.closed = true;
        self.source.clear();
    }
}
