//! A bounded, seekable sparse byte cache.
//!
//! Reads never perform I/O. An uncached read returns [`WouldBlock`] with the
//! exact byte span an async transport must supply before retrying.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::io::{Error, ErrorKind, Read, Result, Seek, SeekFrom};
use std::rc::Rc;

/// Maximum object response size accepted by one range insertion (8 MiB).
pub const MAX_RANGE_BYTES: usize = 8 * 1024 * 1024;
/// Maximum total sparse-cache size (64 MiB).
pub const MAX_CACHE_BYTES: usize = 64 * 1024 * 1024;
/// Cached ranges separated by at most this gap may share one fetch.
pub const COALESCE_GAP_BYTES: u64 = 4096;

/// A typed cache miss. Offset and length remain `u64` until the JS boundary,
/// where they are serialized as decimal strings rather than lossy Numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WouldBlock {
    /// Absolute object offset of the first byte to fetch.
    pub offset: u64,
    /// Number of contiguous bytes to fetch.
    pub len: u64,
}

impl Display for WouldBlock {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "sparse cache miss at byte {} ({} bytes)",
            self.offset, self.len
        )
    }
}

impl StdError for WouldBlock {}

/// Extract a typed sparse miss from an I/O error.
pub fn would_block(error: &Error) -> Option<WouldBlock> {
    error.get_ref()?.downcast_ref::<WouldBlock>().copied()
}

#[derive(Debug)]
struct CachedRange {
    bytes: Vec<u8>,
    last_used: u64,
}

#[derive(Debug, Default)]
struct Cache {
    ranges: BTreeMap<u64, CachedRange>,
    cached_bytes: usize,
    clock: u64,
}

impl Cache {
    fn tick(&mut self) -> u64 {
        self.clock = self.clock.wrapping_add(1);
        self.clock
    }

    fn evict_to_cap(&mut self, max_cache_bytes: usize) {
        while self.cached_bytes > max_cache_bytes {
            let oldest = self
                .ranges
                .iter()
                .min_by_key(|(_, range)| range.last_used)
                .map(|(&start, _)| start);
            match oldest {
                Some(start) => {
                    if let Some(removed) = self.ranges.remove(&start) {
                        self.cached_bytes -= removed.bytes.len();
                    }
                }
                None => break,
            }
        }
    }
}

/// A `Read + Seek` implementation backed by bounded discontiguous ranges.
/// Clones have independent cursors but share the same cache, allowing a
/// consumed `fgba::Reader` operation to be retried after an async fetch.
#[derive(Debug, Clone)]
pub struct SparseRange {
    len: u64,
    position: u64,
    max_range_bytes: usize,
    max_cache_bytes: usize,
    cache: Rc<RefCell<Cache>>,
}

impl SparseRange {
    /// Create an empty sparse source of known object length.
    pub fn new(len: u64, max_range_bytes: usize, max_cache_bytes: usize) -> Result<Self> {
        if max_range_bytes == 0 || max_range_bytes > MAX_RANGE_BYTES {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("max range bytes must be in 1..={MAX_RANGE_BYTES}"),
            ));
        }
        if max_cache_bytes == 0 || max_cache_bytes > MAX_CACHE_BYTES {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("max cache bytes must be in 1..={MAX_CACHE_BYTES}"),
            ));
        }
        Ok(Self {
            len,
            position: 0,
            max_range_bytes,
            max_cache_bytes,
            cache: Rc::new(RefCell::new(Cache::default())),
        })
    }

    /// Current logical cursor position.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Unique cached bytes after overlap normalization.
    pub fn cached_bytes(&self) -> usize {
        self.cache.borrow().cached_bytes
    }

    /// Number of discontiguous cached ranges.
    pub fn cached_ranges(&self) -> usize {
        self.cache.borrow().ranges.len()
    }

    /// Remove every cached byte while retaining the object length and limits.
    pub fn clear(&mut self) {
        *self.cache.borrow_mut() = Cache::default();
        self.position = 0;
    }

    /// Insert one complete fetched range. Overlapping/adjacent cache entries
    /// are normalized so their bytes are accounted only once; newest bytes
    /// replace older bytes in overlaps.
    pub fn insert(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() || bytes.len() > self.max_range_bytes {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("range length must be in 1..={}", self.max_range_bytes),
            ));
        }
        if bytes.len() > self.max_cache_bytes {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "range is larger than the sparse cache",
            ));
        }
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "range end overflows"))?;
        if end > self.len {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "range extends beyond the object",
            ));
        }

        let mut cache = self.cache.borrow_mut();
        let mut merged_start = offset;
        let mut merged_end = end;
        let mut overlapping = Vec::new();
        for (&start, cached) in &cache.ranges {
            let cached_end = start + cached.bytes.len() as u64;
            if start <= merged_end && cached_end >= merged_start {
                merged_start = merged_start.min(start);
                merged_end = merged_end.max(cached_end);
                overlapping.push(start);
            }
        }
        let merged_len = usize::try_from(merged_end - merged_start)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "merged range is too large"))?;
        let mut merged = Vec::new();
        merged
            .try_reserve_exact(merged_len)
            .map_err(|_| Error::new(ErrorKind::OutOfMemory, "cannot allocate merged range"))?;
        merged.resize(merged_len, 0);
        for &start in &overlapping {
            if let Some(cached) = cache.ranges.get(&start) {
                let at = (start - merged_start) as usize;
                merged[at..at + cached.bytes.len()].copy_from_slice(&cached.bytes);
            }
        }
        let new_at = (offset - merged_start) as usize;
        merged[new_at..new_at + bytes.len()].copy_from_slice(bytes);

        for start in overlapping {
            if let Some(removed) = cache.ranges.remove(&start) {
                cache.cached_bytes -= removed.bytes.len();
            }
        }
        let last_used = cache.tick();
        cache.cached_bytes += merged.len();
        cache.ranges.insert(
            merged_start,
            CachedRange {
                bytes: merged,
                last_used,
            },
        );
        cache.evict_to_cap(self.max_cache_bytes);
        Ok(())
    }

    fn miss(&self, wanted: usize) -> Error {
        let remaining = self.len.saturating_sub(self.position);
        let needed = wanted
            .min(self.max_range_bytes)
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let required_end = self.position + needed as u64;
        let mut start = self.position;
        let mut end = required_end;
        let cache = self.cache.borrow();

        // Only gaps between the required span and already-cached ranges are
        // coalesced. There is deliberately no speculative read-ahead, so a
        // small header read can never turn into a whole-object download.
        let mut changed = true;
        while changed {
            changed = false;
            for (&cached_start, cached) in &cache.ranges {
                let cached_end = cached_start + cached.bytes.len() as u64;
                let nearby = cached_start <= end.saturating_add(COALESCE_GAP_BYTES)
                    && start <= cached_end.saturating_add(COALESCE_GAP_BYTES);
                if !nearby {
                    continue;
                }
                let candidate_start = start.min(cached_start).max(self.position);
                let candidate_end = end.max(cached_end);
                if candidate_end - candidate_start > self.max_range_bytes as u64 {
                    continue;
                }
                let whole_object = candidate_start == 0 && candidate_end == self.len;
                let explicitly_requested = self.position == 0 && required_end == self.len;
                if whole_object && !explicitly_requested {
                    continue;
                }
                if candidate_start != start || candidate_end != end {
                    start = candidate_start;
                    end = candidate_end;
                    changed = true;
                }
            }
        }
        Error::new(
            ErrorKind::WouldBlock,
            WouldBlock {
                offset: start,
                len: end - start,
            },
        )
    }
}

impl Read for SparseRange {
    fn read(&mut self, buffer: &mut [u8]) -> Result<usize> {
        if buffer.is_empty() || self.position >= self.len {
            return Ok(0);
        }
        let mut cache = self.cache.borrow_mut();
        let hit = cache
            .ranges
            .range(..=self.position)
            .next_back()
            .and_then(|(&start, cached)| {
                let end = start + cached.bytes.len() as u64;
                (self.position < end).then_some((start, cached.bytes.len()))
            });
        let Some((start, cached_len)) = hit else {
            drop(cache);
            return Err(self.miss(buffer.len()));
        };
        let at = (self.position - start) as usize;
        let wanted = buffer
            .len()
            .min(cached_len - at)
            .min(usize::try_from(self.len - self.position).unwrap_or(usize::MAX));
        let last_used = cache.tick();
        if let Some(cached) = cache.ranges.get_mut(&start) {
            cached.last_used = last_used;
            buffer[..wanted].copy_from_slice(&cached.bytes[at..at + wanted]);
        }
        self.position += wanted as u64;
        Ok(wanted)
    }
}

impl Seek for SparseRange {
    fn seek(&mut self, position: SeekFrom) -> Result<u64> {
        let next = match position {
            SeekFrom::Start(offset) => i128::from(offset),
            SeekFrom::Current(delta) => i128::from(self.position) + i128::from(delta),
            SeekFrom::End(delta) => i128::from(self.len) + i128::from(delta),
        };
        if next < 0 || next > i128::from(u64::MAX) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "seek position is out of range",
            ));
        }
        self.position = next as u64;
        Ok(self.position)
    }
}
