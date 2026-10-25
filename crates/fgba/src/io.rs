//! Shared, instrumented `Read + Seek` adapters.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::{Error, Result};

/// Copy exactly `len` bytes with `std::io::copy`'s bounded buffer.
pub(crate) fn copy_exact(
    source: &mut dyn Read,
    len: u64,
    destination: &mut dyn Write,
    what: &str,
) -> Result<u64> {
    let copied = std::io::copy(&mut source.take(len), destination)?;
    if copied != len {
        return Err(Error::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("copied {copied} {what} bytes, expected exactly {len}"),
        )));
    }
    Ok(copied)
}

/// A cloneable byte-source handle sharing one cursor and byte counters.
///
/// Cloning creates another handle to the same cursor; it does not create an
/// independent reader. Each execution should therefore own its source and
/// adapter rather than reading one handle concurrently.
///
/// [`SharedReader::new_at`] is intended for FlatGeobuf chunk reads. It changes
/// only the first `SeekFrom::Current` (the index skip in `FgbReader::select_all`)
/// into an absolute seek, before the iterator consumes its first length prefix.
pub struct SharedReader<R> {
    inner: Arc<Mutex<R>>,
    position: Arc<AtomicU64>,
    bytes_read: Arc<AtomicU64>,
    start_at: Arc<AtomicU64>,
}

impl<R> SharedReader<R> {
    /// Wrap a source initially positioned at byte zero.
    pub fn new(inner: R) -> Self {
        Self::with_start(inner, u64::MAX)
    }

    /// Wrap a source and override its initial index-skip seek with `start_at`.
    pub fn new_at(inner: R, start_at: u64) -> Self {
        Self::with_start(inner, start_at)
    }

    fn with_start(inner: R, start_at: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
            position: Arc::new(AtomicU64::new(0)),
            bytes_read: Arc::new(AtomicU64::new(0)),
            start_at: Arc::new(AtomicU64::new(start_at)),
        }
    }

    /// Return another handle to this shared cursor and its counters.
    pub fn handle(&self) -> Self {
        self.clone()
    }

    /// Current absolute cursor position.
    pub fn position(&self) -> u64 {
        self.position.load(Ordering::Relaxed)
    }

    /// Total bytes returned by successful reads through all shared handles.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    fn lock(&self) -> io::Result<MutexGuard<'_, R>> {
        self.inner
            .lock()
            .map_err(|_| io::Error::other("shared reader mutex is poisoned"))
    }
}

impl<R> Clone for SharedReader<R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            position: Arc::clone(&self.position),
            bytes_read: Arc::clone(&self.bytes_read),
            start_at: Arc::clone(&self.start_at),
        }
    }
}

impl<R: Read> Read for SharedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let count = self.lock()?.read(buf)?;
        self.position.fetch_add(count as u64, Ordering::Relaxed);
        self.bytes_read.fetch_add(count as u64, Ordering::Relaxed);
        Ok(count)
    }
}

impl<R: Seek> Seek for SharedReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let pos = if matches!(pos, SeekFrom::Current(_)) {
            match self.start_at.swap(u64::MAX, Ordering::Relaxed) {
                u64::MAX => pos,
                start_at => SeekFrom::Start(start_at),
            }
        } else {
            pos
        };
        let position = self.lock()?.seek(pos)?;
        self.position.store(position, Ordering::Relaxed);
        Ok(position)
    }
}
