//! FGBA trailer container format: footer, directory, and CRC-32 (SPEC §4–§7).
//!
//! All integers are little-endian and all offsets are absolute file offsets.
//! Everything decoded here is hostile input: every count and length is
//! checked against the actual byte slice before any allocation.

use crate::error::{Error, Result};

/// Footer magic, bytes 0–8 of the footer: `FGBAGG01` (SPEC §7). The last two
/// bytes are the format version.
pub const MAGIC: [u8; 8] = *b"FGBAGG01";

/// Fixed 4-byte trailer sentinel (`FF FF FF FF`), written immediately after
/// the FlatGeobuf body (SPEC §4.1).
pub const SENTINEL: [u8; 4] = [0xFF; 4];

/// Fixed footer size in bytes (SPEC §7).
pub const FOOTER_SIZE: usize = 32;

/// Directory encoding version written and read by this crate (SPEC §5).
pub const DIRECTORY_VERSION: u8 = 1;

/// Recommended upper bound on the directory size, enforced before the
/// directory bytes are read or allocated (SPEC §12).
pub const MAX_DIR_SIZE: usize = 1 << 20; // 1 MiB

/// Section kind: an aggregate band section (SPEC §6).
pub const KIND_AGGREGATE_BAND: u16 = 1;

/// Section kind: optional body-wide analytical columns and statistics.
pub const KIND_BODY_ANALYTICS: u16 = 2;

/// Section kind: optional derived geometry metrics on the kind-2 chunk grid.
pub const KIND_GEOMETRY_METRICS: u16 = 3;

/// The fixed 32-byte footer at end of file (SPEC §7).
///
/// Layout: magic (8) | directory offset (u64) | directory size (u64) |
/// CRC-32/IEEE of the directory bytes (u32) | reserved zeros (4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    /// Absolute byte offset of the directory.
    pub dir_offset: u64,
    /// Byte size of the directory.
    pub dir_size: u64,
    /// CRC-32 (IEEE 802.3, poly `0xEDB88320`) of the directory bytes.
    pub dir_crc32: u32,
}

impl Footer {
    /// Encode as the fixed 32-byte footer, with the reserved field zeroed.
    pub fn encode(&self) -> [u8; FOOTER_SIZE] {
        let mut out = [0u8; FOOTER_SIZE];
        out[0..8].copy_from_slice(&MAGIC);
        out[8..16].copy_from_slice(&self.dir_offset.to_le_bytes());
        out[16..24].copy_from_slice(&self.dir_size.to_le_bytes());
        out[24..28].copy_from_slice(&self.dir_crc32.to_le_bytes());
        // bytes 28..32 stay zero (reserved)
        out
    }

    /// Decode a footer from exactly the last [`FOOTER_SIZE`] bytes of a file.
    ///
    /// A magic mismatch returns [`Error::NotFgba`] (the file is plain
    /// FlatGeobuf); a short slice returns [`Error::Format`].
    pub fn decode(bytes: &[u8]) -> Result<Footer> {
        if bytes.len() < FOOTER_SIZE {
            return Err(Error::Format(format!(
                "footer truncated: {} bytes, need {FOOTER_SIZE}",
                bytes.len()
            )));
        }
        if bytes[0..8] != MAGIC {
            return Err(Error::NotFgba);
        }
        let read_u64 = |at: usize| -> Result<u64> {
            let slice = bytes
                .get(at..at + 8)
                .ok_or_else(|| Error::Format("footer truncated while reading u64".to_string()))?;
            // 8 bytes always fit a u64 array.
            let mut arr = [0u8; 8];
            arr.copy_from_slice(slice);
            Ok(u64::from_le_bytes(arr))
        };
        let mut crc_bytes = [0u8; 4];
        let crc_slice = bytes
            .get(24..28)
            .ok_or_else(|| Error::Format("footer truncated at CRC field".to_string()))?;
        crc_bytes.copy_from_slice(crc_slice);
        Ok(Footer {
            dir_offset: read_u64(8)?,
            dir_size: read_u64(16)?,
            dir_crc32: u32::from_le_bytes(crc_bytes),
        })
    }
}

/// One kind-tagged section entry of the directory (SPEC §5).
///
/// Unknown kinds are preserved verbatim on decode so a file can round-trip
/// sections this crate does not understand; band accessors skip them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionEntry {
    /// Section type; [`KIND_AGGREGATE_BAND`], [`KIND_BODY_ANALYTICS`], and
    /// [`KIND_GEOMETRY_METRICS`] are defined in v1.
    pub kind: u16,
    /// Reserved flags, `0` in v1.
    pub flags: u16,
    /// Absolute byte offset of the section.
    pub offset: u64,
    /// Byte size of the section.
    pub size: u64,
    /// Per-kind metadata (for bands: one byte, the A5 resolution).
    pub meta: Vec<u8>,
}

/// The kind-tagged table of trailer sections (SPEC §5).
///
/// Encoding: `u8 version` | `u8 n_sections`, then per entry
/// `u16 kind` | `u16 flags` | `u64 offset` | `u64 size` | `u16 meta_len` |
/// `u8[meta_len] meta`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Directory {
    /// Section entries, ordered by ascending offset by convention.
    pub sections: Vec<SectionEntry>,
}

impl Directory {
    /// Encode to bytes per SPEC §5.
    ///
    /// Fails if there are more than 255 sections or a `meta` exceeds
    /// `u16::MAX` bytes (the encoding cannot represent them).
    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.sections.len() > u8::MAX as usize {
            return Err(Error::Format(format!(
                "too many directory sections: {} > {}",
                self.sections.len(),
                u8::MAX
            )));
        }
        let mut out = Vec::new();
        out.push(DIRECTORY_VERSION);
        out.push(self.sections.len() as u8);
        for entry in &self.sections {
            if entry.meta.len() > u16::MAX as usize {
                return Err(Error::Format(format!(
                    "section meta too long: {} > {}",
                    entry.meta.len(),
                    u16::MAX
                )));
            }
            out.extend_from_slice(&entry.kind.to_le_bytes());
            out.extend_from_slice(&entry.flags.to_le_bytes());
            out.extend_from_slice(&entry.offset.to_le_bytes());
            out.extend_from_slice(&entry.size.to_le_bytes());
            out.extend_from_slice(&(entry.meta.len() as u16).to_le_bytes());
            out.extend_from_slice(&entry.meta);
        }
        Ok(out)
    }

    /// Decode a directory from its exact bytes (the footer's `dir_size`
    /// bytes), bounded by the input slice. Unknown kinds are preserved.
    ///
    /// Fails on a wrong version byte or any truncated field; no allocation
    /// exceeds the input length.
    pub fn decode(bytes: &[u8]) -> Result<Directory> {
        let truncated = || Error::Format("directory truncated".to_string());
        let version = *bytes.first().ok_or_else(truncated)?;
        if version != DIRECTORY_VERSION {
            return Err(Error::Format(format!(
                "unsupported directory version {version} (this crate reads {DIRECTORY_VERSION})"
            )));
        }
        let n_sections = *bytes.get(1).ok_or_else(truncated)? as usize;
        let mut sections = Vec::with_capacity(n_sections);
        let mut p = 2usize;
        for _ in 0..n_sections {
            let take = |p: &mut usize, n: usize, what: &str| -> Result<&[u8]> {
                let slice = bytes
                    .get(*p..*p + n)
                    .ok_or_else(|| Error::Format(format!("directory truncated at {what}")))?;
                *p += n;
                Ok(slice)
            };
            let kind = take(&mut p, 2, "kind")?;
            let flags = take(&mut p, 2, "flags")?;
            let offset = take(&mut p, 8, "offset")?;
            let size = take(&mut p, 8, "size")?;
            let meta_len = take(&mut p, 2, "meta_len")?;
            let meta_len = u16::from_le_bytes([meta_len[0], meta_len[1]]) as usize;
            let meta = take(&mut p, meta_len, "meta")?;
            let arr = |b: &[u8]| {
                let mut a = [0u8; 8];
                a.copy_from_slice(b);
                u64::from_le_bytes(a)
            };
            sections.push(SectionEntry {
                kind: u16::from_le_bytes([kind[0], kind[1]]),
                flags: u16::from_le_bytes([flags[0], flags[1]]),
                offset: arr(offset),
                size: arr(size),
                meta: meta.to_vec(),
            });
        }
        if p != bytes.len() {
            return Err(Error::Format(format!(
                "directory has {} trailing bytes after {n_sections} entries",
                bytes.len() - p
            )));
        }
        Ok(Directory { sections })
    }
}

/// CRC-32 (IEEE 802.3, reflected, polynomial `0xEDB88320`, as in PNG/zip),
/// computed over the directory bytes for the footer (SPEC §7).
///
/// Bitwise implementation: directory bytes are capped at [`MAX_DIR_SIZE`]
/// (and are a few dozen bytes in practice), so a table is not worth it.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_ieee_vectors() {
        // Well-known CRC-32/IEEE check values.
        assert_eq!(crc32(b""), 0x0000_0000);
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(
            crc32(b"The quick brown fox jumps over the lazy dog"),
            0x414F_A339
        );
    }

    #[test]
    fn footer_round_trip() {
        let footer = Footer {
            dir_offset: 1720,
            dir_size: 25,
            dir_crc32: 0x41fc_73cb,
        };
        let bytes = footer.encode();
        assert_eq!(&bytes[0..8], b"FGBAGG01");
        assert_eq!(&bytes[28..32], &[0; 4]);
        assert_eq!(Footer::decode(&bytes).ok(), Some(footer));
    }

    #[test]
    fn footer_rejects_wrong_magic_and_short_input() {
        let mut bytes = Footer {
            dir_offset: 0,
            dir_size: 2,
            dir_crc32: 0,
        }
        .encode();
        assert!(matches!(
            Footer::decode(&bytes[..31]),
            Err(Error::Format(_))
        ));
        bytes[7] = b'X';
        assert!(matches!(Footer::decode(&bytes), Err(Error::NotFgba)));
    }

    #[test]
    fn directory_round_trip_with_unknown_kind() {
        let dir = Directory {
            sections: vec![
                SectionEntry {
                    kind: KIND_AGGREGATE_BAND,
                    flags: 0,
                    offset: 1676,
                    size: 44,
                    meta: vec![2],
                },
                SectionEntry {
                    kind: 0x8001, // vendor/private, preserved verbatim
                    flags: 0,
                    offset: 1720,
                    size: 3,
                    meta: vec![0xAA, 0xBB],
                },
            ],
        };
        let bytes = dir.encode().ok().unwrap_or_default();
        let back = Directory::decode(&bytes).ok().unwrap_or_default();
        assert_eq!(back, dir);
    }

    #[test]
    fn directory_decode_bounds_truncation() {
        let dir = Directory {
            sections: vec![SectionEntry {
                kind: 1,
                flags: 0,
                offset: 8,
                size: 44,
                meta: vec![2],
            }],
        };
        let bytes = dir.encode().ok().unwrap_or_default();
        // Every strict prefix must error, never panic.
        for cut in 0..bytes.len() {
            assert!(Directory::decode(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        // Wrong version.
        let mut bad = bytes.clone();
        bad[0] = 9;
        assert!(matches!(Directory::decode(&bad), Err(Error::Format(_))));
        // meta_len inflated beyond the slice must error, not allocate.
        let mut bad = bytes;
        let meta_len_at = 2 + 2 + 2 + 8 + 8;
        bad[meta_len_at] = 0xFF;
        bad[meta_len_at + 1] = 0xFF;
        assert!(Directory::decode(&bad).is_err());
    }
}
