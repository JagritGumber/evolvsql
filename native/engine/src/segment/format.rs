use serde::{Deserialize, Serialize};

use crate::types::Value;

/// Magic bytes at start of every segment file, and at end of footer.
pub const MAGIC: &[u8; 4] = b"EVSG";
/// Segment format version. Bump on breaking layout changes.
pub const VERSION: u16 = 1;

/// Fixed-size header at byte 0 of every segment file (32 bytes).
pub(super) const HEADER_SIZE: usize = 32;

/// Per-column metadata kept in the footer. `offset` and `length` locate
/// the column's data block in the file; `min` and `max` are the zone
/// map values used for predicate skipping.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub type_oid: i32,
    pub offset: u64,
    pub length: u64,
    /// Smallest non-NULL value in this column, or None if all NULL.
    pub min: Option<Value>,
    /// Largest non-NULL value in this column, or None if all NULL.
    pub max: Option<Value>,
    /// Count of NULL values in this column.
    pub null_count: u32,
}

/// Footer written at the end of the segment file. Located via
/// `footer_offset` in the fixed header.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentFooter {
    pub columns: Vec<ColumnMeta>,
    pub row_count: u32,
}

/// Encode the fixed-size header at byte 0 of the segment file.
pub(super) fn encode_header(row_count: u32, col_count: u16, footer_offset: u64) -> [u8; HEADER_SIZE] {
    let mut buf = [0u8; HEADER_SIZE];
    buf[0..4].copy_from_slice(MAGIC);
    buf[4..6].copy_from_slice(&VERSION.to_le_bytes());
    // 6..8 reserved
    buf[8..12].copy_from_slice(&row_count.to_le_bytes());
    buf[12..14].copy_from_slice(&col_count.to_le_bytes());
    // 14..16 reserved
    buf[16..24].copy_from_slice(&footer_offset.to_le_bytes());
    // 24..32 reserved
    buf
}

/// Decode the fixed-size header. Returns (row_count, col_count, footer_offset).
pub(super) fn decode_header(buf: &[u8; HEADER_SIZE]) -> Result<(u32, u16, u64), String> {
    if &buf[0..4] != MAGIC {
        return Err("segment: bad magic".into());
    }
    let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
    if version != VERSION {
        return Err(format!("segment: unsupported version {}", version));
    }
    let row_count = u32::from_le_bytes(buf[8..12].try_into().unwrap());
    let col_count = u16::from_le_bytes(buf[12..14].try_into().unwrap());
    let footer_offset = u64::from_le_bytes(buf[16..24].try_into().unwrap());
    Ok((row_count, col_count, footer_offset))
}
