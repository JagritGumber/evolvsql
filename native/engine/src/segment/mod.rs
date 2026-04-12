//! Immutable segment files: columnar storage with zone maps.
//!
//! A segment is a single file holding a batch of rows in columnar
//! layout. Segments are the on-disk equivalent of the memtable and are
//! written once, then never mutated. Old segments are superseded by
//! compaction (PR 7), not by in-place update.
//!
//! File layout:
//! ```text
//! [header: 32 bytes]
//!   magic("EVSG":4) version(u16) _res(u16) row_count(u32)
//!   col_count(u16) _res(u16) footer_offset(u64) _res(u64)
//! [column data blocks]  // each: bincoded Vec<Value> for that column
//! [footer]              // bincoded SegmentFooter with column metadata
//! ```
//!
//! Writer transposes rows into columns, encodes each column
//! independently, records the (offset, length, min, max) per column,
//! then writes the footer. Reader parses the header, seeks to the
//! footer, and can then read any individual column without loading
//! the others.
//!
//! Scalar columns only in this PR. Vector columns and per-segment
//! HNSW indexes come in PRs 4 and 5.

mod format;
mod writer;
mod reader;
mod zone_map;

#[cfg(test)]
mod tests;

pub use format::{ColumnMeta, SegmentFooter, MAGIC, VERSION};
pub use writer::SegmentWriter;
pub use reader::SegmentReader;
