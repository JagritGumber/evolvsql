//! Write-Ahead Log for crash recovery and durability.
//!
//! The WAL is an append-only file of serialized mutations. Every entry
//! includes a CRC32 checksum so torn writes are detectable on recovery.
//! The log is truncated when a memtable flush makes older entries
//! redundant (PR 3+ wires this into storage).
//!
//! Durability contract: a write is durable once its entry is fsynced to
//! the WAL. The writer batches entries into groups for fsync amortization
//! (PR 8 refines this).
//!
//! Frame layout on disk:
//! ```text
//! [frame_len: u32] [lsn: u64] [op_tag: u8] [payload: bytes] [crc32: u32]
//! ```
//! `frame_len` counts bytes after itself (lsn + op_tag + payload + crc).
//! `crc32` covers lsn + op_tag + payload.

mod entry;
mod writer;
mod reader;
pub mod manager;

#[cfg(test)]
mod tests;

pub use entry::{WalEntry, WalOp};
pub use writer::WalWriter;
pub use reader::WalReader;

/// Log Sequence Number: monotonic u64, unique per WAL entry.
pub type Lsn = u64;
