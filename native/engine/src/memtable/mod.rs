//! In-memory buffer for pending writes before segment flush.
//!
//! The memtable holds rows that have been written to the WAL but not
//! yet persisted to an immutable segment file. It serves three roles
//! in the persistence architecture:
//!
//! 1. **Write absorption**: inserts go here after WAL append, so the
//!    write path doesn't touch on-disk segment files on every mutation.
//! 2. **Read visibility**: reads merge memtable rows with segment rows
//!    so recent writes are immediately queryable (read-your-writes).
//! 3. **Flush source**: when the memtable reaches a size threshold, its
//!    contents are serialized to a new segment file (PR 4 wires this).
//!
//! Per-table memtables are held by the storage layer. Each memtable is
//! a simple ordered vector of rows; no hash indexes yet. Point lookups
//! still scan linearly within the memtable, which is fine for small
//! sizes (a few MB). Larger memtables would benefit from a skiplist or
//! b-tree, but that optimization is deferred until measurement shows
//! the linear scan is a bottleneck.
//!
//! Mutability model:
//! - `insert`/`update_at`/`delete_at` append or mutate in place
//! - `freeze` returns an immutable snapshot for flushing (PR 4)
//! - Flushed memtables are replaced with a fresh empty one
//!
//! Tombstones: deletes are recorded as a marker on the affected row so
//! reads can still skip them. Segment flush drops tombstoned rows
//! (PR 4).

mod table;
mod bytes;

#[cfg(test)]
mod tests;

pub use table::{Memtable, MemtableStats};
