use crate::types::Value;
use crate::wal::Lsn;

use super::bytes::estimate_row_bytes;

/// An in-memory row with its WAL LSN and tombstone flag. Tombstoned
/// rows stay in the table until a segment flush drops them.
#[derive(Debug, Clone)]
pub struct MemRow {
    pub lsn: Lsn,
    pub values: Vec<Value>,
    pub tombstone: bool,
}

/// In-memory buffer for pending writes. Rows are stored in insertion
/// order; linear scan for point lookups is fine at memtable sizes.
#[derive(Debug, Default)]
pub struct Memtable {
    rows: Vec<MemRow>,
    /// Approximate byte size for flush threshold decisions.
    bytes: usize,
}

/// Snapshot of memtable metrics for flush decisions and observability.
#[derive(Debug, Clone, Copy)]
pub struct MemtableStats {
    pub row_count: usize,
    pub live_row_count: usize,
    pub bytes: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a row at the given LSN. Returns the row's position
    /// within the memtable (stable until freeze/drain).
    pub fn insert(&mut self, lsn: Lsn, values: Vec<Value>) -> usize {
        let idx = self.rows.len();
        self.bytes += estimate_row_bytes(&values);
        self.rows.push(MemRow { lsn, values, tombstone: false });
        idx
    }

    /// Mark a row as deleted. Storage stays allocated until flush.
    pub fn delete_at(&mut self, idx: usize, lsn: Lsn) -> Result<(), String> {
        let row = self.rows.get_mut(idx)
            .ok_or_else(|| format!("memtable: delete index {} out of range", idx))?;
        row.tombstone = true;
        row.lsn = lsn;
        Ok(())
    }

    /// Replace a row's values in place at the given LSN. The row must
    /// not already be tombstoned.
    pub fn update_at(&mut self, idx: usize, lsn: Lsn, new_values: Vec<Value>) -> Result<(), String> {
        let row = self.rows.get_mut(idx)
            .ok_or_else(|| format!("memtable: update index {} out of range", idx))?;
        if row.tombstone {
            return Err(format!("memtable: cannot update tombstoned row {}", idx));
        }
        self.bytes = self.bytes
            .saturating_sub(estimate_row_bytes(&row.values))
            .saturating_add(estimate_row_bytes(&new_values));
        row.values = new_values;
        row.lsn = lsn;
        Ok(())
    }

    /// Iterate live (non-tombstoned) rows in insertion order.
    pub fn scan(&self) -> impl Iterator<Item = (usize, &[Value])> {
        self.rows.iter().enumerate()
            .filter(|(_, r)| !r.tombstone)
            .map(|(i, r)| (i, r.values.as_slice()))
    }

    /// Drain all rows and reset the memtable. Used by flush after the
    /// caller has successfully written a segment file. Returns only
    /// live rows; tombstones are dropped.
    pub fn drain_live(&mut self) -> Vec<Vec<Value>> {
        let drained: Vec<Vec<Value>> = self.rows.drain(..)
            .filter(|r| !r.tombstone)
            .map(|r| r.values)
            .collect();
        self.bytes = 0;
        drained
    }

    pub fn stats(&self) -> MemtableStats {
        let live = self.rows.iter().filter(|r| !r.tombstone).count();
        MemtableStats {
            row_count: self.rows.len(),
            live_row_count: live,
            bytes: self.bytes,
        }
    }
}

