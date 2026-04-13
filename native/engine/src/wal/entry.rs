use serde::{Deserialize, Serialize};

use crate::types::Value;
use super::Lsn;

/// A single WAL entry: a log sequence number + the operation it records.
#[derive(Debug, Clone, PartialEq)]
pub struct WalEntry {
    pub lsn: Lsn,
    pub op: WalOp,
}

/// The operation recorded by a WAL entry.
///
/// Mutations record the full OLD and NEW row values rather than a
/// physical row index. This makes recovery robust to storage layout
/// changes: replay finds rows by content match, not by offset. It also
/// avoids tying the WAL format to a specific storage data structure.
///
/// Duplicate-row ambiguity: if multiple rows have identical values,
/// UPDATE/DELETE will affect the first match. This matches PostgreSQL's
/// semantics for tables without a unique key (the "any matching row"
/// contract).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WalOp {
    Insert { schema: String, table: String, row: Vec<Value> },
    Update { schema: String, table: String, old_row: Vec<Value>, new_row: Vec<Value> },
    Delete { schema: String, table: String, old_row: Vec<Value> },
    /// Marks a transaction boundary for group commit.
    Commit { txn_id: u64 },
    /// Marks the LSN up to which a memtable flush has persisted data.
    /// Entries with LSN <= this are safe to truncate.
    Checkpoint { up_to: Lsn },
}

// Op tag byte used in the on-disk frame. Kept stable: never renumber.
pub(super) const OP_INSERT: u8 = 1;
pub(super) const OP_UPDATE: u8 = 2;
pub(super) const OP_DELETE: u8 = 3;
pub(super) const OP_COMMIT: u8 = 4;
pub(super) const OP_CHECKPOINT: u8 = 5;

impl WalOp {
    pub(super) fn tag(&self) -> u8 {
        match self {
            WalOp::Insert { .. } => OP_INSERT,
            WalOp::Update { .. } => OP_UPDATE,
            WalOp::Delete { .. } => OP_DELETE,
            WalOp::Commit { .. } => OP_COMMIT,
            WalOp::Checkpoint { .. } => OP_CHECKPOINT,
        }
    }

    /// Encode the operation payload using bincode.
    pub(super) fn encode_payload(&self) -> Result<Vec<u8>, String> {
        bincode::serialize(self).map_err(|e| format!("WAL encode: {}", e))
    }

    /// Decode an operation from its tag and payload bytes.
    pub(super) fn decode(_tag: u8, payload: &[u8]) -> Result<WalOp, String> {
        bincode::deserialize(payload).map_err(|e| format!("WAL decode: {}", e))
    }
}
