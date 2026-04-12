//! Startup recovery: replay WAL entries into the in-memory storage.
//!
//! Called once on process startup after the WAL is enabled but before
//! any user-facing query. Reads every entry up to the durable frontier
//! (corrupt or torn tail stops the read) and re-applies Insert ops to
//! the storage layer.
//!
//! Recovery assumptions:
//! - The catalog has already been populated with the tables referenced
//!   in the WAL. For now, schema is not logged, so CREATE TABLE must
//!   happen before recovery runs. PR 7 will log DDL.
//! - Storage has been reset (empty tables). Applying WAL entries
//!   rebuilds the row state.
//! - Entries whose target table does not exist are skipped with a
//!   warning, not a fatal error. This lets a dropped table from a
//!   previous run not block recovery of the rest.

use crate::catalog;
use crate::storage;

use super::{manager, WalEntry, WalOp};

/// Replay all durable WAL entries into storage. Returns the count of
/// entries successfully applied. Requires the WAL to be enabled.
pub fn recover() -> Result<usize, String> {
    if !manager::is_enabled() {
        return Err("recover: WAL not enabled".into());
    }
    let entries = manager::read_all()?;
    apply_entries(&entries)
}

/// Apply a list of entries to storage. Split out for testability.
pub(crate) fn apply_entries(entries: &[WalEntry]) -> Result<usize, String> {
    let mut applied = 0;
    for entry in entries {
        match &entry.op {
            WalOp::Insert { schema, table, row } => {
                // Skip if the target table doesn't exist (e.g., dropped
                // in a later WAL entry we don't yet support, or the
                // catalog hasn't been rehydrated for it).
                if catalog::get_table(schema, table).is_none() {
                    continue;
                }
                // Use the unchecked insert path: the row already passed
                // constraint checks when it was originally appended to
                // the WAL. Re-validating here would fail on legitimate
                // cases like circular unique-index dependencies.
                storage::insert(schema, table, row.clone())?;
                applied += 1;
            }
            // Update, Delete, Commit, Checkpoint: deferred to later PRs.
            // For now, treat them as no-ops so unknown entry types don't
            // fail recovery.
            _ => {}
        }
    }
    Ok(applied)
}
