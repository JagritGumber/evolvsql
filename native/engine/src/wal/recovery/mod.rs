//! Startup recovery: replay WAL entries into the in-memory storage.
//!
//! Called once on process startup after the WAL is enabled but before
//! any user-facing query. Reads every entry up to the durable frontier
//! (corrupt or torn tail stops the read) and re-applies Insert, Update,
//! and Delete ops to the storage layer.
//!
//! Recovery assumptions:
//! - The catalog has already been populated with the tables referenced
//!   in the WAL. Schema DDL is not logged yet; CREATE TABLE must happen
//!   before recovery runs. PR 7 will log DDL.
//! - Storage has been reset (empty tables). Applying WAL entries
//!   rebuilds the row state.
//! - Entries whose target table does not exist are skipped, not errored.

mod apply;

use super::manager;

pub use apply::apply_entries;

/// Replay all durable WAL entries into storage. Returns the count of
/// entries successfully applied. Requires the WAL to be enabled.
///
/// The WAL writer is temporarily suspended during replay so storage
/// mutation calls don't re-append the replayed entries. After replay
/// the writer is re-opened with its LSN counter at max_replayed + 1
/// so new writes don't collide with existing entries on disk.
pub fn recover() -> Result<usize, String> {
    if !manager::is_enabled() {
        return Err("recover: WAL not enabled".into());
    }
    let entries = manager::read_all()?;
    let max_lsn = entries.iter().map(|e| e.lsn).max().unwrap_or(0);

    let resume_path = manager::suspend_for_replay();
    let result = apply_entries(&entries);

    if let Some(path) = resume_path {
        manager::enable_at_lsn(&path, max_lsn + 1)?;
    }
    result
}
