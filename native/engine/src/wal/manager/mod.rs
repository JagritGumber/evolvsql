//! Global WAL manager: lifecycle + write hooks for the storage layer.
//!
//! Storage mutation paths call `append_insert`, `append_update`, and
//! `append_delete` before committing. If no WAL is configured, the
//! calls are no-ops and the write path is unchanged.
//!
//! Configuration via env vars:
//! - `EVOLVSQL_WAL_ENABLED=1` turns WAL on
//! - `EVOLVSQL_WAL_PATH=/path/to/wal` sets the log file location
//!   (default: `./evolvsql.wal`)

mod lifecycle;
mod ops;

pub use lifecycle::{enable, enable_at_lsn, enable_from_env, disable, is_enabled, read_all, suspend_for_replay};
pub use ops::{
    append_insert, append_update, append_delete,
    append_create_table, append_drop_table,
    append_alter_add_column, append_alter_drop_column,
    append_rename_table, append_rename_column,
};

use std::sync::Arc;
use parking_lot::RwLock;
use super::WalWriter;

/// Global WAL writer singleton. Shared across the recovery module via
/// the public lifecycle/ops functions.
pub(super) static WAL: RwLock<Option<Arc<WalWriter>>> = RwLock::new(None);
