//! Global WAL manager: lifecycle + write hooks for the storage layer.
//!
//! Storage mutation paths call `append_insert` (and eventually
//! `append_update`, `append_delete`) before committing. If no WAL is
//! configured, the calls are no-ops and the write path is unchanged.
//!
//! Configuration via env vars:
//! - `EVOLVSQL_WAL_ENABLED=1` turns WAL on
//! - `EVOLVSQL_WAL_PATH=/path/to/wal` sets the log file location
//!   (default: `./evolvsql.wal`)
//!
//! A single global WalWriter is shared across all tables. This matches
//! PostgreSQL's approach and is simpler than per-table logs.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::types::Value;

use super::{Lsn, WalEntry, WalOp, WalReader, WalWriter};

static WAL: RwLock<Option<Arc<WalWriter>>> = RwLock::new(None);

/// Enable the WAL with an explicit path. Idempotent: if called twice
/// with the same path, the second call is a no-op.
pub fn enable<P: AsRef<std::path::Path>>(path: P) -> Result<(), String> {
    let mut guard = WAL.write();
    if let Some(existing) = guard.as_ref() {
        if existing.path() == path.as_ref() {
            return Ok(());
        }
    }
    let writer = WalWriter::open(path, 1)?;
    *guard = Some(Arc::new(writer));
    Ok(())
}

/// Enable the WAL from environment variables. Called at startup when
/// `EVOLVSQL_WAL_ENABLED=1`. Returns Ok(true) if enabled, Ok(false) if
/// the env var is off, or Err on open failure.
pub fn enable_from_env() -> Result<bool, String> {
    match std::env::var("EVOLVSQL_WAL_ENABLED") {
        Ok(v) if v == "1" => {
            let path = std::env::var("EVOLVSQL_WAL_PATH")
                .unwrap_or_else(|_| "./evolvsql.wal".to_string());
            enable(&path)?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// Disable the WAL and drop the writer. Does not delete the WAL file.
/// Used by tests to reset between cases.
pub fn disable() {
    *WAL.write() = None;
}

/// True if the WAL is currently enabled.
pub fn is_enabled() -> bool {
    WAL.read().is_some()
}

/// Append an Insert entry. No-op if WAL is disabled. Fsyncs before
/// returning so the write is durable.
pub fn append_insert(schema: &str, table: &str, row: &[Value]) -> Result<Option<Lsn>, String> {
    let guard = WAL.read();
    let Some(writer) = guard.as_ref() else { return Ok(None); };
    let op = WalOp::Insert {
        schema: schema.to_string(),
        table: table.to_string(),
        row: row.to_vec(),
    };
    let lsn = writer.append(op)?;
    writer.flush_sync()?;
    Ok(Some(lsn))
}

/// Read all entries from the configured WAL file. Used for recovery.
/// Returns an empty vec if WAL is disabled or the file doesn't exist.
pub fn read_all() -> Result<Vec<WalEntry>, String> {
    let guard = WAL.read();
    let Some(writer) = guard.as_ref() else { return Ok(vec![]); };
    let path = writer.path().to_path_buf();
    drop(guard);
    if !path.exists() {
        return Ok(vec![]);
    }
    WalReader::open(&path)?.read_all()
}
