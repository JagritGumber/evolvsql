use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::super::{Lsn, WalEntry, WalReader, WalWriter};
use super::WAL;

/// Enable the WAL with an explicit path. Idempotent: if called twice
/// with the same path, the second call is a no-op.
pub fn enable<P: AsRef<Path>>(path: P) -> Result<(), String> {
    enable_at_lsn(path, 1)
}

/// Enable the WAL with an explicit path and starting LSN. Used by
/// recovery to resume the LSN counter past the existing entries.
pub fn enable_at_lsn<P: AsRef<Path>>(path: P, starting_lsn: Lsn) -> Result<(), String> {
    let writer = WalWriter::open(path, starting_lsn)?;
    *WAL.write() = Some(Arc::new(writer));
    Ok(())
}

/// Enable the WAL from environment variables. Returns Ok(true) if
/// enabled, Ok(false) if disabled via env, Err on open failure.
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
pub fn disable() {
    *WAL.write() = None;
}

/// True if the WAL is currently enabled.
pub fn is_enabled() -> bool {
    WAL.read().is_some()
}

/// Suspend the WAL writer for the duration of recovery replay.
/// Returns the previously-configured path so the caller can restore
/// it after replay completes.
pub fn suspend_for_replay() -> Option<PathBuf> {
    let mut guard = WAL.write();
    let path = guard.as_ref().map(|w| w.path().to_path_buf());
    *guard = None;
    path
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
