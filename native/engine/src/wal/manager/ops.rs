use crate::types::Value;

use super::super::{Lsn, WalOp};
use super::WAL;

/// Append an Insert entry. No-op when WAL is disabled. Fsyncs before
/// returning so the write is durable.
pub fn append_insert(schema: &str, table: &str, row: &[Value]) -> Result<Option<Lsn>, String> {
    append_op(WalOp::Insert {
        schema: schema.to_string(),
        table: table.to_string(),
        row: row.to_vec(),
    })
}

/// Append an Update entry. `old_row` identifies the row being replaced;
/// recovery finds the matching row by content.
pub fn append_update(schema: &str, table: &str, old_row: &[Value], new_row: &[Value]) -> Result<Option<Lsn>, String> {
    append_op(WalOp::Update {
        schema: schema.to_string(),
        table: table.to_string(),
        old_row: old_row.to_vec(),
        new_row: new_row.to_vec(),
    })
}

/// Append a Delete entry keyed by the full row values.
pub fn append_delete(schema: &str, table: &str, old_row: &[Value]) -> Result<Option<Lsn>, String> {
    append_op(WalOp::Delete {
        schema: schema.to_string(),
        table: table.to_string(),
        old_row: old_row.to_vec(),
    })
}

/// Shared append + fsync path. Returns the assigned LSN on success,
/// or None if the WAL is disabled (making this a no-op for callers).
fn append_op(op: WalOp) -> Result<Option<Lsn>, String> {
    let guard = WAL.read();
    let Some(writer) = guard.as_ref() else { return Ok(None); };
    let lsn = writer.append(op)?;
    writer.flush_sync()?;
    Ok(Some(lsn))
}
