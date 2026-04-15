use crate::catalog::{Column, Table};
use crate::types::Value;

use super::super::{Lsn, WalOp};
use super::WAL;

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

/// Test-only fault injection: when set, the next `append_op` returns
/// Err without touching the WAL writer. Used to exercise the error
/// path of DDL callers — verifies that a WAL-write failure aborts the
/// whole DDL op so catalog/storage mutations do not happen. Callers
/// set this flag, run the DDL, and then assert the mutation did not
/// land.
#[cfg(test)]
pub(crate) static FAIL_NEXT_APPEND: AtomicBool = AtomicBool::new(false);

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

/// Append a CreateTable entry. The full table definition is serialized
/// so recovery can recreate catalog + storage state without re-parsing SQL.
pub fn append_create_table(table: &Table) -> Result<Option<Lsn>, String> {
    append_op(WalOp::CreateTable { table: table.clone() })
}

/// Append a DropTable entry.
pub fn append_drop_table(schema: &str, table: &str) -> Result<Option<Lsn>, String> {
    append_op(WalOp::DropTable {
        schema: schema.to_string(),
        table: table.to_string(),
    })
}

/// Append an ALTER TABLE ADD COLUMN entry. fill_value is the resolved
/// default used to backfill existing rows at ALTER time.
pub fn append_alter_add_column(schema: &str, table: &str, column: &Column, fill_value: &Value) -> Result<Option<Lsn>, String> {
    append_op(WalOp::AlterAddColumn {
        schema: schema.to_string(),
        table: table.to_string(),
        column: column.clone(),
        fill_value: fill_value.clone(),
    })
}

pub fn append_alter_drop_column(schema: &str, table: &str, column: &str) -> Result<Option<Lsn>, String> {
    append_op(WalOp::AlterDropColumn {
        schema: schema.to_string(),
        table: table.to_string(),
        column: column.to_string(),
    })
}

pub fn append_rename_table(schema: &str, old_name: &str, new_name: &str) -> Result<Option<Lsn>, String> {
    append_op(WalOp::RenameTable {
        schema: schema.to_string(),
        old_name: old_name.to_string(),
        new_name: new_name.to_string(),
    })
}

pub fn append_rename_column(schema: &str, table: &str, old_column: &str, new_column: &str) -> Result<Option<Lsn>, String> {
    append_op(WalOp::RenameColumn {
        schema: schema.to_string(),
        table: table.to_string(),
        old_column: old_column.to_string(),
        new_column: new_column.to_string(),
    })
}

/// Shared append + fsync path. Returns the assigned LSN on success,
/// or None if the WAL is disabled (making this a no-op for callers).
///
/// Goes through `append_sync` rather than the separated `append` +
/// `flush_sync` pair so the two steps happen under a single
/// acquisition of the writer's inner mutex. Without that, a
/// concurrent caller's failing flush could roll back to `durable_len`
/// and silently truncate this caller's already-appended frame,
/// violating the durability contract even though this caller saw
/// `Ok(...)`.
fn append_op(op: WalOp) -> Result<Option<Lsn>, String> {
    #[cfg(test)]
    if FAIL_NEXT_APPEND.swap(false, Ordering::SeqCst) {
        return Err("test injected WAL failure".into());
    }
    let guard = WAL.read();
    let Some(writer) = guard.as_ref() else { return Ok(None); };
    let entry = writer.append_sync(op)?;
    Ok(Some(entry.lsn))
}
