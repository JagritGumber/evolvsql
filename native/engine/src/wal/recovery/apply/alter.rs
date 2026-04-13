//! Replay handlers for ALTER TABLE ops. Split out of apply.rs so the
//! main dispatch stays under the 100-line limit.

use crate::catalog;
use crate::storage;

use super::super::super::WalOp;

/// Apply a single ALTER op. Returns Ok(true) if applied, Ok(false) if
/// skipped (e.g., table missing from a partial replay), Err on
/// unrecoverable catalog errors.
pub(super) fn apply_alter(op: &WalOp) -> Result<bool, String> {
    match op {
        WalOp::AlterAddColumn { schema, table, column, fill_value } => {
            if catalog::get_table(schema, table).is_none() { return Ok(false); }
            catalog::alter_table_add_column(schema, table, column.clone())?;
            storage::alter_add_column(schema, table, fill_value.clone());
            Ok(true)
        }
        WalOp::AlterDropColumn { schema, table, column } => {
            if catalog::get_table(schema, table).is_none() { return Ok(false); }
            let idx = catalog::get_column_index(schema, table, column)?;
            catalog::alter_table_drop_column(schema, table, column)?;
            storage::alter_drop_column(schema, table, idx);
            Ok(true)
        }
        WalOp::RenameTable { schema, old_name, new_name } => {
            if catalog::get_table(schema, old_name).is_none() { return Ok(false); }
            catalog::rename_table(schema, old_name, new_name)?;
            storage::rename_table(schema, old_name, new_name);
            Ok(true)
        }
        WalOp::RenameColumn { schema, table, old_column, new_column } => {
            if catalog::get_table(schema, table).is_none() { return Ok(false); }
            catalog::rename_column(schema, table, old_column, new_column)?;
            Ok(true)
        }
        _ => Ok(false),
    }
}
