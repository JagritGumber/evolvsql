//! Replay handlers for ALTER TABLE ops. Split out of apply.rs so the
//! main dispatch stays under the 100-line limit.
//!
//! Every handler is idempotent: if the target state already exists
//! (column already present, table already renamed, column already
//! renamed, column already dropped) the handler silently skips
//! instead of erroring. This matches the CreateTable/DropTable
//! recovery pattern and is load-bearing for the WAL-first DDL
//! ordering: a DDL op that writes its WAL entry and then fails
//! validation in the live path leaves a phantom entry behind. If
//! recovery replayed that entry into a state where the op is already
//! satisfied it would error and abort the entire replay, bricking
//! the engine on the next restart. Idempotent skip keeps the engine
//! bootable at the cost of tolerating one phantom entry per failed
//! DDL.

use crate::catalog;
use crate::storage;

use super::super::super::WalOp;

/// Apply a single ALTER op. Returns Ok(true) if applied, Ok(false) if
/// skipped (table missing from a partial replay, or target state
/// already reached), Err on unrecoverable catalog errors.
pub(super) fn apply_alter(op: &WalOp) -> Result<bool, String> {
    match op {
        WalOp::AlterAddColumn { schema, table, column, fill_value } => {
            if catalog::get_table(schema, table).is_none() { return Ok(false); }
            if catalog::get_column_index(schema, table, &column.name).is_ok() {
                return Ok(false);
            }
            catalog::alter_table_add_column(schema, table, column.clone())?;
            storage::alter_add_column(schema, table, fill_value.clone());
            Ok(true)
        }
        WalOp::AlterDropColumn { schema, table, column } => {
            if catalog::get_table(schema, table).is_none() { return Ok(false); }
            let Ok(idx) = catalog::get_column_index(schema, table, column) else {
                return Ok(false);
            };
            catalog::alter_table_drop_column(schema, table, column)?;
            storage::alter_drop_column(schema, table, idx);
            Ok(true)
        }
        WalOp::RenameTable { schema, old_name, new_name } => {
            if catalog::get_table(schema, old_name).is_none() { return Ok(false); }
            if catalog::get_table(schema, new_name).is_some() { return Ok(false); }
            catalog::rename_table(schema, old_name, new_name)?;
            storage::rename_table(schema, old_name, new_name);
            Ok(true)
        }
        WalOp::RenameColumn { schema, table, old_column, new_column } => {
            if catalog::get_table(schema, table).is_none() { return Ok(false); }
            if catalog::get_column_index(schema, table, old_column).is_err() {
                return Ok(false);
            }
            if catalog::get_column_index(schema, table, new_column).is_ok() {
                return Ok(false);
            }
            catalog::rename_column(schema, table, old_column, new_column)?;
            Ok(true)
        }
        _ => Ok(false),
    }
}
