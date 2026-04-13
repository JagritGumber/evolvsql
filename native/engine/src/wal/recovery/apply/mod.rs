use crate::catalog;
use crate::storage;
use crate::types::Value;

use super::super::{WalEntry, WalOp};

mod alter;
mod sequences;

/// Apply a list of entries to storage. Insert uses the unchecked path
/// since constraints were already validated when logged. Update and
/// Delete match rows by content (robust to storage layout changes).
pub fn apply_entries(entries: &[WalEntry]) -> Result<usize, String> {
    let mut applied = 0;
    for entry in entries {
        match &entry.op {
            WalOp::Insert { schema, table, row } => {
                if catalog::get_table(schema, table).is_none() { continue; }
                storage::insert(schema, table, row.clone())?;
                applied += 1;
            }
            WalOp::Update { schema, table, old_row, new_row } => {
                if catalog::get_table(schema, table).is_none() { continue; }
                apply_update(schema, table, old_row, new_row)?;
                applied += 1;
            }
            WalOp::Delete { schema, table, old_row } => {
                if catalog::get_table(schema, table).is_none() { continue; }
                apply_delete(schema, table, old_row)?;
                applied += 1;
            }
            WalOp::CreateTable { table } => {
                if catalog::get_table(&table.schema, &table.name).is_some() { continue; }
                catalog::create_table(table.clone())?;
                storage::create_table(&table.schema, &table.name);
                for (i, col) in table.columns.iter().enumerate() {
                    if col.primary_key || col.unique {
                        let _ = storage::add_unique_index(&table.schema, &table.name, i);
                    }
                }
                sequences::recreate_for_table(table);
                applied += 1;
            }
            WalOp::DropTable { schema, table } => {
                if catalog::get_table(schema, table).is_some() {
                    catalog::drop_table(schema, table)?;
                    storage::drop_table(schema, table);
                }
                applied += 1;
            }
            op @ (WalOp::AlterAddColumn { .. }
                | WalOp::AlterDropColumn { .. }
                | WalOp::RenameTable { .. }
                | WalOp::RenameColumn { .. }) => {
                if alter::apply_alter(op)? { applied += 1; }
            }
            _ => {} // Commit, Checkpoint: no-op for now
        }
    }
    // After replay, walk every catalog table and advance any SERIAL
    // sequence past the max value present in storage. This handles
    // both inserts that used nextval and inserts that supplied an
    // explicit higher id.
    sequences::advance_all_to_max();
    Ok(applied)
}

fn apply_update(schema: &str, table: &str, old_row: &[Value], new_row: &[Value]) -> Result<(), String> {
    let old = old_row.to_vec();
    let new = new_row.to_vec();
    let mut matched = false;
    storage::update_rows_checked(
        schema, table,
        |row| { if !matched && row == old.as_slice() { matched = true; true } else { false } },
        |_| Ok(new.clone()),
        |_, _, _| Ok(()),
    )?;
    Ok(())
}

fn apply_delete(schema: &str, table: &str, old_row: &[Value]) -> Result<(), String> {
    let old = old_row.to_vec();
    let mut matched = false;
    storage::delete_where(schema, table, |row| {
        if !matched && row == old.as_slice() { matched = true; true } else { false }
    })?;
    Ok(())
}
