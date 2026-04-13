//! Sequence (SERIAL) recovery. Sequences are not WAL-logged on every
//! nextval — that would balloon log volume — so recovery rebuilds
//! them from the catalog and replayed rows.
//!
//! Two phases:
//! 1. recreate_for_table: at CreateTable replay, instantiate the
//!    sequence backing each NextVal default so it exists by the time
//!    inserts start replaying.
//! 2. advance_all_to_max: after every WAL entry has replayed, scan
//!    each SERIAL column's actual values and setval the sequence to
//!    the max. This is robust to user-supplied ids higher than the
//!    sequence ever produced.

use crate::catalog::{self, DefaultExpr, Table};
use crate::sequence;
use crate::storage;
use crate::types::Value;

/// Parse a NextVal default's "schema.name" string into its parts.
fn split_seq_fqn(fqn: &str) -> Option<(&str, &str)> {
    fqn.split_once('.')
}

/// Create any sequences referenced by NextVal defaults on this table.
/// Idempotent: drops a leftover sequence with the same name first so
/// recovery always starts from a clean state.
pub(super) fn recreate_for_table(table: &Table) {
    for col in &table.columns {
        if let Some(DefaultExpr::NextVal(fqn)) = &col.default_expr {
            if let Some((schema, name)) = split_seq_fqn(fqn) {
                sequence::drop_sequence(schema, name);
                let _ = sequence::create_sequence(schema, name, 1, 1);
            }
        }
    }
}

/// Walk every catalog table; for each column with a NextVal default,
/// scan storage for the max int value and setval the sequence so the
/// next nextval produces a non-colliding id.
pub(super) fn advance_all_to_max() {
    for table in catalog::list_all_tables() {
        for (col_idx, col) in table.columns.iter().enumerate() {
            let Some(DefaultExpr::NextVal(fqn)) = &col.default_expr else { continue; };
            let Some((seq_schema, seq_name)) = split_seq_fqn(fqn) else { continue; };
            let max = max_int_in_column(&table.schema, &table.name, col_idx);
            if let Some(m) = max {
                let _ = sequence::setval(seq_schema, seq_name, m);
            }
        }
    }
}

fn max_int_in_column(schema: &str, table: &str, col_idx: usize) -> Option<i64> {
    storage::scan_with(schema, table, |rows| {
        let mut max: Option<i64> = None;
        for row in rows {
            if let Some(Value::Int(v)) = row.get(col_idx) {
                max = Some(max.map_or(*v, |m| m.max(*v)));
            }
        }
        Ok(max)
    }).ok().flatten()
}
