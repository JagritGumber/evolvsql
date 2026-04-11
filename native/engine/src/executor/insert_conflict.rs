use pg_query::NodeEnum;

use crate::arena::{QueryArena, rows_to_arena};
use crate::catalog::{Column, Table};
use crate::types::Value;

use super::expr::eval_expr;
use super::helpers::check_not_null;
use super::types::{JoinContext, JoinSource};

/// Build list of unique/PK constraint column indices.
pub(crate) fn build_unique_checks(table: &Table) -> (Vec<(usize, String)>, Vec<usize>) {
    let pk_cols: Vec<usize> = table.columns.iter().enumerate().filter(|(_, c)| c.primary_key).map(|(i, _)| i).collect();
    let mut unique_checks = Vec::new();
    for (i, col) in table.columns.iter().enumerate() {
        if col.primary_key && pk_cols.len() > 1 { continue; }
        if col.primary_key || col.unique {
            let cname = if col.primary_key { format!("{}_pkey", table.name) } else { format!("{}_{}_key", table.name, col.name) };
            unique_checks.push((i, cname));
        }
    }
    (unique_checks, pk_cols)
}

/// Parse ON CONFLICT (col1, col2) conflict target into column indices.
pub(crate) fn parse_conflict_columns(index_elems: &[pg_query::protobuf::Node], table: &Table) -> Result<Vec<usize>, String> {
    let mut cols = Vec::new();
    for elem in index_elems {
        if let Some(NodeEnum::IndexElem(ie)) = elem.node.as_ref() {
            cols.push(table.columns.iter().position(|c| c.name == ie.name).ok_or_else(|| format!("column \"{}\" does not exist", ie.name))?);
        }
    }
    Ok(cols)
}

/// Parse ON CONFLICT DO UPDATE SET clauses into (col_idx, expr) pairs.
pub(crate) fn parse_on_conflict_set(target_list: &[pg_query::protobuf::Node], table: &Table) -> Result<Vec<(usize, NodeEnum)>, String> {
    let mut result = Vec::new();
    for node in target_list {
        if let Some(NodeEnum::ResTarget(rt)) = node.node.as_ref() {
            let col_idx = table.columns.iter().position(|c| c.name == rt.name).ok_or_else(|| format!("column \"{}\" does not exist", rt.name))?;
            let expr = rt.val.as_ref().and_then(|v| v.node.as_ref()).ok_or("ON CONFLICT SET missing value expression")?.clone();
            result.push((col_idx, expr));
        }
    }
    Ok(result)
}

/// Apply ON CONFLICT DO UPDATE SET assignments with EXCLUDED pseudo-table.
pub(crate) fn apply_on_conflict_update(
    existing: &[Value], excluded: &[Value], set_clauses: &[(usize, NodeEnum)], table: &Table,
) -> Result<Vec<Value>, String> {
    let mut new_row = existing.to_vec();
    let ncols = table.columns.len();
    let mut combined: Vec<Value> = existing.to_vec();
    combined.extend_from_slice(excluded);
    let excluded_table = Table {
        name: "excluded".into(), schema: String::new(),
        columns: table.columns.iter().map(|c| Column { name: c.name.clone(), ..c.clone() }).collect(),
    };
    let ctx = JoinContext {
        sources: vec![
            JoinSource { alias: table.name.clone(), table_name: table.name.clone(), schema: table.schema.clone(), table_def: table.clone(), col_offset: 0 },
            JoinSource { alias: "excluded".into(), table_name: "excluded".into(), schema: String::new(), table_def: excluded_table, col_offset: ncols },
        ],
        total_columns: ncols * 2,
    };
    let mut arena = QueryArena::new();
    let arena_row = rows_to_arena(&[combined], &mut arena);
    let arena_combined = &arena_row[0];
    for &(col_idx, ref expr) in set_clauses {
        let val = eval_expr(expr, arena_combined, &ctx, &mut arena)?;
        new_row[col_idx] = val.to_value(&arena);
    }
    check_not_null(table, &new_row)?;
    Ok(new_row)
}

/// Validate uniqueness of `new_row` against `all_rows`, excluding row at `skip_idx`.
pub(crate) fn check_unique_against(
    table: &Table, new_row: &[Value], all_rows: &[Vec<Value>], skip_idx: usize,
) -> Result<(), String> {
    let (unique_checks, pk_cols) = build_unique_checks(table);
    if pk_cols.len() > 1 {
        let new_key: Vec<&Value> = pk_cols.iter().map(|&i| &new_row[i]).collect();
        for (idx, erow) in all_rows.iter().enumerate() {
            if idx == skip_idx { continue; }
            let ekey: Vec<&Value> = pk_cols.iter().map(|&i| &erow[i]).collect();
            if new_key == ekey { return Err(format!("duplicate key value violates unique constraint \"{}_pkey\"", table.name)); }
        }
    }
    for &(col_idx, ref cname) in &unique_checks {
        if matches!(new_row[col_idx], Value::Null) { continue; }
        for (idx, erow) in all_rows.iter().enumerate() {
            if idx == skip_idx { continue; }
            if col_idx < erow.len() && erow[col_idx] == new_row[col_idx] {
                return Err(format!("duplicate key value violates unique constraint \"{}\"", cname));
            }
        }
    }
    Ok(())
}
