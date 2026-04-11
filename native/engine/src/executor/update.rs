use pg_query::NodeEnum;

use crate::arena::QueryArena;
use crate::catalog;
use crate::storage;
use crate::types::Value;

use super::filter::{eval_where_value, eval_expr_value};
use super::helpers::check_not_null;
use super::insert_conflict::check_unique_against;
use super::returning::eval_returning;
use super::types::{JoinContext, QueryResult};

/// Execute UPDATE with optional WHERE and RETURNING.
pub(crate) fn exec_update(update: &pg_query::protobuf::UpdateStmt) -> Result<QueryResult, String> {
    let rel = update.relation.as_ref().ok_or("UPDATE missing relation")?;
    let table_name = &rel.relname;
    let schema = if rel.schemaname.is_empty() { "public" } else { &rel.schemaname };
    let table_def = catalog::get_table(schema, table_name)
        .ok_or_else(|| format!("relation \"{}\" does not exist", table_name))?;
    let ctx = JoinContext::single(schema, table_name, table_def.clone());

    let mut assignments: Vec<(usize, NodeEnum)> = Vec::new();
    for target in &update.target_list {
        if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
            let col_idx = table_def.columns.iter().position(|c| c.name == rt.name)
                .ok_or_else(|| format!("column \"{}\" does not exist", rt.name))?;
            let val_node = rt.val.as_ref().and_then(|v| v.node.as_ref())
                .ok_or_else(|| format!("SET {} missing value", rt.name))?;
            assignments.push((col_idx, val_node.clone()));
        }
    }

    let mut validate_arena = QueryArena::new();
    storage::scan_with(schema, table_name, |all_rows| {
        for row in all_rows { eval_where_value(&update.where_clause, row, &ctx, &mut validate_arena)?; }
        Ok(())
    })?;

    let wc = update.where_clause.clone();
    let td = table_def.clone();
    let assigns = assignments.clone();
    let ctx2 = JoinContext::single(schema, table_name, td.clone());
    let has_returning = !update.returning_list.is_empty();
    let mut updated_rows: Vec<Vec<Value>> = Vec::new();
    let mut pred_arena = QueryArena::new();
    let mut expr_arena = QueryArena::new();

    let count = storage::update_rows_checked(
        schema, table_name,
        |row| eval_where_value(&wc, row, &ctx2, &mut pred_arena).unwrap_or(false),
        |row| {
            let ctx_inner = JoinContext::single(schema, table_name, td.clone());
            let mut new_row = row.clone();
            for (col_idx, expr_node) in &assigns { new_row[*col_idx] = eval_expr_value(expr_node, row, &ctx_inner, &mut expr_arena)?; }
            check_not_null(&td, &new_row)?;
            if has_returning { updated_rows.push(new_row.clone()); }
            Ok(new_row)
        },
        |new_row, all_rows, skip_idx| check_unique_against(&td, new_row, all_rows, skip_idx),
    )?;

    let tag = format!("UPDATE {}", count);
    if has_returning { eval_returning(&update.returning_list, &updated_rows, &table_def, schema, table_name, &tag) }
    else { Ok(QueryResult { tag, columns: vec![], rows: vec![] }) }
}

/// Execute TRUNCATE.
pub(crate) fn exec_truncate(trunc: &pg_query::protobuf::TruncateStmt) -> Result<QueryResult, String> {
    for rel_node in &trunc.relations {
        if let Some(pg_query::NodeEnum::RangeVar(rv)) = rel_node.node.as_ref() {
            let schema = if rv.schemaname.is_empty() { "public" } else { &rv.schemaname };
            storage::delete_all(schema, &rv.relname)?;
        }
    }
    Ok(QueryResult { tag: "TRUNCATE TABLE".into(), columns: vec![], rows: vec![] })
}
