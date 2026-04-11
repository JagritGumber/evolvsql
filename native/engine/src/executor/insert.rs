use pg_query::NodeEnum;

use crate::arena::QueryArena;
use crate::catalog;
use crate::storage;
use crate::types::Value;

use super::helpers::{apply_default, check_not_null, eval_const, parse_vector_literal};
use super::insert_conflict::{build_unique_checks, parse_conflict_columns, parse_on_conflict_set, apply_on_conflict_update};
use super::returning::eval_returning;
use super::select::exec_select_raw;
use super::types::QueryResult;

/// Execute INSERT with VALUES, SELECT, defaults, constraints, ON CONFLICT.
pub(crate) fn exec_insert(insert: &pg_query::protobuf::InsertStmt) -> Result<QueryResult, String> {
    let rel = insert.relation.as_ref().ok_or("INSERT missing relation")?;
    let table_name = &rel.relname;
    let schema = if rel.schemaname.is_empty() { "public" } else { &rel.schemaname };
    let table_def = catalog::get_table(schema, table_name).ok_or_else(|| format!("relation \"{}\" does not exist", table_name))?;
    let target_cols: Vec<String> = if insert.cols.is_empty() {
        table_def.columns.iter().map(|c| c.name.clone()).collect()
    } else {
        insert.cols.iter().filter_map(|n| n.node.as_ref()).filter_map(|node| if let NodeEnum::ResTarget(rt) = node { Some(rt.name.clone()) } else { None }).collect()
    };
    let select = insert.select_stmt.as_ref().and_then(|s| s.node.as_ref()).ok_or("INSERT missing VALUES")?;
    let has_returning = !insert.returning_list.is_empty();
    let mut all_rows: Vec<Vec<Value>> = Vec::new();
    if let NodeEnum::SelectStmt(sel) = select {
        if !sel.values_lists.is_empty() {
            for values_list in &sel.values_lists {
                if let Some(NodeEnum::List(list)) = values_list.node.as_ref() {
                    let mut row: Vec<Value> = table_def.columns.iter().map(|col| {
                        if target_cols.contains(&col.name) { Ok(Value::Null) } else { apply_default(&col.default_expr, schema) }
                    }).collect::<Result<Vec<_>, _>>()?;
                    for (i, val_node) in list.items.iter().enumerate() {
                        if i >= target_cols.len() { break; }
                        let col_idx = table_def.columns.iter().position(|c| c.name == target_cols[i]).ok_or_else(|| format!("column \"{}\" does not exist", target_cols[i]))?;
                        if matches!(val_node.node.as_ref(), Some(NodeEnum::SetToDefault(_))) { row[col_idx] = apply_default(&table_def.columns[col_idx].default_expr, schema)?; }
                        else { row[col_idx] = eval_const(val_node.node.as_ref()); }
                    }
                    for (i, col) in table_def.columns.iter().enumerate() {
                        if col.type_oid == crate::types::TypeOid::Vector { if let Value::Text(s) = &row[i] { row[i] = parse_vector_literal(s.trim())?; } }
                    }
                    check_not_null(&table_def, &row)?;
                    all_rows.push(row);
                }
            }
        } else {
            let mut arena = QueryArena::new();
            let (_cols, raw_rows) = exec_select_raw(sel, None, &mut arena)?;
            for raw_row in &raw_rows {
                let mut row: Vec<Value> = table_def.columns.iter().map(|_| Value::Null).collect();
                for (i, val) in raw_row.iter().enumerate() {
                    if i >= target_cols.len() { break; }
                    let col_idx = table_def.columns.iter().position(|c| c.name == target_cols[i]).ok_or_else(|| format!("column \"{}\" does not exist", target_cols[i]))?;
                    row[col_idx] = val.to_value(&arena);
                }
                check_not_null(&table_def, &row)?;
                all_rows.push(row);
            }
        }
    }
    let (unique_checks, pk_cols) = build_unique_checks(&table_def);
    let vector_col_idx = table_def.columns.iter().position(|c| c.type_oid == crate::types::TypeOid::Vector);
    if let Some(col_idx) = vector_col_idx { let _ = storage::ensure_hnsw_index(schema, table_name, col_idx, crate::hnsw::DistanceMetric::L2); }
    if let Some(ref oc) = insert.on_conflict_clause {
        exec_insert_upsert(insert, oc, &table_def, schema, table_name, all_rows, &unique_checks, &pk_cols, has_returning)
    } else {
        exec_insert_standard(insert, &table_def, schema, table_name, all_rows, &unique_checks, &pk_cols, vector_col_idx, has_returning)
    }
}

fn exec_insert_upsert(
    insert: &pg_query::protobuf::InsertStmt, oc: &pg_query::protobuf::OnConflictClause,
    table_def: &catalog::Table, schema: &str, table_name: &str,
    all_rows: Vec<Vec<Value>>, unique_checks: &[(usize, String)], pk_cols: &[usize], has_returning: bool,
) -> Result<QueryResult, String> {
    let do_update = oc.action == pg_query::protobuf::OnConflictAction::OnconflictUpdate as i32;
    let set_clauses = if do_update { parse_on_conflict_set(&oc.target_list, table_def)? } else { Vec::new() };
    let conflict_cols = if let Some(ref infer) = oc.infer { parse_conflict_columns(&infer.index_elems, table_def)? } else { Vec::new() };
    let td_clone = table_def.clone();
    let (inserted, updated, affected_rows) = storage::insert_upsert(
        schema, table_name, all_rows, unique_checks, pk_cols, &conflict_cols, do_update,
        |existing, excluded| apply_on_conflict_update(existing, excluded, &set_clauses, &td_clone),
    )?;
    let tag = format!("INSERT 0 {}", inserted + updated);
    if has_returning { eval_returning(&insert.returning_list, &affected_rows, table_def, schema, table_name, &tag) }
    else { Ok(QueryResult { tag, columns: vec![], rows: vec![] }) }
}

fn exec_insert_standard(
    insert: &pg_query::protobuf::InsertStmt, table_def: &catalog::Table, schema: &str, table_name: &str,
    all_rows: Vec<Vec<Value>>, unique_checks: &[(usize, String)], pk_cols: &[usize],
    vector_col_idx: Option<usize>, has_returning: bool,
) -> Result<QueryResult, String> {
    let row_count = all_rows.len() as u64;
    let needs_row_copy = has_returning || vector_col_idx.is_some();
    let inserted_rows = if needs_row_copy { all_rows.clone() } else { Vec::new() };
    let base_row_id = storage::insert_batch_checked(schema, table_name, all_rows, unique_checks, pk_cols)?;
    if let Some(col_idx) = vector_col_idx {
        if storage::has_hnsw_index(schema, table_name).is_some() {
            for (i, row) in inserted_rows.iter().enumerate() {
                if let Some(crate::types::Value::Vector(v)) = row.get(col_idx) { let _ = storage::hnsw_insert(schema, table_name, base_row_id + i, v.clone()); }
            }
        }
    }
    let tag = format!("INSERT 0 {}", row_count);
    if has_returning { eval_returning(&insert.returning_list, &inserted_rows, table_def, schema, table_name, &tag) }
    else { Ok(QueryResult { tag, columns: vec![], rows: vec![] }) }
}
