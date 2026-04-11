use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};
use crate::types::Value;
use super::select_fast::exec_single_table_fast_path;
use super::select_general::exec_general_select;
use super::select_nofrom::exec_select_raw_no_from;
use super::select_set::exec_set_operation;
use super::types::{JoinContext, QueryResult};

/// Top-level SELECT executor, converts ArenaValue to string results.
pub(crate) fn exec_select(select: &pg_query::protobuf::SelectStmt) -> Result<QueryResult, String> {
    let mut arena = QueryArena::new();
    let (columns, raw_rows) = exec_select_raw(select, None, &mut arena)?;
    let rows: Vec<Vec<Option<String>>> = raw_rows.iter()
        .map(|row| row.iter().map(|v| v.to_text(&arena)).collect())
        .collect();
    let count = rows.len();
    Ok(QueryResult { tag: format!("SELECT {}", count), columns, rows })
}

/// Internal: execute a SELECT and return raw ArenaValue rows + column metadata.
pub(crate) fn exec_select_raw(
    select: &pg_query::protobuf::SelectStmt, outer: Option<(&[ArenaValue], &JoinContext)>, arena: &mut QueryArena,
) -> Result<(Vec<(String, i32)>, Vec<Vec<ArenaValue>>), String> {
    let cte_snapshot = if select.with_clause.is_some() { Some(arena.cte_registry.clone()) } else { None };
    let result = exec_select_raw_body(select, outer, arena);
    if let Some(snapshot) = cte_snapshot { arena.cte_registry = snapshot; }
    result
}

fn exec_select_raw_body(
    select: &pg_query::protobuf::SelectStmt, outer: Option<(&[ArenaValue], &JoinContext)>, arena: &mut QueryArena,
) -> Result<(Vec<(String, i32)>, Vec<Vec<ArenaValue>>), String> {
    // Execute CTEs
    if let Some(ref wc) = select.with_clause {
        if wc.recursive { return Err("WITH RECURSIVE is not yet supported".into()); }
        for cte_node in &wc.ctes {
            if let Some(NodeEnum::CommonTableExpr(cte)) = cte_node.node.as_ref() {
                let inner = cte.ctequery.as_ref().and_then(|n| n.node.as_ref()).ok_or("CTE missing query")?;
                let NodeEnum::SelectStmt(sel) = inner else { return Err("data-modifying CTEs are not yet supported".into()); };
                let (mut cols, rows) = exec_select_raw(sel, None, arena)?;
                if !cte.aliascolnames.is_empty() {
                    if cte.aliascolnames.len() != cols.len() { return Err(format!("WITH query \"{}\" has {} columns but {} aliases specified", cte.ctename, cols.len(), cte.aliascolnames.len())); }
                    for (i, alias_node) in cte.aliascolnames.iter().enumerate() {
                        if let Some(NodeEnum::String(s)) = alias_node.node.as_ref() { cols[i].0 = s.sval.clone(); }
                    }
                }
                let value_rows: Vec<Vec<Value>> = rows.iter().map(|row| row.iter().map(|v| v.to_value(arena)).collect()).collect();
                arena.cte_registry.insert(cte.ctename.clone(), crate::arena::CteEntry { columns: cols, rows: value_rows });
            }
        }
    }
    // Set operations
    let set_op = select.op;
    if set_op == pg_query::protobuf::SetOperation::SetopUnion as i32
        || set_op == pg_query::protobuf::SetOperation::SetopIntersect as i32
        || set_op == pg_query::protobuf::SetOperation::SetopExcept as i32 {
        return exec_set_operation(select, outer, arena, set_op);
    }
    if select.from_clause.is_empty() { return exec_select_raw_no_from(select, outer, arena); }
    // Fast path: single table
    if select.from_clause.len() == 1 && outer.is_none() {
        if let Some(NodeEnum::RangeVar(rv)) = select.from_clause[0].node.as_ref() {
            if !(rv.schemaname.is_empty() && arena.cte_registry.contains_key(&rv.relname)) {
                return exec_single_table_fast_path(select, rv, arena);
            }
        }
    }
    // General path
    exec_general_select(select, outer, arena)
}
