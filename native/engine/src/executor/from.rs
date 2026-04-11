use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena, rows_to_arena};
use crate::catalog::{self, Column, Table};
use crate::storage;
use crate::types::TypeOid;

use super::join::{try_equi_hash_join, nested_loop_join};
use super::select::exec_select_raw;
use super::types::{JoinContext, JoinSource};

/// Execute FROM clause node and return joined rows with JoinContext.
pub(crate) fn execute_from(node: &NodeEnum, arena: &mut QueryArena) -> Result<(Vec<Vec<ArenaValue>>, JoinContext), String> {
    match node {
        NodeEnum::RangeVar(rv) => execute_range_var(rv, arena),
        NodeEnum::JoinExpr(je) => execute_join_expr(je, arena),
        NodeEnum::RangeSubselect(rs) => execute_range_subselect(rs, arena),
        _ => Err("unsupported FROM clause node".into()),
    }
}

fn execute_range_var(rv: &pg_query::protobuf::RangeVar, arena: &mut QueryArena) -> Result<(Vec<Vec<ArenaValue>>, JoinContext), String> {
    let alias = rv.alias.as_ref().map(|a| a.aliasname.clone()).unwrap_or_else(|| rv.relname.clone());
    if rv.schemaname.is_empty() {
        if let Some(cte) = arena.cte_registry.get(&rv.relname) {
            let cte_value_rows = cte.rows.clone();
            let columns: Vec<Column> = cte.columns.iter()
                .map(|(name, oid)| Column { name: name.clone(), type_oid: TypeOid::from_oid(*oid), nullable: true, primary_key: false, unique: false, default_expr: None })
                .collect();
            drop(cte);
            let rows = rows_to_arena(&cte_value_rows, arena);
            let ncols = columns.len();
            let table_def = Table { name: alias.clone(), schema: String::new(), columns };
            let ctx = JoinContext {
                sources: vec![JoinSource { alias, table_name: rv.relname.clone(), schema: String::new(), table_def, col_offset: 0 }],
                total_columns: ncols,
            };
            return Ok((rows, ctx));
        }
    }
    let schema = if rv.schemaname.is_empty() { "public" } else { &rv.schemaname };
    let table_def = catalog::get_table(schema, &rv.relname)
        .ok_or_else(|| format!("relation \"{}\" does not exist", rv.relname))?;
    let value_rows = storage::scan(schema, &rv.relname)?;
    let rows = rows_to_arena(&value_rows, arena);
    let ncols = table_def.columns.len();
    let ctx = JoinContext {
        sources: vec![JoinSource { alias, table_name: rv.relname.clone(), schema: schema.to_string(), table_def, col_offset: 0 }],
        total_columns: ncols,
    };
    Ok((rows, ctx))
}

fn execute_join_expr(je: &pg_query::protobuf::JoinExpr, arena: &mut QueryArena) -> Result<(Vec<Vec<ArenaValue>>, JoinContext), String> {
    let left_node = je.larg.as_ref().and_then(|n| n.node.as_ref()).ok_or("JOIN missing left")?;
    let right_node = je.rarg.as_ref().and_then(|n| n.node.as_ref()).ok_or("JOIN missing right")?;
    let (left_rows, left_ctx) = execute_from(left_node, arena)?;
    let (right_rows, right_ctx) = execute_from(right_node, arena)?;
    let left_width = left_ctx.total_columns;
    let right_width = right_ctx.total_columns;
    let mut sources = left_ctx.sources;
    for mut src in right_ctx.sources { src.col_offset += left_width; sources.push(src); }
    let merged = JoinContext { total_columns: left_width + right_width, sources };
    if !je.using_clause.is_empty() { return Err("JOIN ... USING is not yet supported; use JOIN ... ON instead".into()); }
    if je.is_natural { return Err("NATURAL JOIN is not yet supported; use JOIN ... ON instead".into()); }
    let quals = je.quals.as_ref().and_then(|n| n.node.as_ref());
    let result = match try_equi_hash_join(&left_rows, &right_rows, je.jointype, quals, &merged, left_width, right_width, arena) {
        Some(Ok(rows)) => rows,
        Some(Err(e)) => return Err(e),
        None => nested_loop_join(&left_rows, &right_rows, je.jointype, quals, &merged, left_width, right_width, arena)?,
    };
    Ok((result, merged))
}

fn execute_range_subselect(rs: &pg_query::protobuf::RangeSubselect, arena: &mut QueryArena) -> Result<(Vec<Vec<ArenaValue>>, JoinContext), String> {
    let inner = rs.subquery.as_ref().and_then(|n| n.node.as_ref()).ok_or("RangeSubselect missing subquery")?;
    let NodeEnum::SelectStmt(sel) = inner else { return Err("subquery in FROM is not a SELECT".into()); };
    let (inner_cols, rows) = exec_select_raw(sel, None, arena)?;
    let alias = rs.alias.as_ref().map(|a| a.aliasname.clone()).ok_or("subquery in FROM must have an alias")?;
    let columns: Vec<Column> = inner_cols.iter()
        .map(|(name, oid)| Column { name: name.clone(), type_oid: TypeOid::from_oid(*oid), nullable: true, primary_key: false, unique: false, default_expr: None })
        .collect();
    let ncols = columns.len();
    let table_def = Table { name: alias.clone(), schema: String::new(), columns };
    let ctx = JoinContext {
        sources: vec![JoinSource { alias, table_name: "subquery".into(), schema: String::new(), table_def, col_offset: 0 }],
        total_columns: ncols,
    };
    Ok((rows, ctx))
}

/// Execute the full FROM clause, handling multiple comma-separated tables.
pub(crate) fn execute_from_clause(
    from_clause: &[pg_query::protobuf::Node], arena: &mut QueryArena,
) -> Result<(Vec<Vec<ArenaValue>>, JoinContext), String> {
    let first = from_clause.first().and_then(|n| n.node.as_ref()).ok_or("missing FROM")?;
    let (mut rows, mut ctx) = execute_from(first, arena)?;
    for from_node in &from_clause[1..] {
        let node = from_node.node.as_ref().ok_or("missing FROM node")?;
        let (right_rows, right_ctx) = execute_from(node, arena)?;
        let left_width = ctx.total_columns;
        let right_width = right_ctx.total_columns;
        let mut sources = ctx.sources;
        for mut src in right_ctx.sources { src.col_offset += left_width; sources.push(src); }
        ctx = JoinContext { total_columns: left_width + right_width, sources };
        let mut new_rows = Vec::with_capacity(rows.len() * right_rows.len());
        for left in &rows {
            for right in &right_rows {
                let mut combined = Vec::with_capacity(left_width + right_width);
                combined.extend_from_slice(left); combined.extend_from_slice(right);
                new_rows.push(combined);
            }
        }
        rows = new_rows;
    }
    Ok((rows, ctx))
}
