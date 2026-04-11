use crate::arena::{ArenaValue, QueryArena, rows_to_arena};
use crate::catalog;
use crate::storage;
use crate::types::TypeOid;

use super::expr::eval_expr;
use super::filter::{eval_where_value, try_fast_equality_filter};
use super::helpers::eval_const_i64;
use super::knn::try_detect_knn;
use super::resolve::{resolve_targets, column_type_oid};
use super::select_post::exec_select_raw_post_filter;
use super::types::{JoinContext, JoinSource, SelectTarget};

/// Fast path: single-table query with no outer context.
pub(crate) fn exec_single_table_fast_path(
    select: &pg_query::protobuf::SelectStmt,
    rv: &pg_query::protobuf::RangeVar,
    arena: &mut QueryArena,
) -> Result<(Vec<(String, i32)>, Vec<Vec<ArenaValue>>), String> {
    let schema = if rv.schemaname.is_empty() { "public" } else { &rv.schemaname };
    let table_def = catalog::get_table(schema, &rv.relname)
        .ok_or_else(|| format!("relation \"{}\" does not exist", rv.relname))?;
    let alias = rv.alias.as_ref().map(|a| a.aliasname.clone()).unwrap_or_else(|| rv.relname.clone());
    let ncols = table_def.columns.len();
    let ctx = JoinContext {
        sources: vec![JoinSource {
            alias, table_name: rv.relname.clone(),
            #[allow(dead_code)] schema: schema.to_string(),
            table_def, col_offset: 0,
        }],
        total_columns: ncols,
    };

    // HNSW fast path
    if select.where_clause.is_none() && !select.sort_clause.is_empty() {
        if let Some(knn) = try_detect_knn(select, &ctx, schema, &rv.relname) {
            return exec_knn_fast_path(select, &ctx, schema, &rv.relname, knn, arena);
        }
    }

    // Filter inside the read lock
    let fast_filter = try_fast_equality_filter(&select.where_clause, &ctx, arena);
    let value_rows = storage::scan_with(schema, &rv.relname, |all_rows| {
        let mut filtered = Vec::new();
        if let Some(ref ff) = fast_filter {
            for row in all_rows { if ff.matches_value(row, arena) { filtered.push(row.clone()); } }
        } else {
            let mut scan_arena = QueryArena::new();
            scan_arena.cte_registry = arena.cte_registry.clone();
            for row in all_rows {
                if eval_where_value(&select.where_clause, row, &ctx, &mut scan_arena)? { filtered.push(row.clone()); }
            }
        }
        Ok(filtered)
    })?;
    let rows = rows_to_arena(&value_rows, arena);
    exec_select_raw_post_filter(select, ctx, rows, 0, arena)
}

fn exec_knn_fast_path(
    select: &pg_query::protobuf::SelectStmt, ctx: &JoinContext,
    schema: &str, table_name: &str, knn: super::types::KnnPlan, arena: &mut QueryArena,
) -> Result<(Vec<(String, i32)>, Vec<Vec<ArenaValue>>), String> {
    if let Some(vec_col) = ctx.sources[0].table_def.columns.iter().position(|c| c.type_oid == TypeOid::Vector) {
        let _ = storage::ensure_hnsw_index(schema, table_name, vec_col, knn.metric);
    }
    let offset = select.limit_offset.as_ref()
        .and_then(|n| eval_const_i64(n.node.as_ref())).unwrap_or(0).max(0) as usize;
    let hnsw_results = storage::hnsw_search(schema, table_name, &knn.query_vector, knn.k + offset)?;
    let row_ids: Vec<usize> = hnsw_results.iter().skip(offset).map(|(_, row_id)| *row_id).collect();
    let value_rows = storage::get_rows_by_ids(schema, table_name, &row_ids)?;
    let rows = rows_to_arena(&value_rows, arena);
    let targets = resolve_targets(select, ctx)?;
    let columns: Vec<(String, i32)> = targets.iter()
        .map(|t| match t {
            SelectTarget::Column { name, idx } => Ok((name.clone(), column_type_oid(*idx, ctx)?)),
            SelectTarget::Expr { name, .. } => Ok((name.clone(), TypeOid::Text.oid())),
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mut result_rows = Vec::new();
    for row in &rows {
        let mut result_row = Vec::new();
        for t in &targets {
            let val = match t {
                SelectTarget::Column { idx, .. } => if *idx < row.len() { row[*idx] } else { ArenaValue::Null },
                SelectTarget::Expr { expr, .. } => eval_expr(expr, row, ctx, arena)?,
            };
            result_row.push(val);
        }
        result_rows.push(result_row);
    }
    Ok((columns, result_rows))
}
