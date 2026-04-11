use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};
use crate::types::TypeOid;

use super::aggregate::{exec_select_aggregate, query_has_aggregates};
use super::expr::eval_expr;
use super::filter::dedup_distinct;
use super::helpers::{eval_const_i64, parse_text_to_value};
use super::resolve::{resolve_column, resolve_targets, column_type_oid, extract_func_name};
use super::sort::{compare_rows, resolve_sort_keys_with_exprs, resolve_aggregate_sort_keys};
use super::types::{JoinContext, SelectTarget};

/// Shared post-filter logic: aggregates, ORDER BY, LIMIT, projection.
pub(crate) fn exec_select_raw_post_filter(
    select: &pg_query::protobuf::SelectStmt, merged_ctx: JoinContext,
    mut rows: Vec<Vec<ArenaValue>>, _outer_width: usize, arena: &mut QueryArena,
) -> Result<(Vec<(String, i32)>, Vec<Vec<ArenaValue>>), String> {
    if query_has_aggregates(select) || !select.group_clause.is_empty() {
        return exec_aggregate_post(select, &merged_ctx, rows, arena);
    }
    // ORDER BY
    if !select.sort_clause.is_empty() {
        let (sort_keys, expr_count) = resolve_sort_keys_with_exprs(&select.sort_clause, &merged_ctx, select, &mut rows, arena)?;
        rows.sort_by(|a, b| compare_rows(&sort_keys, a, b, arena));
        if expr_count > 0 { for row in rows.iter_mut() { row.truncate(row.len() - expr_count); } }
    }
    // Window functions
    let window_targets = crate::window::extract_window_targets(select, &merged_ctx, arena)?;
    let window_offset = if rows.is_empty() { 0 } else { rows[0].len() };
    if !window_targets.is_empty() {
        let window_results = crate::window::evaluate_window_functions(&window_targets, &rows, &merged_ctx, arena)?;
        for (i, row) in rows.iter_mut().enumerate() { row.extend_from_slice(&window_results[i]); }
    }
    // Project
    let targets = resolve_targets(select, &merged_ctx)?;
    let columns = build_columns(&targets, &merged_ctx, arena)?;
    let window_positions = build_window_positions(&targets);
    let mut result_rows = Vec::new();
    for row in &rows {
        let mut result_row = Vec::new();
        for (ti, t) in targets.iter().enumerate() {
            let val = if let Some(wi) = window_positions[ti] { row[window_offset + wi] }
            else {
                match t {
                    SelectTarget::Column { idx, .. } => {
                        if *idx < row.len() { row[*idx] }
                        else { return Err(format!("internal error: column index {} out of range for row of width {}", idx, row.len())); }
                    }
                    SelectTarget::Expr { expr, .. } => eval_expr(expr, row, &merged_ctx, arena)?,
                }
            };
            result_row.push(val);
        }
        result_rows.push(result_row);
    }
    result_rows = dedup_distinct(&select.distinct_clause, result_rows, arena);
    apply_offset_limit(select, &mut result_rows);
    Ok((columns, result_rows))
}

fn exec_aggregate_post(
    select: &pg_query::protobuf::SelectStmt, ctx: &JoinContext,
    rows: Vec<Vec<ArenaValue>>, arena: &mut QueryArena,
) -> Result<(Vec<(String, i32)>, Vec<Vec<ArenaValue>>), String> {
    let agg_result = exec_select_aggregate(select, ctx, rows, arena)?;
    let col_oids: Vec<i32> = agg_result.columns.iter().map(|(_, oid)| *oid).collect();
    let mut value_rows: Vec<Vec<ArenaValue>> = agg_result.rows.into_iter()
        .map(|row| row.into_iter().enumerate()
            .map(|(i, cell)| match cell {
                None => ArenaValue::Null,
                Some(s) => { let v = parse_text_to_value(&s, col_oids.get(i).copied().unwrap_or(25)); ArenaValue::from_value(&v, arena) }
            }).collect()
        ).collect();
    value_rows = dedup_distinct(&select.distinct_clause, value_rows, arena);
    if !select.sort_clause.is_empty() {
        let sort_keys = resolve_aggregate_sort_keys(&select.sort_clause, &agg_result.columns, ctx, select)?;
        value_rows.sort_by(|a, b| compare_rows(&sort_keys, a, b, arena));
    }
    apply_offset_limit(select, &mut value_rows);
    Ok((agg_result.columns, value_rows))
}

fn build_columns(targets: &[SelectTarget], ctx: &JoinContext, _arena: &QueryArena) -> Result<Vec<(String, i32)>, String> {
    targets.iter().map(|t| match t {
        SelectTarget::Column { name, idx } => Ok((name.clone(), column_type_oid(*idx, ctx)?)),
        SelectTarget::Expr { name, expr } => {
            if let NodeEnum::FuncCall(fc) = expr {
                if fc.over.is_some() {
                    let fname = extract_func_name(fc);
                    let oid = match fname.as_str() {
                        "row_number" | "rank" | "dense_rank" | "ntile" => TypeOid::Int8.oid(),
                        "lag" | "lead" | "first_value" | "last_value" | "nth_value" | "sum" | "min" | "max" => {
                            fc.args.first().and_then(|a| a.node.as_ref())
                                .and_then(|node| match node { NodeEnum::ColumnRef(cref) => resolve_column(cref, ctx).ok(), _ => None })
                                .and_then(|idx| column_type_oid(idx, ctx).ok()).unwrap_or(TypeOid::Text.oid())
                        }
                        "count" => TypeOid::Int8.oid(),
                        "avg" => TypeOid::Float8.oid(),
                        _ => TypeOid::Text.oid(),
                    };
                    return Ok((name.clone(), oid));
                }
            }
            Ok((name.clone(), TypeOid::Text.oid()))
        }
    }).collect()
}

fn build_window_positions(targets: &[SelectTarget]) -> Vec<Option<usize>> {
    let mut win_idx_base = 0;
    targets.iter().map(|t| {
        if let SelectTarget::Expr { expr, .. } = t {
            if let NodeEnum::FuncCall(fc) = expr {
                if fc.over.is_some() { let idx = win_idx_base; win_idx_base += 1; return Some(idx); }
            }
        }
        None
    }).collect()
}

fn apply_offset_limit(select: &pg_query::protobuf::SelectStmt, rows: &mut Vec<Vec<ArenaValue>>) {
    if let Some(ref offset_node) = select.limit_offset {
        if let Some(n) = eval_const_i64(offset_node.node.as_ref()) {
            let n = n.max(0) as usize;
            if n >= rows.len() { rows.clear(); } else { rows.drain(0..n); }
        }
    }
    if let Some(ref limit_node) = select.limit_count {
        if let Some(n) = eval_const_i64(limit_node.node.as_ref()) { rows.truncate(n.max(0) as usize); }
    }
}
