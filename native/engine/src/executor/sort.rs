use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};

use super::expr::eval_expr;
use super::resolve::{resolve_column, extract_func_name, extract_col_name};
use super::types::{JoinContext, SortKey};

/// Compare two rows by SortKey list for ordering.
pub(crate) fn compare_rows(keys: &[SortKey], a: &[ArenaValue], b: &[ArenaValue], arena: &QueryArena) -> std::cmp::Ordering {
    for k in keys {
        let va = a.get(k.col_idx).copied().unwrap_or(ArenaValue::Null);
        let vb = b.get(k.col_idx).copied().unwrap_or(ArenaValue::Null);
        let ord = match (va, vb) {
            (ArenaValue::Null, ArenaValue::Null) => std::cmp::Ordering::Equal,
            (ArenaValue::Null, _) => if k.nulls_first { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater },
            (_, ArenaValue::Null) => if k.nulls_first { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less },
            _ => va.compare(&vb, arena).unwrap_or(std::cmp::Ordering::Equal),
        };
        let ord = if k.ascending { ord } else { ord.reverse() };
        if ord != std::cmp::Ordering::Equal { return ord; }
    }
    std::cmp::Ordering::Equal
}

/// Extended ORDER BY resolver that supports arbitrary expressions.
pub(crate) fn resolve_sort_keys_with_exprs(
    sort_clause: &[pg_query::protobuf::Node], ctx: &JoinContext,
    select: &pg_query::protobuf::SelectStmt, rows: &mut Vec<Vec<ArenaValue>>, arena: &mut QueryArena,
) -> Result<(Vec<SortKey>, usize), String> {
    let mut keys = Vec::new();
    let mut expr_nodes: Vec<(usize, &NodeEnum)> = Vec::new();
    let base_width = if rows.is_empty() { ctx.total_columns } else { rows[0].len() };
    let mut next_col = base_width;
    for snode in sort_clause {
        if let Some(NodeEnum::SortBy(sb)) = snode.node.as_ref() {
            let inner = sb.node.as_ref().and_then(|n| n.node.as_ref());
            let col_idx = match inner {
                Some(NodeEnum::ColumnRef(cref)) => resolve_column(cref, ctx)?,
                Some(NodeEnum::AConst(ac)) => {
                    let ordinal = match &ac.val { Some(pg_query::protobuf::a_const::Val::Ival(i)) => i.ival as usize, _ => return Err("invalid ORDER BY ordinal".into()) };
                    resolve_ordinal_or_expr(ordinal, select, ctx, &mut next_col, &mut expr_nodes, keys.len())?
                }
                Some(expr_node) => { let idx = next_col; next_col += 1; expr_nodes.push((keys.len(), expr_node)); idx }
                None => return Err("ORDER BY missing expression".into()),
            };
            let ascending = sb.sortby_dir != pg_query::protobuf::SortByDir::SortbyDesc as i32;
            let nulls_first = match sb.sortby_nulls {
                x if x == pg_query::protobuf::SortByNulls::SortbyNullsFirst as i32 => true,
                x if x == pg_query::protobuf::SortByNulls::SortbyNullsLast as i32 => false,
                _ => !ascending,
            };
            keys.push(SortKey { col_idx, ascending, nulls_first });
        }
    }
    let expr_count = next_col - base_width;
    if expr_count > 0 {
        for row in rows.iter_mut() { row.resize(next_col, ArenaValue::Null); }
        let expr_data: Vec<(usize, NodeEnum)> = expr_nodes.iter().map(|(ki, node)| (*ki, (*node).clone())).collect();
        for row in rows.iter_mut() {
            for (key_idx, expr_node) in &expr_data {
                let col_idx = keys[*key_idx].col_idx;
                row[col_idx] = eval_expr(expr_node, row, ctx, arena)?;
            }
        }
    }
    Ok((keys, expr_count))
}

/// Resolve ORDER BY ordinal. For ColumnRef targets, returns the column index.
/// For expression targets, adds to expr_nodes and returns the temp column index.
fn resolve_ordinal_or_expr<'a>(
    ordinal: usize, select: &'a pg_query::protobuf::SelectStmt, ctx: &JoinContext,
    next_col: &mut usize, expr_nodes: &mut Vec<(usize, &'a NodeEnum)>, key_idx: usize,
) -> Result<usize, String> {
    if ordinal == 0 || ordinal > select.target_list.len() {
        return Err(format!("ORDER BY position {} is not in select list", ordinal));
    }
    let target = &select.target_list[ordinal - 1];
    if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
        if let Some(node) = rt.val.as_ref().and_then(|v| v.node.as_ref()) {
            if let NodeEnum::ColumnRef(cref) = node {
                return resolve_column(cref, ctx);
            }
            // Expression target: route through expression evaluation path
            let idx = *next_col;
            *next_col += 1;
            expr_nodes.push((key_idx, node));
            return Ok(idx);
        }
    }
    Err("ORDER BY ordinal must reference a column or expression".into())
}

/// Resolve ORDER BY keys for aggregate query results.
pub(crate) fn resolve_aggregate_sort_keys(
    sort_clause: &[pg_query::protobuf::Node], result_columns: &[(String, i32)],
    _source_ctx: &JoinContext, select: &pg_query::protobuf::SelectStmt,
) -> Result<Vec<SortKey>, String> {
    let mut keys = Vec::new();
    for snode in sort_clause {
        if let Some(NodeEnum::SortBy(sb)) = snode.node.as_ref() {
            let inner = sb.node.as_ref().and_then(|n| n.node.as_ref());
            let col_idx = match inner {
                Some(NodeEnum::AConst(ac)) => {
                    let ordinal = match &ac.val { Some(pg_query::protobuf::a_const::Val::Ival(i)) => i.ival as usize, _ => return Err("invalid ORDER BY ordinal".into()) };
                    if ordinal == 0 || ordinal > result_columns.len() { return Err(format!("ORDER BY position {} is not in select list", ordinal)); }
                    ordinal - 1
                }
                Some(NodeEnum::ColumnRef(cref)) => {
                    let col_name = extract_col_name(cref);
                    result_columns.iter().position(|(name, _)| name.eq_ignore_ascii_case(&col_name))
                        .ok_or_else(|| format!("column \"{}\" not found in aggregate result", col_name))?
                }
                Some(NodeEnum::FuncCall(fc)) => {
                    let sort_func_name = extract_func_name(fc);
                    let mut found = None;
                    for (i, target) in select.target_list.iter().enumerate() {
                        if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
                            if let Some(NodeEnum::FuncCall(tfc)) = rt.val.as_ref().and_then(|v| v.node.as_ref()) {
                                if extract_func_name(tfc) == sort_func_name && fc.args.len() == tfc.args.len() { found = Some(i); break; }
                            }
                        }
                    }
                    found.ok_or_else(|| format!("aggregate {}() in ORDER BY not found in select list", sort_func_name))?
                }
                _ => return Err("unsupported ORDER BY expression in aggregate query".into()),
            };
            let ascending = sb.sortby_dir != pg_query::protobuf::SortByDir::SortbyDesc as i32;
            let nulls_first = match sb.sortby_nulls {
                x if x == pg_query::protobuf::SortByNulls::SortbyNullsFirst as i32 => true,
                x if x == pg_query::protobuf::SortByNulls::SortbyNullsLast as i32 => false,
                _ => !ascending,
            };
            keys.push(SortKey { col_idx, ascending, nulls_first });
        }
    }
    Ok(keys)
}
