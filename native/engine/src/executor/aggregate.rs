use std::collections::HashMap;
use std::hash::{DefaultHasher, Hasher};

use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};

use super::aggregate_compute::compute_aggregate;
use super::having::eval_having;
use super::resolve::{resolve_column, extract_func_name, extract_col_name, column_type_oid};
use super::types::{JoinContext, QueryResult};

/// Check if function name is an aggregate.
pub(crate) fn is_aggregate(name: &str) -> bool {
    matches!(name, "count" | "sum" | "avg" | "min" | "max" | "string_agg" | "bool_and" | "bool_or")
}

/// Check if SELECT contains any aggregate functions.
pub(crate) fn query_has_aggregates(select: &pg_query::protobuf::SelectStmt) -> bool {
    select.target_list.iter().any(|t| {
        if let Some(NodeEnum::ResTarget(rt)) = t.node.as_ref() {
            if let Some(NodeEnum::FuncCall(fc)) = rt.val.as_ref().and_then(|v| v.node.as_ref()) {
                return is_aggregate(&extract_func_name(fc)) && fc.over.is_none();
            }
        }
        false
    })
}

/// Resolve GROUP BY column references.
pub(crate) fn resolve_group_columns(
    group_clause: &[pg_query::protobuf::Node], ctx: &JoinContext,
) -> Result<Vec<usize>, String> {
    let mut indices = Vec::new();
    for node in group_clause {
        if let Some(NodeEnum::ColumnRef(cref)) = node.node.as_ref() { indices.push(resolve_column(cref, ctx)?); }
    }
    Ok(indices)
}

/// Execute GROUP BY with aggregation functions.
pub(crate) fn exec_select_aggregate(
    select: &pg_query::protobuf::SelectStmt, ctx: &JoinContext,
    rows: Vec<Vec<ArenaValue>>, arena: &mut QueryArena,
) -> Result<QueryResult, String> {
    let group_col_indices = resolve_group_columns(&select.group_clause, ctx)?;
    let mut groups: Vec<(Vec<ArenaValue>, Vec<Vec<ArenaValue>>)> = Vec::new();
    let mut group_index: HashMap<u64, Vec<usize>> = HashMap::new();
    if group_col_indices.is_empty() {
        groups.push((vec![], rows));
    } else {
        for row in rows {
            let key: Vec<ArenaValue> = group_col_indices.iter().map(|&i| row[i]).collect();
            let mut hasher = DefaultHasher::new();
            for v in &key { v.hash_with(arena, &mut hasher); }
            let h = hasher.finish();
            let mut found_idx = None;
            if let Some(candidates) = group_index.get(&h) {
                for &ci in candidates {
                    if groups[ci].0.len() == key.len() && groups[ci].0.iter().zip(key.iter()).all(|(a, b)| a.eq_with(b, arena)) {
                        found_idx = Some(ci); break;
                    }
                }
            }
            if let Some(idx) = found_idx { groups[idx].1.push(row); }
            else { let idx = groups.len(); group_index.entry(h).or_default().push(idx); groups.push((key, vec![row])); }
        }
    }
    let mut result_columns = Vec::new();
    let mut result_rows = Vec::new();
    let mut columns_built = false;
    for (group_key, group_rows) in &groups {
        let mut result_row = Vec::new();
        for target in &select.target_list {
            if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
                let val_node = rt.val.as_ref().and_then(|v| v.node.as_ref());
                match val_node {
                    Some(NodeEnum::FuncCall(fc)) => {
                        let name = extract_func_name(fc);
                        if is_aggregate(&name) {
                            let agg_val = compute_aggregate(&name, fc, group_rows, ctx, arena)?;
                            if !columns_built {
                                let alias = if rt.name.is_empty() { name.clone() } else { rt.name.clone() };
                                let oid = match name.as_str() { "count" => 20, "avg" => 701, "bool_and" | "bool_or" => 16, _ => 25 };
                                result_columns.push((alias, oid));
                            }
                            result_row.push(agg_val.to_text(arena));
                        } else { return Err(format!("function {}() is not an aggregate function", name)); }
                    }
                    Some(NodeEnum::ColumnRef(cref)) => {
                        let col_idx = resolve_column(cref, ctx)?;
                        if !group_col_indices.contains(&col_idx) {
                            return Err(format!("column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function", extract_col_name(cref)));
                        }
                        if !columns_built { let alias = if rt.name.is_empty() { extract_col_name(cref) } else { rt.name.clone() }; result_columns.push((alias, column_type_oid(col_idx, ctx)?)); }
                        let key_pos = group_col_indices.iter().position(|&i| i == col_idx).unwrap();
                        result_row.push(group_key[key_pos].to_text(arena));
                    }
                    _ => return Err("invalid expression in aggregate query".into()),
                }
            }
        }
        result_rows.push(result_row);
        columns_built = true;
    }
    if let Some(having_node) = &select.having_clause {
        if let Some(having_expr) = having_node.node.as_ref() {
            let mut keep = vec![false; result_rows.len()];
            for (i, (_, group_rows)) in groups.iter().enumerate() {
                if i < keep.len() && eval_having(having_expr, group_rows, ctx, arena)? { keep[i] = true; }
            }
            let mut idx = 0;
            result_rows.retain(|_| { let k = keep[idx]; idx += 1; k });
        }
    }
    Ok(QueryResult { tag: format!("SELECT {}", result_rows.len()), columns: result_columns, rows: result_rows })
}
