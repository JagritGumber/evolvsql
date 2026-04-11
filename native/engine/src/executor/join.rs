use std::collections::HashMap;
use std::hash::{DefaultHasher, Hasher};

use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};

use super::expr::eval_expr;
use super::resolve::resolve_column;
use super::types::JoinContext;

/// Try to execute an equi-join as a hash join (O(N+M)).
pub(crate) fn try_equi_hash_join(
    left_rows: &[Vec<ArenaValue>], right_rows: &[Vec<ArenaValue>],
    join_type: i32, quals: Option<&NodeEnum>, ctx: &JoinContext,
    left_width: usize, right_width: usize, arena: &mut QueryArena,
) -> Option<Result<Vec<Vec<ArenaValue>>, String>> {
    let quals = quals?;
    let (left_col, right_col) = extract_equi_cols(quals, ctx)?;
    Some(execute_hash_join(left_rows, right_rows, join_type, left_width, right_width, left_col, right_col, arena))
}

/// Extract column indices for a simple equi-join: ON a.x = b.y.
fn extract_equi_cols(quals: &NodeEnum, ctx: &JoinContext) -> Option<(usize, usize)> {
    match quals {
        NodeEnum::AExpr(expr) => {
            let op = expr.name.iter()
                .filter_map(|n| n.node.as_ref())
                .filter_map(|n| if let NodeEnum::String(s) = n { Some(s.sval.as_str()) } else { None })
                .next()?;
            if op != "=" { return None; }
            let left_node = expr.lexpr.as_ref()?.node.as_ref()?;
            let right_node = expr.rexpr.as_ref()?.node.as_ref()?;
            if let (NodeEnum::ColumnRef(lcref), NodeEnum::ColumnRef(rcref)) = (left_node, right_node) {
                let li = resolve_column(lcref, ctx).ok()?;
                let ri = resolve_column(rcref, ctx).ok()?;
                Some((li, ri))
            } else { None }
        }
        NodeEnum::BoolExpr(be) if be.boolop == pg_query::protobuf::BoolExprType::AndExpr as i32 => None,
        _ => None,
    }
}

/// Hash join: build hash table on right side, probe with left side. O(N+M).
fn execute_hash_join(
    left_rows: &[Vec<ArenaValue>], right_rows: &[Vec<ArenaValue>],
    join_type: i32, left_width: usize, right_width: usize,
    left_key_col: usize, right_key_col: usize, arena: &mut QueryArena,
) -> Result<Vec<Vec<ArenaValue>>, String> {
    let null_right = vec![ArenaValue::Null; right_width];
    let null_left = vec![ArenaValue::Null; left_width];
    let is_inner = join_type == pg_query::protobuf::JoinType::JoinInner as i32
        || join_type == pg_query::protobuf::JoinType::Undefined as i32;
    let is_left = join_type == pg_query::protobuf::JoinType::JoinLeft as i32;
    let is_right = join_type == pg_query::protobuf::JoinType::JoinRight as i32;
    let is_full = join_type == pg_query::protobuf::JoinType::JoinFull as i32;
    if !is_inner && !is_left && !is_right && !is_full {
        return Err(format!("unsupported JOIN type: {}", join_type));
    }
    let (left_key_col, right_key_col) = if left_key_col < left_width && right_key_col >= left_width {
        (left_key_col, right_key_col)
    } else if right_key_col < left_width && left_key_col >= left_width {
        (right_key_col, left_key_col)
    } else {
        return Err("JOIN ON condition references columns from the same table on both sides".into());
    };
    let right_local_key = right_key_col - left_width;
    let mut hash_table: HashMap<u64, Vec<usize>> = HashMap::new();
    for (i, row) in right_rows.iter().enumerate() {
        if right_local_key < row.len() {
            let key = row[right_local_key];
            if key.is_null() { continue; }
            let mut hasher = DefaultHasher::new();
            key.hash_with(arena, &mut hasher);
            hash_table.entry(hasher.finish()).or_default().push(i);
        }
    }
    let combined_width = left_width + right_width;
    let mut result = Vec::with_capacity(if is_inner { left_rows.len() * 2 } else { left_rows.len() });
    let mut right_matched = if is_right || is_full { vec![false; right_rows.len()] } else { Vec::new() };
    for left in left_rows {
        if left_key_col >= left.len() { continue; }
        let key = left[left_key_col];
        if key.is_null() {
            if is_left || is_full { let mut row = Vec::with_capacity(combined_width); row.extend_from_slice(left); row.extend_from_slice(&null_right); result.push(row); }
            continue;
        }
        let mut left_matched = false;
        let mut hasher = DefaultHasher::new();
        key.hash_with(arena, &mut hasher);
        if let Some(indices) = hash_table.get(&hasher.finish()) {
            for &ri in indices {
                if !key.eq_with(&right_rows[ri][right_local_key], arena) { continue; }
                left_matched = true;
                if !right_matched.is_empty() { right_matched[ri] = true; }
                let mut combined = Vec::with_capacity(combined_width);
                combined.extend_from_slice(left);
                combined.extend_from_slice(&right_rows[ri]);
                result.push(combined);
            }
        }
        if !left_matched && (is_left || is_full) {
            let mut row = Vec::with_capacity(combined_width); row.extend_from_slice(left); row.extend_from_slice(&null_right); result.push(row);
        }
    }
    if is_right || is_full {
        for (ri, right) in right_rows.iter().enumerate() {
            if !right_matched[ri] { let mut row = null_left.clone(); row.extend_from_slice(right); result.push(row); }
        }
    }
    Ok(result)
}

/// Nested loop join with WHERE clause filtering.
pub(crate) fn nested_loop_join(
    left_rows: &[Vec<ArenaValue>], right_rows: &[Vec<ArenaValue>],
    join_type: i32, quals: Option<&NodeEnum>, ctx: &JoinContext,
    left_width: usize, right_width: usize, arena: &mut QueryArena,
) -> Result<Vec<Vec<ArenaValue>>, String> {
    let null_right = vec![ArenaValue::Null; right_width];
    let null_left = vec![ArenaValue::Null; left_width];
    let mut result = Vec::with_capacity(left_rows.len());
    let is_inner = join_type == pg_query::protobuf::JoinType::JoinInner as i32 || join_type == pg_query::protobuf::JoinType::Undefined as i32;
    let is_left = join_type == pg_query::protobuf::JoinType::JoinLeft as i32;
    let is_right = join_type == pg_query::protobuf::JoinType::JoinRight as i32;
    let is_full = join_type == pg_query::protobuf::JoinType::JoinFull as i32;
    if !is_inner && !is_left && !is_right && !is_full { return Err(format!("unsupported JOIN type: {}", join_type)); }
    let mut right_matched = if is_right || is_full { vec![false; right_rows.len()] } else { Vec::new() };
    for left in left_rows {
        let mut left_matched = false;
        for (ri, right) in right_rows.iter().enumerate() {
            let mut combined = Vec::with_capacity(left_width + right_width);
            combined.extend_from_slice(left); combined.extend_from_slice(right);
            let matches = match quals {
                Some(q) => matches!(eval_expr(q, &combined, ctx, arena)?, ArenaValue::Bool(true)),
                None => true,
            };
            if matches {
                left_matched = true;
                if !right_matched.is_empty() { right_matched[ri] = true; }
                result.push(combined);
            }
        }
        if !left_matched && (is_left || is_full) { let mut row = left.clone(); row.extend_from_slice(&null_right); result.push(row); }
    }
    if is_right || is_full {
        for (ri, right) in right_rows.iter().enumerate() {
            if !right_matched[ri] { let mut row = null_left.clone(); row.extend_from_slice(right); result.push(row); }
        }
    }
    Ok(result)
}
