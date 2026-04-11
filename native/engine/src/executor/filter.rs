use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};
use crate::types::Value;

use super::expr::eval_expr;
use super::helpers::eval_const;
use super::resolve::resolve_column;
use super::types::{FastEqualityFilter, JoinContext};

/// Filter rows by WHERE clause, returns boolean result.
pub(crate) fn eval_where(
    where_clause: &Option<Box<pg_query::protobuf::Node>>, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<bool, String> {
    match where_clause {
        Some(wc) => match wc.node.as_ref() {
            Some(expr) => match eval_expr(expr, row, ctx, arena)? {
                ArenaValue::Bool(b) => Ok(b),
                ArenaValue::Null => Ok(false),
                _ => Err("WHERE clause must return boolean".into()),
            },
            None => Ok(true),
        },
        None => Ok(true),
    }
}

/// DML bridge: evaluate WHERE clause on Value rows using a shared arena.
pub(crate) fn eval_where_value(
    where_clause: &Option<Box<pg_query::protobuf::Node>>, row: &[Value], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<bool, String> {
    match where_clause {
        Some(wc) => match wc.node.as_ref() {
            Some(expr) => {
                let arena_row: Vec<ArenaValue> = row.iter().map(|v| ArenaValue::from_value(v, arena)).collect();
                match eval_expr(expr, &arena_row, ctx, arena)? {
                    ArenaValue::Bool(b) => Ok(b),
                    ArenaValue::Null => Ok(false),
                    _ => Err("WHERE clause must return boolean".into()),
                }
            }
            None => Ok(true),
        },
        None => Ok(true),
    }
}

/// DML bridge: evaluate expression on Value rows using a shared arena.
pub(crate) fn eval_expr_value(
    node: &NodeEnum, row: &[Value], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<Value, String> {
    let arena_row: Vec<ArenaValue> = row.iter().map(|v| ArenaValue::from_value(v, arena)).collect();
    let result = eval_expr(node, &arena_row, ctx, arena)?;
    Ok(result.to_value(arena))
}

/// Comparison operator evaluation (for ALL subqueries).
#[inline(always)]
pub(crate) fn eval_comparison_op(op: &str, left: &ArenaValue, right: &ArenaValue, arena: &QueryArena) -> Result<ArenaValue, String> {
    if left.is_null() || right.is_null() { return Ok(ArenaValue::Null); }
    let cmp = left.compare(right, arena);
    let result = match op {
        "=" => cmp.map(|o| o == std::cmp::Ordering::Equal),
        "<>" | "!=" => cmp.map(|o| o != std::cmp::Ordering::Equal),
        "<" => cmp.map(|o| o == std::cmp::Ordering::Less),
        ">" => cmp.map(|o| o == std::cmp::Ordering::Greater),
        "<=" => cmp.map(|o| o != std::cmp::Ordering::Greater),
        ">=" => cmp.map(|o| o != std::cmp::Ordering::Less),
        _ => return Err(format!("unsupported operator in ALL: {}", op)),
    };
    Ok(result.map(ArenaValue::Bool).unwrap_or(ArenaValue::Null))
}

/// Try to extract a fast equality filter from a WHERE clause.
pub(crate) fn try_fast_equality_filter(
    where_clause: &Option<Box<pg_query::protobuf::Node>>, ctx: &JoinContext, arena: &mut QueryArena,
) -> Option<FastEqualityFilter> {
    let wc = where_clause.as_ref()?;
    let node = wc.node.as_ref()?;
    if let NodeEnum::AExpr(expr) = node {
        let op = super::helpers::extract_op_name(&expr.name).ok()?;
        if op != "=" { return None; }
        let left = expr.lexpr.as_ref()?.node.as_ref()?;
        let right = expr.rexpr.as_ref()?.node.as_ref()?;
        if let NodeEnum::ColumnRef(cref) = left {
            let col_idx = resolve_column(cref, ctx).ok()?;
            let val = eval_const(Some(right));
            if matches!(val, Value::Null) { return None; }
            return Some(FastEqualityFilter { col_idx, value: ArenaValue::from_value(&val, arena) });
        }
        if let NodeEnum::ColumnRef(cref) = right {
            let col_idx = resolve_column(cref, ctx).ok()?;
            let val = eval_const(Some(left));
            if matches!(val, Value::Null) { return None; }
            return Some(FastEqualityFilter { col_idx, value: ArenaValue::from_value(&val, arena) });
        }
    }
    None
}

/// Deduplicate rows when DISTINCT is present.
pub(crate) fn dedup_distinct(
    distinct_clause: &[pg_query::protobuf::Node], rows: Vec<Vec<ArenaValue>>, arena: &QueryArena,
) -> Vec<Vec<ArenaValue>> {
    if distinct_clause.is_empty() { return rows; }
    let mut unique: Vec<Vec<ArenaValue>> = Vec::new();
    'outer: for row in rows {
        for existing in &unique {
            if existing.len() == row.len() && existing.iter().zip(row.iter()).all(|(a, b)| a.eq_with(b, arena)) {
                continue 'outer;
            }
        }
        unique.push(row);
    }
    unique
}
