use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};

use super::expr::eval_expr;
use super::helpers::extract_op_name;
use super::expr_ops_ext::{eval_nullif, eval_between, eval_unary_minus, eval_concat, eval_vector_op};
use super::types::JoinContext;

/// Evaluate A_Expr nodes (IN, LIKE, BETWEEN, arithmetic, comparison, vector ops).
pub(crate) fn eval_a_expr(
    expr: &pg_query::protobuf::AExpr, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    if expr.kind == pg_query::protobuf::AExprKind::AexprIn as i32 { return eval_in(expr, row, ctx, arena); }
    if expr.kind == pg_query::protobuf::AExprKind::AexprLike as i32
        || expr.kind == pg_query::protobuf::AExprKind::AexprIlike as i32 {
        return eval_like(expr, row, ctx, arena);
    }
    if expr.kind == pg_query::protobuf::AExprKind::AexprNullif as i32 { return eval_nullif(expr, row, ctx, arena); }
    if expr.kind == pg_query::protobuf::AExprKind::AexprBetween as i32
        || expr.kind == pg_query::protobuf::AExprKind::AexprNotBetween as i32 {
        return eval_between(expr, row, ctx, arena);
    }
    let op = extract_op_name(&expr.name)?;
    if expr.lexpr.is_none() && op == "-" { return eval_unary_minus(expr, row, ctx, arena); }
    let left_node = expr.lexpr.as_ref().and_then(|n| n.node.as_ref()).ok_or("A_Expr missing left operand")?;
    let right_node = expr.rexpr.as_ref().and_then(|n| n.node.as_ref()).ok_or("A_Expr missing right operand")?;
    let left = eval_expr(left_node, row, ctx, arena)?;
    let right = eval_expr(right_node, row, ctx, arena)?;
    if op == "||" { return eval_concat(&left, &right, arena); }
    if matches!(op.as_str(), "+" | "-" | "*" | "/" | "%") { return eval_arithmetic(&op, &left, &right, arena); }
    if let Some(result) = eval_vector_op(&op, &left, &right, arena)? { return Ok(result); }
    if left.is_null() || right.is_null() { return Ok(ArenaValue::Null); }
    let cmp = left.compare(&right, arena);
    let result = match op.as_str() {
        "=" => cmp.map(|o| o == std::cmp::Ordering::Equal),
        "<>" | "!=" => cmp.map(|o| o != std::cmp::Ordering::Equal),
        "<" => cmp.map(|o| o == std::cmp::Ordering::Less),
        ">" => cmp.map(|o| o == std::cmp::Ordering::Greater),
        "<=" => cmp.map(|o| o != std::cmp::Ordering::Greater),
        ">=" => cmp.map(|o| o != std::cmp::Ordering::Less),
        _ => return Err(format!("unsupported operator: {}", op)),
    };
    Ok(result.map(ArenaValue::Bool).unwrap_or(ArenaValue::Null))
}

#[inline(always)]
pub(crate) fn eval_arithmetic(op: &str, left: &ArenaValue, right: &ArenaValue, arena: &QueryArena) -> Result<ArenaValue, String> {
    if left.is_null() || right.is_null() { return Ok(ArenaValue::Null); }
    match (left, right) {
        (ArenaValue::Int(a), ArenaValue::Int(b)) => match op {
            "+" => Ok(ArenaValue::Int(a.checked_add(*b).ok_or("integer out of range")?)),
            "-" => Ok(ArenaValue::Int(a.checked_sub(*b).ok_or("integer out of range")?)),
            "*" => Ok(ArenaValue::Int(a.checked_mul(*b).ok_or("integer out of range")?)),
            "/" => { if *b == 0 { Err("division by zero".into()) } else { Ok(ArenaValue::Int(a.checked_div(*b).ok_or("integer out of range")?)) } }
            "%" => { if *b == 0 { Err("division by zero".into()) } else { Ok(ArenaValue::Int(a.checked_rem(*b).ok_or("integer out of range")?)) } }
            _ => Err(format!("unsupported arithmetic op: {}", op)),
        },
        (ArenaValue::Float(a), ArenaValue::Float(b)) => match op {
            "+" => Ok(ArenaValue::Float(a + b)),
            "-" => Ok(ArenaValue::Float(a - b)),
            "*" => Ok(ArenaValue::Float(a * b)),
            "/" => { if *b == 0.0 { Err("division by zero".into()) } else { Ok(ArenaValue::Float(a / b)) } }
            "%" => { if *b == 0.0 { Err("division by zero".into()) } else { Ok(ArenaValue::Float(a % b)) } }
            _ => Err(format!("unsupported arithmetic op: {}", op)),
        },
        (ArenaValue::Int(a), ArenaValue::Float(b)) => eval_arithmetic(op, &ArenaValue::Float(*a as f64), &ArenaValue::Float(*b), arena),
        (ArenaValue::Float(a), ArenaValue::Int(b)) => eval_arithmetic(op, &ArenaValue::Float(*a), &ArenaValue::Float(*b as f64), arena),
        _ => Err(format!("cannot apply {} to {:?} and {:?}", op, left, right)),
    }
}

fn eval_in(expr: &pg_query::protobuf::AExpr, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena) -> Result<ArenaValue, String> {
    let in_op = extract_op_name(&expr.name).unwrap_or_default();
    let negated = in_op == "<>";
    let left_node = expr.lexpr.as_ref().and_then(|n| n.node.as_ref()).ok_or("IN missing left operand")?;
    let left_val = eval_expr(left_node, row, ctx, arena)?;
    if left_val.is_null() { return Ok(ArenaValue::Null); }
    let right_list = expr.rexpr.as_ref().and_then(|n| n.node.as_ref()).ok_or("IN missing right operand")?;
    if let NodeEnum::List(list) = right_list {
        let mut has_null = false;
        for item in &list.items {
            if let Some(item_node) = item.node.as_ref() {
                let item_val = eval_expr(item_node, row, ctx, arena)?;
                if item_val.is_null() { has_null = true; continue; }
                let is_eq = left_val.eq_with(&item_val, arena) || left_val.compare(&item_val, arena) == Some(std::cmp::Ordering::Equal);
                if is_eq { return Ok(ArenaValue::Bool(!negated)); }
            }
        }
        return Ok(if has_null { ArenaValue::Null } else { ArenaValue::Bool(negated) });
    }
    Err("IN requires a list on the right".into())
}

fn eval_like(expr: &pg_query::protobuf::AExpr, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena) -> Result<ArenaValue, String> {
    let left_node = expr.lexpr.as_ref().and_then(|n| n.node.as_ref()).ok_or("LIKE missing left operand")?;
    let right_node = expr.rexpr.as_ref().and_then(|n| n.node.as_ref()).ok_or("LIKE missing right operand")?;
    let left = eval_expr(left_node, row, ctx, arena)?;
    let right = eval_expr(right_node, row, ctx, arena)?;
    if left.is_null() || right.is_null() { return Ok(ArenaValue::Null); }
    let text = left.to_text(arena).unwrap_or_default();
    let pattern = right.to_text(arena).unwrap_or_default();
    let case_insensitive = expr.kind == pg_query::protobuf::AExprKind::AexprIlike as i32;
    let matched = super::like::sql_like_match(&text, &pattern, case_insensitive);
    let op_name = extract_op_name(&expr.name).unwrap_or_default();
    let negated = op_name.starts_with("!") || op_name == "not like" || op_name == "not ilike";
    Ok(ArenaValue::Bool(if negated { !matched } else { matched }))
}
