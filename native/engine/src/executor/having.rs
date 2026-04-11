use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};

use super::aggregate_compute::compute_aggregate;
use super::expr::eval_expr;
use super::helpers::extract_op_name;
use super::resolve::extract_func_name;
use super::aggregate::is_aggregate;
use super::types::JoinContext;

/// Evaluate HAVING clause on aggregated group.
pub(crate) fn eval_having(
    expr: &NodeEnum, group_rows: &[Vec<ArenaValue>], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<bool, String> {
    Ok(matches!(eval_having_expr(expr, group_rows, ctx, arena)?, ArenaValue::Bool(true)))
}

fn eval_having_expr(
    node: &NodeEnum, group_rows: &[Vec<ArenaValue>], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    match node {
        NodeEnum::FuncCall(fc) => {
            let name = extract_func_name(fc);
            if is_aggregate(&name) { compute_aggregate(&name, fc, group_rows, ctx, arena) }
            else { Err(format!("non-aggregate function in HAVING: {}", name)) }
        }
        NodeEnum::AExpr(expr) => {
            let op = extract_op_name(&expr.name)?;
            let left = expr.lexpr.as_ref().and_then(|n| n.node.as_ref()).map(|n| eval_having_expr(n, group_rows, ctx, arena)).transpose()?;
            let right = expr.rexpr.as_ref().and_then(|n| n.node.as_ref()).map(|n| eval_having_expr(n, group_rows, ctx, arena)).transpose()?;
            let (l, r) = match (left, right) { (Some(l), Some(r)) => (l, r), _ => return Ok(ArenaValue::Null) };
            if l.is_null() || r.is_null() { return Ok(ArenaValue::Null); }
            let cmp = l.compare(&r, arena);
            let result = match op.as_str() {
                "=" => cmp.map(|o| o == std::cmp::Ordering::Equal),
                "<>" | "!=" => cmp.map(|o| o != std::cmp::Ordering::Equal),
                "<" => cmp.map(|o| o == std::cmp::Ordering::Less),
                ">" => cmp.map(|o| o == std::cmp::Ordering::Greater),
                "<=" => cmp.map(|o| o != std::cmp::Ordering::Greater),
                ">=" => cmp.map(|o| o != std::cmp::Ordering::Less),
                _ => return Err(format!("unsupported operator in HAVING: {}", op)),
            };
            Ok(result.map(ArenaValue::Bool).unwrap_or(ArenaValue::Null))
        }
        NodeEnum::AConst(_) | NodeEnum::Integer(_) | NodeEnum::Float(_) => {
            let dummy_ctx = JoinContext { sources: vec![], total_columns: 0 };
            eval_expr(node, &[], &dummy_ctx, arena)
        }
        NodeEnum::BoolExpr(be) => eval_having_bool(be, group_rows, ctx, arena),
        NodeEnum::ColumnRef(_) => {
            if group_rows.is_empty() { return Ok(ArenaValue::Null); }
            eval_expr(node, &group_rows[0], ctx, arena)
        }
        _ => Err("unsupported expression in HAVING clause".into()),
    }
}

fn eval_having_bool(
    be: &pg_query::protobuf::BoolExpr, group_rows: &[Vec<ArenaValue>], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    let boolop = be.boolop;
    if boolop == pg_query::protobuf::BoolExprType::AndExpr as i32 {
        let mut has_null = false;
        for arg in &be.args {
            if let Some(n) = arg.node.as_ref() {
                match eval_having_expr(n, group_rows, ctx, arena)? {
                    ArenaValue::Bool(false) => return Ok(ArenaValue::Bool(false)),
                    ArenaValue::Bool(true) => continue,
                    _ => { has_null = true; continue; }
                }
            }
        }
        Ok(if has_null { ArenaValue::Null } else { ArenaValue::Bool(true) })
    } else if boolop == pg_query::protobuf::BoolExprType::OrExpr as i32 {
        let mut has_null = false;
        for arg in &be.args {
            if let Some(n) = arg.node.as_ref() {
                match eval_having_expr(n, group_rows, ctx, arena)? {
                    ArenaValue::Bool(true) => return Ok(ArenaValue::Bool(true)),
                    ArenaValue::Bool(false) => continue,
                    _ => { has_null = true; continue; }
                }
            }
        }
        Ok(if has_null { ArenaValue::Null } else { ArenaValue::Bool(false) })
    } else {
        Err("unsupported HAVING boolean expression".into())
    }
}
