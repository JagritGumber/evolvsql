use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};

use super::expr::eval_expr;
use super::types::JoinContext;

/// Evaluate boolean expressions (AND, OR, NOT) with three-valued logic.
pub(crate) fn eval_bool_expr(
    bexpr: &pg_query::protobuf::BoolExpr, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    let boolop = bexpr.boolop;
    if boolop == pg_query::protobuf::BoolExprType::AndExpr as i32 {
        let mut has_null = false;
        for arg in &bexpr.args {
            let inner = arg.node.as_ref().ok_or("BoolExpr: missing arg")?;
            match eval_expr(inner, row, ctx, arena)? {
                ArenaValue::Bool(false) => return Ok(ArenaValue::Bool(false)),
                ArenaValue::Null => has_null = true,
                ArenaValue::Bool(true) => {}
                other => return Err(format!("AND expects bool, got {:?}", other)),
            }
        }
        Ok(if has_null { ArenaValue::Null } else { ArenaValue::Bool(true) })
    } else if boolop == pg_query::protobuf::BoolExprType::OrExpr as i32 {
        let mut has_null = false;
        for arg in &bexpr.args {
            let inner = arg.node.as_ref().ok_or("BoolExpr: missing arg")?;
            match eval_expr(inner, row, ctx, arena)? {
                ArenaValue::Bool(true) => return Ok(ArenaValue::Bool(true)),
                ArenaValue::Null => has_null = true,
                ArenaValue::Bool(false) => {}
                other => return Err(format!("OR expects bool, got {:?}", other)),
            }
        }
        Ok(if has_null { ArenaValue::Null } else { ArenaValue::Bool(false) })
    } else if boolop == pg_query::protobuf::BoolExprType::NotExpr as i32 {
        let inner = bexpr.args.first().and_then(|a| a.node.as_ref()).ok_or("NOT missing arg")?;
        match eval_expr(inner, row, ctx, arena)? {
            ArenaValue::Bool(b) => Ok(ArenaValue::Bool(!b)),
            ArenaValue::Null => Ok(ArenaValue::Null),
            other => Err(format!("NOT expects bool, got {:?}", other)),
        }
    } else {
        Err(format!("unsupported BoolExpr op: {}", boolop))
    }
}

/// Evaluate CASE WHEN expressions (simple and searched forms).
pub(crate) fn eval_case_expr(
    case_expr: &pg_query::protobuf::CaseExpr, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    let test_val = if let Some(ref arg) = case_expr.arg {
        if let Some(ref node) = arg.node { Some(eval_expr(node, row, ctx, arena)?) } else { None }
    } else { None };

    for when_node in &case_expr.args {
        if let Some(NodeEnum::CaseWhen(when)) = when_node.node.as_ref() {
            let cond_node = when.expr.as_ref().and_then(|n| n.node.as_ref()).ok_or("CASE WHEN missing condition")?;
            let matches = if let Some(ref tv) = test_val {
                let when_val = eval_expr(cond_node, row, ctx, arena)?;
                if tv.is_null() || when_val.is_null() { false }
                else { tv.eq_with(&when_val, arena) || tv.compare(&when_val, arena) == Some(std::cmp::Ordering::Equal) }
            } else {
                match eval_expr(cond_node, row, ctx, arena)? {
                    ArenaValue::Bool(b) => b,
                    ArenaValue::Null => false,
                    _ => false,
                }
            };
            if matches {
                let result_node = when.result.as_ref().and_then(|n| n.node.as_ref()).ok_or("CASE WHEN missing result")?;
                return eval_expr(result_node, row, ctx, arena);
            }
        }
    }
    if let Some(ref def) = case_expr.defresult {
        if let Some(ref node) = def.node { return eval_expr(node, row, ctx, arena); }
    }
    Ok(ArenaValue::Null)
}
