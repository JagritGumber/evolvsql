use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};

use super::expr::eval_expr;
use super::types::JoinContext;

pub(crate) fn eval_nullif(
    expr: &pg_query::protobuf::AExpr, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    let left_node = expr.lexpr.as_ref().and_then(|n| n.node.as_ref()).ok_or("NULLIF missing first argument")?;
    let right_node = expr.rexpr.as_ref().and_then(|n| n.node.as_ref()).ok_or("NULLIF missing second argument")?;
    let a = eval_expr(left_node, row, ctx, arena)?;
    let b = eval_expr(right_node, row, ctx, arena)?;
    if a.is_null() { return Ok(ArenaValue::Null); }
    if b.is_null() { return Ok(a); }
    if a.eq_with(&b, arena) || a.compare(&b, arena) == Some(std::cmp::Ordering::Equal) { Ok(ArenaValue::Null) }
    else { Ok(a) }
}

pub(crate) fn eval_between(
    expr: &pg_query::protobuf::AExpr, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    let negated = expr.kind == pg_query::protobuf::AExprKind::AexprNotBetween as i32;
    let left_node = expr.lexpr.as_ref().and_then(|n| n.node.as_ref()).ok_or("BETWEEN missing left operand")?;
    let val = eval_expr(left_node, row, ctx, arena)?;
    if val.is_null() { return Ok(ArenaValue::Null); }
    let bounds = expr.rexpr.as_ref().and_then(|n| n.node.as_ref()).ok_or("BETWEEN missing bounds")?;
    if let NodeEnum::List(list) = bounds {
        if list.items.len() != 2 { return Err("BETWEEN requires exactly two bounds".into()); }
        let low = eval_expr(list.items[0].node.as_ref().ok_or("BETWEEN missing low")?, row, ctx, arena)?;
        let high = eval_expr(list.items[1].node.as_ref().ok_or("BETWEEN missing high")?, row, ctx, arena)?;
        if low.is_null() || high.is_null() { return Ok(ArenaValue::Null); }
        let ge = val.compare(&low, arena).map(|o| o != std::cmp::Ordering::Less).unwrap_or(false);
        let le = val.compare(&high, arena).map(|o| o != std::cmp::Ordering::Greater).unwrap_or(false);
        let in_range = ge && le;
        return Ok(ArenaValue::Bool(if negated { !in_range } else { in_range }));
    }
    Err("BETWEEN bounds must be a list".into())
}

pub(crate) fn eval_unary_minus(
    expr: &pg_query::protobuf::AExpr, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    let right = expr.rexpr.as_ref().and_then(|n| n.node.as_ref()).ok_or("unary - missing operand")?;
    match eval_expr(right, row, ctx, arena)? {
        ArenaValue::Int(n) => Ok(ArenaValue::Int(n.checked_neg().ok_or("integer out of range")?)),
        ArenaValue::Float(f) => Ok(ArenaValue::Float(-f)),
        ArenaValue::Null => Ok(ArenaValue::Null),
        _ => Err("unary minus requires numeric".into()),
    }
}

pub(crate) fn eval_concat(left: &ArenaValue, right: &ArenaValue, arena: &mut QueryArena) -> Result<ArenaValue, String> {
    match (left, right) {
        (ArenaValue::Null, _) | (_, ArenaValue::Null) => Ok(ArenaValue::Null),
        _ => {
            let l = left.to_text(arena).unwrap_or_default();
            let r = right.to_text(arena).unwrap_or_default();
            Ok(ArenaValue::Text(arena.alloc_str(&format!("{}{}", l, r))))
        }
    }
}

pub(crate) fn eval_vector_op(
    op: &str, left: &ArenaValue, right: &ArenaValue, arena: &QueryArena,
) -> Result<Option<ArenaValue>, String> {
    match op {
        "<->" => match (left, right) {
            (ArenaValue::Null, _) | (_, ArenaValue::Null) => Ok(Some(ArenaValue::Null)),
            (ArenaValue::Vector(a), ArenaValue::Vector(b)) => {
                let va = arena.get_vec(*a); let vb = arena.get_vec(*b);
                if va.len() != vb.len() { return Err(format!("different vector dimensions {} and {}", va.len(), vb.len())); }
                let dist_sq: f32 = va.iter().zip(vb.iter()).map(|(x, y)| { let d = x - y; d * d }).sum();
                Ok(Some(ArenaValue::Float(dist_sq.sqrt() as f64)))
            }
            _ => Err("operator <-> requires vector operands".into()),
        },
        "<=>" => match (left, right) {
            (ArenaValue::Null, _) | (_, ArenaValue::Null) => Ok(Some(ArenaValue::Null)),
            (ArenaValue::Vector(a), ArenaValue::Vector(b)) => {
                let va = arena.get_vec(*a); let vb = arena.get_vec(*b);
                if va.len() != vb.len() { return Err(format!("different vector dimensions {} and {}", va.len(), vb.len())); }
                let (dot, na, nb) = va.iter().zip(vb.iter()).fold((0.0f32, 0.0f32, 0.0f32), |(d, na, nb), (x, y)| (d + x * y, na + x * x, nb + y * y));
                let denom = na.sqrt() * nb.sqrt();
                Ok(Some(if denom == 0.0 { ArenaValue::Float(1.0) } else { ArenaValue::Float((1.0 - dot / denom) as f64) }))
            }
            _ => Err("operator <=> requires vector operands".into()),
        },
        "<#>" => match (left, right) {
            (ArenaValue::Null, _) | (_, ArenaValue::Null) => Ok(Some(ArenaValue::Null)),
            (ArenaValue::Vector(a), ArenaValue::Vector(b)) => {
                let va = arena.get_vec(*a); let vb = arena.get_vec(*b);
                if va.len() != vb.len() { return Err(format!("different vector dimensions {} and {}", va.len(), vb.len())); }
                let dot: f32 = va.iter().zip(vb.iter()).map(|(x, y)| x * y).sum();
                Ok(Some(ArenaValue::Float((-dot) as f64)))
            }
            _ => Err("operator <#> requires vector operands".into()),
        },
        _ => Ok(None),
    }
}
