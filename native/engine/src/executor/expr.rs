use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};
use super::helpers::parse_vector_literal;
use super::expr_ops::eval_a_expr;
use super::expr_logic::{eval_bool_expr, eval_case_expr};
use super::func::eval_func_call;
use super::resolve::resolve_column;
use super::types::JoinContext;

/// Core expression evaluator. Dispatches to specialized evaluators.
#[inline(always)]
pub(crate) fn eval_expr(
    node: &NodeEnum, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    match node {
        NodeEnum::ColumnRef(cref) => {
            let idx = resolve_column(cref, ctx)?;
            if idx < row.len() { Ok(row[idx]) }
            else if ctx.total_columns == 0 { Ok(ArenaValue::Null) }
            else { Err(format!("internal error: column index {} out of range for row of width {}", idx, row.len())) }
        }
        NodeEnum::AConst(ac) => eval_aconst(ac, arena),
        NodeEnum::Integer(i) => Ok(ArenaValue::Int(i.ival as i64)),
        NodeEnum::Float(f) => f.fval.parse::<f64>().map(ArenaValue::Float).map_err(|e| e.to_string()),
        NodeEnum::String(s) => eval_string_literal(s, arena),
        NodeEnum::TypeCast(tc) => super::expr_cast::eval_type_cast(tc, row, ctx, arena),
        NodeEnum::AExpr(expr) => eval_a_expr(expr, row, ctx, arena),
        NodeEnum::BoolExpr(bexpr) => eval_bool_expr(bexpr, row, ctx, arena),
        NodeEnum::NullTest(nt) => eval_null_test(nt, row, ctx, arena),
        NodeEnum::CaseExpr(case_expr) => eval_case_expr(case_expr, row, ctx, arena),
        NodeEnum::CoalesceExpr(ce) => eval_coalesce(ce, row, ctx, arena),
        NodeEnum::NullIfExpr(ni) => eval_nullif_expr(ni, row, ctx, arena),
        NodeEnum::FuncCall(fc) => eval_func_call(fc, row, ctx, arena),
        NodeEnum::SubLink(sl) => super::sublink::eval_sublink(sl, row, ctx, arena),
        _ => Err(format!("unsupported expression node: {:?}", std::mem::discriminant(node))),
    }
}

fn eval_aconst(ac: &pg_query::protobuf::AConst, arena: &mut QueryArena) -> Result<ArenaValue, String> {
    if ac.isnull { return Ok(ArenaValue::Null); }
    if let Some(val) = &ac.val {
        match val {
            pg_query::protobuf::a_const::Val::Ival(i) => Ok(ArenaValue::Int(i.ival as i64)),
            pg_query::protobuf::a_const::Val::Fval(f) => f.fval.parse::<f64>().map(ArenaValue::Float).map_err(|e| e.to_string()),
            pg_query::protobuf::a_const::Val::Sval(s) => {
                let trimmed = s.sval.trim();
                if trimmed.starts_with('[') && trimmed.ends_with(']') {
                    let v = parse_vector_literal(trimmed)?;
                    if let crate::types::Value::Vector(data) = v { Ok(ArenaValue::Vector(arena.alloc_vec(&data))) }
                    else { Ok(ArenaValue::Null) }
                } else { Ok(ArenaValue::Text(arena.alloc_str(&s.sval))) }
            }
            pg_query::protobuf::a_const::Val::Bsval(s) => Ok(ArenaValue::Text(arena.alloc_str(&s.bsval))),
            pg_query::protobuf::a_const::Val::Boolval(b) => Ok(ArenaValue::Bool(b.boolval)),
        }
    } else { Ok(ArenaValue::Null) }
}

fn eval_string_literal(s: &pg_query::protobuf::String, arena: &mut QueryArena) -> Result<ArenaValue, String> {
    let trimmed = s.sval.trim();
    if trimmed.starts_with('[') && trimmed.ends_with(']') {
        let v = parse_vector_literal(trimmed)?;
        if let crate::types::Value::Vector(data) = v { Ok(ArenaValue::Vector(arena.alloc_vec(&data))) }
        else { Ok(ArenaValue::Null) }
    } else { Ok(ArenaValue::Text(arena.alloc_str(&s.sval))) }
}

fn eval_null_test(
    nt: &pg_query::protobuf::NullTest, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    let inner = nt.arg.as_ref().and_then(|a| a.node.as_ref()).ok_or("NullTest missing arg")?;
    let val = eval_expr(inner, row, ctx, arena)?;
    let is_null = val.is_null();
    if nt.nulltesttype == pg_query::protobuf::NullTestType::IsNull as i32 { Ok(ArenaValue::Bool(is_null)) }
    else { Ok(ArenaValue::Bool(!is_null)) }
}

fn eval_coalesce(
    ce: &pg_query::protobuf::CoalesceExpr, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    for arg in &ce.args {
        if let Some(ref node) = arg.node {
            let val = eval_expr(node, row, ctx, arena)?;
            if !val.is_null() { return Ok(val); }
        }
    }
    Ok(ArenaValue::Null)
}

fn eval_nullif_expr(
    ni: &pg_query::protobuf::NullIfExpr, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    if ni.args.len() != 2 { return Err("NULLIF requires exactly 2 arguments".into()); }
    let a = eval_expr(ni.args[0].node.as_ref().ok_or("NULLIF missing arg1")?, row, ctx, arena)?;
    let b = eval_expr(ni.args[1].node.as_ref().ok_or("NULLIF missing arg2")?, row, ctx, arena)?;
    if a.is_null() || b.is_null() { Ok(a) }
    else if a.eq_with(&b, arena) || a.compare(&b, arena) == Some(std::cmp::Ordering::Equal) { Ok(ArenaValue::Null) }
    else { Ok(a) }
}
