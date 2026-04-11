use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};

use super::expr::eval_expr;
use super::filter::eval_comparison_op;
use super::select::exec_select_raw;
use super::types::JoinContext;

/// Evaluate SubLink (subquery expressions: EXISTS, IN, ALL, scalar).
pub(crate) fn eval_sublink(
    sl: &pg_query::protobuf::SubLink, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    let inner = sl.subselect.as_ref().and_then(|n| n.node.as_ref()).ok_or("SubLink missing subselect")?;
    match inner {
        NodeEnum::SelectStmt(sel) => {
            let sub_type = sl.sub_link_type;
            if sub_type == pg_query::protobuf::SubLinkType::ExistsSublink as i32 {
                let (_cols, inner_rows) = exec_select_raw(sel, Some((row, ctx)), arena)?;
                return Ok(ArenaValue::Bool(!inner_rows.is_empty()));
            }
            if sub_type == pg_query::protobuf::SubLinkType::AnySublink as i32 {
                return eval_any_sublink(sl, sel, row, ctx, arena);
            }
            if sub_type == pg_query::protobuf::SubLinkType::AllSublink as i32 {
                return eval_all_sublink(sl, sel, row, ctx, arena);
            }
            if sub_type == pg_query::protobuf::SubLinkType::ExprSublink as i32 {
                let (_cols, inner_rows) = exec_select_raw(sel, Some((row, ctx)), arena)?;
                if inner_rows.len() > 1 {
                    return Err("more than one row returned by a subquery used as an expression".into());
                }
                return Ok(inner_rows.first().and_then(|r| r.first()).copied().unwrap_or(ArenaValue::Null));
            }
            Err(format!("unsupported subquery type: {}", sub_type))
        }
        _ => Err("SubLink subselect is not a SELECT".into()),
    }
}

fn eval_any_sublink(
    sl: &pg_query::protobuf::SubLink, sel: &pg_query::protobuf::SelectStmt,
    row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    let test_node = sl.testexpr.as_ref().and_then(|n| n.node.as_ref())
        .ok_or("IN subquery missing test expression")?;
    let test_val = eval_expr(test_node, row, ctx, arena)?;
    let (cols, inner_rows) = exec_select_raw(sel, Some((row, ctx)), arena)?;
    if !cols.is_empty() && cols.len() != 1 { return Err("subquery must return only one column".into()); }
    if test_val.is_null() { return Ok(ArenaValue::Null); }
    let mut has_null = false;
    for inner_row in &inner_rows {
        let inner_val = inner_row.first().copied().unwrap_or(ArenaValue::Null);
        if inner_val.is_null() { has_null = true; continue; }
        let is_eq = test_val.eq_with(&inner_val, arena)
            || test_val.compare(&inner_val, arena) == Some(std::cmp::Ordering::Equal);
        if is_eq { return Ok(ArenaValue::Bool(true)); }
    }
    Ok(if has_null { ArenaValue::Null } else { ArenaValue::Bool(false) })
}

fn eval_all_sublink(
    sl: &pg_query::protobuf::SubLink, sel: &pg_query::protobuf::SelectStmt,
    row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    let test_node = sl.testexpr.as_ref().and_then(|n| n.node.as_ref())
        .ok_or("ALL subquery missing test expression")?;
    let test_val = eval_expr(test_node, row, ctx, arena)?;
    let (_cols, inner_rows) = exec_select_raw(sel, Some((row, ctx)), arena)?;
    if test_val.is_null() {
        return Ok(if inner_rows.is_empty() { ArenaValue::Bool(true) } else { ArenaValue::Null });
    }
    let op = sl.oper_name.iter()
        .filter_map(|n| n.node.as_ref())
        .filter_map(|n| if let NodeEnum::String(s) = n { Some(s.sval.clone()) } else { None })
        .next()
        .unwrap_or_else(|| "=".into());
    let mut has_null = false;
    for inner_row in &inner_rows {
        let inner_val = inner_row.first().copied().unwrap_or(ArenaValue::Null);
        let cmp_result = eval_comparison_op(&op, &test_val, &inner_val, arena)?;
        match cmp_result {
            ArenaValue::Bool(true) => continue,
            ArenaValue::Bool(false) => return Ok(ArenaValue::Bool(false)),
            _ => has_null = true,
        }
    }
    Ok(if has_null { ArenaValue::Null } else { ArenaValue::Bool(true) })
}
