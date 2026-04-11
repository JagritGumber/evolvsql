use crate::arena::{ArenaValue, QueryArena};

use super::expr::eval_expr;
use super::types::JoinContext;

/// Compute aggregate function result (COUNT, SUM, AVG, MIN, MAX, STRING_AGG, BOOL_AND/OR).
pub(crate) fn compute_aggregate(
    name: &str, fc: &pg_query::protobuf::FuncCall,
    rows: &[Vec<ArenaValue>], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    match name {
        "count" => compute_count(fc, rows, ctx, arena),
        "sum" => compute_sum(fc, rows, ctx, arena),
        "avg" => compute_avg(fc, rows, ctx, arena),
        "min" => compute_min_max(fc, rows, ctx, arena, true),
        "max" => compute_min_max(fc, rows, ctx, arena, false),
        "string_agg" => compute_string_agg(fc, rows, ctx, arena),
        "bool_and" => compute_bool_agg(fc, rows, ctx, arena, true),
        "bool_or" => compute_bool_agg(fc, rows, ctx, arena, false),
        _ => Err(format!("unknown aggregate function: {}", name)),
    }
}

fn compute_count(fc: &pg_query::protobuf::FuncCall, rows: &[Vec<ArenaValue>], ctx: &JoinContext, arena: &mut QueryArena) -> Result<ArenaValue, String> {
    if fc.agg_star { return Ok(ArenaValue::Int(rows.len() as i64)); }
    let arg = fc.args.first().and_then(|a| a.node.as_ref()).ok_or("COUNT requires argument")?;
    if fc.agg_distinct {
        let mut seen: Vec<ArenaValue> = Vec::new();
        for row in rows {
            let v = eval_expr(arg, row, ctx, arena)?;
            if v.is_null() { continue; }
            if !seen.iter().any(|s| s.eq_with(&v, arena)) { seen.push(v); }
        }
        Ok(ArenaValue::Int(seen.len() as i64))
    } else {
        let mut count: i64 = 0;
        for row in rows { if !eval_expr(arg, row, ctx, arena)?.is_null() { count += 1; } }
        Ok(ArenaValue::Int(count))
    }
}

fn compute_sum(fc: &pg_query::protobuf::FuncCall, rows: &[Vec<ArenaValue>], ctx: &JoinContext, arena: &mut QueryArena) -> Result<ArenaValue, String> {
    let arg = fc.args.first().and_then(|a| a.node.as_ref()).ok_or("SUM requires argument")?;
    let mut vals: Vec<ArenaValue> = Vec::new();
    for row in rows {
        let v = eval_expr(arg, row, ctx, arena)?;
        if v.is_null() { continue; }
        if fc.agg_distinct && vals.iter().any(|s| s.eq_with(&v, arena)) { continue; }
        vals.push(v);
    }
    if vals.is_empty() { return Ok(ArenaValue::Null); }
    let (mut si, mut sf, mut is_float) = (0i64, 0.0f64, false);
    for v in &vals {
        match v {
            ArenaValue::Int(n) => { si = si.checked_add(*n).ok_or("integer out of range")?; }
            ArenaValue::Float(f) => { sf += f; is_float = true; }
            _ => return Err("SUM requires numeric".into()),
        }
    }
    Ok(if is_float { ArenaValue::Float(sf + si as f64) } else { ArenaValue::Int(si) })
}

fn compute_avg(fc: &pg_query::protobuf::FuncCall, rows: &[Vec<ArenaValue>], ctx: &JoinContext, arena: &mut QueryArena) -> Result<ArenaValue, String> {
    let arg = fc.args.first().and_then(|a| a.node.as_ref()).ok_or("AVG requires argument")?;
    let (mut sum, mut count) = (0.0f64, 0i64);
    for row in rows {
        match eval_expr(arg, row, ctx, arena)? {
            ArenaValue::Int(n) => { sum += n as f64; count += 1; }
            ArenaValue::Float(f) => { sum += f; count += 1; }
            ArenaValue::Null => {}
            _ => return Err("AVG requires numeric".into()),
        }
    }
    Ok(if count == 0 { ArenaValue::Null } else { ArenaValue::Float(sum / count as f64) })
}

fn compute_min_max(fc: &pg_query::protobuf::FuncCall, rows: &[Vec<ArenaValue>], ctx: &JoinContext, arena: &mut QueryArena, is_min: bool) -> Result<ArenaValue, String> {
    let arg = fc.args.first().and_then(|a| a.node.as_ref()).ok_or(if is_min { "MIN requires argument" } else { "MAX requires argument" })?;
    let mut result: Option<ArenaValue> = None;
    let target_ord = if is_min { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater };
    for row in rows {
        let v = eval_expr(arg, row, ctx, arena)?;
        if v.is_null() { continue; }
        result = Some(match result { None => v, Some(cur) => if v.compare(&cur, arena) == Some(target_ord) { v } else { cur } });
    }
    Ok(result.unwrap_or(ArenaValue::Null))
}

fn compute_string_agg(fc: &pg_query::protobuf::FuncCall, rows: &[Vec<ArenaValue>], ctx: &JoinContext, arena: &mut QueryArena) -> Result<ArenaValue, String> {
    let arg = fc.args.first().and_then(|a| a.node.as_ref()).ok_or("STRING_AGG requires argument")?;
    let delim = fc.args.get(1).and_then(|a| a.node.as_ref())
        .map(|d| eval_expr(d, &rows[0], ctx, arena).ok().and_then(|v| v.to_text(arena)).unwrap_or_else(|| ", ".to_string()))
        .unwrap_or_else(|| ", ".to_string());
    let mut parts: Vec<String> = Vec::new();
    for row in rows { let v = eval_expr(arg, row, ctx, arena)?; if let Some(text) = v.to_text(arena) { parts.push(text); } }
    if parts.is_empty() { Ok(ArenaValue::Null) } else {
        if !fc.agg_order.is_empty() { parts.sort(); }
        Ok(ArenaValue::Text(arena.alloc_str(&parts.join(&delim))))
    }
}

fn compute_bool_agg(fc: &pg_query::protobuf::FuncCall, rows: &[Vec<ArenaValue>], ctx: &JoinContext, arena: &mut QueryArena, is_and: bool) -> Result<ArenaValue, String> {
    let arg = fc.args.first().and_then(|a| a.node.as_ref()).ok_or(if is_and { "BOOL_AND requires argument" } else { "BOOL_OR requires argument" })?;
    let mut result = is_and;
    let mut has_val = false;
    for row in rows {
        match eval_expr(arg, row, ctx, arena)? {
            ArenaValue::Bool(b) => { result = if is_and { result && b } else { result || b }; has_val = true; }
            ArenaValue::Null => {}
            _ => return Err(if is_and { "BOOL_AND requires boolean".into() } else { "BOOL_OR requires boolean".into() }),
        }
    }
    if !has_val { Ok(ArenaValue::Null) } else { Ok(ArenaValue::Bool(result)) }
}
