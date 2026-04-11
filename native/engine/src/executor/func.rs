use crate::arena::{ArenaValue, QueryArena};

use super::expr::eval_expr;
use super::helpers::parse_seq_name;
use super::resolve::extract_func_name;
use super::types::JoinContext;

/// Dispatch function calls to eval_scalar_function.
pub(crate) fn eval_func_call(
    fc: &pg_query::protobuf::FuncCall, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    let name = extract_func_name(fc);
    let args: Vec<ArenaValue> = fc.args.iter()
        .map(|a| eval_expr(a.node.as_ref().ok_or("FuncCall: missing arg")?, row, ctx, arena))
        .collect::<Result<_, _>>()?;
    eval_scalar_function(&name, &args, arena)
}

/// Evaluate scalar functions (string, math, sequence, type conversion).
pub(crate) fn eval_scalar_function(name: &str, args: &[ArenaValue], arena: &mut QueryArena) -> Result<ArenaValue, String> {
    match name {
        "upper" => match args.first() {
            Some(ArenaValue::Text(s)) => Ok(ArenaValue::Text(arena.alloc_str(&arena.get_str(*s).to_uppercase()))),
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("upper() requires text argument".into()),
        },
        "lower" => match args.first() {
            Some(ArenaValue::Text(s)) => Ok(ArenaValue::Text(arena.alloc_str(&arena.get_str(*s).to_lowercase()))),
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("lower() requires text argument".into()),
        },
        "length" => match args.first() {
            Some(ArenaValue::Text(s)) => Ok(ArenaValue::Int(arena.get_str(*s).len() as i64)),
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("length() requires text argument".into()),
        },
        "concat" => {
            let parts: String = args.iter().map(|v| match v {
                ArenaValue::Null => String::new(),
                v => v.to_text(arena).unwrap_or_default(),
            }).collect();
            Ok(ArenaValue::Text(arena.alloc_str(&parts)))
        }
        "abs" => match args.first() {
            Some(ArenaValue::Int(n)) => Ok(ArenaValue::Int(n.checked_abs().ok_or("integer out of range")?)),
            Some(ArenaValue::Float(f)) => Ok(ArenaValue::Float(f.abs())),
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("abs() requires numeric argument".into()),
        },
        "nextval" | "currval" | "setval" => eval_seq_func(name, args, arena),
        "coalesce" => { for arg in args { if !arg.is_null() { return Ok(*arg); } } Ok(ArenaValue::Null) }
        "nullif" => {
            if args.len() != 2 { return Err("nullif() requires 2 arguments".into()); }
            if args[0].is_null() { return Ok(ArenaValue::Null); }
            if args[1].is_null() { return Ok(args[0]); }
            if args[0].eq_with(&args[1], arena) || args[0].compare(&args[1], arena) == Some(std::cmp::Ordering::Equal) { Ok(ArenaValue::Null) } else { Ok(args[0]) }
        }
        "substring" | "substr" | "btrim" | "trim" | "ltrim" | "rtrim"
        | "strpos" | "position" | "replace" | "left" | "right" | "split_part" => {
            super::func_str::eval_string_func(name, args, arena)
        }
        "ceil" | "ceiling" | "floor" | "round" | "mod" | "power" | "pow" | "sqrt" => {
            super::func_math::eval_math_func(name, args, arena)
        }
        _ => Err(format!("function {}() does not exist", name)),
    }
}

fn eval_seq_func(name: &str, args: &[ArenaValue], arena: &mut QueryArena) -> Result<ArenaValue, String> {
    match name {
        "nextval" => match args.first() {
            Some(ArenaValue::Text(s)) => {
                let text = arena.get_str(*s).to_string();
                let (schema, name) = parse_seq_name(&text);
                Ok(ArenaValue::Int(crate::sequence::nextval(schema, name)?))
            }
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("nextval() requires text argument".into()),
        },
        "currval" => match args.first() {
            Some(ArenaValue::Text(s)) => {
                let text = arena.get_str(*s).to_string();
                let (schema, name) = parse_seq_name(&text);
                Ok(ArenaValue::Int(crate::sequence::currval(schema, name)?))
            }
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("currval() requires text argument".into()),
        },
        "setval" => match (args.first(), args.get(1)) {
            (Some(ArenaValue::Text(s)), Some(ArenaValue::Int(v))) => {
                let text = arena.get_str(*s).to_string();
                let (schema, name) = parse_seq_name(&text);
                Ok(ArenaValue::Int(crate::sequence::setval(schema, name, *v)?))
            }
            _ => Err("setval() requires (text, integer) arguments".into()),
        },
        _ => unreachable!(),
    }
}
