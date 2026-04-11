use crate::arena::{ArenaValue, QueryArena};

use super::expr_ops::eval_arithmetic;

/// Evaluate math functions: ceil, floor, round, mod, power, sqrt.
pub(crate) fn eval_math_func(name: &str, args: &[ArenaValue], arena: &QueryArena) -> Result<ArenaValue, String> {
    match name {
        "ceil" | "ceiling" => match args.first() {
            Some(ArenaValue::Int(n)) => Ok(ArenaValue::Int(*n)),
            Some(ArenaValue::Float(f)) => Ok(ArenaValue::Int(f.ceil() as i64)),
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("ceil() requires numeric argument".into()),
        },
        "floor" => match args.first() {
            Some(ArenaValue::Int(n)) => Ok(ArenaValue::Int(*n)),
            Some(ArenaValue::Float(f)) => Ok(ArenaValue::Int(f.floor() as i64)),
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("floor() requires numeric argument".into()),
        },
        "round" => {
            let val = args.first().ok_or("round() requires at least 1 argument")?;
            if val.is_null() { return Ok(ArenaValue::Null); }
            let decimals = match args.get(1) {
                Some(ArenaValue::Int(d)) => *d as i32,
                None => 0,
                _ => return Err("round() second argument must be integer".into()),
            };
            let f = match val {
                ArenaValue::Int(n) => *n as f64,
                ArenaValue::Float(f) => *f,
                _ => return Err("round() requires numeric argument".into()),
            };
            if decimals == 0 { Ok(ArenaValue::Int(f.round() as i64)) }
            else {
                let factor = 10f64.powi(decimals);
                Ok(ArenaValue::Float((f * factor).round() / factor))
            }
        }
        "mod" => {
            if args.len() != 2 { return Err("mod() requires 2 arguments".into()); }
            eval_arithmetic("%", &args[0], &args[1], arena)
        }
        "power" | "pow" => {
            if args.len() != 2 { return Err("power() requires 2 arguments".into()); }
            if args[0].is_null() || args[1].is_null() { return Ok(ArenaValue::Null); }
            let base = match &args[0] {
                ArenaValue::Int(n) => *n as f64,
                ArenaValue::Float(f) => *f,
                _ => return Err("power() requires numeric arguments".into()),
            };
            let exp = match &args[1] {
                ArenaValue::Int(n) => *n as f64,
                ArenaValue::Float(f) => *f,
                _ => return Err("power() requires numeric arguments".into()),
            };
            let result = base.powf(exp);
            if result == result.trunc() && result.is_finite() && result.abs() < i64::MAX as f64 {
                Ok(ArenaValue::Int(result as i64))
            } else { Ok(ArenaValue::Float(result)) }
        }
        "sqrt" => match args.first() {
            Some(ArenaValue::Int(n)) => {
                let result = (*n as f64).sqrt();
                if result == result.trunc() { Ok(ArenaValue::Int(result as i64)) }
                else { Ok(ArenaValue::Float(result)) }
            }
            Some(ArenaValue::Float(f)) => Ok(ArenaValue::Float(f.sqrt())),
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("sqrt() requires numeric argument".into()),
        },
        _ => Err(format!("unknown math function: {}", name)),
    }
}
