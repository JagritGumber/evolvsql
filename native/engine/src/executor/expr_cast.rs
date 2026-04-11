use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};

use super::expr::eval_expr;
use super::helpers::parse_vector_literal;
use super::types::JoinContext;

/// Evaluate TypeCast expressions (::int, ::text, ::vector, etc.).
pub(crate) fn eval_type_cast(
    tc: &pg_query::protobuf::TypeCast, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    let inner = tc.arg.as_ref().and_then(|a| a.node.as_ref()).ok_or("TypeCast missing arg")?;
    let val = eval_expr(inner, row, ctx, arena)?;
    if let Some(tn) = &tc.type_name {
        let type_name: String = tn.names.iter()
            .filter_map(|n| n.node.as_ref())
            .filter_map(|node| if let NodeEnum::String(s) = node { Some(s.sval.clone()) } else { None })
            .last().unwrap_or_default();
        match type_name.as_str() {
            "vector" => {
                if let ArenaValue::Text(s) = &val {
                    let text = arena.get_str(*s).to_string();
                    let trimmed = text.trim();
                    if trimmed.starts_with('[') && trimmed.ends_with(']') {
                        let v = parse_vector_literal(trimmed)?;
                        if let crate::types::Value::Vector(data) = v { return Ok(ArenaValue::Vector(arena.alloc_vec(&data))); }
                    }
                }
            }
            "int4" | "int" | "integer" | "int8" | "bigint" => {
                match &val {
                    ArenaValue::Text(s) => { let text = arena.get_str(*s); return text.trim().parse::<i64>().map(ArenaValue::Int).map_err(|_| format!("invalid input syntax for integer: \"{}\"", text)); }
                    ArenaValue::Float(f) => return Ok(ArenaValue::Int(f.round() as i64)),
                    ArenaValue::Int(_) => return Ok(val),
                    ArenaValue::Bool(b) => return Ok(ArenaValue::Int(if *b { 1 } else { 0 })),
                    _ => {}
                }
            }
            "float4" | "float8" | "real" | "double precision" | "numeric" => {
                match &val {
                    ArenaValue::Text(s) => { let text = arena.get_str(*s); return text.trim().parse::<f64>().map(ArenaValue::Float).map_err(|_| format!("invalid input syntax for type double precision: \"{}\"", text)); }
                    ArenaValue::Int(i) => return Ok(ArenaValue::Float(*i as f64)),
                    ArenaValue::Float(_) => return Ok(val),
                    _ => {}
                }
            }
            "text" | "varchar" | "char" | "character varying" => {
                let text = match &val {
                    ArenaValue::Int(i) => i.to_string(),
                    ArenaValue::Float(f) => f.to_string(),
                    ArenaValue::Bool(b) => (if *b { "true" } else { "false" }).to_string(),
                    ArenaValue::Text(_) => return Ok(val),
                    _ => return Ok(val),
                };
                return Ok(ArenaValue::Text(arena.alloc_str(&text)));
            }
            "bool" | "boolean" => {
                match &val {
                    ArenaValue::Text(s) => {
                        let text = arena.get_str(*s).trim().to_lowercase();
                        let b = match text.as_str() {
                            "t" | "true" | "yes" | "on" | "1" => true,
                            "f" | "false" | "no" | "off" | "0" => false,
                            _ => return Err(format!("invalid input syntax for type boolean: \"{}\"", text)),
                        };
                        return Ok(ArenaValue::Bool(b));
                    }
                    ArenaValue::Int(i) => return Ok(ArenaValue::Bool(*i != 0)),
                    ArenaValue::Bool(_) => return Ok(val),
                    _ => {}
                }
            }
            _ => {}
        }
    }
    Ok(val)
}
