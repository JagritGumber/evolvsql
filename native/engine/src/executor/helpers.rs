use std::sync::Arc;

use pg_query::NodeEnum;

use crate::catalog::{self, Table};
use crate::types::Value;

/// Parse vector literal string "[1.0, 2.0, ...]" to Value::Vector.
pub(crate) fn parse_vector_literal(s: &str) -> Result<Value, String> {
    if s.len() < 2 || !s.starts_with('[') || !s.ends_with(']') {
        return Err(format!("malformed vector literal: \"{}\"", s));
    }
    let inner = &s[1..s.len() - 1];
    if inner.trim().is_empty() {
        return Err("vector must have at least 1 dimension".into());
    }
    let parts: Vec<f32> = inner.split(',')
        .map(|p| p.trim().parse::<f32>().map_err(|e| format!("invalid vector element \"{}\": {}", p.trim(), e)))
        .collect::<Result<_, _>>()?;
    if let Some(bad) = parts.iter().find(|f| !f.is_finite()) {
        return Err(format!("vector elements must be finite, got {}", bad));
    }
    Ok(Value::Vector(parts))
}

/// Evaluate constant expressions (literals, simple operations).
pub(crate) fn eval_const(node: Option<&NodeEnum>) -> Value {
    match node {
        Some(NodeEnum::Integer(i)) => Value::Int(i.ival as i64),
        Some(NodeEnum::Float(f)) => f.fval.parse::<f64>().map(Value::Float).unwrap_or(Value::Null),
        Some(NodeEnum::String(s)) => {
            let trimmed = s.sval.trim();
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                parse_vector_literal(trimmed).unwrap_or(Value::Text(Arc::from(s.sval.as_str())))
            } else {
                Value::Text(Arc::from(s.sval.as_str()))
            }
        }
        Some(NodeEnum::AConst(ac)) => {
            if let Some(val) = &ac.val {
                match val {
                    pg_query::protobuf::a_const::Val::Ival(i) => Value::Int(i.ival as i64),
                    pg_query::protobuf::a_const::Val::Fval(f) => {
                        f.fval.parse::<f64>().map(Value::Float).unwrap_or(Value::Null)
                    }
                    pg_query::protobuf::a_const::Val::Sval(s) => {
                        let trimmed = s.sval.trim();
                        if trimmed.starts_with('[') && trimmed.ends_with(']') {
                            parse_vector_literal(trimmed).unwrap_or(Value::Text(Arc::from(s.sval.as_str())))
                        } else {
                            Value::Text(Arc::from(s.sval.as_str()))
                        }
                    }
                    pg_query::protobuf::a_const::Val::Bsval(s) => Value::Text(Arc::from(s.bsval.as_str())),
                    pg_query::protobuf::a_const::Val::Boolval(b) => Value::Bool(b.boolval),
                }
            } else {
                Value::Null
            }
        }
        Some(NodeEnum::TypeCast(tc)) => eval_const(tc.arg.as_ref().and_then(|a| a.node.as_ref())),
        _ => Value::Null,
    }
}

/// Evaluate constant expression to i64 (for LIMIT/OFFSET).
pub(crate) fn eval_const_i64(node: Option<&NodeEnum>) -> Option<i64> {
    match eval_const(node) {
        Value::Int(n) => Some(n),
        Value::Float(f) => Some(f as i64),
        _ => None,
    }
}

/// Parse text string back to typed Value based on column OID.
pub(crate) fn parse_text_to_value(s: &str, oid: i32) -> Value {
    match oid {
        16 => Value::Bool(s == "t" || s == "true"),
        20 | 21 | 23 => s.parse::<i64>().map(Value::Int).unwrap_or(Value::Text(Arc::from(s))),
        700 | 701 | 1700 => s.parse::<f64>().map(Value::Float).unwrap_or(Value::Text(Arc::from(s))),
        16385 => parse_vector_literal(s).unwrap_or(Value::Text(Arc::from(s))),
        _ => Value::Text(Arc::from(s)),
    }
}

/// Parse a sequence name that may be schema-qualified.
pub(crate) fn parse_seq_name(s: &str) -> (&str, &str) {
    match s.split_once('.') { Some((schema, name)) => (schema, name), None => ("public", s) }
}

/// Apply column default value (literal or NEXTVAL for sequences).
pub(crate) fn apply_default(default_expr: &Option<catalog::DefaultExpr>, _schema: &str) -> Result<Value, String> {
    match default_expr {
        Some(catalog::DefaultExpr::Literal(v)) => Ok(v.clone()),
        Some(catalog::DefaultExpr::NextVal(seq_fqn)) => {
            let (seq_schema, seq_name) = parse_seq_name(seq_fqn);
            let val = crate::sequence::nextval(seq_schema, seq_name)?;
            Ok(Value::Int(val))
        }
        None => Ok(Value::Null),
    }
}

/// Validate NOT NULL constraints on inserted/updated row.
pub(crate) fn check_not_null(table: &Table, row: &[Value]) -> Result<(), String> {
    for (i, col) in table.columns.iter().enumerate() {
        if !col.nullable && matches!(row[i], Value::Null) {
            return Err(format!("null value in column \"{}\" violates not-null constraint", col.name));
        }
    }
    Ok(())
}

/// Extract operator name from protobuf nodes.
pub(crate) fn extract_op_name(name_nodes: &[pg_query::protobuf::Node]) -> Result<String, String> {
    name_nodes.iter()
        .filter_map(|n| n.node.as_ref())
        .filter_map(|n| if let NodeEnum::String(s) = n { Some(s.sval.clone()) } else { None })
        .next()
        .ok_or_else(|| "missing operator name".into())
}
