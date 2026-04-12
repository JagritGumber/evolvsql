use crate::types::Value;

/// Compute (min, max, null_count) for a column's values. Used during
/// segment writing to build zone maps. NULL values are excluded from
/// min/max; if all values are NULL, both are None.
///
/// Vector and Bytea values have no meaningful ordering for zone maps,
/// so their min/max are left as None (predicate skipping does not apply
/// to them).
pub(super) fn compute_zone_map(values: &[Value]) -> (Option<Value>, Option<Value>, u32) {
    let mut min: Option<Value> = None;
    let mut max: Option<Value> = None;
    let mut null_count: u32 = 0;
    for v in values {
        if matches!(v, Value::Null) {
            null_count += 1;
            continue;
        }
        if !has_zone_order(v) {
            continue;
        }
        if min.as_ref().is_none_or(|cur| less_than(v, cur)) {
            min = Some(v.clone());
        }
        if max.as_ref().is_none_or(|cur| less_than(cur, v)) {
            max = Some(v.clone());
        }
    }
    (min, max, null_count)
}

/// Whether a value type participates in zone map min/max tracking.
fn has_zone_order(v: &Value) -> bool {
    matches!(v, Value::Int(_) | Value::Float(_) | Value::Text(_) | Value::Bool(_))
}

/// Strict less-than comparison for zone map tracking. Only defined for
/// the ordered scalar types; Vector/Bytea/Null return false.
fn less_than(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x < y,
        (Value::Float(x), Value::Float(y)) => x < y,
        (Value::Int(x), Value::Float(y)) => (*x as f64) < *y,
        (Value::Float(x), Value::Int(y)) => *x < (*y as f64),
        (Value::Text(x), Value::Text(y)) => x.as_ref() < y.as_ref(),
        (Value::Bool(x), Value::Bool(y)) => !*x && *y,
        _ => false,
    }
}

/// Returns true if `value` is definitely outside [min, max]. Used by
/// predicate pushdown: if this returns true, the segment can be
/// skipped entirely for an equality predicate on this value.
pub fn outside_range(value: &Value, min: &Option<Value>, max: &Option<Value>) -> bool {
    match (min, max) {
        (Some(lo), Some(hi)) => less_than(value, lo) || less_than(hi, value),
        _ => false,
    }
}
