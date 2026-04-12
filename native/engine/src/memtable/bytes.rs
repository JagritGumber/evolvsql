use crate::types::Value;

use super::table::MemRow;

/// Rough byte estimate for a row, used for flush threshold decisions.
/// Includes the fixed MemRow overhead plus variable-length payload.
pub(super) fn estimate_row_bytes(values: &[Value]) -> usize {
    let mut bytes = std::mem::size_of::<MemRow>();
    for v in values {
        bytes += match v {
            Value::Null | Value::Bool(_) => 1,
            Value::Int(_) | Value::Float(_) => 8,
            Value::Text(s) => s.len(),
            Value::Bytea(b) => b.len(),
            Value::Vector(v) => v.len() * 4,
        };
    }
    bytes
}
