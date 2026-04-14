//! NaN poisoning in zone map min/max tracking. Float comparisons
//! with NaN return false in both directions, so once NaN lands in
//! the `min` or `max` slot the accumulator is stuck there and every
//! subsequent real Float is silently ignored. Predicate pushdown
//! built on top of these zone maps would then wrongly mark the
//! segment as "outside range" for a legitimate query.

use super::super::*;
use super::tmp_path;
use crate::types::Value;

#[test]
fn zone_map_ignores_nan_and_tracks_real_floats() {
    let path = tmp_path("nan_zm");
    let schema = vec![("x".into(), 701)]; // float8 OID

    // NaN appears first; the tracker must not let it become the
    // running min/max because every real follow-up value would then
    // fail the `less_than` check against NaN.
    let rows: Vec<Vec<Value>> = vec![
        vec![Value::Float(f64::NAN)],
        vec![Value::Float(2.0)],
        vec![Value::Float(-5.0)],
        vec![Value::Float(10.0)],
    ];
    SegmentWriter::write(&path, &schema, &rows).unwrap();

    let mut reader = SegmentReader::open(&path).unwrap();
    let meta = reader.column_meta("x").unwrap();
    assert_eq!(
        meta.min,
        Some(Value::Float(-5.0)),
        "min must be the smallest real value, not poisoned by NaN"
    );
    assert_eq!(
        meta.max,
        Some(Value::Float(10.0)),
        "max must be the largest real value, not poisoned by NaN"
    );

    std::fs::remove_file(&path).ok();
}
