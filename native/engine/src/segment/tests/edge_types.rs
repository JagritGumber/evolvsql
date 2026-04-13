//! Segment round-trip for edge column types (bool, float) and sizes
//! (single row). Catches bincode/zone-map bugs that only show up on
//! non-int/non-text columns.

use super::super::*;
use super::tmp_path;
use crate::types::Value;

#[test]
fn bool_column_roundtrip_and_zone_map() {
    let path = tmp_path("bool");
    let schema = vec![("flag".into(), 16)]; // bool OID
    let rows: Vec<Vec<Value>> = vec![
        vec![Value::Bool(true)],
        vec![Value::Bool(false)],
        vec![Value::Bool(true)],
    ];
    SegmentWriter::write(&path, &schema, &rows).unwrap();

    let mut reader = SegmentReader::open(&path).unwrap();
    let back = reader.read_all_rows().unwrap();
    assert_eq!(back, rows);

    // Zone map on bool: min=false, max=true
    let meta = reader.column_meta("flag").unwrap();
    assert_eq!(meta.min, Some(Value::Bool(false)));
    assert_eq!(meta.max, Some(Value::Bool(true)));
    assert_eq!(meta.null_count, 0);
    std::fs::remove_file(&path).ok();
}

#[test]
fn float_column_roundtrip_and_zone_map() {
    let path = tmp_path("float");
    let schema = vec![("x".into(), 701)]; // float8 OID
    let rows: Vec<Vec<Value>> = vec![
        vec![Value::Float(3.14)],
        vec![Value::Float(-1.5)],
        vec![Value::Float(100.0)],
        vec![Value::Float(0.0)],
    ];
    SegmentWriter::write(&path, &schema, &rows).unwrap();

    let mut reader = SegmentReader::open(&path).unwrap();
    let back = reader.read_all_rows().unwrap();
    assert_eq!(back, rows);

    let meta = reader.column_meta("x").unwrap();
    assert_eq!(meta.min, Some(Value::Float(-1.5)));
    assert_eq!(meta.max, Some(Value::Float(100.0)));
    std::fs::remove_file(&path).ok();
}

#[test]
fn single_row_segment_is_valid() {
    // Degenerate case: one row. The writer still has to produce a
    // valid header + footer, and zone-map min/max must collapse to
    // the same value rather than panic on an empty accumulator.
    let path = tmp_path("one_row");
    let schema = vec![("id".into(), 23), ("name".into(), 25)];
    let rows: Vec<Vec<Value>> = vec![vec![
        Value::Int(42),
        Value::Text(std::sync::Arc::from("solo")),
    ]];
    SegmentWriter::write(&path, &schema, &rows).unwrap();

    let mut reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.row_count(), 1);
    let back = reader.read_all_rows().unwrap();
    assert_eq!(back, rows);

    let id_meta = reader.column_meta("id").unwrap();
    assert_eq!(id_meta.min, Some(Value::Int(42)));
    assert_eq!(id_meta.max, Some(Value::Int(42)));
    std::fs::remove_file(&path).ok();
}
