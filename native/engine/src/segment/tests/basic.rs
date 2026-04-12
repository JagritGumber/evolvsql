use super::super::*;
use super::{tmp_path, user_row, users_schema};
use crate::types::Value;

#[test]
fn write_then_read_all_rows() {
    let path = tmp_path("basic");
    let schema = users_schema();
    let rows = vec![
        user_row(1, "alice"),
        user_row(2, "bob"),
        user_row(3, "carol"),
    ];
    SegmentWriter::write(&path, &schema, &rows).unwrap();

    let mut reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.row_count(), 3);
    let read_back = reader.read_all_rows().unwrap();
    assert_eq!(read_back, rows);
    std::fs::remove_file(&path).ok();
}

#[test]
fn read_single_column_skips_others() {
    let path = tmp_path("single_col");
    let schema = users_schema();
    let rows = vec![
        user_row(10, "x"),
        user_row(20, "y"),
    ];
    SegmentWriter::write(&path, &schema, &rows).unwrap();

    let mut reader = SegmentReader::open(&path).unwrap();
    let ids = reader.read_column("id").unwrap();
    assert_eq!(ids, vec![Value::Int(10), Value::Int(20)]);
    std::fs::remove_file(&path).ok();
}

#[test]
fn empty_segment_roundtrip() {
    let path = tmp_path("empty");
    let schema = users_schema();
    SegmentWriter::write(&path, &schema, &[]).unwrap();

    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.row_count(), 0);
    assert_eq!(reader.columns().len(), 2);
    std::fs::remove_file(&path).ok();
}

#[test]
fn row_arity_mismatch_errors() {
    let path = tmp_path("arity");
    let schema = users_schema(); // 2 columns
    let bad_rows = vec![vec![Value::Int(1)]]; // 1 value
    let r = SegmentWriter::write(&path, &schema, &bad_rows);
    assert!(r.is_err());
    assert!(r.unwrap_err().contains("arity"));
}

#[test]
fn column_metadata_preserved() {
    let path = tmp_path("meta");
    let schema = users_schema();
    let rows = vec![user_row(1, "a")];
    SegmentWriter::write(&path, &schema, &rows).unwrap();

    let reader = SegmentReader::open(&path).unwrap();
    let cols = reader.columns();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].name, "id");
    assert_eq!(cols[0].type_oid, 23);
    assert_eq!(cols[1].name, "name");
    assert_eq!(cols[1].type_oid, 25);
    std::fs::remove_file(&path).ok();
}
