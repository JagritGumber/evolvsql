use super::super::*;
use super::tmp_path;
use crate::types::Value;

fn int_schema() -> Vec<(String, i32)> {
    vec![("x".into(), 23)]
}

#[test]
fn zone_map_tracks_int_min_max() {
    let path = tmp_path("int_zone");
    let rows: Vec<Vec<Value>> = vec![
        vec![Value::Int(42)],
        vec![Value::Int(7)],
        vec![Value::Int(100)],
        vec![Value::Int(13)],
    ];
    SegmentWriter::write(&path, &int_schema(), &rows).unwrap();

    let reader = SegmentReader::open(&path).unwrap();
    let meta = reader.column_meta("x").unwrap();
    assert_eq!(meta.min, Some(Value::Int(7)));
    assert_eq!(meta.max, Some(Value::Int(100)));
    assert_eq!(meta.null_count, 0);
    std::fs::remove_file(&path).ok();
}

#[test]
fn zone_map_excludes_nulls_and_counts_them() {
    let path = tmp_path("nulls");
    let rows: Vec<Vec<Value>> = vec![
        vec![Value::Null],
        vec![Value::Int(5)],
        vec![Value::Null],
        vec![Value::Int(10)],
    ];
    SegmentWriter::write(&path, &int_schema(), &rows).unwrap();

    let reader = SegmentReader::open(&path).unwrap();
    let meta = reader.column_meta("x").unwrap();
    assert_eq!(meta.min, Some(Value::Int(5)));
    assert_eq!(meta.max, Some(Value::Int(10)));
    assert_eq!(meta.null_count, 2);
    std::fs::remove_file(&path).ok();
}

#[test]
fn zone_map_all_null_column() {
    let path = tmp_path("all_null");
    let rows: Vec<Vec<Value>> = vec![
        vec![Value::Null],
        vec![Value::Null],
    ];
    SegmentWriter::write(&path, &int_schema(), &rows).unwrap();

    let reader = SegmentReader::open(&path).unwrap();
    let meta = reader.column_meta("x").unwrap();
    assert_eq!(meta.min, None);
    assert_eq!(meta.max, None);
    assert_eq!(meta.null_count, 2);
    std::fs::remove_file(&path).ok();
}

#[test]
fn zone_map_text_ordering() {
    let path = tmp_path("text_zone");
    let rows: Vec<Vec<Value>> = vec![
        vec![Value::Text("charlie".into())],
        vec![Value::Text("alice".into())],
        vec![Value::Text("bob".into())],
    ];
    SegmentWriter::write(&path, &[("x".into(), 25)], &rows).unwrap();

    let reader = SegmentReader::open(&path).unwrap();
    let meta = reader.column_meta("x").unwrap();
    assert_eq!(meta.min, Some(Value::Text("alice".into())));
    assert_eq!(meta.max, Some(Value::Text("charlie".into())));
    std::fs::remove_file(&path).ok();
}
