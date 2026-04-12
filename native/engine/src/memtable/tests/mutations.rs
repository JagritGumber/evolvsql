use super::super::*;
use super::row;
use crate::types::Value;

#[test]
fn delete_marks_row_as_tombstone_and_hides_from_scan() {
    let mut mt = Memtable::new();
    mt.insert(1, row(1, "a"));
    mt.insert(2, row(2, "b"));
    mt.insert(3, row(3, "c"));

    mt.delete_at(1, 10).unwrap();

    let visible: Vec<_> = mt.scan().map(|(_, r)| r[0].clone()).collect();
    assert_eq!(visible, vec![Value::Int(1), Value::Int(3)]);

    let s = mt.stats();
    assert_eq!(s.row_count, 3, "tombstone kept in row_count");
    assert_eq!(s.live_row_count, 2, "tombstone excluded from live_row_count");
}

#[test]
fn update_replaces_values_in_place() {
    let mut mt = Memtable::new();
    mt.insert(1, row(1, "alice"));
    mt.update_at(0, 2, row(1, "alice_v2")).unwrap();

    let scanned: Vec<_> = mt.scan().map(|(_, r)| r.to_vec()).collect();
    assert_eq!(scanned.len(), 1);
    assert_eq!(scanned[0], row(1, "alice_v2"));
}

#[test]
fn update_fails_on_tombstoned_row() {
    let mut mt = Memtable::new();
    mt.insert(1, row(1, "a"));
    mt.delete_at(0, 2).unwrap();
    let r = mt.update_at(0, 3, row(1, "b"));
    assert!(r.is_err());
    assert!(r.unwrap_err().contains("tombstoned"));
}

#[test]
fn delete_out_of_range_errors() {
    let mut mt = Memtable::new();
    mt.insert(1, row(1, "a"));
    let r = mt.delete_at(99, 2);
    assert!(r.is_err());
}

#[test]
fn update_out_of_range_errors() {
    let mut mt = Memtable::new();
    let r = mt.update_at(0, 1, row(1, "a"));
    assert!(r.is_err());
}
