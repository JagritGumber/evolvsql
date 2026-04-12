use super::super::*;
use super::row;

#[test]
fn drain_live_returns_rows_without_tombstones() {
    let mut mt = Memtable::new();
    mt.insert(1, row(1, "a"));
    mt.insert(2, row(2, "b"));
    mt.insert(3, row(3, "c"));
    mt.delete_at(1, 10).unwrap(); // tombstone row 2

    let drained = mt.drain_live();
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0], row(1, "a"));
    assert_eq!(drained[1], row(3, "c"));
}

#[test]
fn drain_resets_memtable() {
    let mut mt = Memtable::new();
    mt.insert(1, row(1, "a"));
    mt.insert(2, row(2, "b"));

    let _ = mt.drain_live();

    let s = mt.stats();
    assert_eq!(s.row_count, 0);
    assert_eq!(s.live_row_count, 0);
    assert_eq!(s.bytes, 0);
    assert_eq!(mt.scan().count(), 0);
}

#[test]
fn drain_then_insert_starts_fresh() {
    let mut mt = Memtable::new();
    mt.insert(1, row(1, "a"));
    mt.drain_live();

    // New inserts start at index 0 again (drain resets the Vec)
    assert_eq!(mt.insert(10, row(99, "new")), 0);
    let stats = mt.stats();
    assert_eq!(stats.row_count, 1);
}
