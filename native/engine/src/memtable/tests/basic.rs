use super::super::*;
use super::row;

#[test]
fn insert_and_scan_preserves_order() {
    let mut mt = Memtable::new();
    mt.insert(1, row(1, "alice"));
    mt.insert(2, row(2, "bob"));
    mt.insert(3, row(3, "carol"));

    let scanned: Vec<(usize, Vec<_>)> = mt.scan().map(|(i, r)| (i, r.to_vec())).collect();
    assert_eq!(scanned.len(), 3);
    assert_eq!(scanned[0].0, 0);
    assert_eq!(scanned[2].0, 2);
    assert_eq!(scanned[0].1, row(1, "alice"));
    assert_eq!(scanned[2].1, row(3, "carol"));
}

#[test]
fn insert_returns_stable_index() {
    let mut mt = Memtable::new();
    assert_eq!(mt.insert(1, row(1, "a")), 0);
    assert_eq!(mt.insert(2, row(2, "b")), 1);
    assert_eq!(mt.insert(3, row(3, "c")), 2);
}

#[test]
fn empty_memtable_stats() {
    let mt = Memtable::new();
    let s = mt.stats();
    assert_eq!(s.row_count, 0);
    assert_eq!(s.live_row_count, 0);
    assert_eq!(s.bytes, 0);
    assert_eq!(mt.scan().count(), 0);
}

#[test]
fn stats_tracks_row_count_and_bytes() {
    let mut mt = Memtable::new();
    mt.insert(1, row(1, "a"));
    mt.insert(2, row(2, "bob"));
    let s = mt.stats();
    assert_eq!(s.row_count, 2);
    assert_eq!(s.live_row_count, 2);
    // bytes > 0 without locking the exact value (depends on MemRow size)
    assert!(s.bytes > 0);
}
