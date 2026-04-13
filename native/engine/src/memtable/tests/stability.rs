//! Tombstones must not shift indices of surviving rows. If delete ever
//! caused a live row to move, subsequent update_at/delete_at calls
//! would hit the wrong row and silently corrupt data. These tests lock
//! that invariant in place.

use super::super::*;
use super::row;
use crate::types::Value;

#[test]
fn indices_are_stable_across_deletes_and_updates() {
    let mut mt = Memtable::new();
    let i0 = mt.insert(1, row(10, "a"));
    let i1 = mt.insert(2, row(20, "b"));
    let i2 = mt.insert(3, row(30, "c"));
    let i3 = mt.insert(4, row(40, "d"));
    assert_eq!((i0, i1, i2, i3), (0, 1, 2, 3));

    // Delete the middle row, then update a later one. The update MUST
    // still target the original index — not shift down to fill the hole.
    mt.delete_at(i1, 10).unwrap();
    mt.update_at(i2, 11, row(99, "c_v2")).unwrap();

    // Scan returns (index, row). Check that i2 was the row that changed
    // and i0/i3 are untouched.
    let visible: Vec<(usize, Vec<Value>)> =
        mt.scan().map(|(i, r)| (i, r.to_vec())).collect();
    assert_eq!(visible.len(), 3);
    assert_eq!(visible[0], (0, row(10, "a")));
    assert_eq!(visible[1], (2, row(99, "c_v2")));
    assert_eq!(visible[2], (3, row(40, "d")));
}

#[test]
fn inserts_after_delete_keep_appending() {
    // After a delete, the next insert gets the next sequential index.
    // We don't reuse the tombstoned slot. Reuse would break any
    // executor that cached an index from a prior scan.
    let mut mt = Memtable::new();
    mt.insert(1, row(1, "a"));
    mt.insert(2, row(2, "b"));
    mt.delete_at(0, 3).unwrap();

    let new_idx = mt.insert(4, row(3, "c"));
    assert_eq!(new_idx, 2, "new insert must not reuse tombstoned slot 0");

    let stats = mt.stats();
    assert_eq!(stats.row_count, 3);
    assert_eq!(stats.live_row_count, 2);
}

#[test]
fn drain_compacts_tombstones_and_resets_indices() {
    let mut mt = Memtable::new();
    mt.insert(1, row(1, "a"));
    mt.insert(2, row(2, "b"));
    mt.insert(3, row(3, "c"));
    mt.delete_at(0, 10).unwrap();
    mt.delete_at(2, 11).unwrap();

    let drained = mt.drain_live();
    assert_eq!(drained, vec![row(2, "b")]);

    // Post-drain, the index space restarts at 0.
    let new_idx = mt.insert(12, row(99, "z"));
    assert_eq!(new_idx, 0);
}
