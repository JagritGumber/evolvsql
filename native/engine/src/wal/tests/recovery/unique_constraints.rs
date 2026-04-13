//! Recovery interactions with UNIQUE / PRIMARY KEY constraints. The
//! unique index is rebuilt by the CreateTable replay path, so every
//! replayed INSERT has to pass through the same uniqueness check as a
//! live insert would. Two scenarios where naive replay breaks:
//!
//! 1. Insert A, delete A, insert A again: if the unique index replays
//!    all three events without honoring the delete, the second insert
//!    appears to violate the PK.
//! 2. Upsert DO UPDATE: the update is logged as Update, not Insert,
//!    so the index must be updated in place rather than re-added.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn recover_insert_delete_reinsert_same_pk() {
    let path = tmp_recovery_path("reinsert");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int PRIMARY KEY, v int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 10)").unwrap();
    executor::execute("DELETE FROM t WHERE id = 1").unwrap();
    // Reinsert with same PK must succeed because the earlier row is gone
    executor::execute("INSERT INTO t VALUES (1, 20)").unwrap();

    storage::reset();
    catalog::reset();
    recovery::recover().unwrap();

    let r = executor::execute("SELECT id, v FROM t").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("20".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_upsert_do_update_survives_round_trip() {
    // UPSERT that hits a conflict and updates must, post-recovery,
    // still reflect the updated values. The WAL logs Insert+Update;
    // replay must end with one row holding the new value.
    let path = tmp_recovery_path("upsert_rec");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int PRIMARY KEY, v int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 10)").unwrap();
    executor::execute(
        "INSERT INTO t VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
    ).unwrap();

    storage::reset();
    catalog::reset();
    recovery::recover().unwrap();

    let r = executor::execute("SELECT id, v FROM t").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Some("99".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_preserves_unique_index_so_new_inserts_still_conflict() {
    // After recovery, the live unique index must be populated so that
    // a brand-new conflicting INSERT still fails. If recover() rebuilt
    // rows without the index, the next write would silently dup.
    let path = tmp_recovery_path("idx_live");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int PRIMARY KEY)").unwrap();
    executor::execute("INSERT INTO t VALUES (1), (2)").unwrap();

    storage::reset();
    catalog::reset();
    recovery::recover().unwrap();

    let r = executor::execute("INSERT INTO t VALUES (1)");
    assert!(r.is_err(), "unique index must reject duplicate after recovery");

    manager::disable();
    std::fs::remove_file(&path).ok();
}
