//! ALTER TABLE DROP COLUMN must preserve UNIQUE constraints on the
//! OTHER columns. storage::alter_drop_column used to call
//! `t.unique_indexes.clear()` which nuked every unique index on the
//! table, not just the one belonging to the dropped column. A
//! subsequent INSERT with a duplicate value in a still-unique column
//! would silently succeed.
//!
//! This test exercises both live and post-recovery variants: live
//! catches the underlying storage bug; the recovery variant confirms
//! the replay path (which reuses the same storage function) also
//! preserves the constraint.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn alter_drop_column_preserves_unique_on_other_columns_live() {
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();

    executor::execute("CREATE TABLE t (a int UNIQUE, b int UNIQUE, c int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)").unwrap();
    executor::execute("ALTER TABLE t DROP COLUMN c").unwrap();

    // a=1 is still taken: duplicate must be rejected.
    let err = executor::execute("INSERT INTO t VALUES (1, 30)");
    assert!(err.is_err(), "duplicate in still-unique column must be rejected");
    // b=20 still taken too.
    let err = executor::execute("INSERT INTO t VALUES (3, 20)");
    assert!(err.is_err(), "duplicate in other still-unique column must be rejected");
    // Non-duplicate still works.
    executor::execute("INSERT INTO t VALUES (3, 30)").unwrap();
}

#[test]
#[serial_test::serial]
fn alter_drop_column_preserves_unique_through_recovery() {
    let path = tmp_recovery_path("alter_drop_unique");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (a int UNIQUE, b int UNIQUE, c int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)").unwrap();
    executor::execute("ALTER TABLE t DROP COLUMN c").unwrap();

    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    let err = executor::execute("INSERT INTO t VALUES (1, 30)");
    assert!(err.is_err(), "post-recovery duplicate on a must be rejected");

    manager::disable();
    std::fs::remove_file(&path).ok();
}
