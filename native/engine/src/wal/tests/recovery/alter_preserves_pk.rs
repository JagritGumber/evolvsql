//! ALTER DROP COLUMN on a composite-PK table must keep the pk_index
//! on the tuple intact. The old storage code set
//! `t.pk_index = None` after the column removal with a hopeful
//! `// force rebuild on next insert` comment, but insert_batch_checked
//! only checks the pk_index if it exists (line 378). With the index
//! None, post-drop INSERTs silently accept duplicate composite keys.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn alter_drop_non_pk_column_preserves_composite_pk_live() {
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();

    executor::execute("CREATE TABLE t (a int, b int, c int, PRIMARY KEY (a, b))").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 1, 100), (1, 2, 200)").unwrap();
    executor::execute("ALTER TABLE t DROP COLUMN c").unwrap();

    // (1, 1) is still a row — duplicate composite PK must be rejected.
    let err = executor::execute("INSERT INTO t VALUES (1, 1)");
    assert!(err.is_err(), "composite PK duplicate must be rejected after ALTER DROP");

    // New (1, 3) is fine.
    executor::execute("INSERT INTO t VALUES (1, 3)").unwrap();
    let r = executor::execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("3".into()));
}

#[test]
#[serial_test::serial]
fn alter_drop_non_pk_column_preserves_composite_pk_through_recovery() {
    let path = tmp_recovery_path("alter_drop_comp_pk");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (a int, b int, c int, PRIMARY KEY (a, b))").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 1, 100), (1, 2, 200)").unwrap();
    executor::execute("ALTER TABLE t DROP COLUMN c").unwrap();

    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    let err = executor::execute("INSERT INTO t VALUES (1, 1)");
    assert!(err.is_err(), "post-recovery composite PK duplicate must be rejected");

    manager::disable();
    std::fs::remove_file(&path).ok();
}
