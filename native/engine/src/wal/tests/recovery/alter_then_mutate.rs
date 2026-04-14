//! Mutations that cross an ALTER boundary during recovery. Each test
//! interleaves a schema change with inserts/updates/deletes so the
//! WAL contains ops against two different row shapes. Recovery must
//! replay them in LSN order: ops before the ALTER see the old shape,
//! ops after see the new shape.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

fn setup(name: &str) -> std::path::PathBuf {
    let path = tmp_recovery_path(name);
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();
    path
}

fn wipe_and_recover() {
    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();
}

#[test]
#[serial_test::serial]
fn recover_insert_alter_add_insert_with_default() {
    // INSERT, then ALTER ADD with default, then another INSERT. Old
    // rows get the default on replay; new row keeps its explicit value.
    let path = setup("insert_alter_insert");
    executor::execute("CREATE TABLE t (id int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1)").unwrap();
    executor::execute("ALTER TABLE t ADD COLUMN tag text DEFAULT 'old'").unwrap();
    executor::execute("INSERT INTO t VALUES (2, 'new')").unwrap();
    wipe_and_recover();

    let r = executor::execute("SELECT id, tag FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][1], Some("old".into()));
    assert_eq!(r.rows[1][1], Some("new".into()));
    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_update_alter_drop_update() {
    // UPDATE before DROP COLUMN (old shape) and another UPDATE after
    // (new shape). apply_update matches by content, so the WAL must
    // record old_row/new_row in the shape current at log-time.
    let path = setup("upd_alter_upd");
    executor::execute("CREATE TABLE t (id int, a text, b text)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 'a1', 'b1')").unwrap();
    executor::execute("UPDATE t SET a = 'a2' WHERE id = 1").unwrap();
    executor::execute("ALTER TABLE t DROP COLUMN b").unwrap();
    executor::execute("UPDATE t SET a = 'a3' WHERE id = 1").unwrap();
    wipe_and_recover();

    let r = executor::execute("SELECT * FROM t").unwrap();
    assert_eq!(r.rows[0].len(), 2, "b should be dropped");
    assert_eq!(r.rows[0][1], Some("a3".into()));
    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_alter_drop_preserves_remaining_unique_through_inserts() {
    // Regression for PR #58 in a more elaborate shape: multiple UNIQUEs,
    // drop a non-unique column, recover, then probe every remaining
    // UNIQUE constraint with a duplicate to prove they survived.
    let path = setup("alter_drop_multi_unique");
    executor::execute(
        "CREATE TABLE t (a int UNIQUE, b int UNIQUE, c int UNIQUE, d int)",
    ).unwrap();
    executor::execute("INSERT INTO t VALUES (1, 10, 100, 1000), (2, 20, 200, 2000)").unwrap();
    executor::execute("ALTER TABLE t DROP COLUMN d").unwrap();
    wipe_and_recover();

    assert!(executor::execute("INSERT INTO t VALUES (1, 30, 300)").is_err());
    assert!(executor::execute("INSERT INTO t VALUES (3, 20, 300)").is_err());
    assert!(executor::execute("INSERT INTO t VALUES (3, 30, 200)").is_err());
    executor::execute("INSERT INTO t VALUES (3, 30, 300)").unwrap();
    manager::disable();
    std::fs::remove_file(&path).ok();
}
