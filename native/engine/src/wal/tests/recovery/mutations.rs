//! Recovery round-trip for UPDATE and DELETE: run mutations with WAL
//! enabled, simulate a crash by resetting storage, recover, verify the
//! post-mutation state is restored.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn recover_replays_delete() {
    let path = tmp_recovery_path("delete");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    executor::execute("DELETE FROM t WHERE id = 2").unwrap();

    // Simulate crash
    storage::reset();
    catalog::reset();
    executor::execute("CREATE TABLE t (id int)").unwrap();

    let applied = recovery::recover().unwrap();
    // 3 inserts + 1 delete = 4 entries
    assert_eq!(applied, 4);

    let r = executor::execute("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[1][0], Some("3".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_replays_update() {
    let path = tmp_recovery_path("update");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int, name text)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')").unwrap();
    executor::execute("UPDATE t SET name = 'alice_v2' WHERE id = 1").unwrap();

    storage::reset();
    catalog::reset();
    executor::execute("CREATE TABLE t (id int, name text)").unwrap();

    let applied = recovery::recover().unwrap();
    assert_eq!(applied, 3); // 2 inserts + 1 update

    let r = executor::execute("SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows[0][0], Some("alice_v2".into()));
    let r = executor::execute("SELECT name FROM t WHERE id = 2").unwrap();
    assert_eq!(r.rows[0][0], Some("bob".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_handles_mixed_mutations() {
    let path = tmp_recovery_path("mixed");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1), (2), (3), (4)").unwrap();
    executor::execute("DELETE FROM t WHERE id = 1").unwrap();
    executor::execute("UPDATE t SET id = 99 WHERE id = 3").unwrap();
    executor::execute("INSERT INTO t VALUES (5)").unwrap();

    storage::reset();
    catalog::reset();
    executor::execute("CREATE TABLE t (id int)").unwrap();

    recovery::recover().unwrap();
    let r = executor::execute("SELECT id FROM t ORDER BY id").unwrap();
    // Expected: 2, 4, 5, 99
    assert_eq!(r.rows.len(), 4);
    assert_eq!(r.rows[0][0], Some("2".into()));
    assert_eq!(r.rows[1][0], Some("4".into()));
    assert_eq!(r.rows[2][0], Some("5".into()));
    assert_eq!(r.rows[3][0], Some("99".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}
