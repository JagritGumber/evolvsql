//! Recovery for ALTER TABLE ADD/DROP COLUMN. Without logging, a crash
//! after column changes would leave the in-memory schema ahead of the
//! durable log and any queries on the new shape would break.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn recover_alter_add_column_with_default() {
    let path = tmp_recovery_path("alter_add");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1), (2)").unwrap();
    executor::execute("ALTER TABLE t ADD COLUMN name text DEFAULT 'unknown'").unwrap();
    executor::execute("INSERT INTO t VALUES (3, 'carol')").unwrap();

    storage::reset();
    catalog::reset();
    recovery::recover().unwrap();

    let r = executor::execute("SELECT id, name FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Some("unknown".into()));
    assert_eq!(r.rows[1][1], Some("unknown".into()));
    assert_eq!(r.rows[2][1], Some("carol".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_alter_drop_column() {
    let path = tmp_recovery_path("alter_drop");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int, name text, age int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20)").unwrap();
    executor::execute("ALTER TABLE t DROP COLUMN name").unwrap();

    storage::reset();
    catalog::reset();
    recovery::recover().unwrap();

    let r = executor::execute("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows[0].len(), 2, "name column should be gone after recovery");
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("10".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}
