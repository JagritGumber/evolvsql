//! Recovery for ALTER TABLE RENAME TO / RENAME COLUMN.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn recover_rename_table() {
    let path = tmp_recovery_path("rename_tbl");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE old_name (id int)").unwrap();
    executor::execute("INSERT INTO old_name VALUES (1), (2)").unwrap();
    executor::execute("ALTER TABLE old_name RENAME TO new_name").unwrap();

    storage::reset();
    catalog::reset();
    recovery::recover().unwrap();

    let r = executor::execute("SELECT COUNT(*) FROM new_name").unwrap();
    assert_eq!(r.rows[0][0], Some("2".into()));
    assert!(executor::execute("SELECT * FROM old_name").is_err());

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_rename_column() {
    let path = tmp_recovery_path("rename_col");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int, old_col text)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 'x')").unwrap();
    executor::execute("ALTER TABLE t RENAME COLUMN old_col TO new_col").unwrap();

    storage::reset();
    catalog::reset();
    recovery::recover().unwrap();

    let r = executor::execute("SELECT new_col FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("x".into()));
    assert!(executor::execute("SELECT old_col FROM t").is_err());

    manager::disable();
    std::fs::remove_file(&path).ok();
}
