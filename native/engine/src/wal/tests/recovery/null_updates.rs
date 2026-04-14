//! UPDATE recovery when rows cross the NULL boundary. apply_update
//! matches rows by content, so both directions (value -> NULL and
//! NULL -> value) need the serialized old_row and new_row to
//! faithfully round-trip Value::Null.

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
fn recover_update_sets_column_to_null() {
    let path = setup("update_null");
    executor::execute("CREATE TABLE t (id int, name text)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
    executor::execute("UPDATE t SET name = NULL WHERE id = 1").unwrap();
    wipe_and_recover();
    let r = executor::execute("SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows[0][0], None);
    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_update_from_null_to_value() {
    let path = setup("update_from_null");
    executor::execute("CREATE TABLE t (id int, name text)").unwrap();
    executor::execute("INSERT INTO t (id) VALUES (1)").unwrap();
    executor::execute("UPDATE t SET name = 'alice' WHERE id = 1").unwrap();
    wipe_and_recover();
    let r = executor::execute("SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows[0][0], Some("alice".into()));
    manager::disable();
    std::fs::remove_file(&path).ok();
}
