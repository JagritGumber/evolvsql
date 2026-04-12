use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn recover_errors_when_wal_disabled() {
    manager::disable();
    let r = recovery::recover();
    assert!(r.is_err());
    assert!(r.unwrap_err().contains("not enabled"));
}

#[test]
#[serial_test::serial]
fn recover_skips_entries_for_missing_tables() {
    let path = tmp_recovery_path("missing");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE a (id int)").unwrap();
    executor::execute("INSERT INTO a VALUES (1), (2)").unwrap();

    // Simulate restart without recreating table A
    storage::reset();
    catalog::reset();
    executor::execute("CREATE TABLE b (id int)").unwrap();

    let applied = recovery::recover().unwrap();
    assert_eq!(applied, 0, "entries for missing table should be skipped");

    manager::disable();
    std::fs::remove_file(&path).ok();
}
