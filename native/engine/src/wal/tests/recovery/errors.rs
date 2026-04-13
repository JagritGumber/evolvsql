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
fn recover_recreates_tables_from_ddl_log() {
    // With DDL logging (PR 7), recovery rebuilds the catalog from
    // the WAL's CreateTable entries. No external CREATE needed.
    let path = tmp_recovery_path("ddl");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE a (id int, name text)").unwrap();
    executor::execute("INSERT INTO a VALUES (1, 'x'), (2, 'y')").unwrap();

    // Simulate restart: clear everything including catalog
    storage::reset();
    catalog::reset();

    // Recovery should recreate table a AND apply the inserts
    let applied = recovery::recover().unwrap();
    assert_eq!(applied, 3, "1 CreateTable + 2 Inserts");

    let r = executor::execute("SELECT id, name FROM a ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][1], Some("x".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}
