use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn recover_restores_inserted_rows() {
    let path = tmp_recovery_path("basic");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    // Phase 1: write data with WAL enabled
    executor::execute("CREATE TABLE users (id int, name text)").unwrap();
    executor::execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')").unwrap();

    // Phase 2: simulate a crash - reset storage but keep the WAL file
    storage::reset();
    catalog::reset();
    executor::execute("CREATE TABLE users (id int, name text)").unwrap();

    let r = executor::execute("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(r.rows[0][0], Some("0".into()));

    // Phase 3: recover from WAL
    let applied = recovery::recover().unwrap();
    assert_eq!(applied, 3);

    let r = executor::execute("SELECT * FROM users ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Some("alice".into()));
    assert_eq!(r.rows[2][1], Some("carol".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}
