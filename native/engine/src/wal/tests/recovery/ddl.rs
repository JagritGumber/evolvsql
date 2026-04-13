//! Recovery tests for DDL operations. After PR 7, CreateTable and
//! DropTable entries are logged, so recovery can rebuild the entire
//! catalog without any external SQL.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn recover_from_empty_catalog_rebuilds_everything() {
    let path = tmp_recovery_path("full_rebuild");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    // Create multiple tables with data
    executor::execute("CREATE TABLE users (id int, name text)").unwrap();
    executor::execute("CREATE TABLE orders (id int, amount int)").unwrap();
    executor::execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')").unwrap();
    executor::execute("INSERT INTO orders VALUES (10, 100), (20, 200)").unwrap();

    // Total wipe: catalog AND storage
    catalog::reset();
    storage::reset();

    // Recovery alone should rebuild both tables and all rows
    let applied = recovery::recover().unwrap();
    assert_eq!(applied, 6, "2 CreateTable + 4 Inserts");

    let users = executor::execute("SELECT * FROM users ORDER BY id").unwrap();
    assert_eq!(users.rows.len(), 2);
    let orders = executor::execute("SELECT * FROM orders ORDER BY id").unwrap();
    assert_eq!(orders.rows.len(), 2);

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_replays_drop_table() {
    let path = tmp_recovery_path("drop");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE doomed (id int)").unwrap();
    executor::execute("INSERT INTO doomed VALUES (1), (2)").unwrap();
    executor::execute("DROP TABLE doomed").unwrap();

    catalog::reset();
    storage::reset();

    // Recover: the DROP should leave the table gone even though
    // we had earlier CREATE and INSERT entries
    recovery::recover().unwrap();
    let r = executor::execute("SELECT * FROM doomed");
    assert!(r.is_err(), "doomed table should not exist after recovery");

    manager::disable();
    std::fs::remove_file(&path).ok();
}
