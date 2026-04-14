//! DDL ops must be WAL-first: if the WAL append fails, the catalog
//! and storage must stay unchanged. Prior ordering mutated first and
//! logged after, so a crash between the mutation and the WAL flush
//! would leave the in-memory state inconsistent with the durable log
//! — a later query would see a table that no recovery would ever
//! recreate, or a dropped table that would reappear on restart.
//!
//! We use the test-only `FAIL_NEXT_APPEND` fault injection to
//! simulate a WAL failure at the exact point each DDL op logs.

use std::sync::atomic::Ordering;

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

fn setup(path_name: &str) -> std::path::PathBuf {
    let path = tmp_recovery_path(path_name);
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();
    path
}

fn teardown(path: &std::path::PathBuf) {
    manager::disable();
    let _ = std::fs::remove_file(path);
}

#[test]
#[serial_test::serial]
fn create_table_wal_failure_leaves_catalog_untouched() {
    let path = setup("ddl_create_fail");

    manager::FAIL_NEXT_APPEND.store(true, Ordering::SeqCst);
    let res = executor::execute("CREATE TABLE t (id int, name text)");
    assert!(res.is_err(), "CREATE TABLE must fail when WAL append fails");

    assert!(
        catalog::get_table("public", "t").is_none(),
        "catalog must not contain the table"
    );
    // A follow-up query must fail with a missing-table error, not see
    // a phantom half-created table.
    assert!(executor::execute("SELECT * FROM t").is_err());

    teardown(&path);
}

#[test]
#[serial_test::serial]
fn drop_table_wal_failure_leaves_table_intact() {
    let path = setup("ddl_drop_fail");
    executor::execute("CREATE TABLE t (id int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1), (2)").unwrap();

    manager::FAIL_NEXT_APPEND.store(true, Ordering::SeqCst);
    let res = executor::execute("DROP TABLE t");
    assert!(res.is_err(), "DROP TABLE must fail when WAL append fails");

    assert!(
        catalog::get_table("public", "t").is_some(),
        "catalog must still have the table"
    );
    let r = executor::execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(
        r.rows[0][0],
        Some("2".into()),
        "rows must survive the failed drop"
    );

    teardown(&path);
}

#[test]
#[serial_test::serial]
fn alter_add_column_wal_failure_leaves_shape_unchanged() {
    let path = setup("ddl_add_col_fail");
    executor::execute("CREATE TABLE t (id int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1)").unwrap();

    manager::FAIL_NEXT_APPEND.store(true, Ordering::SeqCst);
    let res = executor::execute("ALTER TABLE t ADD COLUMN name text DEFAULT 'x'");
    assert!(res.is_err(), "ALTER ADD COLUMN must fail when WAL append fails");

    // The new column must not be visible.
    assert!(
        executor::execute("SELECT name FROM t").is_err(),
        "new column must not be in the catalog"
    );
    let r = executor::execute("SELECT id FROM t").unwrap();
    assert_eq!(r.rows.len(), 1);

    teardown(&path);
}

#[test]
#[serial_test::serial]
fn alter_drop_column_wal_failure_leaves_column_intact() {
    let path = setup("ddl_drop_col_fail");
    executor::execute("CREATE TABLE t (id int, name text)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    manager::FAIL_NEXT_APPEND.store(true, Ordering::SeqCst);
    let res = executor::execute("ALTER TABLE t DROP COLUMN name");
    assert!(res.is_err());

    let r = executor::execute("SELECT name FROM t").unwrap();
    assert_eq!(
        r.rows[0][0],
        Some("a".into()),
        "column data must still be readable after failed drop"
    );

    teardown(&path);
}

#[test]
#[serial_test::serial]
fn rename_table_wal_failure_leaves_name_unchanged() {
    let path = setup("ddl_rename_table_fail");
    executor::execute("CREATE TABLE t (id int)").unwrap();

    manager::FAIL_NEXT_APPEND.store(true, Ordering::SeqCst);
    let res = executor::execute("ALTER TABLE t RENAME TO t2");
    assert!(res.is_err());

    assert!(catalog::get_table("public", "t").is_some());
    assert!(catalog::get_table("public", "t2").is_none());

    teardown(&path);
}

#[test]
#[serial_test::serial]
fn rename_column_wal_failure_leaves_column_name_unchanged() {
    let path = setup("ddl_rename_col_fail");
    executor::execute("CREATE TABLE t (id int, name text)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    manager::FAIL_NEXT_APPEND.store(true, Ordering::SeqCst);
    let res = executor::execute("ALTER TABLE t RENAME COLUMN name TO label");
    assert!(res.is_err());

    let r = executor::execute("SELECT name FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("a".into()));
    assert!(executor::execute("SELECT label FROM t").is_err());

    teardown(&path);
}
