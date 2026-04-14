//! WAL-first DDL reorder creates a failure mode where the WAL entry
//! is written BEFORE the live-path validation runs. If the validation
//! then rejects the op (column already exists, target name taken,
//! source column missing), the WAL holds a phantom entry for an op
//! that logically never happened. Recovery must handle these
//! idempotently instead of aborting the entire replay on "column
//! already exists" or "relation already exists".
//!
//! Each test induces a phantom entry by running a DDL op that's
//! doomed to fail at the catalog mutation step, then simulates a
//! crash/recover cycle and asserts recovery still runs to completion
//! and the user-visible state survives intact.

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

fn simulate_restart(path: &std::path::Path) {
    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(path).unwrap();
    recovery::recover().unwrap();
}

fn teardown(path: &std::path::PathBuf) {
    manager::disable();
    let _ = std::fs::remove_file(path);
}

#[test]
#[serial_test::serial]
fn phantom_alter_add_column_does_not_break_recovery() {
    // An ALTER ADD COLUMN that gets as far as the WAL append but then
    // trips on "column already exists" must not crash the next
    // recovery. Without the idempotent replay check, the replayed
    // AlterAddColumn would call catalog::alter_table_add_column on a
    // column that already exists and abort the whole replay.
    let path = setup("phantom_add_col");
    executor::execute("CREATE TABLE t (id int, name text)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    // Duplicate-column ALTER. The WAL append lands because the WAL
    // doesn't pre-check; the catalog mutation then fails.
    let res = executor::execute("ALTER TABLE t ADD COLUMN name text");
    assert!(res.is_err(), "live duplicate ADD COLUMN must still error");

    // Simulate crash + recovery. This should not panic or error.
    simulate_restart(&path);

    let r = executor::execute("SELECT id, name FROM t").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("a".into()));

    teardown(&path);
}

#[test]
#[serial_test::serial]
fn phantom_rename_table_does_not_break_recovery() {
    let path = setup("phantom_rename_table");
    executor::execute("CREATE TABLE a (id int)").unwrap();
    executor::execute("CREATE TABLE b (id int)").unwrap();
    executor::execute("INSERT INTO a VALUES (1)").unwrap();
    executor::execute("INSERT INTO b VALUES (2)").unwrap();

    // Rename 'a' to 'b' — doomed because b exists. WAL gets the
    // phantom entry, catalog rejects the mutation.
    let res = executor::execute("ALTER TABLE a RENAME TO b");
    assert!(res.is_err());

    simulate_restart(&path);

    assert!(catalog::get_table("public", "a").is_some());
    assert!(catalog::get_table("public", "b").is_some());
    let r = executor::execute("SELECT id FROM a").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));
    let r = executor::execute("SELECT id FROM b").unwrap();
    assert_eq!(r.rows[0][0], Some("2".into()));

    teardown(&path);
}

#[test]
#[serial_test::serial]
fn phantom_rename_column_does_not_break_recovery() {
    let path = setup("phantom_rename_col");
    executor::execute("CREATE TABLE t (id int, name text)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    // Rename missing column — doomed at catalog::rename_column but
    // only after the WAL entry has landed.
    let res = executor::execute("ALTER TABLE t RENAME COLUMN ghost TO x");
    assert!(res.is_err());

    simulate_restart(&path);

    let r = executor::execute("SELECT id, name FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("a".into()));

    teardown(&path);
}

#[test]
#[serial_test::serial]
fn create_table_with_serial_can_retry_after_wal_failure() {
    // Regression for sequence leak: if WAL append fails, the SERIAL
    // sequence that parse_column_def created up-front must be
    // cleaned up so a retry of the same CREATE TABLE succeeds.
    use std::sync::atomic::Ordering;

    let path = setup("serial_retry");

    manager::FAIL_NEXT_APPEND.store(true, Ordering::SeqCst);
    let res = executor::execute("CREATE TABLE t (id SERIAL, name text)");
    assert!(res.is_err(), "first attempt must fail due to injected WAL failure");

    // Retry must succeed — if the sequence was leaked on the first
    // attempt, create_sequence would find it already exists and fail.
    executor::execute("CREATE TABLE t (id SERIAL, name text)").unwrap();
    executor::execute("INSERT INTO t (name) VALUES ('a'), ('b')").unwrap();
    let r = executor::execute("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[1][0], Some("2".into()));

    teardown(&path);
}
