//! Sequence cleanup for partial CREATE TABLE failures. `parse_column_def`
//! eagerly creates the SERIAL sequence before any of the WAL / catalog /
//! storage steps run. Every failure mode after that point has to drop
//! the sequence again, or a running server accumulates orphans and
//! later re-attempts of the same DDL fail with "sequence already
//! exists" forever.
//!
//! The live-path failures to cover:
//! - WAL append fails (FAIL_NEXT_APPEND)
//! - catalog::create_table fails (racing thread already created the
//!   table under a different definition)
//! - storage::create_table fails (storage and catalog drifted — a bug,
//!   but the cleanup path still needs to fire)
//! - setup_table_indexes fails (a constraint error at index build)

use std::sync::atomic::Ordering;

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, sequence, storage};

fn setup(name: &str) -> std::path::PathBuf {
    let path = tmp_recovery_path(name);
    catalog::reset();
    storage::reset();
    sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();
    path
}

fn teardown(path: &std::path::PathBuf) {
    manager::disable();
    let _ = std::fs::remove_file(path);
}

fn seq_exists(schema: &str, name: &str) -> bool {
    // `currval` is a pure read, unlike `nextval`, so it doesn't
    // mutate the sequence we're probing for existence.
    sequence::currval(schema, name).is_ok()
}

#[test]
#[serial_test::serial]
fn wal_failure_cleans_up_serial_sequence() {
    // Baseline: WAL append failure. Already covered elsewhere but
    // pinned here with a direct assertion on the sequence.
    let path = setup("serial_wal_fail");

    manager::FAIL_NEXT_APPEND.store(true, Ordering::SeqCst);
    let res = executor::execute("CREATE TABLE t (id SERIAL, name text)");
    assert!(res.is_err());

    // The sequence must be gone, not merely dormant.
    assert!(
        !seq_exists("public", "t_id_seq"),
        "WAL-failure path must drop the SERIAL sequence"
    );

    teardown(&path);
}

#[test]
#[serial_test::serial]
fn catalog_conflict_cleans_up_serial_sequence() {
    // Racing definition scenario. Another caller creates table `t`
    // without SERIAL; ours tries to create `t` with SERIAL. The WAL
    // append lands (WAL does not pre-check catalog state), then
    // catalog::create_table rejects with "relation already exists".
    // The SERIAL sequence we created in parse_column_def must be
    // dropped, not leaked.
    let path = setup("serial_catalog_conflict");

    executor::execute("CREATE TABLE t (id int)").unwrap();
    assert!(!seq_exists("public", "t_id_seq"));

    let res = executor::execute("CREATE TABLE t (id SERIAL, name text)");
    assert!(
        res.is_err(),
        "conflicting definition must fail at catalog::create_table"
    );
    assert!(
        !seq_exists("public", "t_id_seq"),
        "orphan sequence survived a failed CREATE TABLE — recent retry of \
         the same DDL would fail with 'already exists' forever"
    );

    // And the retry path (drop + recreate with SERIAL) must succeed.
    executor::execute("DROP TABLE t").unwrap();
    executor::execute("CREATE TABLE t (id SERIAL, name text)").unwrap();
    executor::execute("INSERT INTO t (name) VALUES ('a')").unwrap();
    let r = executor::execute("SELECT id FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));

    teardown(&path);
}
