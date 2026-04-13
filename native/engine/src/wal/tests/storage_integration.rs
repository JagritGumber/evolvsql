//! Integration test for the WAL hook in storage::insert_batch_checked.
//! Verifies the full flow: enable manager -> INSERT via executor ->
//! WAL file contains the expected entries.

use super::super::*;
use crate::{catalog, executor, storage, types::Value};

fn tmp_integration_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("evolvsql_integ_{}_{}.log", name, std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

#[test]
#[serial_test::serial]
fn insert_batch_appends_to_wal_when_enabled() {
    let path = tmp_integration_path("insert_batch");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE users (id int, name text)").unwrap();
    executor::execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')").unwrap();

    let entries = manager::read_all().unwrap();
    // 1 CreateTable + 2 Inserts
    assert_eq!(entries.len(), 3);
    assert!(matches!(&entries[0].op, WalOp::CreateTable { .. }));
    let inserts: Vec<_> = entries.iter().filter(|e| matches!(e.op, WalOp::Insert { .. })).collect();
    assert_eq!(inserts.len(), 2);

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn insert_without_wal_unchanged() {
    // Sanity: existing behavior unchanged when WAL is disabled
    catalog::reset();
    storage::reset();
    manager::disable();

    executor::execute("CREATE TABLE u2 (id int)").unwrap();
    executor::execute("INSERT INTO u2 VALUES (1), (2), (3)").unwrap();
    let r = executor::execute("SELECT COUNT(*) FROM u2").unwrap();
    assert_eq!(r.rows[0][0], Some("3".into()));
}

#[test]
#[serial_test::serial]
fn wal_has_row_values_including_vector() {
    let path = tmp_integration_path("vec");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE embeds (id int, vec vector)").unwrap();
    executor::execute("INSERT INTO embeds VALUES (1, '[0.1, 0.2, 0.3]')").unwrap();

    let entries = manager::read_all().unwrap();
    // 1 CreateTable + 1 Insert
    assert_eq!(entries.len(), 2);
    let insert_entry = entries.iter().find(|e| matches!(e.op, WalOp::Insert { .. }))
        .expect("insert entry missing");
    if let WalOp::Insert { row, .. } = &insert_entry.op {
        assert!(matches!(row[0], Value::Int(1)));
        assert!(matches!(row[1], Value::Vector(_)));
    } else {
        panic!("expected Insert");
    }

    manager::disable();
    std::fs::remove_file(&path).ok();
}
