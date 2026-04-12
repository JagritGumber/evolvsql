use super::super::*;
use crate::types::Value;

fn tmp_manager_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("evolvsql_mgr_{}_{}.log", name, std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

fn int_row(id: i64) -> Vec<Value> {
    vec![Value::Int(id)]
}

#[test]
#[serial_test::serial]
fn append_noop_when_disabled() {
    manager::disable();
    assert!(!manager::is_enabled());
    let result = manager::append_insert("public", "t", &int_row(1)).unwrap();
    assert_eq!(result, None, "disabled WAL should return None");
}

#[test]
#[serial_test::serial]
fn enable_and_append_persists_to_disk() {
    let path = tmp_manager_path("persist");
    manager::disable();
    manager::enable(&path).unwrap();
    assert!(manager::is_enabled());

    let lsn = manager::append_insert("public", "t", &int_row(42)).unwrap();
    assert_eq!(lsn, Some(1));

    // Read back via the manager's read_all
    let entries = manager::read_all().unwrap();
    assert_eq!(entries.len(), 1);
    assert!(matches!(&entries[0].op, WalOp::Insert { .. }));

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn read_all_returns_empty_when_disabled() {
    manager::disable();
    let entries = manager::read_all().unwrap();
    assert!(entries.is_empty());
}

#[test]
#[serial_test::serial]
fn multiple_appends_assigned_sequential_lsns() {
    let path = tmp_manager_path("seq");
    manager::disable();
    manager::enable(&path).unwrap();

    let l1 = manager::append_insert("public", "t", &int_row(1)).unwrap().unwrap();
    let l2 = manager::append_insert("public", "t", &int_row(2)).unwrap().unwrap();
    let l3 = manager::append_insert("public", "t", &int_row(3)).unwrap().unwrap();
    assert_eq!((l1, l2, l3), (1, 2, 3));

    manager::disable();
    std::fs::remove_file(&path).ok();
}
