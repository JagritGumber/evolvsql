//! LSN continuation and idempotence: a recovered database must keep
//! writing to the same WAL file with monotonically increasing LSNs, and
//! replaying the same WAL file twice must produce the same state.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn post_recovery_writes_get_monotonic_lsns() {
    let path = tmp_recovery_path("lsn_cont");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1), (2)").unwrap();

    // Capture pre-crash WAL state
    let pre = manager::read_all().unwrap();
    let max_pre_lsn = pre.iter().map(|e| e.lsn).max().unwrap();

    // Crash + recover
    storage::reset();
    catalog::reset();
    recovery::recover().unwrap();

    // Post-recovery insert must log with LSN strictly greater than any
    // pre-recovery entry. Without enable_at_lsn this would collide.
    executor::execute("INSERT INTO t VALUES (3)").unwrap();

    let post = manager::read_all().unwrap();
    let new_entries: Vec<_> = post.iter().filter(|e| e.lsn > max_pre_lsn).collect();
    assert!(
        !new_entries.is_empty(),
        "no new WAL entries written after recovery"
    );
    for e in &new_entries {
        assert!(e.lsn > max_pre_lsn, "LSN collision: {} <= {}", e.lsn, max_pre_lsn);
    }

    let r = executor::execute("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 3);

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_twice_from_same_wal_produces_same_state() {
    // Recovery must be a pure function of the WAL file: two runs with
    // the same input produce the same end state. Anything else means
    // replay has hidden side effects that break crash-safety.
    let path = tmp_recovery_path("idempotent");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int, name text)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
    executor::execute("DELETE FROM t WHERE id = 2").unwrap();

    // First recovery
    storage::reset();
    catalog::reset();
    let first = recovery::recover().unwrap();
    let r1 = executor::execute("SELECT id, name FROM t ORDER BY id").unwrap();

    // Second recovery from the same file
    storage::reset();
    catalog::reset();
    let second = recovery::recover().unwrap();
    let r2 = executor::execute("SELECT id, name FROM t ORDER BY id").unwrap();

    assert_eq!(first, second, "applied-entry count must be stable");
    assert_eq!(r1.rows, r2.rows, "recovered rows must be identical");

    manager::disable();
    std::fs::remove_file(&path).ok();
}
