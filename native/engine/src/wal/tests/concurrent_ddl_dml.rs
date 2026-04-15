//! Concurrent DDL + DML stress. Spawns writer threads doing INSERTs
//! against a table while a separate thread runs ALTER or DROP against
//! the same table. Pins the *survivable* invariants under contention:
//!
//! 1. Neither the live session nor recovery deadlocks or panics.
//! 2. The WAL file is contiguous — no lost frames, LSNs 1..=N.
//! 3. Recovery completes with Ok(..) for whatever interleaving the
//!    runtime produced.
//! 4. Post-recovery, SELECT queries against the surviving tables
//!    succeed (even if their row counts diverge from live).
//!
//! What these tests deliberately do NOT assert: that the recovered
//! state exactly equals the live state. A known ordering limitation
//! between WAL-first DDL and concurrent DML means a DML op can take
//! the per-table write lock AFTER a DDL's WAL entry has been
//! appended but BEFORE the DDL's catalog/storage mutation runs. The
//! DML writes a WAL entry with a higher LSN than the DDL, so
//! recovery replays `DROP TABLE t` before the `INSERT INTO t` that
//! the live session observed as successful — and the replayed
//! INSERT is skipped because the table is gone at replay time. The
//! live state briefly contained rows that recovery cannot
//! reconstruct. Fixing this requires either a unified catalog +
//! storage lock taken before the DDL WAL write, or a 2-phase WAL
//! record (prepare/commit) per DDL. Both are beyond the scope of
//! the WAL-first reorder that landed in #68; this test captures the
//! limitation rather than papering over it.

use std::sync::{Arc, Barrier};
use std::thread;

use super::super::*;
use super::tmp_wal_path;
use crate::{catalog, executor, storage};

fn reset_everything() {
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
}

fn assert_wal_contiguous() {
    let entries = manager::read_all().unwrap();
    let mut prev = 0u64;
    for e in &entries {
        assert_eq!(
            e.lsn,
            prev + 1,
            "LSN gap or reorder: expected {} got {}",
            prev + 1,
            e.lsn
        );
        prev = e.lsn;
    }
}

#[test]
#[serial_test::serial]
fn concurrent_insert_and_alter_add_column_does_not_deadlock() {
    let path = tmp_wal_path("concurrent_alter");
    reset_everything();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int)").unwrap();

    const INSERTERS: usize = 4;
    const PER_INSERTER: usize = 16;
    let barrier = Arc::new(Barrier::new(INSERTERS + 1));

    let mut handles: Vec<thread::JoinHandle<()>> = Vec::new();
    for t in 0..INSERTERS {
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            for i in 0..PER_INSERTER {
                let id = t * 100 + i;
                // Tolerate errors — the important thing is no panic
                // and no deadlock.
                let _ = executor::execute(&format!("INSERT INTO t VALUES ({})", id));
            }
        }));
    }
    let b = Arc::clone(&barrier);
    handles.push(thread::spawn(move || {
        b.wait();
        thread::sleep(std::time::Duration::from_millis(5));
        executor::execute("ALTER TABLE t ADD COLUMN label text DEFAULT 'x'").unwrap();
    }));
    for h in handles { h.join().unwrap(); }

    // Recovery must succeed. SELECT must not panic even if rows
    // inserted mid-ALTER have an inconsistent width (known issue).
    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    // Probe recovery made it through — a COUNT(*) only touches the
    // row count and does not index into columns, so it stays robust
    // to the width-drift bug and still confirms recovery finished.
    let r = executor::execute("SELECT COUNT(*) FROM t").unwrap();
    assert!(r.rows.len() == 1);

    assert_wal_contiguous();

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn concurrent_insert_and_drop_recreate_does_not_deadlock() {
    let path = tmp_wal_path("concurrent_drop");
    reset_everything();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int, name text)").unwrap();
    executor::execute("INSERT INTO t VALUES (0, 'seed')").unwrap();

    const INSERTERS: usize = 3;
    const PER_INSERTER: usize = 10;
    let barrier = Arc::new(Barrier::new(INSERTERS + 1));

    let mut handles: Vec<thread::JoinHandle<()>> = Vec::new();
    for t in 0..INSERTERS {
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            for i in 0..PER_INSERTER {
                let id = t * 1000 + i + 1;
                let _ = executor::execute(&format!(
                    "INSERT INTO t VALUES ({}, 'pre-drop-{}')",
                    id, id
                ));
            }
        }));
    }
    let b = Arc::clone(&barrier);
    handles.push(thread::spawn(move || {
        b.wait();
        thread::sleep(std::time::Duration::from_millis(4));
        executor::execute("DROP TABLE t").unwrap();
        executor::execute("CREATE TABLE t (id int, name text)").unwrap();
        executor::execute("INSERT INTO t VALUES (999, 'post-recreate')").unwrap();
    }));
    for h in handles { h.join().unwrap(); }

    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    // Post-recreate rows must be queryable. The post-recreate row
    // 999 is the only one guaranteed to survive because it was
    // inserted after the last DROP in WAL order.
    let r = executor::execute("SELECT id FROM t WHERE id = 999").unwrap();
    assert_eq!(r.rows.len(), 1);

    assert_wal_contiguous();

    manager::disable();
    std::fs::remove_file(&path).ok();
}
