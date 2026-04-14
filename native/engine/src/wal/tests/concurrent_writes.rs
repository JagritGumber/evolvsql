//! End-to-end concurrent INSERT stress with WAL enabled. Exercises
//! the full executor -> storage -> WAL writer path from multiple
//! threads writing to separate tables, which is the scenario the
//! LSN-ordering fix in writer.rs was meant to handle.
//!
//! Checks (post-recovery):
//! - every row written by every thread is present;
//! - WAL file order matches LSN order for the whole session;
//! - no LSN gaps.

use std::sync::{Arc, Barrier};
use std::thread;

use super::super::*;
use super::tmp_wal_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn concurrent_inserts_to_multiple_tables_recover_cleanly() {
    let path = tmp_wal_path("concurrent_inserts");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    const TABLES: usize = 4;
    const PER_TABLE: usize = 32;

    for t in 0..TABLES {
        executor::execute(&format!("CREATE TABLE t{} (id int, v text)", t)).unwrap();
    }

    let barrier = Arc::new(Barrier::new(TABLES));
    let mut handles = Vec::with_capacity(TABLES);
    for t in 0..TABLES {
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            for i in 0..PER_TABLE {
                let sql = format!("INSERT INTO t{} VALUES ({}, 'row{}')", t, i, i);
                executor::execute(&sql).unwrap();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    // Recover from the WAL and verify every row made it through.
    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    for t in 0..TABLES {
        let r = executor::execute(&format!("SELECT COUNT(*) FROM t{}", t)).unwrap();
        assert_eq!(
            r.rows[0][0],
            Some(PER_TABLE.to_string()),
            "table t{} lost rows during concurrent insert + recovery",
            t
        );
    }

    // WAL file order must still match LSN order AND be contiguous —
    // the writer.rs fix puts the fetch_add inside the mutex so the
    // Nth frame has LSN = starting_lsn + N - 1. A gap (e.g. 1, 2, 4)
    // would mean the writer lost a frame between append and flush,
    // which is the exact durability hole #62 closed.
    let entries = manager::read_all().unwrap();
    let mut prev = 0u64;
    for e in &entries {
        assert_eq!(
            e.lsn,
            prev + 1,
            "LSN gap or reorder: expected {} but got {}",
            prev + 1,
            e.lsn
        );
        prev = e.lsn;
    }

    manager::disable();
    std::fs::remove_file(&path).ok();
}
