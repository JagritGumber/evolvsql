//! Concurrent append stress. LSN assignment used to happen via an
//! atomic fetch_add outside the writer mutex, so a thread with a
//! lower LSN could lose the race to the lock and write its frame
//! AFTER a thread with a higher LSN. Recovery reads frames in file
//! order, so out-of-order LSNs meant out-of-order replay — and a
//! torn-write tail could silently keep a higher-LSN frame while
//! dropping the lower-LSN one that the next thread was still
//! writing. This test runs many concurrent appends against a shared
//! writer and asserts:
//!   1. LSNs are unique and contiguous (1..=N).
//!   2. File order exactly matches LSN order.

use std::sync::Arc;
use std::thread;

use super::super::*;
use super::{insert_op, tmp_wal_path};

#[test]
fn concurrent_appends_preserve_lsn_file_order() {
    let path = tmp_wal_path("concurrent_lsn");
    let writer = Arc::new(WalWriter::open(&path, 1).unwrap());

    const THREADS: usize = 8;
    const PER_THREAD: usize = 64;

    let mut handles = Vec::with_capacity(THREADS);
    for t in 0..THREADS {
        let w = Arc::clone(&writer);
        handles.push(thread::spawn(move || {
            let mut lsns = Vec::with_capacity(PER_THREAD);
            for i in 0..PER_THREAD {
                let op = insert_op((t * 1000 + i) as i64, "x");
                lsns.push(w.append(op).unwrap());
            }
            lsns
        }));
    }
    let mut all_lsns: Vec<Lsn> = Vec::new();
    for h in handles {
        all_lsns.extend(h.join().unwrap());
    }
    writer.flush_sync().unwrap();
    drop(writer);

    // Uniqueness + contiguity.
    all_lsns.sort();
    let total = (THREADS * PER_THREAD) as u64;
    let expected: Vec<Lsn> = (1..=total).collect();
    assert_eq!(all_lsns, expected, "LSNs must be unique and contiguous");

    // File order must match LSN order.
    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(entries.len(), total as usize);
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e.lsn, (i as u64) + 1, "entry {} has LSN {} (expected {})", i, e.lsn, i + 1);
    }
    std::fs::remove_file(&path).ok();
}
