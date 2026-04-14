//! Regression tests for the durable_len / rollback behavior of the
//! WAL writer. The pre-fix writer wrapped its File in a BufWriter:
//! a failed `flush_sync` left the encoded frame in the buffer, and
//! the next successful `flush_sync` (for a completely unrelated
//! operation) would quietly commit the failed frame to disk. At the
//! caller level that first op had returned Err and been rolled back
//! at the SQL layer; recovery would then replay the resurrected
//! frame and produce a phantom row / ghost write.

use std::sync::atomic::Ordering;

use super::super::*;
use super::insert_op;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "evolvsql_rollback_{}_{}.log",
        name,
        std::process::id()
    ));
    let _ = std::fs::remove_file(&p);
    p
}

#[test]
fn flush_sync_failure_drops_buffered_frame() {
    let path = tmp("flush_fail_drops");
    let writer = WalWriter::open(&path, 1).unwrap();

    // Frame 1 — this will be lost.
    writer.append(insert_op(1, "frame-one")).unwrap();
    writer.fail_next_sync.store(true, Ordering::SeqCst);
    let res = writer.flush_sync();
    assert!(res.is_err(), "injected fsync must surface as Err");

    // Frame 2 — this is what really commits. If the rollback didn't
    // truncate frame 1, this flush_sync would make BOTH frames durable
    // and recovery would see the lost frame come back from the dead.
    writer.append(insert_op(2, "frame-two")).unwrap();
    writer.flush_sync().unwrap();

    drop(writer);
    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(
        entries.len(),
        1,
        "only the successfully flushed frame should be on disk"
    );
    // The surviving frame is the second append — the one the caller
    // was told landed.
    match &entries[0].op {
        WalOp::Insert { row, .. } => {
            assert!(
                format!("{:?}", row).contains("frame-two"),
                "surviving frame must be frame-two, got {:?}",
                row
            );
        }
        other => panic!("expected Insert, got {:?}", other),
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn flush_sync_failure_rewinds_next_lsn() {
    // If the LSN counter were to keep advancing across a failed
    // flush, the next successful append would leave a gap in the
    // on-disk LSN sequence. Recovery's contiguity checks assume
    // successive frames are contiguous in LSN, so a gap would look
    // identical to a torn-write tail and recovery would stop at the
    // gap, losing every subsequent durable frame.
    let path = tmp("flush_fail_rewinds_lsn");
    let writer = WalWriter::open(&path, 1).unwrap();

    let first_lsn = writer.append(insert_op(1, "a")).unwrap();
    assert_eq!(first_lsn, 1);
    writer.fail_next_sync.store(true, Ordering::SeqCst);
    assert!(writer.flush_sync().is_err());

    // After rollback, next_lsn must be back at the start of the lost
    // batch, not advanced past it.
    assert_eq!(
        writer.peek_next_lsn(),
        1,
        "next_lsn must rewind so retries re-use the reclaimed LSN"
    );

    let retry_lsn = writer.append(insert_op(2, "b")).unwrap();
    assert_eq!(retry_lsn, 1, "retry after rollback must re-use LSN 1");
    writer.flush_sync().unwrap();

    drop(writer);
    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].lsn, 1);

    std::fs::remove_file(&path).ok();
}

#[test]
fn concurrent_append_sync_with_failing_peer_preserves_other_thread_durability() {
    // Devin-found concurrency hole: `append` and `flush_sync` are not
    // atomic. Without the combined `append_sync` path holding the
    // mutex across both, the following interleaving silently loses
    // data:
    //
    //   T1: append(A)        // frame A on disk, mutex released
    //   T2: append(B)        // frame B on disk, mutex released
    //   T1: flush_sync fails // rollback truncates BOTH frames
    //   T2: flush_sync       // sync on empty tail, returns Ok(())
    //
    // T2's caller believes B is durable but it was truncated.
    // The production path now uses `append_sync`, which holds the
    // inner mutex across both steps. T2 cannot acquire the lock
    // until T1's rollback has finished and returned an error, so on
    // its own entry T2 starts from the durable tail.
    use std::sync::Arc;
    use std::thread;

    let path = tmp("concurrent_failing_peer");
    let writer = Arc::new(WalWriter::open(&path, 1).unwrap());

    // Pre-arm one failed sync. The thread that grabs the lock first
    // will fail; the thread that comes second must still land
    // durably.
    writer.fail_next_sync.store(true, Ordering::SeqCst);

    let w1 = Arc::clone(&writer);
    let w2 = Arc::clone(&writer);
    let h1 = thread::spawn(move || w1.append_sync(insert_op(1, "first")));
    let h2 = thread::spawn(move || w2.append_sync(insert_op(2, "second")));
    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();

    assert_eq!(
        r1.is_err() as u8 + r2.is_err() as u8,
        1,
        "exactly one append_sync should fail: {:?} {:?}",
        r1,
        r2
    );
    let ok_entry = match (r1, r2) {
        (Ok(e), Err(_)) | (Err(_), Ok(e)) => e,
        other => panic!("unexpected outcome: {:?}", other),
    };

    drop(writer);
    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(
        entries.len(),
        1,
        "the successful append_sync must land durably"
    );
    assert_eq!(entries[0].lsn, ok_entry.lsn);

    std::fs::remove_file(&path).ok();
}

#[test]
fn failing_append_sync_with_prior_batched_frame_does_not_leave_lsn_gap() {
    // Devin-found follow-up: when `append_sync` fails with prior
    // `append` calls still un-flushed, its old rollback truncated
    // the file but did not rewind `next_lsn`. The buffered frames
    // were destroyed from the file but their LSNs stayed consumed,
    // so the next write skipped past the destroyed range and
    // produced a permanent gap that recovery mistakes for a torn
    // tail, halting replay at the gap and losing every later frame.
    //
    //   1. append(A)       → lsn=1, undurable_start_lsn=Some(1)
    //   2. append_sync(B)  → fails during sync. Old rollback
    //                        truncated file but left next_lsn=2.
    //   3. append_sync(C)  → would get lsn=2 under old behavior,
    //                        leaving lsn=1 as a permanent gap.
    //
    // The fix routes append_sync's error paths through the full
    // `rollback` helper, which rewinds next_lsn to the start of the
    // destroyed batch.
    let path = tmp("append_sync_gap");
    let writer = WalWriter::open(&path, 1).unwrap();

    let lsn_a = writer.append(insert_op(1, "a")).unwrap();
    assert_eq!(lsn_a, 1);

    writer.fail_next_sync.store(true, Ordering::SeqCst);
    assert!(writer.append_sync(insert_op(2, "b")).is_err());

    assert_eq!(
        writer.peek_next_lsn(),
        1,
        "append_sync rollback must rewind past destroyed batched frames"
    );

    let entry_c = writer.append_sync(insert_op(3, "c")).unwrap();
    assert_eq!(entry_c.lsn, 1, "retry must reclaim the reclaimed LSN");

    drop(writer);
    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].lsn, 1);

    std::fs::remove_file(&path).ok();
}

#[test]
fn append_sync_clears_undurable_marker_so_later_rollback_does_not_rewind_too_far() {
    // Devin-found: append_sync's success path used to leave
    // `undurable_start_lsn` stale. A subsequent batched append +
    // failing flush_sync would then see the stale marker and rewind
    // `next_lsn` past frames that were already durable, causing
    // duplicate LSNs on the next write. Sequence:
    //
    //   1. append(A)          → undurable_start_lsn = Some(LSN_A)
    //   2. append_sync(B)     → fsyncs A+B, but forgets to clear
    //                           undurable_start_lsn
    //   3. append(C)          → no-op on the marker (already set)
    //   4. flush_sync fails   → rollback reads stale Some(LSN_A) and
    //                           stores next_lsn = LSN_A, past already
    //                           durable frames
    //   5. append(D)          → reuses LSN_A, duplicate on disk
    let path = tmp("append_sync_clears_marker");
    let writer = WalWriter::open(&path, 1).unwrap();

    let lsn_a = writer.append(insert_op(1, "a")).unwrap();
    assert_eq!(lsn_a, 1);
    let entry_b = writer.append_sync(insert_op(2, "b")).unwrap();
    assert_eq!(entry_b.lsn, 2);
    let lsn_c = writer.append(insert_op(3, "c")).unwrap();
    assert_eq!(lsn_c, 3);
    writer.fail_next_sync.store(true, Ordering::SeqCst);
    assert!(writer.flush_sync().is_err());

    // The rollback must only unwind frame C, NOT frames A and B.
    // Without the fix, next_lsn would rewind to 1 and the retry
    // below would reuse LSN 1, leaving two frames with LSN 1 on disk.
    assert_eq!(
        writer.peek_next_lsn(),
        3,
        "rollback must not rewind past the durable tail"
    );

    let entry_d = writer.append_sync(insert_op(4, "d")).unwrap();
    assert_eq!(entry_d.lsn, 3);

    drop(writer);
    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].lsn, 1);
    assert_eq!(entries[1].lsn, 2);
    assert_eq!(entries[2].lsn, 3);

    std::fs::remove_file(&path).ok();
}

#[test]
fn flush_sync_failure_keeps_prior_durable_entries() {
    // A failed flush must NOT discard anything that had been made
    // durable by an earlier successful flush. The truncate-back
    // target is the last durable_len, not zero.
    let path = tmp("flush_fail_keeps_prior");
    let writer = WalWriter::open(&path, 1).unwrap();

    writer.append_sync(insert_op(1, "keep-me")).unwrap();

    writer.append(insert_op(2, "lose-me")).unwrap();
    writer.fail_next_sync.store(true, Ordering::SeqCst);
    assert!(writer.flush_sync().is_err());

    // Counter is back at LSN 2 (the last durable was LSN 1).
    assert_eq!(writer.peek_next_lsn(), 2);
    writer.append_sync(insert_op(3, "and-me")).unwrap();

    drop(writer);
    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].lsn, 1);
    assert_eq!(entries[1].lsn, 2, "LSN slot 2 is reused by the retry");

    std::fs::remove_file(&path).ok();
}
