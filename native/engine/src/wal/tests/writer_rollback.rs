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
