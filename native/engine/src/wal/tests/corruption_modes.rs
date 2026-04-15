//! Additional WAL corruption modes beyond the torn-tail happy path.
//! The reader MUST treat every malformed `frame_len` header as a
//! torn-tail and stop cleanly, not panic / OOM / read garbage. A
//! single corrupt frame should never take down the engine: recovery
//! has to get the prefix of durable entries and nothing else.

use std::fs::OpenOptions;
use std::io::Write;

use super::super::*;
use super::{insert_op, tmp_wal_path};

/// Overwrite the frame_len header of the Nth frame (0-indexed) with
/// `value`. Returns the frame body length of every preceding frame
/// for offset math the caller might need.
fn overwrite_frame_len(path: &std::path::Path, target_idx: usize, value: u32) {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = OpenOptions::new().read(true).write(true).open(path).unwrap();
    let mut offset = 0u64;
    for _ in 0..target_idx {
        f.seek(SeekFrom::Start(offset)).unwrap();
        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf).unwrap();
        let frame_len = u32::from_le_bytes(len_buf) as u64;
        offset += 4 + frame_len;
    }
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(&value.to_le_bytes()).unwrap();
}

#[test]
fn frame_len_below_minimum_treated_as_torn() {
    // 8 (lsn) + 1 (tag) + 4 (crc) = 13 bytes minimum. A header of 5
    // can't possibly describe a real frame; the reader must stop
    // without trying to read 5 bytes and interpret them as lsn/tag/crc.
    let path = tmp_wal_path("corrupt_frame_len_too_small");
    let writer = WalWriter::open(&path, 1).unwrap();
    writer.append_sync(insert_op(1, "a")).unwrap();
    writer.append_sync(insert_op(2, "b")).unwrap();
    drop(writer);

    overwrite_frame_len(&path, 1, 5);

    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(
        entries.len(),
        1,
        "reader must stop at the undersized frame, returning only the valid prefix"
    );
    assert_eq!(entries[0].lsn, 1);

    std::fs::remove_file(&path).ok();
}

#[test]
fn frame_len_zero_treated_as_torn() {
    let path = tmp_wal_path("corrupt_frame_len_zero");
    let writer = WalWriter::open(&path, 1).unwrap();
    writer.append_sync(insert_op(1, "a")).unwrap();
    writer.append_sync(insert_op(2, "b")).unwrap();
    drop(writer);

    overwrite_frame_len(&path, 1, 0);

    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(entries.len(), 1);

    std::fs::remove_file(&path).ok();
}

#[test]
fn frame_len_exceeding_max_does_not_oom() {
    // A frame_len of u32::MAX (~4GB) could trigger a huge allocation
    // if the reader tried to honor it. The MAX_FRAME bound in
    // reader.rs caps this at 64MB and treats anything larger as torn
    // so the process stays alive on a corrupt file.
    let path = tmp_wal_path("corrupt_frame_len_huge");
    let writer = WalWriter::open(&path, 1).unwrap();
    writer.append_sync(insert_op(1, "a")).unwrap();
    writer.append_sync(insert_op(2, "b")).unwrap();
    drop(writer);

    overwrite_frame_len(&path, 1, u32::MAX);

    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(entries.len(), 1);

    std::fs::remove_file(&path).ok();
}

#[test]
fn frame_len_claims_more_bytes_than_remain_treated_as_torn() {
    // Valid minimum size but claims a body larger than the remaining
    // file. read_exact returns UnexpectedEof; reader must handle that
    // as torn-tail, not as a hard error.
    let path = tmp_wal_path("corrupt_frame_len_overshoots_file");
    let writer = WalWriter::open(&path, 1).unwrap();
    writer.append_sync(insert_op(1, "a")).unwrap();
    drop(writer);

    // Append just the 4-byte header of a ghost frame claiming 1 MB.
    let mut f = OpenOptions::new().append(true).open(&path).unwrap();
    f.write_all(&(1_000_000u32).to_le_bytes()).unwrap();
    drop(f);

    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(
        entries.len(),
        1,
        "reader must return the durable prefix and stop at the incomplete frame"
    );

    std::fs::remove_file(&path).ok();
}

#[test]
fn corrupt_first_frame_returns_empty_log_not_error() {
    // If the very first frame is corrupt, the reader must return an
    // empty Vec — not surface an error to recovery. An error here
    // would abort every subsequent startup until a human intervenes;
    // returning empty lets recovery proceed as if the WAL were fresh,
    // which is the only safe default for a torn-first-frame file
    // (e.g., a crash during the first-ever write).
    let path = tmp_wal_path("corrupt_first_frame");
    let writer = WalWriter::open(&path, 1).unwrap();
    writer.append_sync(insert_op(1, "a")).unwrap();
    drop(writer);

    overwrite_frame_len(&path, 0, 5);

    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert!(entries.is_empty());

    std::fs::remove_file(&path).ok();
}
