use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

use super::super::*;
use super::{insert_op, tmp_wal_path};

#[test]
fn corrupt_tail_treated_as_eof() {
    let path = tmp_wal_path("corrupt");
    let writer = WalWriter::open(&path, 1).unwrap();
    writer.append_sync(insert_op(1, "a")).unwrap();
    writer.append_sync(insert_op(2, "b")).unwrap();
    drop(writer);

    // Append garbage bytes to the end of the file to simulate torn write
    let mut f = OpenOptions::new().append(true).open(&path).unwrap();
    f.write_all(&[0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00]).unwrap();
    drop(f);

    // Reader should return the 2 valid entries and stop at the torn frame
    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].lsn, 1);
    assert_eq!(entries[1].lsn, 2);
    std::fs::remove_file(&path).ok();
}

#[test]
fn mid_file_bit_flip_stops_at_damaged_frame() {
    // Write three entries, then flip a byte inside the second frame's
    // payload. The reader should return entry 1, then treat frame 2 as
    // the end of the durable log (CRC mismatch). Entry 3 is unreachable
    // even though it's well-formed: WAL is a linear scan, not random
    // access, so damage in the middle truncates everything after it.
    let path = tmp_wal_path("bitflip");
    let writer = WalWriter::open(&path, 1).unwrap();
    writer.append_sync(insert_op(1, "a")).unwrap();
    writer.append_sync(insert_op(2, "b")).unwrap();
    writer.append_sync(insert_op(3, "c")).unwrap();
    drop(writer);

    // Read first frame_len to locate frame 2
    let mut f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
    let mut len_buf = [0u8; 4];
    f.read_exact(&mut len_buf).unwrap();
    let frame1_len = u32::from_le_bytes(len_buf) as u64;
    // Flip a byte inside frame 2's payload region (skip the len + lsn
    // + tag to land in the payload). Offset: 4 (frame1 len prefix) +
    // frame1_len (frame1 body) + 4 (frame2 len prefix) + 8 (lsn) + 1 (tag).
    let flip_offset = 4 + frame1_len + 4 + 8 + 1;
    f.seek(SeekFrom::Start(flip_offset)).unwrap();
    let mut b = [0u8; 1];
    f.read_exact(&mut b).unwrap();
    f.seek(SeekFrom::Start(flip_offset)).unwrap();
    f.write_all(&[b[0] ^ 0xFF]).unwrap();
    drop(f);

    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(entries.len(), 1, "reader must stop at the corrupt frame");
    assert_eq!(entries[0].lsn, 1);
    std::fs::remove_file(&path).ok();
}

#[test]
fn empty_wal_file_reads_zero_entries() {
    let path = tmp_wal_path("empty");
    File::create(&path).unwrap();
    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert!(entries.is_empty());
    std::fs::remove_file(&path).ok();
}

#[test]
fn truncated_header_is_not_an_error() {
    // A crash between fsync calls can leave the file with fewer than
    // 4 bytes of a new frame. That is indistinguishable from clean EOF
    // and must not surface as an error: recovery's contract is "return
    // everything durable, stop at damage."
    let path = tmp_wal_path("trunc_hdr");
    let writer = WalWriter::open(&path, 1).unwrap();
    writer.append_sync(insert_op(1, "a")).unwrap();
    drop(writer);

    let mut f = OpenOptions::new().append(true).open(&path).unwrap();
    f.write_all(&[0x12, 0x34]).unwrap(); // 2 bytes, not a full len header
    drop(f);

    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(entries.len(), 1);
    std::fs::remove_file(&path).ok();
}
