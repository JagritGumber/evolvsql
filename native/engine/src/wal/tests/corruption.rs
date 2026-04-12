use std::fs::OpenOptions;
use std::io::Write;

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
