use super::super::*;
use super::{insert_op, tmp_wal_path};

#[test]
fn append_and_read_single_entry() {
    let path = tmp_wal_path("single");
    let writer = WalWriter::open(&path, 1).unwrap();
    let entry = writer.append_sync(insert_op(1, "alice")).unwrap();
    assert_eq!(entry.lsn, 1);
    drop(writer);

    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].lsn, 1);
    assert_eq!(entries[0].op, insert_op(1, "alice"));
    std::fs::remove_file(&path).ok();
}

#[test]
fn append_multiple_preserves_order() {
    let path = tmp_wal_path("multi");
    let writer = WalWriter::open(&path, 1).unwrap();
    for i in 1..=50 {
        writer.append(insert_op(i, "row")).unwrap();
    }
    writer.flush_sync().unwrap();
    drop(writer);

    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(entries.len(), 50);
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e.lsn, (i + 1) as Lsn);
    }
    std::fs::remove_file(&path).ok();
}

#[test]
fn lsn_starts_from_passed_value() {
    let path = tmp_wal_path("lsn_start");
    let writer = WalWriter::open(&path, 100).unwrap();
    let e = writer.append_sync(insert_op(1, "x")).unwrap();
    assert_eq!(e.lsn, 100);
    assert_eq!(writer.peek_next_lsn(), 101);
    std::fs::remove_file(&path).ok();
}
