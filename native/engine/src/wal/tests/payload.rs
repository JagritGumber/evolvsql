use super::super::*;
use super::{insert_op, tmp_wal_path};
use crate::types::Value;

#[test]
fn all_op_variants_round_trip() {
    let path = tmp_wal_path("variants");
    let writer = WalWriter::open(&path, 1).unwrap();
    writer.append(insert_op(1, "a")).unwrap();
    writer.append(WalOp::Update {
        schema: "public".into(), table: "users".into(),
        row_id: 42, new_row: vec![Value::Int(99)],
    }).unwrap();
    writer.append(WalOp::Delete {
        schema: "public".into(), table: "users".into(), row_id: 7,
    }).unwrap();
    writer.append(WalOp::Commit { txn_id: 123 }).unwrap();
    writer.append(WalOp::Checkpoint { up_to: 50 }).unwrap();
    writer.flush_sync().unwrap();
    drop(writer);

    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(entries.len(), 5);
    assert!(matches!(entries[0].op, WalOp::Insert { .. }));
    assert!(matches!(entries[1].op, WalOp::Update { row_id: 42, .. }));
    assert!(matches!(entries[2].op, WalOp::Delete { row_id: 7, .. }));
    assert!(matches!(entries[3].op, WalOp::Commit { txn_id: 123 }));
    assert!(matches!(entries[4].op, WalOp::Checkpoint { up_to: 50 }));
    std::fs::remove_file(&path).ok();
}

#[test]
fn vector_payload_round_trip() {
    let path = tmp_wal_path("vector");
    let writer = WalWriter::open(&path, 1).unwrap();
    let vec_row = vec![Value::Int(1), Value::Vector(vec![0.1, 0.2, 0.3, 0.4])];
    writer.append_sync(WalOp::Insert {
        schema: "public".into(), table: "embeds".into(), row: vec_row.clone(),
    }).unwrap();
    drop(writer);

    let entries = WalReader::open(&path).unwrap().read_all().unwrap();
    assert_eq!(entries.len(), 1);
    if let WalOp::Insert { row, .. } = &entries[0].op {
        assert_eq!(row, &vec_row);
    } else {
        panic!("expected Insert");
    }
    std::fs::remove_file(&path).ok();
}
