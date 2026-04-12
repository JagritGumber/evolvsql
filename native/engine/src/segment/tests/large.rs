use super::super::*;
use super::{tmp_path, user_row, users_schema};

/// Stress test: write many rows and verify round-trip. Exercises the
/// bincode buffer sizing and zone map computation on a larger dataset.
#[test]
fn round_trip_10k_rows() {
    let path = tmp_path("large10k");
    let schema = users_schema();
    let rows: Vec<Vec<crate::types::Value>> = (0..10_000)
        .map(|i| user_row(i, &format!("user_{}", i)))
        .collect();
    SegmentWriter::write(&path, &schema, &rows).unwrap();

    let mut reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.row_count(), 10_000);

    // Spot-check via column read (not full row read, which is slower).
    let ids = reader.read_column("id").unwrap();
    assert_eq!(ids.len(), 10_000);
    assert_eq!(ids[0], crate::types::Value::Int(0));
    assert_eq!(ids[9_999], crate::types::Value::Int(9_999));

    // Zone map should have picked up the full range.
    let id_meta = reader.column_meta("id").unwrap();
    assert_eq!(id_meta.min, Some(crate::types::Value::Int(0)));
    assert_eq!(id_meta.max, Some(crate::types::Value::Int(9_999)));

    std::fs::remove_file(&path).ok();
}
