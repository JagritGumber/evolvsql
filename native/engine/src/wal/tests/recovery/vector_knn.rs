//! Recovery for vector KNN queries. HNSW indexes are NOT persisted to
//! the WAL — they're lazily rebuilt from storage the first time a
//! query needs them. This test verifies that the rebuild produces
//! correct KNN results after a crash, not just that the raw vector
//! bytes survive (already covered in vector.rs).

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, storage};

#[test]
#[serial_test::serial]
fn recover_knn_query_returns_correct_nearest_neighbor() {
    let path = tmp_recovery_path("knn");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    crate::executor::execute("CREATE TABLE pts (id int, pos vector)").unwrap();
    crate::executor::execute("INSERT INTO pts VALUES (1, '[0.0, 0.0]')").unwrap();
    crate::executor::execute("INSERT INTO pts VALUES (2, '[1.0, 0.0]')").unwrap();
    crate::executor::execute("INSERT INTO pts VALUES (3, '[0.0, 1.0]')").unwrap();
    crate::executor::execute("INSERT INTO pts VALUES (4, '[10.0, 10.0]')").unwrap();

    // Warm the HNSW index so we know it worked pre-crash.
    let pre = crate::executor::execute(
        "SELECT id FROM pts ORDER BY pos <-> '[0.1, 0.1]' LIMIT 2",
    )
    .unwrap();
    assert_eq!(pre.rows[0][0], Some("1".into()));

    // Simulate crash: wipe everything including sequence state.
    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    // HNSW must be rebuilt lazily on first KNN query, and return the
    // same nearest neighbor. If the rebuild path is broken we'd see
    // either a missing index error or stale row ids.
    let post = crate::executor::execute(
        "SELECT id FROM pts ORDER BY pos <-> '[0.1, 0.1]' LIMIT 2",
    )
    .unwrap();
    assert_eq!(post.rows.len(), 2);
    assert_eq!(post.rows[0][0], Some("1".into()));
    assert_eq!(post.rows[1][0], Some("2".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_knn_after_delete_excludes_deleted_row() {
    // A row deleted pre-crash must not appear in post-recovery KNN
    // results. If the HNSW rebuild uses stale row ids, the deleted
    // row's vector could still be returned.
    let path = tmp_recovery_path("knn_del");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    crate::executor::execute("CREATE TABLE pts (id int, pos vector)").unwrap();
    crate::executor::execute("INSERT INTO pts VALUES (1, '[0.0, 0.0]')").unwrap();
    crate::executor::execute("INSERT INTO pts VALUES (2, '[0.05, 0.05]')").unwrap();
    crate::executor::execute("INSERT INTO pts VALUES (3, '[1.0, 1.0]')").unwrap();
    crate::executor::execute("DELETE FROM pts WHERE id = 2").unwrap();

    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    let r = crate::executor::execute(
        "SELECT id FROM pts ORDER BY pos <-> '[0.04, 0.04]' LIMIT 3",
    )
    .unwrap();
    // id=2 was deleted, must not appear.
    for row in &r.rows {
        assert_ne!(row[0], Some("2".into()), "deleted row must not appear in KNN");
    }
    // Remaining rows are id=1 and id=3.
    assert_eq!(r.rows.len(), 2);

    manager::disable();
    std::fs::remove_file(&path).ok();
}
