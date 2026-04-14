//! Vector columns through ALTER TABLE. Tests that HNSW index lazy
//! rebuild still works after recovery when the table has been
//! reshaped. A bug in alter_add_column's interaction with vector
//! columns would surface as stale row ids or a mis-keyed HNSW
//! returning rows from the wrong table shape.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn recover_knn_after_alter_add_column() {
    let path = tmp_recovery_path("knn_alter_add");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE pts (id int, pos vector)").unwrap();
    executor::execute("INSERT INTO pts VALUES (1, '[0.0, 0.0]')").unwrap();
    executor::execute("INSERT INTO pts VALUES (2, '[1.0, 0.0]')").unwrap();
    // Add an unrelated column after the rows exist.
    executor::execute("ALTER TABLE pts ADD COLUMN label text DEFAULT 'none'").unwrap();
    executor::execute("INSERT INTO pts VALUES (3, '[0.0, 1.0]', 'hi')").unwrap();

    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    // KNN must still work and return the nearest point.
    let r = executor::execute("SELECT id FROM pts ORDER BY pos <-> '[0.1, 0.1]' LIMIT 1").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));

    // The added column must be visible on old and new rows.
    let r = executor::execute("SELECT label FROM pts WHERE id = 1").unwrap();
    assert_eq!(r.rows[0][0], Some("none".into()));
    let r = executor::execute("SELECT label FROM pts WHERE id = 3").unwrap();
    assert_eq!(r.rows[0][0], Some("hi".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_knn_after_alter_drop_non_vector_column() {
    // Dropping a non-vector column shifts positional row indices in
    // storage. The HNSW col_idx is the vector column's index in the
    // new shape. If recovery or alter_drop_column miscalculates this,
    // KNN would probe a non-vector column and panic or return junk.
    let path = tmp_recovery_path("knn_alter_drop");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE pts (id int, tag text, pos vector)").unwrap();
    executor::execute("INSERT INTO pts VALUES (1, 'a', '[0.0, 0.0]')").unwrap();
    executor::execute("INSERT INTO pts VALUES (2, 'b', '[1.0, 0.0]')").unwrap();
    executor::execute("ALTER TABLE pts DROP COLUMN tag").unwrap();

    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    let r = executor::execute("SELECT id FROM pts ORDER BY pos <-> '[0.1, 0.1]' LIMIT 1").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));

    let r = executor::execute("SELECT COUNT(*) FROM pts").unwrap();
    assert_eq!(r.rows[0][0], Some("2".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}
