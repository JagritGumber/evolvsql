use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage, types::Value};

#[test]
#[serial_test::serial]
fn recover_preserves_vector_data() {
    let path = tmp_recovery_path("vector");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE embeds (id int, v vector)").unwrap();
    executor::execute("INSERT INTO embeds VALUES (1, '[0.1, 0.2, 0.3]')").unwrap();

    // Simulate crash
    storage::reset();
    catalog::reset();
    executor::execute("CREATE TABLE embeds (id int, v vector)").unwrap();

    let applied = recovery::recover().unwrap();
    assert_eq!(applied, 1);

    // Verify the vector was restored
    let rows = storage::scan("public", "embeds").unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][1], Value::Vector(_)));
    if let Value::Vector(v) = &rows[0][1] {
        assert_eq!(v, &vec![0.1f32, 0.2, 0.3]);
    }

    manager::disable();
    std::fs::remove_file(&path).ok();
}
