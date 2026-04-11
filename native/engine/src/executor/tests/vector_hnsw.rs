use super::*;

#[test]
#[serial_test::serial]
fn hnsw_knn_basic() {
    setup();
    execute("CREATE TABLE items (id int, embedding vector)").unwrap();
    // Insert vectors -- HNSW index is auto-created on first insert
    for i in 0..50 {
        let v: Vec<f32> = (0..8)
            .map(|d| ((i * 7 + d * 3) as f32 * 0.02) % 1.0)
            .collect();
        let vstr = format!(
            "[{}]",
            v.iter()
                .map(|f| format!("{:.4}", f))
                .collect::<Vec<_>>()
                .join(",")
        );
        execute(&format!("INSERT INTO items VALUES ({}, '{}')", i, vstr)).unwrap();
    }
    // KNN search should use HNSW
    let r = execute(
        "SELECT id FROM items ORDER BY embedding <-> '[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]' LIMIT 5",
    )
    .unwrap();
    assert_eq!(r.rows.len(), 5);
}

#[test]
#[serial_test::serial]
fn hnsw_knn_returns_closest() {
    setup();
    execute("CREATE TABLE pts (id int, pos vector)").unwrap();
    execute("INSERT INTO pts VALUES (1, '[0.0, 0.0]')").unwrap();
    execute("INSERT INTO pts VALUES (2, '[1.0, 0.0]')").unwrap();
    execute("INSERT INTO pts VALUES (3, '[0.0, 1.0]')").unwrap();
    execute("INSERT INTO pts VALUES (4, '[10.0, 10.0]')").unwrap();
    // Nearest to [0.1, 0.1] should be id=1
    let r = execute("SELECT id FROM pts ORDER BY pos <-> '[0.1, 0.1]' LIMIT 2").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("1".into()));
}

#[test]
#[serial_test::serial]
fn hnsw_insert_correct_row_ids() {
    setup();
    execute("CREATE TABLE vec_t (id INT, v VECTOR)").unwrap();
    execute("INSERT INTO vec_t VALUES (1, '[1,0,0]'), (2, '[0,1,0]'), (3, '[0,0,1]')").unwrap();
    let r = execute("SELECT id FROM vec_t ORDER BY v <-> '[1,0,0]' LIMIT 1").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("1".to_string()));
}

#[test]
#[serial_test::serial]
fn pk_null_rejected_at_storage() {
    setup();
    execute("CREATE TABLE pk_t (id INT PRIMARY KEY, name TEXT)").unwrap();
    let r = execute("INSERT INTO pk_t VALUES (NULL, 'test')");
    assert!(r.is_err());
}

#[test]
#[serial_test::serial]
fn default_function_error() {
    setup();
    let r = execute("CREATE TABLE df_t (id INT, created_at TEXT DEFAULT now())");
    assert!(r.is_err());
    assert!(r.unwrap_err().contains("not yet supported"));
}
