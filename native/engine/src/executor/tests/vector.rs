use super::*;

#[test]
#[serial_test::serial]
fn vector_create_insert_select() {
    setup();
    execute("CREATE TABLE items (id int, embedding vector)").unwrap();
    execute("INSERT INTO items VALUES (1, '[1.0, 2.0, 3.0]')").unwrap();
    execute("INSERT INTO items VALUES (2, '[4.0, 5.0, 6.0]')").unwrap();
    let r = execute("SELECT * FROM items").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][1], Some("[1,2,3]".into()));
}

#[test]
#[serial_test::serial]
fn vector_l2_distance() {
    setup();
    execute("CREATE TABLE items (id int, embedding vector)").unwrap();
    execute("INSERT INTO items VALUES (1, '[1.0, 0.0, 0.0]')").unwrap();
    execute("INSERT INTO items VALUES (2, '[0.0, 1.0, 0.0]')").unwrap();
    execute("INSERT INTO items VALUES (3, '[1.0, 1.0, 0.0]')").unwrap();
    // L2 distance from [1,0,0]: item 1=0, item 3=1, item 2=sqrt(2)
    let r = execute("SELECT id FROM items ORDER BY embedding <-> '[1.0, 0.0, 0.0]' LIMIT 2")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("1".into())); // closest
    assert_eq!(r.rows[1][0], Some("3".into())); // second closest
}

#[test]
#[serial_test::serial]
fn vector_cosine_distance() {
    setup();
    execute("CREATE TABLE items (id int, embedding vector)").unwrap();
    execute("INSERT INTO items VALUES (1, '[1.0, 0.0]')").unwrap();
    execute("INSERT INTO items VALUES (2, '[0.0, 1.0]')").unwrap();
    execute("INSERT INTO items VALUES (3, '[0.707, 0.707]')").unwrap();
    // Cosine distance from [1,0]: item 1=0, item 3~0.29, item 2=1
    let r = execute("SELECT id FROM items ORDER BY embedding <=> '[1.0, 0.0]' LIMIT 2")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("1".into()));
}

#[test]
#[serial_test::serial]
fn vector_inner_product() {
    setup();
    execute("CREATE TABLE items (id int, embedding vector)").unwrap();
    execute("INSERT INTO items VALUES (1, '[1.0, 2.0, 3.0]')").unwrap();
    execute("INSERT INTO items VALUES (2, '[3.0, 2.0, 1.0]')").unwrap();
    // Inner product with [1,0,0]: item 1=1, item 2=3
    // Negative inner product: item 1=-1, item 2=-3
    // ORDER BY <#> (ascending): item 2 first (most similar via inner product)
    let r =
        execute("SELECT id FROM items ORDER BY embedding <#> '[1.0, 0.0, 0.0]'").unwrap();
    assert_eq!(r.rows[0][0], Some("2".into())); // highest inner product
}

#[test]
#[serial_test::serial]
fn vector_dimension_mismatch() {
    setup();
    execute("CREATE TABLE items (id int, embedding vector)").unwrap();
    execute("INSERT INTO items VALUES (1, '[1.0, 2.0]')").unwrap();
    execute("INSERT INTO items VALUES (2, '[1.0, 2.0, 3.0]')").unwrap();
    // Different dimensions should error
    let err = execute("SELECT id FROM items ORDER BY embedding <-> '[1.0, 2.0]'");
    // This will error during the sort comparison
    assert!(err.is_err());
}

#[test]
#[serial_test::serial]
fn vector_knn_search() {
    setup();
    execute("CREATE TABLE points (id int, pos vector)").unwrap();
    execute("INSERT INTO points VALUES (1, '[0.0, 0.0]')").unwrap();
    execute("INSERT INTO points VALUES (2, '[1.0, 1.0]')").unwrap();
    execute("INSERT INTO points VALUES (3, '[2.0, 2.0]')").unwrap();
    execute("INSERT INTO points VALUES (4, '[10.0, 10.0]')").unwrap();
    // KNN: 3 nearest to [1.5, 1.5]
    let r = execute("SELECT id FROM points ORDER BY pos <-> '[1.5, 1.5]' LIMIT 3").unwrap();
    assert_eq!(r.rows.len(), 3);
    // [1,1] and [2,2] are equidistant (0.707) from [1.5,1.5] -- tie order is
    // implementation-defined (HNSW vs brute-force may differ).
    let ids: Vec<String> = r.rows.iter().filter_map(|row| row[0].clone()).collect();
    assert!(ids.contains(&"2".to_string())); // [1,1]
    assert!(ids.contains(&"3".to_string())); // [2,2]
    assert!(ids.contains(&"1".to_string())); // [0,0] -- closer than [10,10]
    assert!(!ids.contains(&"4".to_string())); // [10,10] is farthest
}
