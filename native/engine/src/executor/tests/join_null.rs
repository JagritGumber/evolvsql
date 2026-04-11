use super::*;

#[test]
#[serial_test::serial]
fn hash_join_null_keys() {
    setup();
    execute("CREATE TABLE a (id int, val text)").unwrap();
    execute("CREATE TABLE b (id int, data text)").unwrap();
    execute("INSERT INTO a VALUES (1, 'x')").unwrap();
    execute("INSERT INTO a VALUES (NULL, 'z')").unwrap();
    execute("INSERT INTO b VALUES (1, 'p')").unwrap();
    execute("INSERT INTO b VALUES (NULL, 'q')").unwrap();
    let r = execute("SELECT a.val, b.data FROM a JOIN b ON a.id = b.id").unwrap();
    assert_eq!(r.rows.len(), 1); // only id=1 matches, NOT NULL=NULL
    assert_eq!(r.rows[0][0], Some("x".into()));
    assert_eq!(r.rows[0][1], Some("p".into()));
}

#[test]
#[serial_test::serial]
fn left_join_null_keys() {
    setup();
    execute("CREATE TABLE a (id int, val text)").unwrap();
    execute("CREATE TABLE b (id int, data text)").unwrap();
    execute("INSERT INTO a VALUES (1, 'x')").unwrap();
    execute("INSERT INTO a VALUES (2, 'y')").unwrap();
    execute("INSERT INTO a VALUES (NULL, 'z')").unwrap();
    execute("INSERT INTO b VALUES (1, 'p')").unwrap();
    execute("INSERT INTO b VALUES (NULL, 'q')").unwrap();
    let r = execute(
        "SELECT a.val, b.data FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.val",
    )
    .unwrap();
    assert_eq!(r.rows.len(), 3);
    // x matches p, y gets NULL, z gets NULL (NOT matched with q!)
}
