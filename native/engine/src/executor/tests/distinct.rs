use super::*;

#[test]
#[serial_test::serial]
fn distinct_basic() {
    setup();
    execute("CREATE TABLE dup (color TEXT)").unwrap();
    execute("INSERT INTO dup VALUES ('red'), ('blue'), ('red'), ('green'), ('blue')").unwrap();
    let r = execute("SELECT DISTINCT color FROM dup ORDER BY color").unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Some("blue".into()));
    assert_eq!(r.rows[1][0], Some("green".into()));
    assert_eq!(r.rows[2][0], Some("red".into()));
}

#[test]
#[serial_test::serial]
fn distinct_with_null() {
    setup();
    execute("CREATE TABLE dup2 (x INT)").unwrap();
    execute("INSERT INTO dup2 VALUES (1), (NULL), (2), (NULL), (1)").unwrap();
    let r = execute("SELECT DISTINCT x FROM dup2 ORDER BY x").unwrap();
    // PostgreSQL: NULL groups as one, ORDER BY puts NULLs last
    // We should have 3 distinct values: 1, 2, NULL
    assert_eq!(r.rows.len(), 3);
}

#[test]
#[serial_test::serial]
fn distinct_multi_column() {
    setup();
    execute("CREATE TABLE dup3 (a INT, b TEXT)").unwrap();
    execute("INSERT INTO dup3 VALUES (1, 'x'), (1, 'y'), (1, 'x'), (2, 'x')").unwrap();
    let r = execute("SELECT DISTINCT a, b FROM dup3 ORDER BY a, b").unwrap();
    assert_eq!(r.rows.len(), 3); // (1,x), (1,y), (2,x)
}

#[test]
#[serial_test::serial]
fn distinct_with_limit() {
    setup();
    execute("CREATE TABLE dup5 (x INT)").unwrap();
    execute("INSERT INTO dup5 VALUES (1), (1), (2), (2), (3), (3)").unwrap();
    // DISTINCT first (3 unique), then LIMIT 2
    let r = execute("SELECT DISTINCT x FROM dup5 ORDER BY x LIMIT 2").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[1][0], Some("2".into()));
}

#[test]
#[serial_test::serial]
fn distinct_preserves_order() {
    setup();
    execute("CREATE TABLE dup4 (x INT)").unwrap();
    execute("INSERT INTO dup4 VALUES (3), (1), (2), (1), (3)").unwrap();
    let r = execute("SELECT DISTINCT x FROM dup4 ORDER BY x").unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[1][0], Some("2".into()));
    assert_eq!(r.rows[2][0], Some("3".into()));
}
