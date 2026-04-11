use super::*;

#[test]
#[serial_test::serial]
fn create_and_insert_and_select() {
    setup();
    execute("CREATE TABLE users (id integer, name text)").unwrap();
    execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
    execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
    let result = execute("SELECT * FROM users").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Some("1".into()));
    assert_eq!(result.rows[0][1], Some("alice".into()));
}

#[test]
#[serial_test::serial]
fn select_specific_columns() {
    setup();
    execute("CREATE TABLE t (a int, b text, c int)").unwrap();
    execute("INSERT INTO t VALUES (1, 'x', 10)").unwrap();
    let result = execute("SELECT b, c FROM t").unwrap();
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.columns[0].0, "b");
    assert_eq!(result.rows[0][0], Some("x".into()));
    assert_eq!(result.rows[0][1], Some("10".into()));
}

#[test]
#[serial_test::serial]
fn select_no_from() {
    setup();
    let result = execute("SELECT 42").unwrap();
    assert_eq!(result.rows[0][0], Some("42".into()));
}

#[test]
#[serial_test::serial]
fn drop_table() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    execute("DROP TABLE t").unwrap();
    assert!(execute("SELECT * FROM t").is_err());
}

#[test]
#[serial_test::serial]
fn insert_into_nonexistent() {
    setup();
    assert!(execute("INSERT INTO ghost VALUES (1)").is_err());
}

#[test]
#[serial_test::serial]
fn truncate() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    execute("INSERT INTO t VALUES (1)").unwrap();
    execute("INSERT INTO t VALUES (2)").unwrap();
    execute("TRUNCATE t").unwrap();
    let result = execute("SELECT * FROM t").unwrap();
    assert_eq!(result.rows.len(), 0);
}
