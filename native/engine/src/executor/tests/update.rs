use super::*;

#[test]
#[serial_test::serial]
fn update_with_where() {
    setup_test_table();
    let r = execute("UPDATE t SET name = 'updated' WHERE id = 1").unwrap();
    assert_eq!(r.tag, "UPDATE 1");
    let sel = execute("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(sel.rows[0][1], Some("updated".into()));
}

#[test]
#[serial_test::serial]
fn update_all_rows() {
    setup_test_table();
    let r = execute("UPDATE t SET name = 'all'").unwrap();
    assert_eq!(r.tag, "UPDATE 3");
}

#[test]
#[serial_test::serial]
fn update_self_referential() {
    setup_test_table();
    execute("UPDATE t SET id = id + 1 WHERE id = 1").unwrap();
    let sel = execute("SELECT * FROM t WHERE name = 'alice'").unwrap();
    assert_eq!(sel.rows[0][0], Some("2".into()));
}

#[test]
#[serial_test::serial]
fn update_division_by_zero_errors() {
    setup();
    execute("CREATE TABLE t (id int, val int)").unwrap();
    execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let err = execute("UPDATE t SET val = val / 0 WHERE id = 1");
    assert!(err.is_err());
    assert!(err.unwrap_err().contains("division by zero"));
    // Row should be unchanged
    let r = execute("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows[0][0], Some("10".into()));
}

#[test]
#[serial_test::serial]
fn update_enforces_unique_constraint() {
    setup();
    execute("CREATE TABLE t (id int PRIMARY KEY, name text)").unwrap();
    execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    let err = execute("UPDATE t SET id = 1 WHERE id = 2");
    assert!(err.is_err());
    assert!(err.unwrap_err().contains("unique constraint"));
}

#[test]
#[serial_test::serial]
fn update_enforces_not_null() {
    setup();
    execute("CREATE TABLE t (id int PRIMARY KEY, name text NOT NULL)").unwrap();
    execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let err = execute("UPDATE t SET name = NULL WHERE id = 1");
    assert!(err.is_err());
    assert!(err.unwrap_err().contains("not-null"));
}
