use super::*;

#[test]
#[serial_test::serial]
fn select_where_eq() {
    setup_test_table();
    let r = execute("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Some("alice".into()));
}

#[test]
#[serial_test::serial]
fn select_where_gt() {
    setup_test_table();
    let r = execute("SELECT * FROM t WHERE id > 1").unwrap();
    assert_eq!(r.rows.len(), 2);
}

#[test]
#[serial_test::serial]
fn select_where_and() {
    setup_test_table();
    let r = execute("SELECT * FROM t WHERE id > 1 AND name = 'bob'").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("2".into()));
}

#[test]
#[serial_test::serial]
fn select_where_or() {
    setup_test_table();
    let r = execute("SELECT * FROM t WHERE id = 1 OR id = 3").unwrap();
    assert_eq!(r.rows.len(), 2);
}

#[test]
#[serial_test::serial]
fn select_where_is_null() {
    setup();
    execute("CREATE TABLE t (id integer, name text)").unwrap();
    execute("INSERT INTO t (id) VALUES (1)").unwrap();
    execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
    let r = execute("SELECT * FROM t WHERE name IS NULL").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("1".into()));
}

#[test]
#[serial_test::serial]
fn select_where_is_not_null() {
    setup();
    execute("CREATE TABLE t (id integer, name text)").unwrap();
    execute("INSERT INTO t (id) VALUES (1)").unwrap();
    execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
    let r = execute("SELECT * FROM t WHERE name IS NOT NULL").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("2".into()));
}
