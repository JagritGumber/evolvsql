use super::*;

#[test]
#[serial_test::serial]
fn in_subquery() {
    setup();
    execute("CREATE TABLE users (id int, name text)").unwrap();
    execute("CREATE TABLE orders (user_id int)").unwrap();
    execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')").unwrap();
    execute("INSERT INTO orders VALUES (1), (1), (3)").unwrap();
    let r =
        execute("SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)").unwrap();
    assert_eq!(r.rows.len(), 2); // alice, carol (not bob)
}

#[test]
#[serial_test::serial]
fn not_in_subquery() {
    setup();
    execute("CREATE TABLE users (id int, name text)").unwrap();
    execute("CREATE TABLE orders (user_id int)").unwrap();
    execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')").unwrap();
    execute("INSERT INTO orders VALUES (1), (3)").unwrap();
    let r = execute("SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM orders)")
        .unwrap();
    assert_eq!(r.rows.len(), 1); // bob only
}

#[test]
#[serial_test::serial]
fn exists_subquery() {
    setup();
    execute("CREATE TABLE users (id int, name text)").unwrap();
    execute("CREATE TABLE orders (user_id int)").unwrap();
    execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')").unwrap();
    execute("INSERT INTO orders VALUES (1)").unwrap();
    let r = execute(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
    )
    .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("alice".into()));
}

#[test]
#[serial_test::serial]
fn scalar_subquery() {
    setup();
    execute("CREATE TABLE t (id int, val int)").unwrap();
    execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    let r = execute("SELECT (SELECT SUM(val) FROM t)").unwrap();
    assert_eq!(r.rows[0][0], Some("60".into()));
}

#[test]
#[serial_test::serial]
fn in_literal_list() {
    setup();
    execute("CREATE TABLE t (id int, name text)").unwrap();
    execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
    let r = execute("SELECT * FROM t WHERE id IN (1, 3)").unwrap();
    assert_eq!(r.rows.len(), 2);
}

#[test]
#[serial_test::serial]
fn not_in_literal_list() {
    setup();
    execute("CREATE TABLE t (id int, name text)").unwrap();
    execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
    let r = execute("SELECT * FROM t WHERE id NOT IN (1, 3)").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Some("b".into()));
}

#[test]
#[serial_test::serial]
fn derived_table() {
    setup();
    execute("CREATE TABLE t (id int, name text)").unwrap();
    execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
    let r = execute(
        "SELECT sub.name FROM (SELECT id, name FROM t WHERE id > 1) AS sub ORDER BY sub.name",
    )
    .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("b".into()));
    assert_eq!(r.rows[1][0], Some("c".into()));
}

#[test]
#[serial_test::serial]
fn scalar_subquery_too_many_rows() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    execute("INSERT INTO t VALUES (1), (2)").unwrap();
    let err = execute("SELECT (SELECT id FROM t)");
    assert!(err.is_err());
    assert!(err.unwrap_err().contains("more than one row"));
}
