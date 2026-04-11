use super::*;

#[test]
#[serial_test::serial]
fn three_way_join() {
    setup();
    execute("CREATE TABLE a (id int, name text)").unwrap();
    execute("CREATE TABLE b (id int, a_id int)").unwrap();
    execute("CREATE TABLE c (id int, b_id int, val text)").unwrap();
    execute("INSERT INTO a VALUES (1, 'root')").unwrap();
    execute("INSERT INTO b VALUES (10, 1)").unwrap();
    execute("INSERT INTO c VALUES (100, 10, 'leaf')").unwrap();
    let r = execute(
        "SELECT a.name, c.val FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id",
    )
    .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("root".into()));
    assert_eq!(r.rows[0][1], Some("leaf".into()));
}

#[test]
#[serial_test::serial]
fn right_join() {
    setup();
    execute("CREATE TABLE orders (id int, user_id int)").unwrap();
    execute("CREATE TABLE users (id int, name text)").unwrap();
    execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
    execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
    execute("INSERT INTO orders VALUES (10, 1)").unwrap();
    let r = execute(
        "SELECT orders.id, users.name FROM orders RIGHT JOIN users ON users.id = orders.user_id",
    )
    .unwrap();
    assert_eq!(r.rows.len(), 2); // alice with order, bob with NULL order
}

#[test]
#[serial_test::serial]
fn join_with_aggregate() {
    setup();
    execute("CREATE TABLE users (id int, name text)").unwrap();
    execute("CREATE TABLE orders (id int, user_id int, total int)").unwrap();
    execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
    execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
    execute("INSERT INTO orders VALUES (10, 1, 100)").unwrap();
    execute("INSERT INTO orders VALUES (11, 1, 200)").unwrap();
    execute("INSERT INTO orders VALUES (12, 2, 50)").unwrap();
    let r = execute(
        "SELECT users.name, SUM(orders.total) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name",
    )
    .unwrap();
    assert_eq!(r.rows.len(), 2);
}

#[test]
#[serial_test::serial]
fn ambiguous_column_error() {
    setup();
    execute("CREATE TABLE a (id int, name text)").unwrap();
    execute("CREATE TABLE b (id int, data text)").unwrap();
    execute("INSERT INTO a VALUES (1, 'x')").unwrap();
    execute("INSERT INTO b VALUES (1, 'y')").unwrap();
    let err = execute("SELECT id FROM a JOIN b ON a.id = b.id");
    assert!(err.is_err());
    assert!(err.unwrap_err().contains("ambiguous"));
}

#[test]
#[serial_test::serial]
fn join_select_star() {
    setup();
    execute("CREATE TABLE t1 (a int, b text)").unwrap();
    execute("CREATE TABLE t2 (c int, d text)").unwrap();
    execute("INSERT INTO t1 VALUES (1, 'x')").unwrap();
    execute("INSERT INTO t2 VALUES (2, 'y')").unwrap();
    let r = execute("SELECT * FROM t1 CROSS JOIN t2").unwrap();
    assert_eq!(r.columns.len(), 4); // a, b, c, d
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("x".into()));
    assert_eq!(r.rows[0][2], Some("2".into()));
    assert_eq!(r.rows[0][3], Some("y".into()));
}
