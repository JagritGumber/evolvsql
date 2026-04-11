use super::*;

#[test]
#[serial_test::serial]
fn inner_join() {
    setup();
    execute("CREATE TABLE users (id int, name text)").unwrap();
    execute("CREATE TABLE orders (id int, user_id int, total int)").unwrap();
    execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
    execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
    execute("INSERT INTO orders VALUES (10, 1, 100)").unwrap();
    execute("INSERT INTO orders VALUES (11, 1, 200)").unwrap();
    execute("INSERT INTO orders VALUES (12, 2, 50)").unwrap();
    let r = execute(
        "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id",
    )
    .unwrap();
    assert_eq!(r.rows.len(), 3);
}

#[test]
#[serial_test::serial]
fn left_join() {
    setup();
    execute("CREATE TABLE users (id int, name text)").unwrap();
    execute("CREATE TABLE orders (id int, user_id int, total int)").unwrap();
    execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
    execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
    execute("INSERT INTO users VALUES (3, 'carol')").unwrap();
    execute("INSERT INTO orders VALUES (10, 1, 100)").unwrap();
    let r = execute(
        "SELECT users.name, orders.total FROM users LEFT JOIN orders ON users.id = orders.user_id",
    )
    .unwrap();
    assert_eq!(r.rows.len(), 3); // alice with order, bob NULL, carol NULL
}

#[test]
#[serial_test::serial]
fn cross_join() {
    setup();
    execute("CREATE TABLE colors (name text)").unwrap();
    execute("CREATE TABLE sizes (size text)").unwrap();
    execute("INSERT INTO colors VALUES ('red')").unwrap();
    execute("INSERT INTO colors VALUES ('blue')").unwrap();
    execute("INSERT INTO sizes VALUES ('S')").unwrap();
    execute("INSERT INTO sizes VALUES ('M')").unwrap();
    execute("INSERT INTO sizes VALUES ('L')").unwrap();
    let r = execute("SELECT * FROM colors CROSS JOIN sizes").unwrap();
    assert_eq!(r.rows.len(), 6); // 2 * 3
}

#[test]
#[serial_test::serial]
fn implicit_join() {
    setup();
    execute("CREATE TABLE a (id int, val text)").unwrap();
    execute("CREATE TABLE b (a_id int, data text)").unwrap();
    execute("INSERT INTO a VALUES (1, 'x')").unwrap();
    execute("INSERT INTO a VALUES (2, 'y')").unwrap();
    execute("INSERT INTO b VALUES (1, 'linked')").unwrap();
    let r = execute("SELECT a.val, b.data FROM a, b WHERE a.id = b.a_id").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("x".into()));
    assert_eq!(r.rows[0][1], Some("linked".into()));
}

#[test]
#[serial_test::serial]
fn join_with_aliases() {
    setup();
    execute("CREATE TABLE users (id int, name text)").unwrap();
    execute("CREATE TABLE orders (id int, user_id int, total int)").unwrap();
    execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
    execute("INSERT INTO orders VALUES (10, 1, 100)").unwrap();
    let r = execute(
        "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
    )
    .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("alice".into()));
    assert_eq!(r.rows[0][1], Some("100".into()));
}
