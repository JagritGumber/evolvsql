use super::*;

#[test]
#[serial_test::serial]
fn cte_basic() {
    setup();
    let r = execute("WITH cte AS (SELECT 1 AS x) SELECT * FROM cte").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.columns[0].0, "x");
}

#[test]
#[serial_test::serial]
fn cte_from_table() {
    setup();
    execute("CREATE TABLE cte_t (id int, name text)").unwrap();
    execute("INSERT INTO cte_t VALUES (1, 'alice')").unwrap();
    execute("INSERT INTO cte_t VALUES (2, 'bob')").unwrap();
    let r = execute(
        "WITH active AS (SELECT * FROM cte_t WHERE id = 1) SELECT * FROM active",
    ).unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Some("alice".into()));
}

#[test]
#[serial_test::serial]
fn cte_with_where() {
    setup();
    execute("CREATE TABLE cte_w (id int, val int)").unwrap();
    execute("INSERT INTO cte_w VALUES (1, 10)").unwrap();
    execute("INSERT INTO cte_w VALUES (2, 20)").unwrap();
    execute("INSERT INTO cte_w VALUES (3, 30)").unwrap();
    let r = execute(
        "WITH data AS (SELECT * FROM cte_w) SELECT * FROM data WHERE val > 15 ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("2".into()));
    assert_eq!(r.rows[1][0], Some("3".into()));
}

#[test]
#[serial_test::serial]
fn cte_multiple() {
    setup();
    let r = execute(
        "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a, b",
    ).unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("2".into()));
}

#[test]
#[serial_test::serial]
fn cte_with_join() {
    setup();
    execute("CREATE TABLE cte_users (id int, name text)").unwrap();
    execute("CREATE TABLE cte_orders (id int, user_id int, amount int)").unwrap();
    execute("INSERT INTO cte_users VALUES (1, 'alice')").unwrap();
    execute("INSERT INTO cte_orders VALUES (1, 1, 100)").unwrap();
    let r = execute(
        "WITH u AS (SELECT * FROM cte_users) \
         SELECT u.name, o.amount FROM u JOIN cte_orders o ON u.id = o.user_id",
    ).unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("alice".into()));
    assert_eq!(r.rows[0][1], Some("100".into()));
}

#[test]
#[serial_test::serial]
fn cte_column_alias() {
    setup();
    execute("CREATE TABLE cte_ca (id int, name text)").unwrap();
    execute("INSERT INTO cte_ca VALUES (1, 'alice')").unwrap();
    let r = execute(
        "WITH cte(x, y) AS (SELECT id, name FROM cte_ca) SELECT x, y FROM cte",
    ).unwrap();
    assert_eq!(r.columns[0].0, "x");
    assert_eq!(r.columns[1].0, "y");
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("alice".into()));
}

#[test]
#[serial_test::serial]
fn cte_referenced_twice() {
    setup();
    let r = execute(
        "WITH nums AS (SELECT 1 AS n UNION ALL SELECT 2) \
         SELECT a.n, b.n FROM nums a, nums b ORDER BY a.n, b.n",
    ).unwrap();
    assert_eq!(r.rows.len(), 4); // 2x2 cross join
}
