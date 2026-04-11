use super::*;

#[test]
#[serial_test::serial]
fn column_alias() {
    setup();
    execute("CREATE TABLE ca (first_name TEXT)").unwrap();
    execute("INSERT INTO ca VALUES ('alice')").unwrap();
    let r = execute("SELECT first_name AS name FROM ca").unwrap();
    assert_eq!(r.columns[0].0, "name");
    assert_eq!(r.rows[0][0], Some("alice".into()));
}

#[test]
#[serial_test::serial]
fn aggregate_order_by() {
    setup();
    execute("CREATE TABLE sales (region text, amount int)").unwrap();
    execute("INSERT INTO sales VALUES ('east', 100)").unwrap();
    execute("INSERT INTO sales VALUES ('west', 200)").unwrap();
    execute("INSERT INTO sales VALUES ('east', 150)").unwrap();
    let r = execute(
        "SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) DESC",
    )
    .unwrap();
    assert_eq!(r.rows[0][0], Some("east".into())); // east=250 > west=200
}

#[test]
#[serial_test::serial]
fn aggregate_limit() {
    setup();
    execute("CREATE TABLE t (grp text, val int)").unwrap();
    execute("INSERT INTO t VALUES ('a', 1)").unwrap();
    execute("INSERT INTO t VALUES ('b', 2)").unwrap();
    execute("INSERT INTO t VALUES ('c', 3)").unwrap();
    let r = execute("SELECT grp, COUNT(*) FROM t GROUP BY grp LIMIT 2").unwrap();
    assert_eq!(r.rows.len(), 2);
}

#[test]
#[serial_test::serial]
fn multi_row_insert_atomic() {
    setup();
    execute("CREATE TABLE t (id int PRIMARY KEY)").unwrap();
    let err = execute("INSERT INTO t VALUES (1), (1)"); // duplicate in same batch
    assert!(err.is_err());
    // Table should be empty -- nothing committed
    let r = execute("SELECT * FROM t").unwrap();
    assert_eq!(r.rows.len(), 0);
}

#[test]
#[serial_test::serial]
fn update_intra_batch_uniqueness() {
    setup();
    execute("CREATE TABLE t (id int PRIMARY KEY, name text)").unwrap();
    execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    let err = execute("UPDATE t SET id = 5"); // both rows get id=5 -- should error
    assert!(err.is_err());
    // Original data should be unchanged
    let r = execute("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("1".into()));
}
