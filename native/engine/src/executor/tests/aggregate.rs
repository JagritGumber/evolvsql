use super::*;

#[test]
#[serial_test::serial]
fn count_star() {
    setup();
    execute("CREATE TABLE t (id int, name text)").unwrap();
    execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    let r = execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("2".into()));
}

#[test]
#[serial_test::serial]
fn count_column_skips_nulls() {
    setup();
    execute("CREATE TABLE t (id int, name text)").unwrap();
    execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    let r = execute("SELECT COUNT(name) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));
}

#[test]
#[serial_test::serial]
fn count_empty_table() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    let r = execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("0".into()));
}

#[test]
#[serial_test::serial]
fn sum_basic() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    execute("INSERT INTO t VALUES (10)").unwrap();
    execute("INSERT INTO t VALUES (20)").unwrap();
    execute("INSERT INTO t VALUES (30)").unwrap();
    let r = execute("SELECT SUM(id) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("60".into()));
}

#[test]
#[serial_test::serial]
fn sum_empty_is_null() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    let r = execute("SELECT SUM(id) FROM t").unwrap();
    assert_eq!(r.rows[0][0], None);
}

#[test]
#[serial_test::serial]
fn avg_basic() {
    setup();
    execute("CREATE TABLE t (val int)").unwrap();
    execute("INSERT INTO t VALUES (10)").unwrap();
    execute("INSERT INTO t VALUES (20)").unwrap();
    let r = execute("SELECT AVG(val) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("15".into()));
}

#[test]
#[serial_test::serial]
fn min_max() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    execute("INSERT INTO t VALUES (3)").unwrap();
    execute("INSERT INTO t VALUES (1)").unwrap();
    execute("INSERT INTO t VALUES (5)").unwrap();
    let r = execute("SELECT MIN(id), MAX(id) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("5".into()));
}

#[test]
#[serial_test::serial]
fn group_by_basic() {
    setup();
    execute("CREATE TABLE emp (dept text, salary int)").unwrap();
    execute("INSERT INTO emp VALUES ('eng', 100)").unwrap();
    execute("INSERT INTO emp VALUES ('eng', 200)").unwrap();
    execute("INSERT INTO emp VALUES ('sales', 150)").unwrap();
    let r = execute("SELECT dept, COUNT(*) FROM emp GROUP BY dept").unwrap();
    assert_eq!(r.rows.len(), 2);
}