use super::*;

#[test]
#[serial_test::serial]
fn select_arithmetic() {
    setup();
    execute("CREATE TABLE t (id int, price float8)").unwrap();
    execute("INSERT INTO t VALUES (1, 100.0)").unwrap();
    let r = execute("SELECT id, price * 1.1 FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));
    let val: f64 = r.rows[0][1].as_ref().unwrap().parse().unwrap();
    assert!((val - 110.0).abs() < 0.01);
}

#[test]
#[serial_test::serial]
fn select_upper() {
    setup();
    execute("CREATE TABLE t (name text)").unwrap();
    execute("INSERT INTO t VALUES ('hello')").unwrap();
    let r = execute("SELECT upper(name) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("HELLO".into()));
}

#[test]
#[serial_test::serial]
fn select_lower() {
    setup();
    execute("CREATE TABLE t (name text)").unwrap();
    execute("INSERT INTO t VALUES ('HELLO')").unwrap();
    let r = execute("SELECT lower(name) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("hello".into()));
}

#[test]
#[serial_test::serial]
fn select_length() {
    setup();
    execute("CREATE TABLE t (name text)").unwrap();
    execute("INSERT INTO t VALUES ('hello')").unwrap();
    let r = execute("SELECT length(name) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("5".into()));
}

#[test]
#[serial_test::serial]
fn select_concat_func() {
    setup();
    execute("CREATE TABLE t (a text, b text)").unwrap();
    execute("INSERT INTO t VALUES ('hello', 'world')").unwrap();
    let r = execute("SELECT concat(a, ' ', b) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("hello world".into()));
}

#[test]
#[serial_test::serial]
fn select_concat_op() {
    setup();
    execute("CREATE TABLE t (a text, b text)").unwrap();
    execute("INSERT INTO t VALUES ('hello', 'world')").unwrap();
    let r = execute("SELECT a || ' ' || b FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("hello world".into()));
}

#[test]
#[serial_test::serial]
fn select_unary_minus() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    execute("INSERT INTO t VALUES (5)").unwrap();
    let r = execute("SELECT -id FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("-5".into()));
}

#[test]
#[serial_test::serial]
fn select_int_division() {
    setup();
    let r = execute("SELECT 5 / 2").unwrap();
    assert_eq!(r.rows[0][0], Some("2".into()));
}

#[test]
#[serial_test::serial]
fn select_expr_with_alias() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    execute("INSERT INTO t VALUES (1)").unwrap();
    let r = execute("SELECT id + 1 AS next_id FROM t").unwrap();
    assert_eq!(r.columns[0].0, "next_id");
    assert_eq!(r.rows[0][0], Some("2".into()));
}
