use super::*;

#[test]
#[serial_test::serial]
fn order_by_asc() {
    setup();
    execute("CREATE TABLE t (id int, name text)").unwrap();
    execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    let r = execute("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[1][0], Some("2".into()));
    assert_eq!(r.rows[2][0], Some("3".into()));
}

#[test]
#[serial_test::serial]
fn order_by_desc() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    execute("INSERT INTO t VALUES (1)").unwrap();
    execute("INSERT INTO t VALUES (3)").unwrap();
    execute("INSERT INTO t VALUES (2)").unwrap();
    let r = execute("SELECT * FROM t ORDER BY id DESC").unwrap();
    assert_eq!(r.rows[0][0], Some("3".into()));
    assert_eq!(r.rows[2][0], Some("1".into()));
}

#[test]
#[serial_test::serial]
fn limit_basic() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    for i in 1..=5 {
        execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
    }
    let r = execute("SELECT * FROM t LIMIT 2").unwrap();
    assert_eq!(r.rows.len(), 2);
}

#[test]
#[serial_test::serial]
fn limit_offset() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    for i in 1..=5 {
        execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
    }
    let r = execute("SELECT * FROM t ORDER BY id LIMIT 2 OFFSET 1").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("2".into()));
    assert_eq!(r.rows[1][0], Some("3".into()));
}

#[test]
#[serial_test::serial]
fn order_by_desc_limit() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    for i in 1..=5 {
        execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
    }
    let r = execute("SELECT * FROM t ORDER BY id DESC LIMIT 2").unwrap();
    assert_eq!(r.rows[0][0], Some("5".into()));
    assert_eq!(r.rows[1][0], Some("4".into()));
}

#[test]
#[serial_test::serial]
fn offset_beyond() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    execute("INSERT INTO t VALUES (1)").unwrap();
    let r = execute("SELECT * FROM t OFFSET 1000").unwrap();
    assert_eq!(r.rows.len(), 0);
}

#[test]
#[serial_test::serial]
fn limit_zero() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    execute("INSERT INTO t VALUES (1)").unwrap();
    let r = execute("SELECT * FROM t LIMIT 0").unwrap();
    assert_eq!(r.rows.len(), 0);
}
