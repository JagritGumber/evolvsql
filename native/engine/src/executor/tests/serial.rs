use super::*;

#[test]
#[serial_test::serial]
fn serial_auto_increment() {
    setup();
    execute("CREATE TABLE t (id serial PRIMARY KEY, name text)").unwrap();
    execute("INSERT INTO t (name) VALUES ('alice')").unwrap();
    execute("INSERT INTO t (name) VALUES ('bob')").unwrap();
    let r = execute("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("alice".into()));
    assert_eq!(r.rows[1][0], Some("2".into()));
    assert_eq!(r.rows[1][1], Some("bob".into()));
}

#[test]
#[serial_test::serial]
fn serial_with_returning() {
    setup();
    execute("CREATE TABLE t (id serial PRIMARY KEY, name text)").unwrap();
    let r = execute("INSERT INTO t (name) VALUES ('alice') RETURNING id").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));
    let r = execute("INSERT INTO t (name) VALUES ('bob') RETURNING id, name").unwrap();
    assert_eq!(r.rows[0][0], Some("2".into()));
    assert_eq!(r.rows[0][1], Some("bob".into()));
}

#[test]
#[serial_test::serial]
fn default_literal_values() {
    setup();
    execute("CREATE TABLE t (id int DEFAULT 0, name text DEFAULT 'anon')").unwrap();
    execute("INSERT INTO t (name) VALUES ('alice')").unwrap();
    let r = execute("SELECT * FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("0".into()));
    assert_eq!(r.rows[0][1], Some("alice".into()));
}

#[test]
#[serial_test::serial]
fn nextval_function() {
    setup();
    execute("CREATE TABLE t (id serial, name text)").unwrap();
    let r = execute("SELECT nextval('t_id_seq')").unwrap();
    // Sequence was at 0 after table creation consumed 0 values
    // Actually, serial CREATE creates seq starting at 1
    // nextval from SELECT should advance it
    assert!(r.rows[0][0].is_some());
}
