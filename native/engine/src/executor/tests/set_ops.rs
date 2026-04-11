use super::*;

#[test]
#[serial_test::serial]
fn union_all() {
    setup();
    execute("CREATE TABLE u1 (x INT)").unwrap();
    execute("CREATE TABLE u2 (x INT)").unwrap();
    execute("INSERT INTO u1 VALUES (1), (2)").unwrap();
    execute("INSERT INTO u2 VALUES (2), (3)").unwrap();
    let r = execute("SELECT x FROM u1 UNION ALL SELECT x FROM u2 ORDER BY x").unwrap();
    assert_eq!(r.rows.len(), 4); // 1, 2, 2, 3
}

#[test]
#[serial_test::serial]
fn union_dedup() {
    setup();
    execute("CREATE TABLE u3 (x INT)").unwrap();
    execute("CREATE TABLE u4 (x INT)").unwrap();
    execute("INSERT INTO u3 VALUES (1), (2)").unwrap();
    execute("INSERT INTO u4 VALUES (2), (3)").unwrap();
    let r = execute("SELECT x FROM u3 UNION SELECT x FROM u4 ORDER BY x").unwrap();
    assert_eq!(r.rows.len(), 3); // 1, 2, 3 (deduped)
}

#[test]
#[serial_test::serial]
fn intersect_basic() {
    setup();
    execute("CREATE TABLE i1 (x INT)").unwrap();
    execute("CREATE TABLE i2 (x INT)").unwrap();
    execute("INSERT INTO i1 VALUES (1), (2), (3)").unwrap();
    execute("INSERT INTO i2 VALUES (2), (3), (4)").unwrap();
    let r = execute("SELECT x FROM i1 INTERSECT SELECT x FROM i2 ORDER BY x").unwrap();
    assert_eq!(r.rows.len(), 2); // 2, 3
}

#[test]
#[serial_test::serial]
fn except_basic() {
    setup();
    execute("CREATE TABLE e1 (x INT)").unwrap();
    execute("CREATE TABLE e2 (x INT)").unwrap();
    execute("INSERT INTO e1 VALUES (1), (2), (3)").unwrap();
    execute("INSERT INTO e2 VALUES (2), (3), (4)").unwrap();
    let r = execute("SELECT x FROM e1 EXCEPT SELECT x FROM e2 ORDER BY x").unwrap();
    assert_eq!(r.rows.len(), 1); // 1
    assert_eq!(r.rows[0][0], Some("1".into()));
}
