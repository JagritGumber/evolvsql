use super::*;

#[test]
#[serial_test::serial]
fn between_basic() {
    setup();
    execute("CREATE TABLE bet1 (x INT)").unwrap();
    execute("INSERT INTO bet1 VALUES (1), (5), (10), (15), (20)").unwrap();
    let r = execute("SELECT x FROM bet1 WHERE x BETWEEN 5 AND 15 ORDER BY x").unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Some("5".into()));
    assert_eq!(r.rows[1][0], Some("10".into()));
    assert_eq!(r.rows[2][0], Some("15".into()));
}

#[test]
#[serial_test::serial]
fn not_between() {
    setup();
    execute("CREATE TABLE bet2 (x INT)").unwrap();
    execute("INSERT INTO bet2 VALUES (1), (5), (10)").unwrap();
    let r = execute("SELECT x FROM bet2 WHERE x NOT BETWEEN 3 AND 7 ORDER BY x").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[1][0], Some("10".into()));
}
