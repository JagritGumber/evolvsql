use super::*;

#[test]
#[serial_test::serial]
#[ignore = "ORDER BY alias resolution not yet implemented"]
fn expression_aliases_in_order_by() {
    setup();
    execute("CREATE TABLE ea (price INT, qty INT)").unwrap();
    execute("INSERT INTO ea VALUES (10, 5), (20, 2), (5, 10)").unwrap();
    let r = execute("SELECT price * qty AS total FROM ea ORDER BY total").unwrap();
    assert_eq!(r.rows[0][0], Some("40".into()));
    assert_eq!(r.rows[1][0], Some("50".into()));
    assert_eq!(r.rows[2][0], Some("50".into()));
}

#[test]
#[serial_test::serial]
fn negative_literal() {
    setup();
    let r = execute("SELECT -5").unwrap();
    assert_eq!(r.rows[0][0], Some("-5".into()));
}

#[test]
#[serial_test::serial]
fn modulo_operator() {
    setup();
    let r = execute("SELECT 10 % 3").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));
}

#[test]
#[serial_test::serial]
fn insert_select() {
    setup();
    execute("CREATE TABLE src (x INT)").unwrap();
    execute("CREATE TABLE dst (x INT)").unwrap();
    execute("INSERT INTO src VALUES (1), (2), (3)").unwrap();
    execute("INSERT INTO dst SELECT x FROM src WHERE x > 1").unwrap();
    let r = execute("SELECT x FROM dst ORDER BY x").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("2".into()));
    assert_eq!(r.rows[1][0], Some("3".into()));
}
