use super::*;

#[test]
#[serial_test::serial]
fn case_searched() {
    setup();
    execute("CREATE TABLE scores (name TEXT, score INT)").unwrap();
    execute("INSERT INTO scores VALUES ('a', 90), ('b', 60), ('c', 40)").unwrap();
    let r = execute(
        "SELECT name, CASE WHEN score >= 80 THEN 'A' WHEN score >= 50 THEN 'B' ELSE 'F' END FROM scores ORDER BY name"
    ).unwrap();
    assert_eq!(r.rows[0][1], Some("A".into()));
    assert_eq!(r.rows[1][1], Some("B".into()));
    assert_eq!(r.rows[2][1], Some("F".into()));
}

#[test]
#[serial_test::serial]
fn case_simple() {
    setup();
    execute("CREATE TABLE status (code INT)").unwrap();
    execute("INSERT INTO status VALUES (1), (2), (3)").unwrap();
    let r = execute(
        "SELECT CASE code WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM status ORDER BY code"
    ).unwrap();
    assert_eq!(r.rows[0][0], Some("one".into()));
    assert_eq!(r.rows[1][0], Some("two".into()));
    assert_eq!(r.rows[2][0], Some("other".into()));
}

#[test]
#[serial_test::serial]
fn case_no_else_returns_null() {
    setup();
    execute("CREATE TABLE ce (x INT)").unwrap();
    execute("INSERT INTO ce VALUES (1), (99)").unwrap();
    let r = execute("SELECT CASE WHEN x = 1 THEN 'yes' END FROM ce ORDER BY x").unwrap();
    assert_eq!(r.rows[0][0], Some("yes".into()));
    assert_eq!(r.rows[1][0], None); // no ELSE -> NULL
}

#[test]
#[serial_test::serial]
fn case_in_where() {
    setup();
    execute("CREATE TABLE cw (x INT)").unwrap();
    execute("INSERT INTO cw VALUES (1), (2), (3)").unwrap();
    let r = execute(
        "SELECT x FROM cw WHERE CASE WHEN x > 1 THEN true ELSE false END ORDER BY x"
    ).unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("2".into()));
    assert_eq!(r.rows[1][0], Some("3".into()));
}
