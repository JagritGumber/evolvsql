use super::*;

#[test]
#[serial_test::serial]
fn cast_text_to_int() {
    setup();
    let r = execute("SELECT CAST('42' AS INT)").unwrap();
    assert_eq!(r.rows[0][0], Some("42".into()));
}

#[test]
#[serial_test::serial]
fn cast_int_to_text() {
    setup();
    let r = execute("SELECT CAST(42 AS TEXT)").unwrap();
    assert_eq!(r.rows[0][0], Some("42".into()));
}

#[test]
#[serial_test::serial]
fn cast_float_to_int_rounds() {
    setup();
    // PostgreSQL: CAST(3.7 AS INT) = 4 (rounds)
    let r = execute("SELECT CAST(3.7 AS INT)").unwrap();
    assert_eq!(r.rows[0][0], Some("4".into()));
}

#[test]
#[serial_test::serial]
fn cast_shorthand_syntax() {
    setup();
    let r = execute("SELECT '123'::INT").unwrap();
    assert_eq!(r.rows[0][0], Some("123".into()));
}

#[test]
#[serial_test::serial]
fn cast_int_to_float() {
    setup();
    let r = execute("SELECT 42::FLOAT8").unwrap();
    // Should be a float representation
    let val = r.rows[0][0].as_ref().unwrap();
    assert!(val == "42" || val == "42.0");
}

#[test]
#[serial_test::serial]
fn cast_bool_to_text() {
    setup();
    let r = execute("SELECT true::TEXT").unwrap();
    assert_eq!(r.rows[0][0], Some("true".into()));
}

#[test]
#[serial_test::serial]
fn cast_in_where() {
    setup();
    execute("CREATE TABLE cast_t (val TEXT)").unwrap();
    execute("INSERT INTO cast_t VALUES ('10'), ('20'), ('3')").unwrap();
    let r = execute("SELECT val FROM cast_t WHERE val::INT > 5 ORDER BY val::INT").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("10".into()));
    assert_eq!(r.rows[1][0], Some("20".into()));
}
