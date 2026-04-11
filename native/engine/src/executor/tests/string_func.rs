use super::*;

#[test]
#[serial_test::serial]
fn substring_from_for() {
    setup();
    // SUBSTRING('hello' FROM 2 FOR 3) = 'ell'
    let r = execute("SELECT SUBSTRING('hello' FROM 2 FOR 3)").unwrap();
    assert_eq!(r.rows[0][0], Some("ell".into()));
}

#[test]
#[serial_test::serial]
fn substring_from_only() {
    setup();
    // SUBSTRING('hello' FROM 3) = 'llo'
    let r = execute("SELECT SUBSTRING('hello' FROM 3)").unwrap();
    assert_eq!(r.rows[0][0], Some("llo".into()));
}

#[test]
#[serial_test::serial]
fn trim_basic() {
    setup();
    let r = execute("SELECT TRIM('  hello  ')").unwrap();
    assert_eq!(r.rows[0][0], Some("hello".into()));
}

#[test]
#[serial_test::serial]
fn trim_leading_trailing() {
    setup();
    let r = execute("SELECT TRIM(LEADING ' ' FROM '  hello  ')").unwrap();
    assert_eq!(r.rows[0][0], Some("hello  ".into()));
}

#[test]
#[serial_test::serial]
fn replace_function() {
    setup();
    let r = execute("SELECT REPLACE('hello world', 'world', 'rust')").unwrap();
    assert_eq!(r.rows[0][0], Some("hello rust".into()));
}

#[test]
#[serial_test::serial]
fn position_function() {
    setup();
    // POSITION('lo' IN 'hello') = 4 (1-based)
    let r = execute("SELECT POSITION('lo' IN 'hello')").unwrap();
    assert_eq!(r.rows[0][0], Some("4".into()));
}

#[test]
#[serial_test::serial]
fn left_right_functions() {
    setup();
    let r1 = execute("SELECT LEFT('hello', 3)").unwrap();
    assert_eq!(r1.rows[0][0], Some("hel".into()));
    let r2 = execute("SELECT RIGHT('hello', 3)").unwrap();
    assert_eq!(r2.rows[0][0], Some("llo".into()));
}
