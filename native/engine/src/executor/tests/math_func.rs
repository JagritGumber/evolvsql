use super::*;

#[test]
#[serial_test::serial]
fn ceil_floor_round() {
    setup();
    let r1 = execute("SELECT CEIL(4.2)").unwrap();
    assert_eq!(r1.rows[0][0], Some("5".into()));
    let r2 = execute("SELECT FLOOR(4.8)").unwrap();
    assert_eq!(r2.rows[0][0], Some("4".into()));
    let r3 = execute("SELECT ROUND(4.567, 2)").unwrap();
    assert_eq!(r3.rows[0][0], Some("4.57".into()));
}

#[test]
#[serial_test::serial]
fn mod_function() {
    setup();
    let r = execute("SELECT MOD(10, 3)").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));
}

#[test]
#[serial_test::serial]
fn power_sqrt() {
    setup();
    let r1 = execute("SELECT POWER(2, 10)").unwrap();
    assert_eq!(r1.rows[0][0], Some("1024".into()));
    let r2 = execute("SELECT SQRT(144)").unwrap();
    assert_eq!(r2.rows[0][0], Some("12".into()));
}
