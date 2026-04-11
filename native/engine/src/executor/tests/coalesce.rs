use super::*;

#[test]
#[serial_test::serial]
fn coalesce_basic() {
    setup();
    execute("CREATE TABLE coal (a INT, b INT, c INT)").unwrap();
    execute("INSERT INTO coal VALUES (NULL, NULL, 3)").unwrap();
    execute("INSERT INTO coal VALUES (NULL, 2, 3)").unwrap();
    execute("INSERT INTO coal VALUES (1, 2, 3)").unwrap();
    let r = execute("SELECT COALESCE(a, b, c) FROM coal ORDER BY a, b, c").unwrap();
    // Row 1: COALESCE(1,2,3) = 1
    // Row 2: COALESCE(NULL,2,3) = 2
    // Row 3: COALESCE(NULL,NULL,3) = 3
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[1][0], Some("2".into()));
    assert_eq!(r.rows[2][0], Some("3".into()));
}

#[test]
#[serial_test::serial]
fn coalesce_all_null() {
    setup();
    let r = execute("SELECT COALESCE(NULL, NULL, NULL)").unwrap();
    assert_eq!(r.rows[0][0], None);
}

#[test]
#[serial_test::serial]
fn nullif_equal() {
    setup();
    // NULLIF(a, b) returns NULL if a = b, else a
    let r = execute("SELECT NULLIF(5, 5)").unwrap();
    assert_eq!(r.rows[0][0], None);
}

#[test]
#[serial_test::serial]
fn nullif_not_equal() {
    setup();
    let r = execute("SELECT NULLIF(5, 3)").unwrap();
    assert_eq!(r.rows[0][0], Some("5".into()));
}
