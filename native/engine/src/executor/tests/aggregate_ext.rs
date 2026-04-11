use super::*;

#[test]
#[serial_test::serial]
fn count_distinct() {
    setup();
    execute("CREATE TABLE cd (color TEXT)").unwrap();
    execute("INSERT INTO cd VALUES ('red'), ('blue'), ('red'), ('green'), ('blue')").unwrap();
    let r = execute("SELECT COUNT(DISTINCT color) FROM cd").unwrap();
    assert_eq!(r.rows[0][0], Some("3".into()));
}

#[test]
#[serial_test::serial]
fn sum_distinct() {
    setup();
    execute("CREATE TABLE sd (x INT)").unwrap();
    execute("INSERT INTO sd VALUES (1), (2), (2), (3), (3), (3)").unwrap();
    let r = execute("SELECT SUM(DISTINCT x) FROM sd").unwrap();
    assert_eq!(r.rows[0][0], Some("6".into())); // 1+2+3
}

#[test]
#[serial_test::serial]
fn avg_function() {
    setup();
    execute("CREATE TABLE av (x INT)").unwrap();
    execute("INSERT INTO av VALUES (10), (20), (30)").unwrap();
    let r = execute("SELECT AVG(x) FROM av").unwrap();
    assert_eq!(r.rows[0][0], Some("20".into()));
}

#[test]
#[serial_test::serial]
fn string_agg() {
    setup();
    execute("CREATE TABLE sa (name TEXT)").unwrap();
    execute("INSERT INTO sa VALUES ('a'), ('b'), ('c')").unwrap();
    let r = execute("SELECT STRING_AGG(name, ', ' ORDER BY name) FROM sa").unwrap();
    assert_eq!(r.rows[0][0], Some("a, b, c".into()));
}

#[test]
#[serial_test::serial]
fn bool_and_or() {
    setup();
    execute("CREATE TABLE ba (x BOOL)").unwrap();
    execute("INSERT INTO ba VALUES (true), (true), (false)").unwrap();
    let r1 = execute("SELECT BOOL_AND(x) FROM ba").unwrap();
    assert_eq!(r1.rows[0][0], Some("f".into()));
    let r2 = execute("SELECT BOOL_OR(x) FROM ba").unwrap();
    assert_eq!(r2.rows[0][0], Some("t".into()));
}

#[test]
#[serial_test::serial]
fn group_by_having() {
    setup();
    execute("CREATE TABLE emp (dept text, salary int)").unwrap();
    execute("INSERT INTO emp VALUES ('eng', 100)").unwrap();
    execute("INSERT INTO emp VALUES ('eng', 200)").unwrap();
    execute("INSERT INTO emp VALUES ('sales', 150)").unwrap();
    let r = execute(
        "SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 1",
    )
    .unwrap();
    assert_eq!(r.rows.len(), 1);
}

#[test]
#[serial_test::serial]
fn mixed_agg_no_group_by_error() {
    setup();
    execute("CREATE TABLE t (id int, name text)").unwrap();
    execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let err = execute("SELECT id, COUNT(*) FROM t");
    assert!(err.is_err());
    assert!(err.unwrap_err().contains("GROUP BY"));
}
