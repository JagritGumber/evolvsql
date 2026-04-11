use super::*;

#[test]
#[serial_test::serial]
fn like_percent_wildcard() {
    setup();
    execute("CREATE TABLE likes (name TEXT)").unwrap();
    execute("INSERT INTO likes VALUES ('alice'), ('bob'), ('alicia'), ('charlie')").unwrap();
    let r = execute("SELECT name FROM likes WHERE name LIKE 'ali%' ORDER BY name").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("alice".into()));
    assert_eq!(r.rows[1][0], Some("alicia".into()));
}

#[test]
#[serial_test::serial]
fn like_underscore_wildcard() {
    setup();
    execute("CREATE TABLE like2 (code TEXT)").unwrap();
    execute("INSERT INTO like2 VALUES ('A1'), ('A2'), ('AB'), ('A12')").unwrap();
    let r = execute("SELECT code FROM like2 WHERE code LIKE 'A_' ORDER BY code").unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Some("A1".into()));
    assert_eq!(r.rows[1][0], Some("A2".into()));
    assert_eq!(r.rows[2][0], Some("AB".into()));
}

#[test]
#[serial_test::serial]
fn not_like() {
    setup();
    execute("CREATE TABLE like3 (name TEXT)").unwrap();
    execute("INSERT INTO like3 VALUES ('foo'), ('bar'), ('foobar')").unwrap();
    let r = execute("SELECT name FROM like3 WHERE name NOT LIKE 'foo%' ORDER BY name").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("bar".into()));
}

#[test]
#[serial_test::serial]
fn ilike_case_insensitive() {
    setup();
    execute("CREATE TABLE like4 (name TEXT)").unwrap();
    execute("INSERT INTO like4 VALUES ('Alice'), ('BOB'), ('alice')").unwrap();
    let r = execute("SELECT name FROM like4 WHERE name ILIKE 'alice' ORDER BY name").unwrap();
    assert_eq!(r.rows.len(), 2);
    // PostgreSQL: case-insensitive match returns both Alice and alice
}

#[test]
#[serial_test::serial]
fn like_null_propagation() {
    setup();
    execute("CREATE TABLE like5 (name TEXT)").unwrap();
    execute("INSERT INTO like5 VALUES ('a'), (NULL)").unwrap();
    let r = execute("SELECT name FROM like5 WHERE name LIKE '%'").unwrap();
    assert_eq!(r.rows.len(), 1); // NULL LIKE '%' = NULL, excluded from WHERE
}

#[test]
#[serial_test::serial]
fn like_escaped_percent() {
    setup();
    execute("CREATE TABLE like6 (s TEXT)").unwrap();
    execute("INSERT INTO like6 VALUES ('100%'), ('100x'), ('100')").unwrap();
    let r = execute(r#"SELECT s FROM like6 WHERE s LIKE '100\%'"#).unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("100%".into()));
}
