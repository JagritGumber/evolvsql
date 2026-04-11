use super::*;

#[test]
#[serial_test::serial]
fn create_table_as_select() {
    setup();
    execute("CREATE TABLE ctas_src (id INT, name TEXT)").unwrap();
    execute("INSERT INTO ctas_src VALUES (1, 'alice'), (2, 'bob')").unwrap();
    execute("CREATE TABLE ctas_dst AS SELECT * FROM ctas_src WHERE id = 1").unwrap();
    let r = execute("SELECT id, name FROM ctas_dst").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Some("alice".into()));
}

#[test]
#[serial_test::serial]
fn create_table_if_not_exists() {
    setup();
    execute("CREATE TABLE ine (id INT)").unwrap();
    // Should not error
    execute("CREATE TABLE IF NOT EXISTS ine (id INT)").unwrap();
    execute("INSERT INTO ine VALUES (1)").unwrap();
    let r = execute("SELECT id FROM ine").unwrap();
    assert_eq!(r.rows.len(), 1);
}

#[test]
#[serial_test::serial]
fn drop_table_if_exists() {
    setup();
    // Should not error even if table doesn't exist
    execute("DROP TABLE IF EXISTS nonexistent").unwrap();
}

#[test]
#[serial_test::serial]
fn insert_default_keyword() {
    setup();
    execute("CREATE TABLE def1 (id SERIAL, name TEXT DEFAULT 'unnamed')").unwrap();
    execute("INSERT INTO def1 (name) VALUES (DEFAULT)").unwrap();
    let r = execute("SELECT id, name FROM def1").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("unnamed".into()));
}
