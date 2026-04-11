use super::*;

#[test]
#[serial_test::serial]
fn alter_table_add_column() {
    setup();
    execute("CREATE TABLE alt1 (id INT, name TEXT)").unwrap();
    execute("INSERT INTO alt1 VALUES (1, 'alice')").unwrap();
    execute("ALTER TABLE alt1 ADD COLUMN age INT").unwrap();
    let r = execute("SELECT id, name, age FROM alt1").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][2], None); // new column is NULL for existing rows
}

#[test]
#[serial_test::serial]
fn alter_table_add_column_with_default() {
    setup();
    execute("CREATE TABLE alt2 (id INT)").unwrap();
    execute("INSERT INTO alt2 VALUES (1), (2)").unwrap();
    execute("ALTER TABLE alt2 ADD COLUMN status TEXT DEFAULT 'active'").unwrap();
    let r = execute("SELECT id, status FROM alt2 ORDER BY id").unwrap();
    assert_eq!(r.rows[0][1], Some("active".into()));
    assert_eq!(r.rows[1][1], Some("active".into()));
}

#[test]
#[serial_test::serial]
fn alter_table_drop_column() {
    setup();
    execute("CREATE TABLE alt3 (id INT, name TEXT, age INT)").unwrap();
    execute("INSERT INTO alt3 VALUES (1, 'alice', 30)").unwrap();
    execute("ALTER TABLE alt3 DROP COLUMN age").unwrap();
    let r = execute("SELECT * FROM alt3").unwrap();
    assert_eq!(r.columns.len(), 2);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("alice".into()));
}

#[test]
#[serial_test::serial]
fn alter_table_rename_column() {
    setup();
    execute("CREATE TABLE alt4 (id INT, name TEXT)").unwrap();
    execute("INSERT INTO alt4 VALUES (1, 'alice')").unwrap();
    execute("ALTER TABLE alt4 RENAME COLUMN name TO full_name").unwrap();
    let r = execute("SELECT full_name FROM alt4").unwrap();
    assert_eq!(r.rows[0][0], Some("alice".into()));
}

#[test]
#[serial_test::serial]
fn alter_table_rename_table() {
    setup();
    execute("CREATE TABLE alt5 (id INT)").unwrap();
    execute("INSERT INTO alt5 VALUES (1)").unwrap();
    execute("ALTER TABLE alt5 RENAME TO alt5_renamed").unwrap();
    let r = execute("SELECT id FROM alt5_renamed").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));
}
