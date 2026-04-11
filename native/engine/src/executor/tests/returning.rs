use super::*;

#[test]
#[serial_test::serial]
fn insert_returning_star() {
    setup();
    execute("CREATE TABLE t (id int, name text)").unwrap();
    let r = execute("INSERT INTO t VALUES (1, 'alice') RETURNING *").unwrap();
    assert_eq!(r.tag, "INSERT 0 1");
    assert_eq!(r.columns.len(), 2);
    assert_eq!(r.columns[0].0, "id");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("alice".into()));
}

#[test]
#[serial_test::serial]
fn insert_returning_specific_column() {
    setup();
    execute("CREATE TABLE t (id int, name text)").unwrap();
    let r = execute("INSERT INTO t VALUES (1, 'alice') RETURNING id").unwrap();
    assert_eq!(r.columns.len(), 1);
    assert_eq!(r.columns[0].0, "id");
    assert_eq!(r.rows[0][0], Some("1".into()));
}

#[test]
#[serial_test::serial]
fn insert_returning_multi_row() {
    setup();
    execute("CREATE TABLE t (id int, name text)").unwrap();
    let r = execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob') RETURNING id, name").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[1][0], Some("2".into()));
}

#[test]
#[serial_test::serial]
fn insert_returning_expression() {
    setup();
    execute("CREATE TABLE t (id int, name text)").unwrap();
    let r = execute("INSERT INTO t VALUES (1, 'alice') RETURNING id, upper(name)").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("ALICE".into()));
}

#[test]
#[serial_test::serial]
fn update_returning() {
    setup();
    execute("CREATE TABLE t (id int, name text)").unwrap();
    execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
    execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
    let r = execute("UPDATE t SET name = 'updated' WHERE id = 1 RETURNING *").unwrap();
    assert_eq!(r.tag, "UPDATE 1");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("updated".into()));
}

#[test]
#[serial_test::serial]
fn delete_returning() {
    setup();
    execute("CREATE TABLE t (id int, name text)").unwrap();
    execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
    execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
    let r = execute("DELETE FROM t WHERE id = 1 RETURNING *").unwrap();
    assert_eq!(r.tag, "DELETE 1");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("alice".into()));
    // Verify row was actually deleted
    let sel = execute("SELECT * FROM t").unwrap();
    assert_eq!(sel.rows.len(), 1);
}

#[test]
#[serial_test::serial]
fn delete_all_returning() {
    setup();
    execute("CREATE TABLE t (id int)").unwrap();
    execute("INSERT INTO t VALUES (1)").unwrap();
    execute("INSERT INTO t VALUES (2)").unwrap();
    let r = execute("DELETE FROM t RETURNING id").unwrap();
    assert_eq!(r.rows.len(), 2);
}
