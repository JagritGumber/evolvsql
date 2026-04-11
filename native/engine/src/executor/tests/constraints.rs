use super::*;

#[test]
#[serial_test::serial]
fn pk_prevents_duplicate() {
    setup();
    execute("CREATE TABLE t (id int PRIMARY KEY, name text)").unwrap();
    execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let err = execute("INSERT INTO t VALUES (1, 'b')");
    assert!(err.is_err());
    assert!(err.unwrap_err().contains("unique constraint"));
}

#[test]
#[serial_test::serial]
fn pk_prevents_null() {
    setup();
    execute("CREATE TABLE t (id int PRIMARY KEY, name text)").unwrap();
    let err = execute("INSERT INTO t (name) VALUES ('a')");
    assert!(err.is_err());
    assert!(err.unwrap_err().contains("not-null"));
}

#[test]
#[serial_test::serial]
fn unique_prevents_duplicate() {
    setup();
    execute("CREATE TABLE t (id int, email text UNIQUE)").unwrap();
    execute("INSERT INTO t VALUES (1, 'a@b.com')").unwrap();
    assert!(execute("INSERT INTO t VALUES (2, 'a@b.com')").is_err());
}

#[test]
#[serial_test::serial]
fn unique_allows_multiple_nulls() {
    setup();
    execute("CREATE TABLE t (id int, email text UNIQUE)").unwrap();
    execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    let r = execute("SELECT * FROM t").unwrap();
    assert_eq!(r.rows.len(), 2);
}

#[test]
#[serial_test::serial]
fn not_null_constraint() {
    setup();
    execute("CREATE TABLE t (id int NOT NULL, name text)").unwrap();
    let err = execute("INSERT INTO t (name) VALUES ('a')");
    assert!(err.is_err());
    assert!(err.unwrap_err().contains("not-null"));
}

#[test]
#[serial_test::serial]
fn composite_pk() {
    setup();
    execute("CREATE TABLE t (a int, b int, c text, PRIMARY KEY (a, b))").unwrap();
    execute("INSERT INTO t VALUES (1, 1, 'x')").unwrap();
    execute("INSERT INTO t VALUES (1, 2, 'y')").unwrap(); // ok
    assert!(execute("INSERT INTO t VALUES (1, 1, 'z')").is_err()); // duplicate
}
