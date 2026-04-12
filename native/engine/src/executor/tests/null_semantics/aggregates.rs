use super::super::*;

// ── Aggregates on all-NULL columns ────────────────────────────────────
// Distinguished from "empty table" cases: the table has rows, but the
// aggregated column is all NULL. SUM/AVG should return NULL, COUNT(col)
// should return 0 (NOT NULL - COUNT never returns NULL except from
// COUNT(col) on an empty table where it would be 0 anyway).
#[test]
#[serial_test::serial]
fn sum_all_null_is_null() {
    setup();
    execute("CREATE TABLE san (x int)").unwrap();
    execute("INSERT INTO san VALUES (NULL), (NULL)").unwrap();
    let r = execute("SELECT SUM(x) FROM san").unwrap();
    assert_eq!(r.rows[0][0], None, "SUM of all NULLs should be NULL");
}

#[test]
#[serial_test::serial]
fn avg_all_null_is_null() {
    setup();
    execute("CREATE TABLE aan (x int)").unwrap();
    execute("INSERT INTO aan VALUES (NULL), (NULL)").unwrap();
    let r = execute("SELECT AVG(x) FROM aan").unwrap();
    assert_eq!(r.rows[0][0], None, "AVG of all NULLs should be NULL");
}

#[test]
#[serial_test::serial]
fn count_all_null_column_is_zero() {
    setup();
    execute("CREATE TABLE can (x int)").unwrap();
    execute("INSERT INTO can VALUES (NULL), (NULL)").unwrap();
    let r = execute("SELECT COUNT(x) FROM can").unwrap();
    assert_eq!(r.rows[0][0], Some("0".into()), "COUNT(col) of all NULLs should be 0");
}
