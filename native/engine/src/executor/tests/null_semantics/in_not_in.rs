use super::super::*;

// ── NOT IN with NULL in the list ──────────────────────────────────────
// PostgreSQL: `x NOT IN (1, 2, NULL)` returns NULL when x doesn't match.
// NULL in WHERE filters the row out. So the whole query returns 0 rows.
#[test]
#[serial_test::serial]
fn not_in_null_in_list_filters_row() {
    setup();
    execute("CREATE TABLE nin (x int)").unwrap();
    execute("INSERT INTO nin VALUES (5), (10), (15)").unwrap();
    let r = execute("SELECT x FROM nin WHERE x NOT IN (1, 2, NULL)").unwrap();
    assert_eq!(r.rows.len(), 0, "NOT IN with NULL should filter all rows");
}

// IN with NULL in the list returns TRUE if a non-NULL match exists.
#[test]
#[serial_test::serial]
fn in_null_in_list_still_matches_known_values() {
    setup();
    execute("CREATE TABLE iin (x int)").unwrap();
    execute("INSERT INTO iin VALUES (5), (10)").unwrap();
    let r = execute("SELECT x FROM iin WHERE x IN (5, NULL)").unwrap();
    // 5 matches -> TRUE. 10 doesn't match any non-NULL -> NULL -> filtered.
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("5".into()));
}
