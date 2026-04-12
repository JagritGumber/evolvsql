use super::super::*;

// ── PR #35: INTERSECT/EXCEPT didn't dedup without ALL ─────────────────
#[test]
#[serial_test::serial]
fn intersect_dedups_duplicates() {
    setup();
    execute("CREATE TABLE idl (x int)").unwrap();
    execute("CREATE TABLE idr (x int)").unwrap();
    // Both sides have duplicates of 1; INTERSECT should return ONE 1.
    execute("INSERT INTO idl VALUES (1), (1), (2)").unwrap();
    execute("INSERT INTO idr VALUES (1), (1)").unwrap();
    let r = execute("SELECT x FROM idl INTERSECT SELECT x FROM idr").unwrap();
    assert_eq!(r.rows.len(), 1, "INTERSECT should dedup: got {:?}", r.rows);
}

#[test]
#[serial_test::serial]
fn except_dedups_duplicates() {
    setup();
    execute("CREATE TABLE edl (x int)").unwrap();
    execute("CREATE TABLE edr (x int)").unwrap();
    // Left has 1,1,2,2; right has 3. EXCEPT should return 1,2 (one each).
    execute("INSERT INTO edl VALUES (1), (1), (2), (2)").unwrap();
    execute("INSERT INTO edr VALUES (3)").unwrap();
    let r = execute("SELECT x FROM edl EXCEPT SELECT x FROM edr ORDER BY x").unwrap();
    assert_eq!(r.rows.len(), 2, "EXCEPT should dedup: got {:?}", r.rows);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[1][0], Some("2".into()));
}
