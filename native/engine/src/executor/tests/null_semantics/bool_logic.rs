use super::super::*;

// ── Three-valued AND/OR ───────────────────────────────────────────────
// Truth table for WHERE:
//   TRUE  AND NULL = NULL  -> filtered
//   FALSE AND NULL = FALSE -> filtered
//   TRUE  OR  NULL = TRUE  -> kept
//   FALSE OR  NULL = NULL  -> filtered
//
// These tests verify short-circuit behavior: OR returns early on TRUE,
// AND returns early on FALSE, regardless of NULL operands.

#[test]
#[serial_test::serial]
fn true_or_null_is_true() {
    setup();
    execute("CREATE TABLE tv (x int, y int)").unwrap();
    execute("INSERT INTO tv VALUES (1, NULL)").unwrap();
    // (x = 1) is TRUE, (y = 5) is NULL. TRUE OR NULL = TRUE -> row kept.
    let r = execute("SELECT x FROM tv WHERE x = 1 OR y = 5").unwrap();
    assert_eq!(r.rows.len(), 1);
}

#[test]
#[serial_test::serial]
fn false_and_null_is_false() {
    setup();
    execute("CREATE TABLE fv (x int, y int)").unwrap();
    execute("INSERT INTO fv VALUES (1, NULL)").unwrap();
    // (x = 2) is FALSE. FALSE AND anything = FALSE -> row filtered.
    let r = execute("SELECT x FROM fv WHERE x = 2 AND y = 5").unwrap();
    assert_eq!(r.rows.len(), 0);
}

#[test]
#[serial_test::serial]
fn true_and_null_is_null_filters_row() {
    setup();
    execute("CREATE TABLE tn (x int, y int)").unwrap();
    execute("INSERT INTO tn VALUES (1, NULL)").unwrap();
    // (x = 1) is TRUE, (y = 5) is NULL. TRUE AND NULL = NULL -> filtered.
    let r = execute("SELECT x FROM tn WHERE x = 1 AND y = 5").unwrap();
    assert_eq!(r.rows.len(), 0);
}
