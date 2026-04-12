use super::super::*;

// ── PR #35: fast equality filter ignored TypeCast ─────────────────────
// WHERE id = '5'::int on the single-table fast path extracted Text("5")
// and compared it against Int columns. The cross-type comparison never
// matched, silently returning 0 rows. Fixed by bailing to the slow path
// when either side is a TypeCast.
#[test]
#[serial_test::serial]
fn where_typecast_equality() {
    setup();
    execute("CREATE TABLE te (id int, name text)").unwrap();
    execute("INSERT INTO te VALUES (5, 'five'), (10, 'ten')").unwrap();
    let r = execute("SELECT name FROM te WHERE id = '5'::int").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("five".into()));
}

// ── PR #35: INSERT...SELECT didn't apply column defaults ──────────────
// The VALUES path used apply_default for non-target columns but the
// SELECT path initialized them all to NULL, breaking SERIAL columns.
#[test]
#[serial_test::serial]
fn insert_select_applies_serial_default() {
    setup();
    execute("CREATE TABLE src_d (name text)").unwrap();
    execute("CREATE TABLE dst_d (id serial, name text)").unwrap();
    execute("INSERT INTO src_d VALUES ('alice'), ('bob')").unwrap();
    execute("INSERT INTO dst_d (name) SELECT name FROM src_d").unwrap();
    let r = execute("SELECT id, name FROM dst_d ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    // id should be populated by SERIAL, not NULL
    assert!(r.rows[0][0].is_some(), "id should not be NULL");
    assert!(r.rows[1][0].is_some(), "id should not be NULL");
}

// ── PR #35: ORDER BY ordinal rejected expression targets ──────────────
// SELECT a + b FROM t ORDER BY 1 errored with "ORDER BY ordinal must
// reference a column". Fixed by routing expression targets through the
// same expression-eval path that arbitrary ORDER BY exprs use.
#[test]
#[serial_test::serial]
fn order_by_ordinal_expression_target() {
    setup();
    execute("CREATE TABLE oe (a int, b int)").unwrap();
    execute("INSERT INTO oe VALUES (1, 10), (2, 5), (3, 1)").unwrap();
    let r = execute("SELECT a + b FROM oe ORDER BY 1").unwrap();
    assert_eq!(r.rows[0][0], Some("4".into())); // 3+1
    assert_eq!(r.rows[1][0], Some("7".into())); // 2+5
    assert_eq!(r.rows[2][0], Some("11".into())); // 1+10
}
