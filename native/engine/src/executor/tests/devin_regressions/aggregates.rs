use super::super::*;

// ── PR #35: aggregate OID for SUM defaulted to text ───────────────────
// Original test used values (250, 200) where lexicographic and numeric
// sort agree. This version uses (5, 100) where they disagree:
//   lex:     "100" < "5"  → "100", "5"
//   numeric: 5 < 100      → "5", "100"
#[test]
#[serial_test::serial]
fn aggregate_order_by_numeric_not_lexicographic() {
    setup();
    execute("CREATE TABLE s (region text, amount int)").unwrap();
    execute("INSERT INTO s VALUES ('a', 3), ('a', 2)").unwrap(); // SUM = 5
    execute("INSERT INTO s VALUES ('b', 60), ('b', 40)").unwrap(); // SUM = 100
    let r = execute(
        "SELECT region, SUM(amount) FROM s GROUP BY region ORDER BY SUM(amount) ASC",
    )
    .unwrap();
    // Numeric: 5 < 100 → a first. Lexicographic would give "100" < "5" → b first.
    assert_eq!(r.rows[0][0], Some("a".into()));
    assert_eq!(r.rows[1][0], Some("b".into()));
}

// ── PR #35: string_agg panicked on empty groups (rows[0]) ─────────────
#[test]
#[serial_test::serial]
fn string_agg_empty_table() {
    setup();
    execute("CREATE TABLE sae (name text)").unwrap();
    // No rows inserted - should return NULL, not panic
    let r = execute("SELECT STRING_AGG(name, ',') FROM sae").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], None);
}
