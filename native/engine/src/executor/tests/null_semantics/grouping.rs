use super::super::*;

// ── GROUP BY on NULL ──────────────────────────────────────────────────
// PostgreSQL: NULLs are considered equal for GROUP BY, so they form a
// single group. This is the one place NULL = NULL is TRUE (alongside
// DISTINCT and set operations).
#[test]
#[serial_test::serial]
fn group_by_groups_nulls_together() {
    setup();
    execute("CREATE TABLE gbn (category text, val int)").unwrap();
    execute("INSERT INTO gbn VALUES ('a', 1)").unwrap();
    execute("INSERT INTO gbn VALUES (NULL, 2)").unwrap();
    execute("INSERT INTO gbn VALUES (NULL, 3)").unwrap();
    execute("INSERT INTO gbn VALUES ('a', 4)").unwrap();
    let r = execute("SELECT category, COUNT(*) FROM gbn GROUP BY category").unwrap();
    assert_eq!(r.rows.len(), 2, "Two groups: 'a' and NULL");
    // Find the NULL group - it should have count 2
    let null_group = r.rows.iter().find(|row| row[0].is_none())
        .expect("NULL group missing from GROUP BY result");
    assert_eq!(null_group[1], Some("2".into()));
}
