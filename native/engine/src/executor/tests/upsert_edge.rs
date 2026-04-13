//! UPSERT edge cases: NULL in unique cols, RETURNING on mixed batch,
//! vector updates, partial SET preservation, and constraint fallback.

use super::*;

#[test]
#[serial_test::serial]
fn null_in_unique_column_does_not_conflict() {
    // PostgreSQL semantics: NULL != NULL, so two rows with NULL in a
    // UNIQUE column can coexist. A DO NOTHING upsert with a NULL key
    // must insert the new row, not silently skip it.
    setup();
    execute("CREATE TABLE un (id int, email text UNIQUE)").unwrap();
    execute("INSERT INTO un VALUES (1, NULL)").unwrap();
    let r = execute(
        "INSERT INTO un VALUES (2, NULL) ON CONFLICT DO NOTHING",
    ).unwrap();
    assert_eq!(r.tag, "INSERT 0 1", "NULLs should not collide");
    let r = execute("SELECT COUNT(*) FROM un").unwrap();
    assert_eq!(r.rows[0][0], Some("2".into()));
}

#[test]
#[serial_test::serial]
fn upsert_returning_reports_inserted_and_updated_rows() {
    // Batch upsert with a mix of inserts and updates must return all
    // rows, reflecting post-update values for conflicts and new values
    // for inserts. Missing either case would break drivers that rely
    // on RETURNING for id lookup.
    setup();
    execute("CREATE TABLE urb (id int PRIMARY KEY, val int)").unwrap();
    execute("INSERT INTO urb VALUES (1, 10)").unwrap();
    let r = execute(
        "INSERT INTO urb VALUES (1, 100), (2, 200) \
         ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val \
         RETURNING id, val",
    ).unwrap();
    assert_eq!(r.rows.len(), 2);
    // Sort by id for deterministic check
    let mut rows = r.rows.clone();
    rows.sort_by_key(|r| r[0].clone());
    assert_eq!(rows[0], vec![Some("1".into()), Some("100".into())]);
    assert_eq!(rows[1], vec![Some("2".into()), Some("200".into())]);
}

#[test]
#[serial_test::serial]
fn upsert_with_vector_column_round_trips() {
    // Vector is the riskiest Value variant: Arc<Vec<f32>> with custom
    // serialization. An upsert that updates a vector must preserve the
    // new embedding, not the old one.
    setup();
    execute("CREATE TABLE uv (id int PRIMARY KEY, emb vector)").unwrap();
    execute("INSERT INTO uv VALUES (1, '[0.1, 0.2, 0.3]')").unwrap();
    execute(
        "INSERT INTO uv VALUES (1, '[0.9, 0.8, 0.7]') \
         ON CONFLICT (id) DO UPDATE SET emb = EXCLUDED.emb",
    ).unwrap();
    let r = execute("SELECT emb FROM uv WHERE id = 1").unwrap();
    let s = r.rows[0][0].as_deref().unwrap();
    assert!(s.contains("0.9"), "updated vector not reflected: {}", s);
    assert!(!s.contains("0.1"), "old vector still present: {}", s);
}

#[test]
#[serial_test::serial]
fn upsert_partial_update_leaves_other_columns_intact() {
    // A DO UPDATE that only sets one column must NOT touch the others.
    // Naive implementations might rebuild the row from EXCLUDED and
    // zero out unmentioned fields.
    setup();
    execute("CREATE TABLE up (id int PRIMARY KEY, a text, b text, c int)").unwrap();
    execute("INSERT INTO up VALUES (1, 'x', 'y', 42)").unwrap();
    execute(
        "INSERT INTO up VALUES (1, 'x2', 'y2', 99) \
         ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a",
    ).unwrap();
    let r = execute("SELECT a, b, c FROM up WHERE id = 1").unwrap();
    assert_eq!(r.rows[0][0], Some("x2".into()));
    assert_eq!(r.rows[0][1], Some("y".into()), "b must not change");
    assert_eq!(r.rows[0][2], Some("42".into()), "c must not change");
}

#[test]
#[serial_test::serial]
fn upsert_do_nothing_on_unique_email_without_explicit_target() {
    // DO NOTHING without ON CONFLICT target must still catch the
    // conflict on ANY unique constraint (PK or UNIQUE column).
    setup();
    execute("CREATE TABLE ue (id int PRIMARY KEY, email text UNIQUE)").unwrap();
    execute("INSERT INTO ue VALUES (1, 'a@b.com')").unwrap();
    let r = execute(
        "INSERT INTO ue VALUES (2, 'a@b.com') ON CONFLICT DO NOTHING",
    ).unwrap();
    assert_eq!(r.tag, "INSERT 0 0");
    let r = execute("SELECT COUNT(*) FROM ue").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));
}
