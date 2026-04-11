use super::*;

#[test]
#[serial_test::serial]
fn upsert_do_nothing() {
    setup();
    execute("CREATE TABLE udn (id int PRIMARY KEY, name text)").unwrap();
    execute("INSERT INTO udn VALUES (1, 'alice')").unwrap();
    // Conflicting insert should be silently skipped
    let r = execute("INSERT INTO udn VALUES (1, 'bob') ON CONFLICT DO NOTHING").unwrap();
    assert_eq!(r.tag, "INSERT 0 0");
    // Original row unchanged
    let r = execute("SELECT * FROM udn").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Some("alice".into()));
}

#[test]
#[serial_test::serial]
fn upsert_do_nothing_no_conflict() {
    setup();
    execute("CREATE TABLE udnn (id int PRIMARY KEY, name text)").unwrap();
    execute("INSERT INTO udnn VALUES (1, 'alice')").unwrap();
    // No conflict - should insert normally
    let r = execute("INSERT INTO udnn VALUES (2, 'bob') ON CONFLICT DO NOTHING").unwrap();
    assert_eq!(r.tag, "INSERT 0 1");
    let r = execute("SELECT * FROM udnn ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
}

#[test]
#[serial_test::serial]
fn upsert_do_update() {
    setup();
    execute("CREATE TABLE udu (id int PRIMARY KEY, name text, val int)").unwrap();
    execute("INSERT INTO udu VALUES (1, 'alice', 10)").unwrap();
    let r = execute(
        "INSERT INTO udu VALUES (1, 'bob', 20) \
         ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, val = EXCLUDED.val",
    ).unwrap();
    assert_eq!(r.tag, "INSERT 0 1");
    let r = execute("SELECT * FROM udu").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Some("bob".into()));
    assert_eq!(r.rows[0][2], Some("20".into()));
}

#[test]
#[serial_test::serial]
fn upsert_do_update_partial() {
    setup();
    execute("CREATE TABLE udup (id int PRIMARY KEY, name text, val int)").unwrap();
    execute("INSERT INTO udup VALUES (1, 'alice', 10)").unwrap();
    // Only update val, keep existing name
    let r = execute(
        "INSERT INTO udup VALUES (1, 'bob', 20) \
         ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val",
    ).unwrap();
    assert_eq!(r.tag, "INSERT 0 1");
    let r = execute("SELECT * FROM udup").unwrap();
    assert_eq!(r.rows[0][1], Some("alice".into())); // unchanged
    assert_eq!(r.rows[0][2], Some("20".into()));    // updated
}

#[test]
#[serial_test::serial]
fn upsert_do_update_expression() {
    setup();
    execute("CREATE TABLE udue (id int PRIMARY KEY, counter int)").unwrap();
    execute("INSERT INTO udue VALUES (1, 10)").unwrap();
    // Increment counter using existing value + excluded value
    let r = execute(
        "INSERT INTO udue VALUES (1, 5) \
         ON CONFLICT (id) DO UPDATE SET counter = udue.counter + EXCLUDED.counter",
    ).unwrap();
    assert_eq!(r.tag, "INSERT 0 1");
    let r = execute("SELECT * FROM udue").unwrap();
    assert_eq!(r.rows[0][1], Some("15".into())); // 10 + 5
}

#[test]
#[serial_test::serial]
fn upsert_batch_mixed() {
    setup();
    execute("CREATE TABLE ubm (id int PRIMARY KEY, val int)").unwrap();
    execute("INSERT INTO ubm VALUES (1, 10)").unwrap();
    // Batch: id=1 conflicts (update), id=2 is new (insert)
    let r = execute(
        "INSERT INTO ubm VALUES (1, 100), (2, 200) \
         ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val",
    ).unwrap();
    assert_eq!(r.tag, "INSERT 0 2");
    let r = execute("SELECT * FROM ubm ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][1], Some("100".into())); // updated
    assert_eq!(r.rows[1][1], Some("200".into())); // inserted
}
