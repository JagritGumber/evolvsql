use super::*;

#[test]
#[serial_test::serial]
fn upsert_unique_constraint() {
    setup();
    execute("CREATE TABLE uuc (id int, email text UNIQUE, name text)").unwrap();
    execute("INSERT INTO uuc VALUES (1, 'a@b.com', 'alice')").unwrap();
    let r = execute(
        "INSERT INTO uuc VALUES (2, 'a@b.com', 'bob') ON CONFLICT DO NOTHING",
    ).unwrap();
    assert_eq!(r.tag, "INSERT 0 0");
    let r = execute("SELECT * FROM uuc").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][2], Some("alice".into()));
}

#[test]
#[serial_test::serial]
fn upsert_returning() {
    setup();
    execute("CREATE TABLE ur (id int PRIMARY KEY, val int)").unwrap();
    execute("INSERT INTO ur VALUES (1, 10)").unwrap();
    let r = execute(
        "INSERT INTO ur VALUES (1, 20) \
         ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val \
         RETURNING id, val",
    ).unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("20".into()));
}
