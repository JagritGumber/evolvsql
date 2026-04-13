//! Recovery for SERIAL columns. Sequences live in their own global
//! map and are NOT logged to the WAL on every nextval (would balloon
//! log volume). Instead recovery rebuilds sequence state from the
//! replayed rows: recreate the sequence at CreateTable replay, then
//! advance it to the max value seen for the column.
//!
//! Without this the next live INSERT after recovery would call
//! nextval and get 1, colliding with the existing PK.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn recover_serial_advances_sequence_past_existing_max() {
    let path = tmp_recovery_path("seq_advance");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id SERIAL PRIMARY KEY, name text)").unwrap();
    executor::execute("INSERT INTO t (name) VALUES ('a'), ('b'), ('c')").unwrap();

    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    // Next insert must NOT collide with id=1,2,3 from the replayed rows.
    executor::execute("INSERT INTO t (name) VALUES ('d')").unwrap();
    let r = executor::execute("SELECT id FROM t WHERE name = 'd'").unwrap();
    let id = r.rows[0][0].as_deref().unwrap().parse::<i64>().unwrap();
    assert!(id > 3, "post-recovery sequence must produce id > 3, got {}", id);

    // No PK conflict
    let r = executor::execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("4".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_serial_across_multiple_tables() {
    // advance_all_to_max must walk every table and advance every
    // SERIAL column independently. A bug that only advances the last
    // table's sequences would be caught by adding a second table.
    let path = tmp_recovery_path("seq_multi");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE a (id SERIAL PRIMARY KEY, v text)").unwrap();
    executor::execute("CREATE TABLE b (id SERIAL PRIMARY KEY, v text)").unwrap();
    executor::execute("INSERT INTO a (v) VALUES ('a1'), ('a2'), ('a3'), ('a4'), ('a5')").unwrap();
    executor::execute("INSERT INTO b (v) VALUES ('b1'), ('b2')").unwrap();

    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    executor::execute("INSERT INTO a (v) VALUES ('a_new')").unwrap();
    executor::execute("INSERT INTO b (v) VALUES ('b_new')").unwrap();

    let r = executor::execute("SELECT id FROM a WHERE v = 'a_new'").unwrap();
    let a_id = r.rows[0][0].as_deref().unwrap().parse::<i64>().unwrap();
    assert!(a_id > 5, "table a sequence must advance past 5, got {}", a_id);

    let r = executor::execute("SELECT id FROM b WHERE v = 'b_new'").unwrap();
    let b_id = r.rows[0][0].as_deref().unwrap().parse::<i64>().unwrap();
    assert!(b_id > 2, "table b sequence must advance past 2, got {}", b_id);

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_serial_in_non_public_schema() {
    // Regression for Devin PR #53 finding: advance_all_to_max was
    // hardcoded to "public", so a SERIAL column in any other schema
    // kept its sequence stuck at 0 after recovery and the next
    // nextval collided with replayed rows.
    let path = tmp_recovery_path("seq_schema");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE app.users (id SERIAL PRIMARY KEY, name text)").unwrap();
    executor::execute("INSERT INTO app.users (name) VALUES ('a'), ('b'), ('c')").unwrap();

    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    executor::execute("INSERT INTO app.users (name) VALUES ('d')").unwrap();
    let r = executor::execute("SELECT id FROM app.users WHERE name = 'd'").unwrap();
    let id = r.rows[0][0].as_deref().unwrap().parse::<i64>().unwrap();
    assert!(id > 3, "non-public schema sequence must advance past 3, got {}", id);

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_serial_with_user_supplied_higher_id() {
    // If the user inserted a row with an explicit id higher than the
    // sequence, the sequence should still advance past it on recovery
    // so the next nextval doesn't collide.
    let path = tmp_recovery_path("seq_user");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id SERIAL PRIMARY KEY, name text)").unwrap();
    executor::execute("INSERT INTO t (id, name) VALUES (100, 'big')").unwrap();

    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    executor::execute("INSERT INTO t (name) VALUES ('next')").unwrap();
    let r = executor::execute("SELECT id FROM t WHERE name = 'next'").unwrap();
    let id = r.rows[0][0].as_deref().unwrap().parse::<i64>().unwrap();
    assert!(id > 100, "sequence must advance past user-supplied id, got {}", id);

    manager::disable();
    std::fs::remove_file(&path).ok();
}
