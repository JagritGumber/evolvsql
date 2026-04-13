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
