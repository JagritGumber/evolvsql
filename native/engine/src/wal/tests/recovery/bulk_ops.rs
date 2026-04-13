//! Recovery for bulk operations that bypassed WAL: TRUNCATE (via
//! delete_all) and CREATE TABLE AS SELECT. Both routed to unchecked
//! storage paths with no append calls, so a crash after either would
//! leave the in-memory state ahead of the durable log.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn recover_replays_truncate() {
    let path = tmp_recovery_path("truncate");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    executor::execute("TRUNCATE TABLE t").unwrap();

    storage::reset();
    catalog::reset();
    recovery::recover().unwrap();

    let r = executor::execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(
        r.rows[0][0], Some("0".into()),
        "truncated rows must not reappear after recovery"
    );

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_replays_delete_without_where() {
    // DELETE FROM t with no WHERE also routes through delete_all. Same
    // durability requirement as TRUNCATE.
    let path = tmp_recovery_path("delete_all");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int, name text)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')").unwrap();
    executor::execute("DELETE FROM t").unwrap();

    storage::reset();
    catalog::reset();
    recovery::recover().unwrap();

    let r = executor::execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("0".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_replays_create_table_as() {
    // CTAS builds a new table from a SELECT. Without WAL hooks the
    // new table and its rows vanish on crash, leaving the source
    // table alone but losing the derived one.
    let path = tmp_recovery_path("ctas");
    catalog::reset();
    storage::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE src (id int, v int)").unwrap();
    executor::execute("INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    executor::execute("CREATE TABLE dst AS SELECT id, v FROM src WHERE v > 10").unwrap();

    storage::reset();
    catalog::reset();
    recovery::recover().unwrap();

    let r = executor::execute("SELECT id, v FROM dst ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2, "CTAS rows must survive recovery");
    assert_eq!(r.rows[0][0], Some("2".into()));
    assert_eq!(r.rows[1][0], Some("3".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}
