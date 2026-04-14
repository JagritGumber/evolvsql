//! Recovery for UPDATE statements that change the primary key value.
//! apply_update matches by row content, so the old_row in the WAL
//! must match the in-memory state at the time of replay. If anything
//! (rebuild_indexes, sequence advancement) observes a stale PK, the
//! next live INSERT after recovery would collide.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn recover_upsert_with_composite_pk_do_update() {
    // UPSERT on a composite PK must route through the pk_index (not
    // per-column unique indexes) to detect conflicts. If recovery
    // rebuilt the wrong index shape, a post-recovery ON CONFLICT
    // DO UPDATE could either skip a real conflict or spuriously
    // reject a legitimate row.
    let path = tmp_recovery_path("upsert_comp_pk");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (a int, b int, v text, PRIMARY KEY (a, b))").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 1, 'x'), (1, 2, 'y')").unwrap();
    // This UPSERT must update row (1,1) to 'x_updated' because (1,1) exists.
    executor::execute(
        "INSERT INTO t VALUES (1, 1, 'x_updated') ON CONFLICT (a, b) DO UPDATE SET v = EXCLUDED.v",
    )
    .unwrap();

    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    let r = executor::execute("SELECT v FROM t WHERE a = 1 AND b = 1").unwrap();
    assert_eq!(r.rows[0][0], Some("x_updated".into()));

    // Post-recovery UPSERT must still see composite conflicts: a new
    // row with (1, 2) conflicts and should update, not duplicate.
    executor::execute(
        "INSERT INTO t VALUES (1, 2, 'y_updated') ON CONFLICT (a, b) DO UPDATE SET v = EXCLUDED.v",
    )
    .unwrap();

    let r = executor::execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("2".into()));
    let r = executor::execute("SELECT v FROM t WHERE a = 1 AND b = 2").unwrap();
    assert_eq!(r.rows[0][0], Some("y_updated".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_update_that_changes_primary_key() {
    let path = tmp_recovery_path("update_pk");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int PRIMARY KEY, name text)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
    executor::execute("UPDATE t SET id = 99 WHERE id = 2").unwrap();

    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    // Row with new PK must exist, old PK must not.
    let r = executor::execute("SELECT name FROM t WHERE id = 99").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("b".into()));
    let r = executor::execute("SELECT COUNT(*) FROM t WHERE id = 2").unwrap();
    assert_eq!(r.rows[0][0], Some("0".into()));

    // Post-recovery PK index must reflect the new value: inserting a
    // row with id=99 must be rejected, id=2 must succeed.
    let err = executor::execute("INSERT INTO t VALUES (99, 'dup')");
    assert!(err.is_err(), "rebuilt PK index must reject id=99");
    executor::execute("INSERT INTO t VALUES (2, 'reused')").unwrap();

    let r = executor::execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("4".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}
