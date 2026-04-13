//! Composite primary key recovery. The live path (ddl::setup_indexes)
//! creates a dedicated pk_index on the full tuple when PK spans more
//! than one column, and deliberately does NOT add per-column unique
//! indexes. The recovery path used to unconditionally add a per-column
//! unique index for every PK column, which over-constrained composite
//! PK tables post-recovery: any two rows sharing a value in one PK
//! column would collide on the next live INSERT.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn recover_composite_pk_allows_repeated_values_in_one_column() {
    let path = tmp_recovery_path("composite_pk");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (a int, b int, v text, PRIMARY KEY (a, b))").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z')").unwrap();

    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    let r = executor::execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("3".into()));

    // Inserting a new row that shares column 'a' with existing rows
    // must succeed because (a, b) is the unique constraint, not 'a'
    // alone. Before the fix, recovery added a per-column unique
    // index on 'a', so this INSERT would have failed with a duplicate
    // key error.
    executor::execute("INSERT INTO t VALUES (1, 3, 'w')").unwrap();

    let r = executor::execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("4".into()));

    // Inserting a row that duplicates the full composite key must fail.
    let err = executor::execute("INSERT INTO t VALUES (1, 1, 'dup')");
    assert!(err.is_err(), "duplicate composite PK must be rejected");
    let msg = err.unwrap_err();
    assert!(
        msg.contains("duplicate") || msg.contains("unique") || msg.contains("pkey"),
        "expected uniqueness error, got: {}",
        msg
    );

    manager::disable();
    std::fs::remove_file(&path).ok();
}
