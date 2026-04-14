//! ALTER ADD COLUMN with varied default value types. The WAL entry
//! stores the resolved default as a Value in fill_value, so bincode
//! must serialize every Value variant correctly. A bug in any
//! variant's Serialize impl would surface as a decode error or wrong
//! value on recovery.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

fn setup(name: &str) -> std::path::PathBuf {
    let path = tmp_recovery_path(name);
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();
    path
}

fn wipe_and_recover() {
    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();
}

#[test]
#[serial_test::serial]
fn recover_alter_add_int_default() {
    let path = setup("alter_int");
    executor::execute("CREATE TABLE t (id int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1)").unwrap();
    executor::execute("ALTER TABLE t ADD COLUMN score int DEFAULT 42").unwrap();
    wipe_and_recover();
    let r = executor::execute("SELECT score FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows[0][0], Some("42".into()));
    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_alter_add_bool_default() {
    let path = setup("alter_bool");
    executor::execute("CREATE TABLE t (id int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1)").unwrap();
    executor::execute("ALTER TABLE t ADD COLUMN active bool DEFAULT true").unwrap();
    wipe_and_recover();
    let r = executor::execute("SELECT active FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows[0][0], Some("t".into()));
    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_alter_add_null_default() {
    // ALTER ADD COLUMN without a DEFAULT must set existing rows to
    // NULL, and NULL must survive bincode round-trip.
    let path = setup("alter_null");
    executor::execute("CREATE TABLE t (id int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1), (2)").unwrap();
    executor::execute("ALTER TABLE t ADD COLUMN note text").unwrap();
    wipe_and_recover();
    let r = executor::execute("SELECT note FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows[0][0], None);
    let r = executor::execute("SELECT COUNT(*) FROM t WHERE note IS NULL").unwrap();
    assert_eq!(r.rows[0][0], Some("2".into()));
    manager::disable();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn recover_alter_add_float_default() {
    let path = setup("alter_float");
    executor::execute("CREATE TABLE t (id int)").unwrap();
    executor::execute("INSERT INTO t VALUES (1)").unwrap();
    executor::execute("ALTER TABLE t ADD COLUMN ratio float DEFAULT 3.14").unwrap();
    wipe_and_recover();
    let r = executor::execute("SELECT ratio FROM t WHERE id = 1").unwrap();
    let got: f64 = r.rows[0][0].as_deref().unwrap().parse().unwrap();
    assert!((got - 3.14).abs() < 1e-9, "got {}", got);
    manager::disable();
    std::fs::remove_file(&path).ok();
}
