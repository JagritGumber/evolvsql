//! End-to-end recovery after a torn write. The reader stops at the
//! damaged frame (corruption.rs covers that at the reader level);
//! this module verifies that full recovery through the executor does
//! the right thing: clean prefix restored, post-recovery LSNs stay
//! monotonic so new writes don't collide with the truncated tail.

use std::fs::OpenOptions;
use std::io::Write;

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn recover_from_torn_tail_restores_clean_prefix() {
    let path = tmp_recovery_path("torn");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id int, v text)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();

    // Force fsync to disk, then detach the writer so we can mangle the file.
    manager::disable();

    // Append garbage bytes to simulate a crash mid-frame write.
    let mut f = OpenOptions::new().append(true).open(&path).unwrap();
    f.write_all(&[0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88]).unwrap();
    drop(f);

    // Wipe in-memory state and recover from the torn file.
    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    manager::enable(&path).unwrap();
    let applied = recovery::recover().unwrap();
    assert_eq!(applied, 4, "1 CreateTable + 3 Inserts must survive the torn tail");

    let r = executor::execute("SELECT id, v FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Some("a".into()));
    assert_eq!(r.rows[2][1], Some("c".into()));

    // Post-recovery writes must succeed and keep LSNs monotonic so
    // they can't collide with whatever LSN the torn tail half-wrote.
    executor::execute("INSERT INTO t VALUES (4, 'd')").unwrap();
    let r = executor::execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("4".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}
