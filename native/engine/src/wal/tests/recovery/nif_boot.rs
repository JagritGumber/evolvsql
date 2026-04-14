//! Verifies that the NIF load path (extracted as `boot_wal_from_env`)
//! actually enables the WAL and replays recovery. Without this hook
//! being wired into `rustler::init!`, the entire WAL subsystem would
//! be dead code in production — `enable_from_env` exists but nothing
//! calls it at NIF load time, so a real engine process would start up
//! in-memory even with `EVOLVSQL_WAL_ENABLED=1` set.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

fn set_env(path: &std::path::Path) {
    // SAFETY: test is #[serial] and nothing else in this process
    // manipulates these env vars concurrently.
    unsafe {
        std::env::set_var("EVOLVSQL_WAL_ENABLED", "1");
        std::env::set_var("EVOLVSQL_WAL_PATH", path);
    }
}

fn clear_env() {
    unsafe {
        std::env::remove_var("EVOLVSQL_WAL_ENABLED");
        std::env::remove_var("EVOLVSQL_WAL_PATH");
    }
}

#[test]
#[serial_test::serial]
fn boot_enables_wal_and_replays_recovery() {
    let path = tmp_recovery_path("nif_boot");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();

    // Phase 1: simulate a previous run that wrote some data.
    manager::enable(&path).unwrap();
    executor::execute("CREATE TABLE t (id int, name text)").unwrap();
    executor::execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')").unwrap();
    manager::disable();

    // Phase 2: fresh process — storage wiped, WAL disabled,
    // env vars set as the deploy would set them.
    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    assert!(!manager::is_enabled());
    set_env(&path);

    // The exact function `rustler::init!(load = on_load)` runs.
    crate::boot_wal_from_env();

    assert!(manager::is_enabled(), "boot must enable the WAL");
    let r = executor::execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(
        r.rows[0][0],
        Some("2".into()),
        "boot must also run recovery so pre-crash rows are visible"
    );

    manager::disable();
    clear_env();
    std::fs::remove_file(&path).ok();
}

#[test]
#[serial_test::serial]
fn boot_with_env_disabled_stays_in_memory() {
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    clear_env();

    crate::boot_wal_from_env();

    assert!(
        !manager::is_enabled(),
        "without EVOLVSQL_WAL_ENABLED=1 the engine must not touch disk"
    );
}

#[test]
#[serial_test::serial]
fn boot_with_corrupt_wal_disables_rather_than_panics() {
    // A broken WAL file must not brick the whole engine. boot should
    // log the error and leave the WAL disabled so the process still
    // comes up — losing durability is acceptable; refusing to start
    // is not.
    let path = tmp_recovery_path("nif_boot_corrupt");
    std::fs::write(&path, b"\xff\xff\xff\xff\xff\xff\xff\xff").unwrap();

    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    set_env(&path);

    crate::boot_wal_from_env();

    // The file is gibberish: recovery should either fail gracefully
    // or apply zero entries. In either case the process must not
    // panic and the engine must still serve queries.
    executor::execute("CREATE TABLE probe (x int)").unwrap();
    executor::execute("INSERT INTO probe VALUES (1)").unwrap();
    let r = executor::execute("SELECT COUNT(*) FROM probe").unwrap();
    assert_eq!(r.rows[0][0], Some("1".into()));

    manager::disable();
    clear_env();
    std::fs::remove_file(&path).ok();
}
