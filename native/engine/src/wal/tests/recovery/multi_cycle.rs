//! Multi-cycle crash recovery. Real workloads crash more than once:
//! write -> crash -> recover -> write -> crash -> recover -> ... Each
//! cycle must extend the WAL with monotonic LSNs and produce the same
//! logical state as if everything had run without crashes.

use super::super::super::*;
use super::tmp_recovery_path;
use crate::{catalog, executor, storage};

#[test]
#[serial_test::serial]
fn recover_survives_three_crash_cycles() {
    let path = tmp_recovery_path("multi_cycle");
    catalog::reset();
    storage::reset();
    crate::sequence::reset();
    manager::disable();
    manager::enable(&path).unwrap();

    executor::execute("CREATE TABLE t (id SERIAL PRIMARY KEY, v text)").unwrap();
    executor::execute("INSERT INTO t (v) VALUES ('cycle0_a'), ('cycle0_b')").unwrap();

    for cycle in 1..=3 {
        storage::reset();
        catalog::reset();
        crate::sequence::reset();
        recovery::recover().unwrap();

        // Every row from every prior cycle must survive.
        let r = executor::execute("SELECT COUNT(*) FROM t").unwrap();
        let expected = 2 + (cycle - 1) * 2;
        assert_eq!(
            r.rows[0][0],
            Some(expected.to_string()),
            "cycle {} should see {} rows",
            cycle,
            expected
        );

        // Next insert must not collide with any replayed SERIAL id.
        let v_a = format!("cycle{}_a", cycle);
        let v_b = format!("cycle{}_b", cycle);
        executor::execute(&format!("INSERT INTO t (v) VALUES ('{}'), ('{}')", v_a, v_b)).unwrap();
    }

    // Final recovery pass
    storage::reset();
    catalog::reset();
    crate::sequence::reset();
    recovery::recover().unwrap();

    let r = executor::execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("8".into()));

    // SERIAL ids must all be distinct
    let r = executor::execute("SELECT COUNT(DISTINCT id) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Some("8".into()));

    manager::disable();
    std::fs::remove_file(&path).ok();
}
