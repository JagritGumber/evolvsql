use super::*;

#[test]
#[serial_test::serial]
fn window_lag_with_offset() {
    setup();
    execute("CREATE TABLE wlag2 (id int, val int)").unwrap();
    execute("INSERT INTO wlag2 VALUES (1, 10)").unwrap();
    execute("INSERT INTO wlag2 VALUES (2, 20)").unwrap();
    execute("INSERT INTO wlag2 VALUES (3, 30)").unwrap();
    execute("INSERT INTO wlag2 VALUES (4, 40)").unwrap();
    let r = execute(
        "SELECT id, LAG(val, 2) OVER (ORDER BY id) AS prev2 FROM wlag2 ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 4);
    assert_eq!(r.rows[0][1], None);
    assert_eq!(r.rows[1][1], None);
    assert_eq!(r.rows[2][1], Some("10".into()));
    assert_eq!(r.rows[3][1], Some("20".into()));
}

#[test]
#[serial_test::serial]
fn window_lag_with_default() {
    setup();
    execute("CREATE TABLE wlagd (id int, val int)").unwrap();
    execute("INSERT INTO wlagd VALUES (1, 10)").unwrap();
    execute("INSERT INTO wlagd VALUES (2, 20)").unwrap();
    let r = execute(
        "SELECT id, LAG(val, 1, 0) OVER (ORDER BY id) AS prev_val FROM wlagd ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][1], Some("0".into())); // default value
    assert_eq!(r.rows[1][1], Some("10".into()));
}

#[test]
#[serial_test::serial]
fn window_lag_with_partition() {
    setup();
    execute("CREATE TABLE wlagp (id int, dept text, salary int)").unwrap();
    execute("INSERT INTO wlagp VALUES (1, 'eng', 100)").unwrap();
    execute("INSERT INTO wlagp VALUES (2, 'eng', 200)").unwrap();
    execute("INSERT INTO wlagp VALUES (3, 'sales', 300)").unwrap();
    let r = execute(
        "SELECT id, dept, LAG(salary) OVER (PARTITION BY dept ORDER BY id) AS prev \
         FROM wlagp ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][2], None); // first in eng partition
    assert_eq!(r.rows[1][2], Some("100".into())); // prev in eng
    assert_eq!(r.rows[2][2], None); // first in sales partition
}

#[test]
#[serial_test::serial]
fn window_sum_over() {
    setup();
    execute("CREATE TABLE wsum (id int, val int)").unwrap();
    execute("INSERT INTO wsum VALUES (1, 10)").unwrap();
    execute("INSERT INTO wsum VALUES (2, 20)").unwrap();
    execute("INSERT INTO wsum VALUES (3, 30)").unwrap();
    let r = execute(
        "SELECT id, val, SUM(val) OVER (ORDER BY id) AS running_sum FROM wsum ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][2], Some("10".into()));
    assert_eq!(r.rows[1][2], Some("30".into()));
    assert_eq!(r.rows[2][2], Some("60".into()));
}

#[test]
#[serial_test::serial]
fn window_count_over() {
    setup();
    execute("CREATE TABLE wcnt (id int, val int)").unwrap();
    execute("INSERT INTO wcnt VALUES (1, 10)").unwrap();
    execute("INSERT INTO wcnt VALUES (2, 20)").unwrap();
    execute("INSERT INTO wcnt VALUES (3, 30)").unwrap();
    let r = execute(
        "SELECT id, COUNT(*) OVER (ORDER BY id) AS running_count FROM wcnt ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Some("1".into()));
    assert_eq!(r.rows[1][1], Some("2".into()));
    assert_eq!(r.rows[2][1], Some("3".into()));
}
