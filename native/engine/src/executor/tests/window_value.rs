use super::*;

#[test]
#[serial_test::serial]
fn window_ntile() {
    setup();
    execute("CREATE TABLE wntile (id int)").unwrap();
    execute("INSERT INTO wntile VALUES (1)").unwrap();
    execute("INSERT INTO wntile VALUES (2)").unwrap();
    execute("INSERT INTO wntile VALUES (3)").unwrap();
    execute("INSERT INTO wntile VALUES (4)").unwrap();
    execute("INSERT INTO wntile VALUES (5)").unwrap();
    let r = execute(
        "SELECT id, NTILE(3) OVER (ORDER BY id) AS bucket FROM wntile",
    ).unwrap();
    assert_eq!(r.rows.len(), 5);
    // 5 rows into 3 buckets: [1,2] [3,4] [5] -> buckets 1,1,2,2,3
    assert_eq!(r.rows[0][1], Some("1".into()));
    assert_eq!(r.rows[1][1], Some("1".into()));
    assert_eq!(r.rows[2][1], Some("2".into()));
    assert_eq!(r.rows[3][1], Some("2".into()));
    assert_eq!(r.rows[4][1], Some("3".into()));
}

#[test]
#[serial_test::serial]
fn window_multiple_functions() {
    setup();
    execute("CREATE TABLE wmulti (id int, val int)").unwrap();
    execute("INSERT INTO wmulti VALUES (1, 10)").unwrap();
    execute("INSERT INTO wmulti VALUES (2, 20)").unwrap();
    execute("INSERT INTO wmulti VALUES (3, 20)").unwrap();
    let r = execute(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY val) AS rn, \
         RANK() OVER (ORDER BY val) AS rnk FROM wmulti",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.columns[1].0, "rn");
    assert_eq!(r.columns[2].0, "rnk");
}

#[test]
#[serial_test::serial]
fn window_with_where() {
    setup();
    execute("CREATE TABLE wwhere (id int, dept text, salary int)").unwrap();
    execute("INSERT INTO wwhere VALUES (1, 'eng', 100)").unwrap();
    execute("INSERT INTO wwhere VALUES (2, 'eng', 200)").unwrap();
    execute("INSERT INTO wwhere VALUES (3, 'sales', 150)").unwrap();
    let r = execute(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY salary) AS rn FROM wwhere WHERE dept = 'eng'",
    ).unwrap();
    assert_eq!(r.rows.len(), 2);
    // Only eng rows: salary 100 -> rn=1, salary 200 -> rn=2
    assert_eq!(r.rows[0][1], Some("1".into()));
    assert_eq!(r.rows[1][1], Some("2".into()));
}

#[test]
#[serial_test::serial]
fn window_empty_table() {
    setup();
    execute("CREATE TABLE wempty (id int)").unwrap();
    let r = execute(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM wempty",
    ).unwrap();
    assert_eq!(r.rows.len(), 0);
}

#[test]
#[serial_test::serial]
fn window_lag_basic() {
    setup();
    execute("CREATE TABLE wlag (id int, val int)").unwrap();
    execute("INSERT INTO wlag VALUES (1, 10)").unwrap();
    execute("INSERT INTO wlag VALUES (2, 20)").unwrap();
    execute("INSERT INTO wlag VALUES (3, 30)").unwrap();
    let r = execute(
        "SELECT id, val, LAG(val) OVER (ORDER BY id) AS prev_val FROM wlag ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][2], None); // first row has no previous
    assert_eq!(r.rows[1][2], Some("10".into()));
    assert_eq!(r.rows[2][2], Some("20".into()));
}
