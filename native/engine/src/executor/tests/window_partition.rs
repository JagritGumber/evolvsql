use super::*;

#[test]
#[serial_test::serial]
fn window_sum_partition() {
    setup();
    execute("CREATE TABLE wsump (id int, dept text, salary int)").unwrap();
    execute("INSERT INTO wsump VALUES (1, 'eng', 100)").unwrap();
    execute("INSERT INTO wsump VALUES (2, 'eng', 200)").unwrap();
    execute("INSERT INTO wsump VALUES (3, 'sales', 300)").unwrap();
    let r = execute(
        "SELECT id, dept, SUM(salary) OVER (PARTITION BY dept ORDER BY id) AS rsum \
         FROM wsump ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][2], Some("100".into())); // eng: 100
    assert_eq!(r.rows[1][2], Some("300".into())); // eng: 100+200
    assert_eq!(r.rows[2][2], Some("300".into())); // sales: 300
}

#[test]
#[serial_test::serial]
fn window_sum_no_order() {
    setup();
    execute("CREATE TABLE wsumno (id int, val int)").unwrap();
    execute("INSERT INTO wsumno VALUES (1, 10)").unwrap();
    execute("INSERT INTO wsumno VALUES (2, 20)").unwrap();
    execute("INSERT INTO wsumno VALUES (3, 30)").unwrap();
    // No ORDER BY means all rows are peers -> total sum for all rows
    let r = execute(
        "SELECT id, SUM(val) OVER () AS total FROM wsumno ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Some("60".into()));
    assert_eq!(r.rows[1][1], Some("60".into()));
    assert_eq!(r.rows[2][1], Some("60".into()));
}

#[test]
#[serial_test::serial]
fn window_count_column() {
    setup();
    execute("CREATE TABLE wcntc (id int, val int)").unwrap();
    execute("INSERT INTO wcntc VALUES (1, 10)").unwrap();
    execute("INSERT INTO wcntc VALUES (2, NULL)").unwrap();
    execute("INSERT INTO wcntc VALUES (3, 30)").unwrap();
    // COUNT(val) skips NULLs
    let r = execute(
        "SELECT id, COUNT(val) OVER (ORDER BY id) AS cnt FROM wcntc ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Some("1".into()));
    assert_eq!(r.rows[1][1], Some("1".into())); // NULL skipped
    assert_eq!(r.rows[2][1], Some("2".into()));
}

#[test]
#[serial_test::serial]
fn window_min_max_over() {
    setup();
    execute("CREATE TABLE wmm (id int, val int)").unwrap();
    execute("INSERT INTO wmm VALUES (1, 30)").unwrap();
    execute("INSERT INTO wmm VALUES (2, 10)").unwrap();
    execute("INSERT INTO wmm VALUES (3, 20)").unwrap();
    let r = execute(
        "SELECT id, MIN(val) OVER (ORDER BY id) AS rmin, \
         MAX(val) OVER (ORDER BY id) AS rmax FROM wmm ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    // Running min: 30, min(30,10)=10, min(30,10,20)=10
    assert_eq!(r.rows[0][1], Some("30".into()));
    assert_eq!(r.rows[1][1], Some("10".into()));
    assert_eq!(r.rows[2][1], Some("10".into()));
    // Running max: 30, max(30,10)=30, max(30,10,20)=30
    assert_eq!(r.rows[0][2], Some("30".into()));
    assert_eq!(r.rows[1][2], Some("30".into()));
    assert_eq!(r.rows[2][2], Some("30".into()));
}

#[test]
#[serial_test::serial]
fn window_avg_over() {
    setup();
    execute("CREATE TABLE wavg (id int, val int)").unwrap();
    execute("INSERT INTO wavg VALUES (1, 10)").unwrap();
    execute("INSERT INTO wavg VALUES (2, 20)").unwrap();
    execute("INSERT INTO wavg VALUES (3, 30)").unwrap();
    let r = execute(
        "SELECT id, AVG(val) OVER (ORDER BY id) AS running_avg FROM wavg ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Some("10".into()));
    assert_eq!(r.rows[1][1], Some("15".into()));
    assert_eq!(r.rows[2][1], Some("20".into()));
}
