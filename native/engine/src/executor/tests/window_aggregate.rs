use super::*;

#[test]
#[serial_test::serial]
fn window_lead_basic() {
    setup();
    execute("CREATE TABLE wlead (id int, val int)").unwrap();
    execute("INSERT INTO wlead VALUES (1, 10)").unwrap();
    execute("INSERT INTO wlead VALUES (2, 20)").unwrap();
    execute("INSERT INTO wlead VALUES (3, 30)").unwrap();
    let r = execute(
        "SELECT id, val, LEAD(val) OVER (ORDER BY id) AS next_val FROM wlead ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][2], Some("20".into()));
    assert_eq!(r.rows[1][2], Some("30".into()));
    assert_eq!(r.rows[2][2], None); // last row has no next
}

#[test]
#[serial_test::serial]
fn window_first_value() {
    setup();
    execute("CREATE TABLE wfirst (id int, dept text, salary int)").unwrap();
    execute("INSERT INTO wfirst VALUES (1, 'eng', 100)").unwrap();
    execute("INSERT INTO wfirst VALUES (2, 'eng', 200)").unwrap();
    execute("INSERT INTO wfirst VALUES (3, 'sales', 300)").unwrap();
    let r = execute(
        "SELECT id, FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY id) AS fv \
         FROM wfirst ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Some("100".into())); // eng first
    assert_eq!(r.rows[1][1], Some("100".into())); // eng first
    assert_eq!(r.rows[2][1], Some("300".into())); // sales first
}

#[test]
#[serial_test::serial]
fn window_last_value() {
    setup();
    execute("CREATE TABLE wlast (id int, val int)").unwrap();
    execute("INSERT INTO wlast VALUES (1, 10)").unwrap();
    execute("INSERT INTO wlast VALUES (2, 20)").unwrap();
    execute("INSERT INTO wlast VALUES (3, 30)").unwrap();
    // Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    // No peers (unique ORDER BY values), so LAST_VALUE = current row
    let r = execute(
        "SELECT id, LAST_VALUE(val) OVER (ORDER BY id) AS lv FROM wlast ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Some("10".into()));
    assert_eq!(r.rows[1][1], Some("20".into()));
    assert_eq!(r.rows[2][1], Some("30".into()));
}

#[test]
#[serial_test::serial]
fn window_last_value_peers() {
    setup();
    execute("CREATE TABLE wlastp (id int, val int)").unwrap();
    execute("INSERT INTO wlastp VALUES (1, 10)").unwrap();
    execute("INSERT INTO wlastp VALUES (2, 20)").unwrap();
    execute("INSERT INTO wlastp VALUES (3, 20)").unwrap();
    execute("INSERT INTO wlastp VALUES (4, 30)").unwrap();
    // RANGE frame: peers with val=20 (id=2,3) share frame end
    let r = execute(
        "SELECT id, LAST_VALUE(id) OVER (ORDER BY val) AS lv FROM wlastp ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 4);
    assert_eq!(r.rows[0][1], Some("1".into()));
    assert_eq!(r.rows[1][1], Some("3".into()));
    assert_eq!(r.rows[2][1], Some("3".into()));
    assert_eq!(r.rows[3][1], Some("4".into()));
}

#[test]
#[serial_test::serial]
fn window_nth_value() {
    setup();
    execute("CREATE TABLE wnth (id int, val int)").unwrap();
    execute("INSERT INTO wnth VALUES (1, 10)").unwrap();
    execute("INSERT INTO wnth VALUES (2, 20)").unwrap();
    execute("INSERT INTO wnth VALUES (3, 30)").unwrap();
    let r = execute(
        "SELECT id, NTH_VALUE(val, 2) OVER (ORDER BY id) AS nv FROM wnth ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    // Frame: UNBOUNDED PRECEDING to CURRENT ROW
    assert_eq!(r.rows[0][1], None);
    assert_eq!(r.rows[1][1], Some("20".into()));
    assert_eq!(r.rows[2][1], Some("20".into()));
}
