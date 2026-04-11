use super::*;

#[test]
#[serial_test::serial]
fn subquery_in_select_list() {
    setup();
    execute("CREATE TABLE sq1 (id INT, name TEXT)").unwrap();
    execute("CREATE TABLE sq2 (user_id INT, score INT)").unwrap();
    execute("INSERT INTO sq1 VALUES (1, 'alice'), (2, 'bob')").unwrap();
    execute("INSERT INTO sq2 VALUES (1, 100), (1, 200), (2, 50)").unwrap();
    let r = execute(
        "SELECT name, (SELECT SUM(score) FROM sq2 WHERE sq2.user_id = sq1.id) FROM sq1 ORDER BY name"
    ).unwrap();
    assert_eq!(r.rows[0][0], Some("alice".into()));
    assert_eq!(r.rows[0][1], Some("300".into()));
    assert_eq!(r.rows[1][0], Some("bob".into()));
    assert_eq!(r.rows[1][1], Some("50".into()));
}
