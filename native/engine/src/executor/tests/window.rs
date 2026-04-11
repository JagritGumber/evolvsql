use super::*;

#[test]
#[serial_test::serial]
fn window_row_number_partition() {
    setup();
    execute("CREATE TABLE wemp (id int, dept text, salary int)").unwrap();
    execute("INSERT INTO wemp VALUES (1, 'eng', 100)").unwrap();
    execute("INSERT INTO wemp VALUES (2, 'eng', 200)").unwrap();
    execute("INSERT INTO wemp VALUES (3, 'sales', 150)").unwrap();
    execute("INSERT INTO wemp VALUES (4, 'sales', 250)").unwrap();
    let r = execute(
        "SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) AS rn FROM wemp",
    ).unwrap();
    assert_eq!(r.rows.len(), 4);
    assert_eq!(r.columns[2].0, "rn");
    // eng partition: id=1 salary=100 -> rn=1, id=2 salary=200 -> rn=2
    // sales partition: id=3 salary=150 -> rn=1, id=4 salary=250 -> rn=2
    // Rows are in original order, so check by id
    for row in &r.rows {
        let id: i64 = row[0].as_ref().unwrap().parse().unwrap();
        let rn = row[2].as_ref().unwrap();
        match id {
            1 => assert_eq!(rn, "1"),
            2 => assert_eq!(rn, "2"),
            3 => assert_eq!(rn, "1"),
            4 => assert_eq!(rn, "2"),
            _ => panic!("unexpected id"),
        }
    }
}

#[test]
#[serial_test::serial]
fn window_row_number_no_partition() {
    setup();
    execute("CREATE TABLE wnp (id int, val int)").unwrap();
    execute("INSERT INTO wnp VALUES (3, 30)").unwrap();
    execute("INSERT INTO wnp VALUES (1, 10)").unwrap();
    execute("INSERT INTO wnp VALUES (2, 20)").unwrap();
    let r = execute(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM wnp ORDER BY id",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    // All rows in one partition, ordered by id
    assert_eq!(r.rows[0][0], Some("1".into()));
    assert_eq!(r.rows[0][1], Some("1".into()));
    assert_eq!(r.rows[1][0], Some("2".into()));
    assert_eq!(r.rows[1][1], Some("2".into()));
    assert_eq!(r.rows[2][0], Some("3".into()));
    assert_eq!(r.rows[2][1], Some("3".into()));
}

#[test]
#[serial_test::serial]
fn window_rank_with_ties() {
    setup();
    execute("CREATE TABLE wrank (player text, score int)").unwrap();
    execute("INSERT INTO wrank VALUES ('alice', 100)").unwrap();
    execute("INSERT INTO wrank VALUES ('bob', 100)").unwrap();
    execute("INSERT INTO wrank VALUES ('carol', 90)").unwrap();
    let r = execute(
        "SELECT player, RANK() OVER (ORDER BY score DESC) AS rnk FROM wrank",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    // alice and bob tie at score 100 -> rank 1, carol at 90 -> rank 3
    for row in &r.rows {
        let player = row[0].as_ref().unwrap().as_str();
        let rnk = row[1].as_ref().unwrap();
        match player {
            "alice" | "bob" => assert_eq!(rnk, "1"),
            "carol" => assert_eq!(rnk, "3"),
            _ => panic!("unexpected player"),
        }
    }
}

#[test]
#[serial_test::serial]
fn window_dense_rank() {
    setup();
    execute("CREATE TABLE wdense (player text, score int)").unwrap();
    execute("INSERT INTO wdense VALUES ('alice', 100)").unwrap();
    execute("INSERT INTO wdense VALUES ('bob', 100)").unwrap();
    execute("INSERT INTO wdense VALUES ('carol', 90)").unwrap();
    let r = execute(
        "SELECT player, DENSE_RANK() OVER (ORDER BY score DESC) AS rnk FROM wdense",
    ).unwrap();
    assert_eq!(r.rows.len(), 3);
    // alice and bob -> dense_rank 1, carol -> dense_rank 2 (no gap)
    for row in &r.rows {
        let player = row[0].as_ref().unwrap().as_str();
        let rnk = row[1].as_ref().unwrap();
        match player {
            "alice" | "bob" => assert_eq!(rnk, "1"),
            "carol" => assert_eq!(rnk, "2"),
            _ => panic!("unexpected player"),
        }
    }
}
