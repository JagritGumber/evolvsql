use super::*;

#[test]
#[serial_test::serial]
fn cte_chained() {
    setup();
    let r = execute(
        "WITH a AS (SELECT 1 AS x), b AS (SELECT x + 1 AS y FROM a) SELECT * FROM b",
    ).unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("2".into()));
}

#[test]
#[serial_test::serial]
fn cte_shadows_table() {
    setup();
    execute("CREATE TABLE shadow_t (id int)").unwrap();
    execute("INSERT INTO shadow_t VALUES (999)").unwrap();
    // CTE named "shadow_t" should shadow the real table
    let r = execute(
        "WITH shadow_t AS (SELECT 1 AS id) SELECT * FROM shadow_t",
    ).unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("1".into()));
}

#[test]
#[serial_test::serial]
fn cte_empty() {
    setup();
    execute("CREATE TABLE cte_e (id int)").unwrap();
    let r = execute(
        "WITH empty AS (SELECT * FROM cte_e) SELECT * FROM empty",
    ).unwrap();
    assert_eq!(r.rows.len(), 0);
}

#[test]
#[serial_test::serial]
fn cte_in_subquery() {
    setup();
    execute("CREATE TABLE cte_sq (id int, val int)").unwrap();
    execute("INSERT INTO cte_sq VALUES (1, 10)").unwrap();
    execute("INSERT INTO cte_sq VALUES (2, 20)").unwrap();
    let r = execute(
        "WITH target_ids AS (SELECT 1 AS tid) \
         SELECT * FROM cte_sq WHERE id IN (SELECT tid FROM target_ids)",
    ).unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Some("1".into()));
}

#[test]
#[serial_test::serial]
fn cte_text_in_subquery() {
    setup();
    execute("CREATE TABLE cte_txt (id int, name text)").unwrap();
    execute("INSERT INTO cte_txt VALUES (1, 'alice')").unwrap();
    execute("INSERT INTO cte_txt VALUES (2, 'bob')").unwrap();
    // CTE with text data referenced in WHERE subquery - tests that
    // ArenaValue text offsets don't dangle across arena boundaries
    let r = execute(
        "WITH names AS (SELECT 'alice' AS n) \
         SELECT * FROM cte_txt WHERE name IN (SELECT n FROM names)",
    ).unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Some("alice".into()));
}

#[test]
#[serial_test::serial]
fn cte_recursive_error() {
    setup();
    let r = execute(
        "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT 1) SELECT * FROM cte",
    );
    assert!(r.is_err());
    assert!(r.unwrap_err().contains("WITH RECURSIVE"));
}
