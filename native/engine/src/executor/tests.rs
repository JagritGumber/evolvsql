use super::*;
use crate::catalog;
use crate::storage;

    fn setup() {
        catalog::reset();
        storage::reset();
    }

    fn setup_test_table() {
        setup();
        execute("CREATE TABLE t (id integer, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
        execute("INSERT INTO t VALUES (3, 'carol')").unwrap();
    }

    // ── Basic CRUD ────────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn create_and_insert_and_select() {
        setup();
        execute("CREATE TABLE users (id integer, name text)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
        let result = execute("SELECT * FROM users").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Some("1".into()));
        assert_eq!(result.rows[0][1], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_specific_columns() {
        setup();
        execute("CREATE TABLE t (a int, b text, c int)").unwrap();
        execute("INSERT INTO t VALUES (1, 'x', 10)").unwrap();
        let result = execute("SELECT b, c FROM t").unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].0, "b");
        assert_eq!(result.rows[0][0], Some("x".into()));
        assert_eq!(result.rows[0][1], Some("10".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_no_from() {
        setup();
        let result = execute("SELECT 42").unwrap();
        assert_eq!(result.rows[0][0], Some("42".into()));
    }

    #[test]
    #[serial_test::serial]
    fn drop_table() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("DROP TABLE t").unwrap();
        assert!(execute("SELECT * FROM t").is_err());
    }

    #[test]
    #[serial_test::serial]
    fn insert_into_nonexistent() {
        setup();
        assert!(execute("INSERT INTO ghost VALUES (1)").is_err());
    }

    #[test]
    #[serial_test::serial]
    fn truncate() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (1)").unwrap();
        execute("INSERT INTO t VALUES (2)").unwrap();
        execute("TRUNCATE t").unwrap();
        let result = execute("SELECT * FROM t").unwrap();
        assert_eq!(result.rows.len(), 0);
    }

    // ── WHERE ─────────────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn select_where_eq() {
        setup_test_table();
        let r = execute("SELECT * FROM t WHERE id = 1").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_where_gt() {
        setup_test_table();
        let r = execute("SELECT * FROM t WHERE id > 1").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn select_where_and() {
        setup_test_table();
        let r = execute("SELECT * FROM t WHERE id > 1 AND name = 'bob'").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("2".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_where_or() {
        setup_test_table();
        let r = execute("SELECT * FROM t WHERE id = 1 OR id = 3").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn select_where_is_null() {
        setup();
        execute("CREATE TABLE t (id integer, name text)").unwrap();
        execute("INSERT INTO t (id) VALUES (1)").unwrap();
        execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
        let r = execute("SELECT * FROM t WHERE name IS NULL").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_where_is_not_null() {
        setup();
        execute("CREATE TABLE t (id integer, name text)").unwrap();
        execute("INSERT INTO t (id) VALUES (1)").unwrap();
        execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
        let r = execute("SELECT * FROM t WHERE name IS NOT NULL").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("2".into()));
    }

    // ── UPDATE ────────────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn update_with_where() {
        setup_test_table();
        let r = execute("UPDATE t SET name = 'updated' WHERE id = 1").unwrap();
        assert_eq!(r.tag, "UPDATE 1");
        let sel = execute("SELECT * FROM t WHERE id = 1").unwrap();
        assert_eq!(sel.rows[0][1], Some("updated".into()));
    }

    #[test]
    #[serial_test::serial]
    fn update_all_rows() {
        setup_test_table();
        let r = execute("UPDATE t SET name = 'all'").unwrap();
        assert_eq!(r.tag, "UPDATE 3");
    }

    #[test]
    #[serial_test::serial]
    fn update_self_referential() {
        setup_test_table();
        execute("UPDATE t SET id = id + 1 WHERE id = 1").unwrap();
        let sel = execute("SELECT * FROM t WHERE name = 'alice'").unwrap();
        assert_eq!(sel.rows[0][0], Some("2".into()));
    }

    // ── DELETE WHERE ──────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn delete_where() {
        setup_test_table();
        let r = execute("DELETE FROM t WHERE id = 1").unwrap();
        assert_eq!(r.tag, "DELETE 1");
        assert_eq!(execute("SELECT * FROM t").unwrap().rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn delete_where_no_match() {
        setup_test_table();
        let r = execute("DELETE FROM t WHERE id = 999").unwrap();
        assert_eq!(r.tag, "DELETE 0");
        assert_eq!(execute("SELECT * FROM t").unwrap().rows.len(), 3);
    }

    // ── ORDER BY / LIMIT / OFFSET ─────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn order_by_asc() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (3, 'c')").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let r = execute("SELECT * FROM t ORDER BY id").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[1][0], Some("2".into()));
        assert_eq!(r.rows[2][0], Some("3".into()));
    }

    #[test]
    #[serial_test::serial]
    fn order_by_desc() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (1)").unwrap();
        execute("INSERT INTO t VALUES (3)").unwrap();
        execute("INSERT INTO t VALUES (2)").unwrap();
        let r = execute("SELECT * FROM t ORDER BY id DESC").unwrap();
        assert_eq!(r.rows[0][0], Some("3".into()));
        assert_eq!(r.rows[2][0], Some("1".into()));
    }

    #[test]
    #[serial_test::serial]
    fn limit_basic() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        for i in 1..=5 {
            execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
        }
        let r = execute("SELECT * FROM t LIMIT 2").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn limit_offset() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        for i in 1..=5 {
            execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
        }
        let r = execute("SELECT * FROM t ORDER BY id LIMIT 2 OFFSET 1").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("2".into()));
        assert_eq!(r.rows[1][0], Some("3".into()));
    }

    #[test]
    #[serial_test::serial]
    fn order_by_desc_limit() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        for i in 1..=5 {
            execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
        }
        let r = execute("SELECT * FROM t ORDER BY id DESC LIMIT 2").unwrap();
        assert_eq!(r.rows[0][0], Some("5".into()));
        assert_eq!(r.rows[1][0], Some("4".into()));
    }

    #[test]
    #[serial_test::serial]
    fn offset_beyond() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (1)").unwrap();
        let r = execute("SELECT * FROM t OFFSET 1000").unwrap();
        assert_eq!(r.rows.len(), 0);
    }

    #[test]
    #[serial_test::serial]
    fn limit_zero() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (1)").unwrap();
        let r = execute("SELECT * FROM t LIMIT 0").unwrap();
        assert_eq!(r.rows.len(), 0);
    }

    // ── Expressions in SELECT ─────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn select_arithmetic() {
        setup();
        execute("CREATE TABLE t (id int, price float8)").unwrap();
        execute("INSERT INTO t VALUES (1, 100.0)").unwrap();
        let r = execute("SELECT id, price * 1.1 FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
        let val: f64 = r.rows[0][1].as_ref().unwrap().parse().unwrap();
        assert!((val - 110.0).abs() < 0.01);
    }

    #[test]
    #[serial_test::serial]
    fn select_upper() {
        setup();
        execute("CREATE TABLE t (name text)").unwrap();
        execute("INSERT INTO t VALUES ('hello')").unwrap();
        let r = execute("SELECT upper(name) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("HELLO".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_lower() {
        setup();
        execute("CREATE TABLE t (name text)").unwrap();
        execute("INSERT INTO t VALUES ('HELLO')").unwrap();
        let r = execute("SELECT lower(name) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("hello".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_length() {
        setup();
        execute("CREATE TABLE t (name text)").unwrap();
        execute("INSERT INTO t VALUES ('hello')").unwrap();
        let r = execute("SELECT length(name) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("5".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_concat_func() {
        setup();
        execute("CREATE TABLE t (a text, b text)").unwrap();
        execute("INSERT INTO t VALUES ('hello', 'world')").unwrap();
        let r = execute("SELECT concat(a, ' ', b) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("hello world".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_concat_op() {
        setup();
        execute("CREATE TABLE t (a text, b text)").unwrap();
        execute("INSERT INTO t VALUES ('hello', 'world')").unwrap();
        let r = execute("SELECT a || ' ' || b FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("hello world".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_unary_minus() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (5)").unwrap();
        let r = execute("SELECT -id FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("-5".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_int_division() {
        setup();
        let r = execute("SELECT 5 / 2").unwrap();
        assert_eq!(r.rows[0][0], Some("2".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_expr_with_alias() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (1)").unwrap();
        let r = execute("SELECT id + 1 AS next_id FROM t").unwrap();
        assert_eq!(r.columns[0].0, "next_id");
        assert_eq!(r.rows[0][0], Some("2".into()));
    }

    // ── Constraints ───────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn pk_prevents_duplicate() {
        setup();
        execute("CREATE TABLE t (id int PRIMARY KEY, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        let err = execute("INSERT INTO t VALUES (1, 'b')");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("unique constraint"));
    }

    #[test]
    #[serial_test::serial]
    fn pk_prevents_null() {
        setup();
        execute("CREATE TABLE t (id int PRIMARY KEY, name text)").unwrap();
        let err = execute("INSERT INTO t (name) VALUES ('a')");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("not-null"));
    }

    #[test]
    #[serial_test::serial]
    fn unique_prevents_duplicate() {
        setup();
        execute("CREATE TABLE t (id int, email text UNIQUE)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a@b.com')").unwrap();
        assert!(execute("INSERT INTO t VALUES (2, 'a@b.com')").is_err());
    }

    #[test]
    #[serial_test::serial]
    fn unique_allows_multiple_nulls() {
        setup();
        execute("CREATE TABLE t (id int, email text UNIQUE)").unwrap();
        execute("INSERT INTO t VALUES (1, NULL)").unwrap();
        execute("INSERT INTO t VALUES (2, NULL)").unwrap();
        let r = execute("SELECT * FROM t").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn not_null_constraint() {
        setup();
        execute("CREATE TABLE t (id int NOT NULL, name text)").unwrap();
        let err = execute("INSERT INTO t (name) VALUES ('a')");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("not-null"));
    }

    #[test]
    #[serial_test::serial]
    fn composite_pk() {
        setup();
        execute("CREATE TABLE t (a int, b int, c text, PRIMARY KEY (a, b))").unwrap();
        execute("INSERT INTO t VALUES (1, 1, 'x')").unwrap();
        execute("INSERT INTO t VALUES (1, 2, 'y')").unwrap(); // ok
        assert!(execute("INSERT INTO t VALUES (1, 1, 'z')").is_err()); // duplicate
    }

    // ── Aggregates / GROUP BY / HAVING ────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn count_star() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let r = execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("2".into()));
    }

    #[test]
    #[serial_test::serial]
    fn count_column_skips_nulls() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        execute("INSERT INTO t VALUES (2, NULL)").unwrap();
        let r = execute("SELECT COUNT(name) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    #[test]
    #[serial_test::serial]
    fn count_empty_table() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        let r = execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("0".into()));
    }

    #[test]
    #[serial_test::serial]
    fn sum_basic() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (10)").unwrap();
        execute("INSERT INTO t VALUES (20)").unwrap();
        execute("INSERT INTO t VALUES (30)").unwrap();
        let r = execute("SELECT SUM(id) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("60".into()));
    }

    #[test]
    #[serial_test::serial]
    fn sum_empty_is_null() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        let r = execute("SELECT SUM(id) FROM t").unwrap();
        assert_eq!(r.rows[0][0], None);
    }

    #[test]
    #[serial_test::serial]
    fn avg_basic() {
        setup();
        execute("CREATE TABLE t (val int)").unwrap();
        execute("INSERT INTO t VALUES (10)").unwrap();
        execute("INSERT INTO t VALUES (20)").unwrap();
        let r = execute("SELECT AVG(val) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("15".into()));
    }

    #[test]
    #[serial_test::serial]
    fn min_max() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (3)").unwrap();
        execute("INSERT INTO t VALUES (1)").unwrap();
        execute("INSERT INTO t VALUES (5)").unwrap();
        let r = execute("SELECT MIN(id), MAX(id) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("5".into()));
    }

    #[test]
    #[serial_test::serial]
    fn group_by_basic() {
        setup();
        execute("CREATE TABLE emp (dept text, salary int)").unwrap();
        execute("INSERT INTO emp VALUES ('eng', 100)").unwrap();
        execute("INSERT INTO emp VALUES ('eng', 200)").unwrap();
        execute("INSERT INTO emp VALUES ('sales', 150)").unwrap();
        let r = execute("SELECT dept, COUNT(*) FROM emp GROUP BY dept").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn group_by_having() {
        setup();
        execute("CREATE TABLE emp (dept text, salary int)").unwrap();
        execute("INSERT INTO emp VALUES ('eng', 100)").unwrap();
        execute("INSERT INTO emp VALUES ('eng', 200)").unwrap();
        execute("INSERT INTO emp VALUES ('sales', 150)").unwrap();
        let r = execute(
            "SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 1",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    #[serial_test::serial]
    fn mixed_agg_no_group_by_error() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        let err = execute("SELECT id, COUNT(*) FROM t");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("GROUP BY"));
    }

    // ── Devin review fixes ────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn update_division_by_zero_errors() {
        setup();
        execute("CREATE TABLE t (id int, val int)").unwrap();
        execute("INSERT INTO t VALUES (1, 10)").unwrap();
        let err = execute("UPDATE t SET val = val / 0 WHERE id = 1");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("division by zero"));
        // Row should be unchanged
        let r = execute("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(r.rows[0][0], Some("10".into()));
    }

    #[test]
    #[serial_test::serial]
    fn update_enforces_unique_constraint() {
        setup();
        execute("CREATE TABLE t (id int PRIMARY KEY, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let err = execute("UPDATE t SET id = 1 WHERE id = 2");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("unique constraint"));
    }

    #[test]
    #[serial_test::serial]
    fn update_enforces_not_null() {
        setup();
        execute("CREATE TABLE t (id int PRIMARY KEY, name text NOT NULL)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        let err = execute("UPDATE t SET name = NULL WHERE id = 1");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("not-null"));
    }

    // ── JOIN tests ────────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn inner_join() {
        setup();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("CREATE TABLE orders (id int, user_id int, total int)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
        execute("INSERT INTO orders VALUES (10, 1, 100)").unwrap();
        execute("INSERT INTO orders VALUES (11, 1, 200)").unwrap();
        execute("INSERT INTO orders VALUES (12, 2, 50)").unwrap();
        let r = execute(
            "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 3);
    }

    #[test]
    #[serial_test::serial]
    fn left_join() {
        setup();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("CREATE TABLE orders (id int, user_id int, total int)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
        execute("INSERT INTO users VALUES (3, 'carol')").unwrap();
        execute("INSERT INTO orders VALUES (10, 1, 100)").unwrap();
        let r = execute(
            "SELECT users.name, orders.total FROM users LEFT JOIN orders ON users.id = orders.user_id",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 3); // alice with order, bob NULL, carol NULL
    }

    #[test]
    #[serial_test::serial]
    fn cross_join() {
        setup();
        execute("CREATE TABLE colors (name text)").unwrap();
        execute("CREATE TABLE sizes (size text)").unwrap();
        execute("INSERT INTO colors VALUES ('red')").unwrap();
        execute("INSERT INTO colors VALUES ('blue')").unwrap();
        execute("INSERT INTO sizes VALUES ('S')").unwrap();
        execute("INSERT INTO sizes VALUES ('M')").unwrap();
        execute("INSERT INTO sizes VALUES ('L')").unwrap();
        let r = execute("SELECT * FROM colors CROSS JOIN sizes").unwrap();
        assert_eq!(r.rows.len(), 6); // 2 * 3
    }

    #[test]
    #[serial_test::serial]
    fn implicit_join() {
        setup();
        execute("CREATE TABLE a (id int, val text)").unwrap();
        execute("CREATE TABLE b (a_id int, data text)").unwrap();
        execute("INSERT INTO a VALUES (1, 'x')").unwrap();
        execute("INSERT INTO a VALUES (2, 'y')").unwrap();
        execute("INSERT INTO b VALUES (1, 'linked')").unwrap();
        let r = execute("SELECT a.val, b.data FROM a, b WHERE a.id = b.a_id").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("x".into()));
        assert_eq!(r.rows[0][1], Some("linked".into()));
    }

    #[test]
    #[serial_test::serial]
    fn join_with_aliases() {
        setup();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("CREATE TABLE orders (id int, user_id int, total int)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO orders VALUES (10, 1, 100)").unwrap();
        let r = execute(
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("alice".into()));
        assert_eq!(r.rows[0][1], Some("100".into()));
    }

    #[test]
    #[serial_test::serial]
    fn three_way_join() {
        setup();
        execute("CREATE TABLE a (id int, name text)").unwrap();
        execute("CREATE TABLE b (id int, a_id int)").unwrap();
        execute("CREATE TABLE c (id int, b_id int, val text)").unwrap();
        execute("INSERT INTO a VALUES (1, 'root')").unwrap();
        execute("INSERT INTO b VALUES (10, 1)").unwrap();
        execute("INSERT INTO c VALUES (100, 10, 'leaf')").unwrap();
        let r = execute(
            "SELECT a.name, c.val FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("root".into()));
        assert_eq!(r.rows[0][1], Some("leaf".into()));
    }

    #[test]
    #[serial_test::serial]
    fn right_join() {
        setup();
        execute("CREATE TABLE orders (id int, user_id int)").unwrap();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
        execute("INSERT INTO orders VALUES (10, 1)").unwrap();
        let r = execute(
            "SELECT orders.id, users.name FROM orders RIGHT JOIN users ON users.id = orders.user_id",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 2); // alice with order, bob with NULL order
    }

    #[test]
    #[serial_test::serial]
    fn join_with_aggregate() {
        setup();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("CREATE TABLE orders (id int, user_id int, total int)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
        execute("INSERT INTO orders VALUES (10, 1, 100)").unwrap();
        execute("INSERT INTO orders VALUES (11, 1, 200)").unwrap();
        execute("INSERT INTO orders VALUES (12, 2, 50)").unwrap();
        let r = execute(
            "SELECT users.name, SUM(orders.total) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn ambiguous_column_error() {
        setup();
        execute("CREATE TABLE a (id int, name text)").unwrap();
        execute("CREATE TABLE b (id int, data text)").unwrap();
        execute("INSERT INTO a VALUES (1, 'x')").unwrap();
        execute("INSERT INTO b VALUES (1, 'y')").unwrap();
        let err = execute("SELECT id FROM a JOIN b ON a.id = b.id");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("ambiguous"));
    }

    #[test]
    #[serial_test::serial]
    fn join_select_star() {
        setup();
        execute("CREATE TABLE t1 (a int, b text)").unwrap();
        execute("CREATE TABLE t2 (c int, d text)").unwrap();
        execute("INSERT INTO t1 VALUES (1, 'x')").unwrap();
        execute("INSERT INTO t2 VALUES (2, 'y')").unwrap();
        let r = execute("SELECT * FROM t1 CROSS JOIN t2").unwrap();
        assert_eq!(r.columns.len(), 4); // a, b, c, d
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("x".into()));
        assert_eq!(r.rows[0][2], Some("2".into()));
        assert_eq!(r.rows[0][3], Some("y".into()));
    }

    // ── RETURNING clause tests ────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn insert_returning_star() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        let r = execute("INSERT INTO t VALUES (1, 'alice') RETURNING *").unwrap();
        assert_eq!(r.tag, "INSERT 0 1");
        assert_eq!(r.columns.len(), 2);
        assert_eq!(r.columns[0].0, "id");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn insert_returning_specific_column() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        let r = execute("INSERT INTO t VALUES (1, 'alice') RETURNING id").unwrap();
        assert_eq!(r.columns.len(), 1);
        assert_eq!(r.columns[0].0, "id");
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    #[test]
    #[serial_test::serial]
    fn insert_returning_multi_row() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        let r = execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob') RETURNING id, name").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[1][0], Some("2".into()));
    }

    #[test]
    #[serial_test::serial]
    fn insert_returning_expression() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        let r = execute("INSERT INTO t VALUES (1, 'alice') RETURNING id, upper(name)").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("ALICE".into()));
    }

    #[test]
    #[serial_test::serial]
    fn update_returning() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
        let r = execute("UPDATE t SET name = 'updated' WHERE id = 1 RETURNING *").unwrap();
        assert_eq!(r.tag, "UPDATE 1");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("updated".into()));
    }

    #[test]
    #[serial_test::serial]
    fn delete_returning() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
        let r = execute("DELETE FROM t WHERE id = 1 RETURNING *").unwrap();
        assert_eq!(r.tag, "DELETE 1");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("alice".into()));
        // Verify row was actually deleted
        let sel = execute("SELECT * FROM t").unwrap();
        assert_eq!(sel.rows.len(), 1);
    }

    #[test]
    #[serial_test::serial]
    fn delete_all_returning() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (1)").unwrap();
        execute("INSERT INTO t VALUES (2)").unwrap();
        let r = execute("DELETE FROM t RETURNING id").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    // ── DEFAULT + SERIAL tests ────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn serial_auto_increment() {
        setup();
        execute("CREATE TABLE t (id serial PRIMARY KEY, name text)").unwrap();
        execute("INSERT INTO t (name) VALUES ('alice')").unwrap();
        execute("INSERT INTO t (name) VALUES ('bob')").unwrap();
        let r = execute("SELECT * FROM t ORDER BY id").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("alice".into()));
        assert_eq!(r.rows[1][0], Some("2".into()));
        assert_eq!(r.rows[1][1], Some("bob".into()));
    }

    #[test]
    #[serial_test::serial]
    fn serial_with_returning() {
        setup();
        execute("CREATE TABLE t (id serial PRIMARY KEY, name text)").unwrap();
        let r = execute("INSERT INTO t (name) VALUES ('alice') RETURNING id").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
        let r = execute("INSERT INTO t (name) VALUES ('bob') RETURNING id, name").unwrap();
        assert_eq!(r.rows[0][0], Some("2".into()));
        assert_eq!(r.rows[0][1], Some("bob".into()));
    }

    #[test]
    #[serial_test::serial]
    fn default_literal_values() {
        setup();
        execute("CREATE TABLE t (id int DEFAULT 0, name text DEFAULT 'anon')").unwrap();
        execute("INSERT INTO t (name) VALUES ('alice')").unwrap();
        let r = execute("SELECT * FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("0".into()));
        assert_eq!(r.rows[0][1], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn nextval_function() {
        setup();
        execute("CREATE TABLE t (id serial, name text)").unwrap();
        let r = execute("SELECT nextval('t_id_seq')").unwrap();
        // Sequence was at 0 after table creation consumed 0 values
        // Actually, serial CREATE creates seq starting at 1
        // nextval from SELECT should advance it
        assert!(r.rows[0][0].is_some());
    }

    // ── Subquery tests ──────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn in_subquery() {
        setup();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("CREATE TABLE orders (user_id int)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')").unwrap();
        execute("INSERT INTO orders VALUES (1), (1), (3)").unwrap();
        let r =
            execute("SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)").unwrap();
        assert_eq!(r.rows.len(), 2); // alice, carol (not bob)
    }

    #[test]
    #[serial_test::serial]
    fn not_in_subquery() {
        setup();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("CREATE TABLE orders (user_id int)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')").unwrap();
        execute("INSERT INTO orders VALUES (1), (3)").unwrap();
        let r = execute("SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM orders)")
            .unwrap();
        assert_eq!(r.rows.len(), 1); // bob only
    }

    #[test]
    #[serial_test::serial]
    fn exists_subquery() {
        setup();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("CREATE TABLE orders (user_id int)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')").unwrap();
        execute("INSERT INTO orders VALUES (1)").unwrap();
        let r = execute(
            "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn scalar_subquery() {
        setup();
        execute("CREATE TABLE t (id int, val int)").unwrap();
        execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
        let r = execute("SELECT (SELECT SUM(val) FROM t)").unwrap();
        assert_eq!(r.rows[0][0], Some("60".into()));
    }

    #[test]
    #[serial_test::serial]
    fn in_literal_list() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
        let r = execute("SELECT * FROM t WHERE id IN (1, 3)").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn not_in_literal_list() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
        let r = execute("SELECT * FROM t WHERE id NOT IN (1, 3)").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1], Some("b".into()));
    }

    #[test]
    #[serial_test::serial]
    fn derived_table() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
        let r = execute(
            "SELECT sub.name FROM (SELECT id, name FROM t WHERE id > 1) AS sub ORDER BY sub.name",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("b".into()));
        assert_eq!(r.rows[1][0], Some("c".into()));
    }

    #[test]
    #[serial_test::serial]
    fn scalar_subquery_too_many_rows() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (1), (2)").unwrap();
        let err = execute("SELECT (SELECT id FROM t)");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("more than one row"));
    }

    // ── Vector type tests ────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn vector_create_insert_select() {
        setup();
        execute("CREATE TABLE items (id int, embedding vector)").unwrap();
        execute("INSERT INTO items VALUES (1, '[1.0, 2.0, 3.0]')").unwrap();
        execute("INSERT INTO items VALUES (2, '[4.0, 5.0, 6.0]')").unwrap();
        let r = execute("SELECT * FROM items").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][1], Some("[1,2,3]".into()));
    }

    #[test]
    #[serial_test::serial]
    fn vector_l2_distance() {
        setup();
        execute("CREATE TABLE items (id int, embedding vector)").unwrap();
        execute("INSERT INTO items VALUES (1, '[1.0, 0.0, 0.0]')").unwrap();
        execute("INSERT INTO items VALUES (2, '[0.0, 1.0, 0.0]')").unwrap();
        execute("INSERT INTO items VALUES (3, '[1.0, 1.0, 0.0]')").unwrap();
        // L2 distance from [1,0,0]: item 1=0, item 3=1, item 2=sqrt(2)
        let r = execute("SELECT id FROM items ORDER BY embedding <-> '[1.0, 0.0, 0.0]' LIMIT 2")
            .unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into())); // closest
        assert_eq!(r.rows[1][0], Some("3".into())); // second closest
    }

    #[test]
    #[serial_test::serial]
    fn vector_cosine_distance() {
        setup();
        execute("CREATE TABLE items (id int, embedding vector)").unwrap();
        execute("INSERT INTO items VALUES (1, '[1.0, 0.0]')").unwrap();
        execute("INSERT INTO items VALUES (2, '[0.0, 1.0]')").unwrap();
        execute("INSERT INTO items VALUES (3, '[0.707, 0.707]')").unwrap();
        // Cosine distance from [1,0]: item 1=0, item 3~0.29, item 2=1
        let r = execute("SELECT id FROM items ORDER BY embedding <=> '[1.0, 0.0]' LIMIT 2")
            .unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    #[test]
    #[serial_test::serial]
    fn vector_inner_product() {
        setup();
        execute("CREATE TABLE items (id int, embedding vector)").unwrap();
        execute("INSERT INTO items VALUES (1, '[1.0, 2.0, 3.0]')").unwrap();
        execute("INSERT INTO items VALUES (2, '[3.0, 2.0, 1.0]')").unwrap();
        // Inner product with [1,0,0]: item 1=1, item 2=3
        // Negative inner product: item 1=-1, item 2=-3
        // ORDER BY <#> (ascending): item 2 first (most similar via inner product)
        let r =
            execute("SELECT id FROM items ORDER BY embedding <#> '[1.0, 0.0, 0.0]'").unwrap();
        assert_eq!(r.rows[0][0], Some("2".into())); // highest inner product
    }

    #[test]
    #[serial_test::serial]
    fn vector_dimension_mismatch() {
        setup();
        execute("CREATE TABLE items (id int, embedding vector)").unwrap();
        execute("INSERT INTO items VALUES (1, '[1.0, 2.0]')").unwrap();
        execute("INSERT INTO items VALUES (2, '[1.0, 2.0, 3.0]')").unwrap();
        // Different dimensions should error
        let err = execute("SELECT id FROM items ORDER BY embedding <-> '[1.0, 2.0]'");
        // This will error during the sort comparison
        assert!(err.is_err());
    }

    #[test]
    #[serial_test::serial]
    fn vector_knn_search() {
        setup();
        execute("CREATE TABLE points (id int, pos vector)").unwrap();
        execute("INSERT INTO points VALUES (1, '[0.0, 0.0]')").unwrap();
        execute("INSERT INTO points VALUES (2, '[1.0, 1.0]')").unwrap();
        execute("INSERT INTO points VALUES (3, '[2.0, 2.0]')").unwrap();
        execute("INSERT INTO points VALUES (4, '[10.0, 10.0]')").unwrap();
        // KNN: 3 nearest to [1.5, 1.5]
        let r = execute("SELECT id FROM points ORDER BY pos <-> '[1.5, 1.5]' LIMIT 3").unwrap();
        assert_eq!(r.rows.len(), 3);
        // [1,1] and [2,2] are equidistant (0.707) from [1.5,1.5] — tie order is
        // implementation-defined (HNSW vs brute-force may differ).
        let ids: Vec<String> = r.rows.iter().filter_map(|row| row[0].clone()).collect();
        assert!(ids.contains(&"2".to_string())); // [1,1]
        assert!(ids.contains(&"3".to_string())); // [2,2]
        assert!(ids.contains(&"1".to_string())); // [0,0] — closer than [10,10]
        assert!(!ids.contains(&"4".to_string())); // [10,10] is farthest
    }

    // ── Bug fix regression tests ─────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn hash_join_null_keys() {
        setup();
        execute("CREATE TABLE a (id int, val text)").unwrap();
        execute("CREATE TABLE b (id int, data text)").unwrap();
        execute("INSERT INTO a VALUES (1, 'x')").unwrap();
        execute("INSERT INTO a VALUES (NULL, 'z')").unwrap();
        execute("INSERT INTO b VALUES (1, 'p')").unwrap();
        execute("INSERT INTO b VALUES (NULL, 'q')").unwrap();
        let r = execute("SELECT a.val, b.data FROM a JOIN b ON a.id = b.id").unwrap();
        assert_eq!(r.rows.len(), 1); // only id=1 matches, NOT NULL=NULL
        assert_eq!(r.rows[0][0], Some("x".into()));
        assert_eq!(r.rows[0][1], Some("p".into()));
    }

    #[test]
    #[serial_test::serial]
    fn left_join_null_keys() {
        setup();
        execute("CREATE TABLE a (id int, val text)").unwrap();
        execute("CREATE TABLE b (id int, data text)").unwrap();
        execute("INSERT INTO a VALUES (1, 'x')").unwrap();
        execute("INSERT INTO a VALUES (2, 'y')").unwrap();
        execute("INSERT INTO a VALUES (NULL, 'z')").unwrap();
        execute("INSERT INTO b VALUES (1, 'p')").unwrap();
        execute("INSERT INTO b VALUES (NULL, 'q')").unwrap();
        let r = execute("SELECT a.val, b.data FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.val").unwrap();
        assert_eq!(r.rows.len(), 3);
        // x matches p, y gets NULL, z gets NULL (NOT matched with q!)
    }

    #[test]
    #[serial_test::serial]
    fn aggregate_order_by() {
        setup();
        execute("CREATE TABLE sales (region text, amount int)").unwrap();
        execute("INSERT INTO sales VALUES ('east', 100)").unwrap();
        execute("INSERT INTO sales VALUES ('west', 200)").unwrap();
        execute("INSERT INTO sales VALUES ('east', 150)").unwrap();
        let r = execute("SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) DESC").unwrap();
        assert_eq!(r.rows[0][0], Some("east".into())); // east=250 > west=200
    }

    #[test]
    #[serial_test::serial]
    fn aggregate_limit() {
        setup();
        execute("CREATE TABLE t (grp text, val int)").unwrap();
        execute("INSERT INTO t VALUES ('a', 1)").unwrap();
        execute("INSERT INTO t VALUES ('b', 2)").unwrap();
        execute("INSERT INTO t VALUES ('c', 3)").unwrap();
        let r = execute("SELECT grp, COUNT(*) FROM t GROUP BY grp LIMIT 2").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn multi_row_insert_atomic() {
        setup();
        execute("CREATE TABLE t (id int PRIMARY KEY)").unwrap();
        let err = execute("INSERT INTO t VALUES (1), (1)"); // duplicate in same batch
        assert!(err.is_err());
        // Table should be empty — nothing committed
        let r = execute("SELECT * FROM t").unwrap();
        assert_eq!(r.rows.len(), 0);
    }

    #[test]
    #[serial_test::serial]
    fn update_intra_batch_uniqueness() {
        setup();
        execute("CREATE TABLE t (id int PRIMARY KEY, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let err = execute("UPDATE t SET id = 5"); // both rows get id=5 — should error
        assert!(err.is_err());
        // Original data should be unchanged
        let r = execute("SELECT * FROM t ORDER BY id").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    // ── HNSW index tests ──────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn hnsw_knn_basic() {
        setup();
        execute("CREATE TABLE items (id int, embedding vector)").unwrap();
        // Insert vectors — HNSW index is auto-created on first insert
        for i in 0..50 {
            let v: Vec<f32> = (0..8)
                .map(|d| ((i * 7 + d * 3) as f32 * 0.02) % 1.0)
                .collect();
            let vstr = format!(
                "[{}]",
                v.iter()
                    .map(|f| format!("{:.4}", f))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            execute(&format!("INSERT INTO items VALUES ({}, '{}')", i, vstr)).unwrap();
        }
        // KNN search should use HNSW
        let r = execute(
            "SELECT id FROM items ORDER BY embedding <-> '[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]' LIMIT 5",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 5);
    }

    #[test]
    #[serial_test::serial]
    fn hnsw_knn_returns_closest() {
        setup();
        execute("CREATE TABLE pts (id int, pos vector)").unwrap();
        execute("INSERT INTO pts VALUES (1, '[0.0, 0.0]')").unwrap();
        execute("INSERT INTO pts VALUES (2, '[1.0, 0.0]')").unwrap();
        execute("INSERT INTO pts VALUES (3, '[0.0, 1.0]')").unwrap();
        execute("INSERT INTO pts VALUES (4, '[10.0, 10.0]')").unwrap();
        // Nearest to [0.1, 0.1] should be id=1
        let r = execute("SELECT id FROM pts ORDER BY pos <-> '[0.1, 0.1]' LIMIT 2").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    #[test]
    #[serial_test::serial]
    fn hnsw_recall_test() {
        setup();
        execute("CREATE TABLE recall_t (id int, emb vector)").unwrap();
        let dim = 8;
        let n = 200;
        let mut vectors: Vec<Vec<f32>> = Vec::new();
        for i in 0..n {
            let v: Vec<f32> = (0..dim)
                .map(|d| ((i * 13 + d * 7) as f32 * 0.005) % 1.0)
                .collect();
            let vstr = format!(
                "[{}]",
                v.iter()
                    .map(|f| format!("{:.6}", f))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            execute(&format!("INSERT INTO recall_t VALUES ({}, '{}')", i, vstr)).unwrap();
            vectors.push(v);
        }
        let query = vec![0.5f32; dim];
        let k = 10;

        // Brute force ground truth
        let mut brute: Vec<(f32, usize)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let d: f32 = v
                    .iter()
                    .zip(query.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f32>()
                    .sqrt();
                (d, i)
            })
            .collect();
        brute.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let truth: std::collections::HashSet<String> = brute
            .iter()
            .take(k)
            .map(|(_, id)| id.to_string())
            .collect();

        // HNSW result
        let qstr = format!(
            "[{}]",
            query
                .iter()
                .map(|f| format!("{:.6}", f))
                .collect::<Vec<_>>()
                .join(",")
        );
        let r = execute(&format!(
            "SELECT id FROM recall_t ORDER BY emb <-> '{}' LIMIT {}",
            qstr, k
        ))
        .unwrap();
        let hnsw_ids: std::collections::HashSet<String> = r
            .rows
            .iter()
            .filter_map(|row| row[0].clone())
            .collect();

        let overlap = truth.intersection(&hnsw_ids).count();
        let recall = overlap as f32 / k as f32;
        assert!(
            recall >= 0.7,
            "HNSW recall {:.0}% ({}/{}) is below 70% threshold",
            recall * 100.0,
            overlap,
            k
        );
    }

    #[test]
    #[serial_test::serial]
    fn hnsw_insert_correct_row_ids() {
        setup();
        execute("CREATE TABLE vec_t (id INT, v VECTOR)").unwrap();
        execute("INSERT INTO vec_t VALUES (1, '[1,0,0]'), (2, '[0,1,0]'), (3, '[0,0,1]')").unwrap();
        let r = execute("SELECT id FROM vec_t ORDER BY v <-> '[1,0,0]' LIMIT 1").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("1".to_string()));
    }

    #[test]
    #[serial_test::serial]
    fn pk_null_rejected_at_storage() {
        setup();
        execute("CREATE TABLE pk_t (id INT PRIMARY KEY, name TEXT)").unwrap();
        let r = execute("INSERT INTO pk_t VALUES (NULL, 'test')");
        assert!(r.is_err());
    }

    #[test]
    #[serial_test::serial]
    fn default_function_error() {
        setup();
        let r = execute("CREATE TABLE df_t (id INT, created_at TEXT DEFAULT now())");
        assert!(r.is_err());
        assert!(r.unwrap_err().contains("not yet supported"));
    }

    // ── LIKE / ILIKE tests ──────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn like_percent_wildcard() {
        setup();
        execute("CREATE TABLE likes (name TEXT)").unwrap();
        execute("INSERT INTO likes VALUES ('alice'), ('bob'), ('alicia'), ('charlie')").unwrap();
        let r = execute("SELECT name FROM likes WHERE name LIKE 'ali%' ORDER BY name").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("alice".into()));
        assert_eq!(r.rows[1][0], Some("alicia".into()));
    }

    #[test]
    #[serial_test::serial]
    fn like_underscore_wildcard() {
        setup();
        execute("CREATE TABLE like2 (code TEXT)").unwrap();
        execute("INSERT INTO like2 VALUES ('A1'), ('A2'), ('AB'), ('A12')").unwrap();
        let r = execute("SELECT code FROM like2 WHERE code LIKE 'A_' ORDER BY code").unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0][0], Some("A1".into()));
        assert_eq!(r.rows[1][0], Some("A2".into()));
        assert_eq!(r.rows[2][0], Some("AB".into()));
    }

    #[test]
    #[serial_test::serial]
    fn not_like() {
        setup();
        execute("CREATE TABLE like3 (name TEXT)").unwrap();
        execute("INSERT INTO like3 VALUES ('foo'), ('bar'), ('foobar')").unwrap();
        let r = execute("SELECT name FROM like3 WHERE name NOT LIKE 'foo%' ORDER BY name").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("bar".into()));
    }

    #[test]
    #[serial_test::serial]
    fn ilike_case_insensitive() {
        setup();
        execute("CREATE TABLE like4 (name TEXT)").unwrap();
        execute("INSERT INTO like4 VALUES ('Alice'), ('BOB'), ('alice')").unwrap();
        let r = execute("SELECT name FROM like4 WHERE name ILIKE 'alice' ORDER BY name").unwrap();
        assert_eq!(r.rows.len(), 2);
        // PostgreSQL: case-insensitive match returns both Alice and alice
    }

    #[test]
    #[serial_test::serial]
    fn like_null_propagation() {
        setup();
        execute("CREATE TABLE like5 (name TEXT)").unwrap();
        execute("INSERT INTO like5 VALUES ('a'), (NULL)").unwrap();
        let r = execute("SELECT name FROM like5 WHERE name LIKE '%'").unwrap();
        assert_eq!(r.rows.len(), 1); // NULL LIKE '%' = NULL, excluded from WHERE
    }

    // ── CASE WHEN tests ─────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn case_searched() {
        setup();
        execute("CREATE TABLE scores (name TEXT, score INT)").unwrap();
        execute("INSERT INTO scores VALUES ('a', 90), ('b', 60), ('c', 40)").unwrap();
        let r = execute(
            "SELECT name, CASE WHEN score >= 80 THEN 'A' WHEN score >= 50 THEN 'B' ELSE 'F' END FROM scores ORDER BY name"
        ).unwrap();
        assert_eq!(r.rows[0][1], Some("A".into()));
        assert_eq!(r.rows[1][1], Some("B".into()));
        assert_eq!(r.rows[2][1], Some("F".into()));
    }

    #[test]
    #[serial_test::serial]
    fn case_simple() {
        setup();
        execute("CREATE TABLE status (code INT)").unwrap();
        execute("INSERT INTO status VALUES (1), (2), (3)").unwrap();
        let r = execute(
            "SELECT CASE code WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM status ORDER BY code"
        ).unwrap();
        assert_eq!(r.rows[0][0], Some("one".into()));
        assert_eq!(r.rows[1][0], Some("two".into()));
        assert_eq!(r.rows[2][0], Some("other".into()));
    }

    #[test]
    #[serial_test::serial]
    fn case_no_else_returns_null() {
        setup();
        execute("CREATE TABLE ce (x INT)").unwrap();
        execute("INSERT INTO ce VALUES (1), (99)").unwrap();
        let r = execute("SELECT CASE WHEN x = 1 THEN 'yes' END FROM ce ORDER BY x").unwrap();
        assert_eq!(r.rows[0][0], Some("yes".into()));
        assert_eq!(r.rows[1][0], None); // no ELSE → NULL
    }

    #[test]
    #[serial_test::serial]
    fn case_in_where() {
        setup();
        execute("CREATE TABLE cw (x INT)").unwrap();
        execute("INSERT INTO cw VALUES (1), (2), (3)").unwrap();
        let r = execute(
            "SELECT x FROM cw WHERE CASE WHEN x > 1 THEN true ELSE false END ORDER BY x"
        ).unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("2".into()));
        assert_eq!(r.rows[1][0], Some("3".into()));
    }

    // ── DISTINCT tests ──────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn distinct_basic() {
        setup();
        execute("CREATE TABLE dup (color TEXT)").unwrap();
        execute("INSERT INTO dup VALUES ('red'), ('blue'), ('red'), ('green'), ('blue')").unwrap();
        let r = execute("SELECT DISTINCT color FROM dup ORDER BY color").unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0][0], Some("blue".into()));
        assert_eq!(r.rows[1][0], Some("green".into()));
        assert_eq!(r.rows[2][0], Some("red".into()));
    }

    #[test]
    #[serial_test::serial]
    fn distinct_with_null() {
        setup();
        execute("CREATE TABLE dup2 (x INT)").unwrap();
        execute("INSERT INTO dup2 VALUES (1), (NULL), (2), (NULL), (1)").unwrap();
        let r = execute("SELECT DISTINCT x FROM dup2 ORDER BY x").unwrap();
        // PostgreSQL: NULL groups as one, ORDER BY puts NULLs last
        // We should have 3 distinct values: 1, 2, NULL
        assert_eq!(r.rows.len(), 3);
    }

    #[test]
    #[serial_test::serial]
    fn distinct_multi_column() {
        setup();
        execute("CREATE TABLE dup3 (a INT, b TEXT)").unwrap();
        execute("INSERT INTO dup3 VALUES (1, 'x'), (1, 'y'), (1, 'x'), (2, 'x')").unwrap();
        let r = execute("SELECT DISTINCT a, b FROM dup3 ORDER BY a, b").unwrap();
        assert_eq!(r.rows.len(), 3); // (1,x), (1,y), (2,x)
    }

    #[test]
    #[serial_test::serial]
    fn like_escaped_percent() {
        setup();
        execute("CREATE TABLE like6 (s TEXT)").unwrap();
        execute("INSERT INTO like6 VALUES ('100%'), ('100x'), ('100')").unwrap();
        let r = execute(r#"SELECT s FROM like6 WHERE s LIKE '100\%'"#).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("100%".into()));
    }

    #[test]
    #[serial_test::serial]
    fn distinct_with_limit() {
        setup();
        execute("CREATE TABLE dup5 (x INT)").unwrap();
        execute("INSERT INTO dup5 VALUES (1), (1), (2), (2), (3), (3)").unwrap();
        // DISTINCT first (3 unique), then LIMIT 2
        let r = execute("SELECT DISTINCT x FROM dup5 ORDER BY x LIMIT 2").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[1][0], Some("2".into()));
    }

    #[test]
    #[serial_test::serial]
    fn distinct_preserves_order() {
        setup();
        execute("CREATE TABLE dup4 (x INT)").unwrap();
        execute("INSERT INTO dup4 VALUES (3), (1), (2), (1), (3)").unwrap();
        let r = execute("SELECT DISTINCT x FROM dup4 ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[1][0], Some("2".into()));
        assert_eq!(r.rows[2][0], Some("3".into()));
    }

    // ═══════════════════════════════════════════════════════════════
    // SPEC TESTS — SQL Completeness Target
    // Each test below defines a PostgreSQL behavior we must match.
    // ═══════════════════════════════════════════════════════════════

    // ── ALTER TABLE ─────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn alter_table_add_column() {
        setup();
        execute("CREATE TABLE alt1 (id INT, name TEXT)").unwrap();
        execute("INSERT INTO alt1 VALUES (1, 'alice')").unwrap();
        execute("ALTER TABLE alt1 ADD COLUMN age INT").unwrap();
        let r = execute("SELECT id, name, age FROM alt1").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][2], None); // new column is NULL for existing rows
    }

    #[test]
    #[serial_test::serial]
    fn alter_table_add_column_with_default() {
        setup();
        execute("CREATE TABLE alt2 (id INT)").unwrap();
        execute("INSERT INTO alt2 VALUES (1), (2)").unwrap();
        execute("ALTER TABLE alt2 ADD COLUMN status TEXT DEFAULT 'active'").unwrap();
        let r = execute("SELECT id, status FROM alt2 ORDER BY id").unwrap();
        assert_eq!(r.rows[0][1], Some("active".into()));
        assert_eq!(r.rows[1][1], Some("active".into()));
    }

    #[test]
    #[serial_test::serial]
    fn alter_table_drop_column() {
        setup();
        execute("CREATE TABLE alt3 (id INT, name TEXT, age INT)").unwrap();
        execute("INSERT INTO alt3 VALUES (1, 'alice', 30)").unwrap();
        execute("ALTER TABLE alt3 DROP COLUMN age").unwrap();
        let r = execute("SELECT * FROM alt3").unwrap();
        assert_eq!(r.columns.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn alter_table_rename_column() {
        setup();
        execute("CREATE TABLE alt4 (id INT, name TEXT)").unwrap();
        execute("INSERT INTO alt4 VALUES (1, 'alice')").unwrap();
        execute("ALTER TABLE alt4 RENAME COLUMN name TO full_name").unwrap();
        let r = execute("SELECT full_name FROM alt4").unwrap();
        assert_eq!(r.rows[0][0], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn alter_table_rename_table() {
        setup();
        execute("CREATE TABLE alt5 (id INT)").unwrap();
        execute("INSERT INTO alt5 VALUES (1)").unwrap();
        execute("ALTER TABLE alt5 RENAME TO alt5_renamed").unwrap();
        let r = execute("SELECT id FROM alt5_renamed").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    // ── Type Casting ────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn cast_text_to_int() {
        setup();
        let r = execute("SELECT CAST('42' AS INT)").unwrap();
        assert_eq!(r.rows[0][0], Some("42".into()));
    }

    #[test]
    #[serial_test::serial]
    fn cast_int_to_text() {
        setup();
        let r = execute("SELECT CAST(42 AS TEXT)").unwrap();
        assert_eq!(r.rows[0][0], Some("42".into()));
    }

    #[test]
    #[serial_test::serial]
    fn cast_float_to_int_rounds() {
        setup();
        // PostgreSQL: CAST(3.7 AS INT) = 4 (rounds)
        let r = execute("SELECT CAST(3.7 AS INT)").unwrap();
        assert_eq!(r.rows[0][0], Some("4".into()));
    }

    #[test]
    #[serial_test::serial]
    fn cast_shorthand_syntax() {
        setup();
        let r = execute("SELECT '123'::INT").unwrap();
        assert_eq!(r.rows[0][0], Some("123".into()));
    }

    #[test]
    #[serial_test::serial]
    fn cast_int_to_float() {
        setup();
        let r = execute("SELECT 42::FLOAT8").unwrap();
        // Should be a float representation
        let val = r.rows[0][0].as_ref().unwrap();
        assert!(val == "42" || val == "42.0");
    }

    #[test]
    #[serial_test::serial]
    fn cast_bool_to_text() {
        setup();
        let r = execute("SELECT true::TEXT").unwrap();
        assert_eq!(r.rows[0][0], Some("true".into()));
    }

    #[test]
    #[serial_test::serial]
    fn cast_in_where() {
        setup();
        execute("CREATE TABLE cast_t (val TEXT)").unwrap();
        execute("INSERT INTO cast_t VALUES ('10'), ('20'), ('3')").unwrap();
        let r = execute("SELECT val FROM cast_t WHERE val::INT > 5 ORDER BY val::INT").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("10".into()));
        assert_eq!(r.rows[1][0], Some("20".into()));
    }

    // ── BETWEEN ─────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn between_basic() {
        setup();
        execute("CREATE TABLE bet1 (x INT)").unwrap();
        execute("INSERT INTO bet1 VALUES (1), (5), (10), (15), (20)").unwrap();
        let r = execute("SELECT x FROM bet1 WHERE x BETWEEN 5 AND 15 ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0][0], Some("5".into()));
        assert_eq!(r.rows[1][0], Some("10".into()));
        assert_eq!(r.rows[2][0], Some("15".into()));
    }

    #[test]
    #[serial_test::serial]
    fn not_between() {
        setup();
        execute("CREATE TABLE bet2 (x INT)").unwrap();
        execute("INSERT INTO bet2 VALUES (1), (5), (10)").unwrap();
        let r = execute("SELECT x FROM bet2 WHERE x NOT BETWEEN 3 AND 7 ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[1][0], Some("10".into()));
    }

    // ── COALESCE / NULLIF ───────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn coalesce_basic() {
        setup();
        execute("CREATE TABLE coal (a INT, b INT, c INT)").unwrap();
        execute("INSERT INTO coal VALUES (NULL, NULL, 3)").unwrap();
        execute("INSERT INTO coal VALUES (NULL, 2, 3)").unwrap();
        execute("INSERT INTO coal VALUES (1, 2, 3)").unwrap();
        let r = execute("SELECT COALESCE(a, b, c) FROM coal ORDER BY a, b, c").unwrap();
        // Row 1: COALESCE(1,2,3) = 1
        // Row 2: COALESCE(NULL,2,3) = 2
        // Row 3: COALESCE(NULL,NULL,3) = 3
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[1][0], Some("2".into()));
        assert_eq!(r.rows[2][0], Some("3".into()));
    }

    #[test]
    #[serial_test::serial]
    fn coalesce_all_null() {
        setup();
        let r = execute("SELECT COALESCE(NULL, NULL, NULL)").unwrap();
        assert_eq!(r.rows[0][0], None);
    }

    #[test]
    #[serial_test::serial]
    fn nullif_equal() {
        setup();
        // NULLIF(a, b) returns NULL if a = b, else a
        let r = execute("SELECT NULLIF(5, 5)").unwrap();
        assert_eq!(r.rows[0][0], None);
    }

    #[test]
    #[serial_test::serial]
    fn nullif_not_equal() {
        setup();
        let r = execute("SELECT NULLIF(5, 3)").unwrap();
        assert_eq!(r.rows[0][0], Some("5".into()));
    }

    // ── String Functions ────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn substring_from_for() {
        setup();
        // SUBSTRING('hello' FROM 2 FOR 3) = 'ell'
        let r = execute("SELECT SUBSTRING('hello' FROM 2 FOR 3)").unwrap();
        assert_eq!(r.rows[0][0], Some("ell".into()));
    }

    #[test]
    #[serial_test::serial]
    fn substring_from_only() {
        setup();
        // SUBSTRING('hello' FROM 3) = 'llo'
        let r = execute("SELECT SUBSTRING('hello' FROM 3)").unwrap();
        assert_eq!(r.rows[0][0], Some("llo".into()));
    }

    #[test]
    #[serial_test::serial]
    fn trim_basic() {
        setup();
        let r = execute("SELECT TRIM('  hello  ')").unwrap();
        assert_eq!(r.rows[0][0], Some("hello".into()));
    }

    #[test]
    #[serial_test::serial]
    fn trim_leading_trailing() {
        setup();
        let r = execute("SELECT TRIM(LEADING ' ' FROM '  hello  ')").unwrap();
        assert_eq!(r.rows[0][0], Some("hello  ".into()));
    }

    #[test]
    #[serial_test::serial]
    fn replace_function() {
        setup();
        let r = execute("SELECT REPLACE('hello world', 'world', 'rust')").unwrap();
        assert_eq!(r.rows[0][0], Some("hello rust".into()));
    }

    #[test]
    #[serial_test::serial]
    fn position_function() {
        setup();
        // POSITION('lo' IN 'hello') = 4 (1-based)
        let r = execute("SELECT POSITION('lo' IN 'hello')").unwrap();
        assert_eq!(r.rows[0][0], Some("4".into()));
    }

    #[test]
    #[serial_test::serial]
    fn left_right_functions() {
        setup();
        let r1 = execute("SELECT LEFT('hello', 3)").unwrap();
        assert_eq!(r1.rows[0][0], Some("hel".into()));
        let r2 = execute("SELECT RIGHT('hello', 3)").unwrap();
        assert_eq!(r2.rows[0][0], Some("llo".into()));
    }

    // ── Math Functions ──────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn ceil_floor_round() {
        setup();
        let r1 = execute("SELECT CEIL(4.2)").unwrap();
        assert_eq!(r1.rows[0][0], Some("5".into()));
        let r2 = execute("SELECT FLOOR(4.8)").unwrap();
        assert_eq!(r2.rows[0][0], Some("4".into()));
        let r3 = execute("SELECT ROUND(4.567, 2)").unwrap();
        assert_eq!(r3.rows[0][0], Some("4.57".into()));
    }

    #[test]
    #[serial_test::serial]
    fn mod_function() {
        setup();
        let r = execute("SELECT MOD(10, 3)").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    #[test]
    #[serial_test::serial]
    fn power_sqrt() {
        setup();
        let r1 = execute("SELECT POWER(2, 10)").unwrap();
        assert_eq!(r1.rows[0][0], Some("1024".into()));
        let r2 = execute("SELECT SQRT(144)").unwrap();
        assert_eq!(r2.rows[0][0], Some("12".into()));
    }

    // ── Aggregate Enhancements ──────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn count_distinct() {
        setup();
        execute("CREATE TABLE cd (color TEXT)").unwrap();
        execute("INSERT INTO cd VALUES ('red'), ('blue'), ('red'), ('green'), ('blue')").unwrap();
        let r = execute("SELECT COUNT(DISTINCT color) FROM cd").unwrap();
        assert_eq!(r.rows[0][0], Some("3".into()));
    }

    #[test]
    #[serial_test::serial]
    fn sum_distinct() {
        setup();
        execute("CREATE TABLE sd (x INT)").unwrap();
        execute("INSERT INTO sd VALUES (1), (2), (2), (3), (3), (3)").unwrap();
        let r = execute("SELECT SUM(DISTINCT x) FROM sd").unwrap();
        assert_eq!(r.rows[0][0], Some("6".into())); // 1+2+3
    }

    #[test]
    #[serial_test::serial]
    fn avg_function() {
        setup();
        execute("CREATE TABLE av (x INT)").unwrap();
        execute("INSERT INTO av VALUES (10), (20), (30)").unwrap();
        let r = execute("SELECT AVG(x) FROM av").unwrap();
        assert_eq!(r.rows[0][0], Some("20".into()));
    }

    #[test]
    #[serial_test::serial]
    fn string_agg() {
        setup();
        execute("CREATE TABLE sa (name TEXT)").unwrap();
        execute("INSERT INTO sa VALUES ('a'), ('b'), ('c')").unwrap();
        let r = execute("SELECT STRING_AGG(name, ', ' ORDER BY name) FROM sa").unwrap();
        assert_eq!(r.rows[0][0], Some("a, b, c".into()));
    }

    #[test]
    #[serial_test::serial]
    fn bool_and_or() {
        setup();
        execute("CREATE TABLE ba (x BOOL)").unwrap();
        execute("INSERT INTO ba VALUES (true), (true), (false)").unwrap();
        let r1 = execute("SELECT BOOL_AND(x) FROM ba").unwrap();
        assert_eq!(r1.rows[0][0], Some("f".into()));
        let r2 = execute("SELECT BOOL_OR(x) FROM ba").unwrap();
        assert_eq!(r2.rows[0][0], Some("t".into()));
    }

    // ── UNION / UNION ALL / INTERSECT / EXCEPT ──────────────────

    #[test]
    #[serial_test::serial]
    fn union_all() {
        setup();
        execute("CREATE TABLE u1 (x INT)").unwrap();
        execute("CREATE TABLE u2 (x INT)").unwrap();
        execute("INSERT INTO u1 VALUES (1), (2)").unwrap();
        execute("INSERT INTO u2 VALUES (2), (3)").unwrap();
        let r = execute("SELECT x FROM u1 UNION ALL SELECT x FROM u2 ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 4); // 1, 2, 2, 3
    }

    #[test]
    #[serial_test::serial]
    fn union_dedup() {
        setup();
        execute("CREATE TABLE u3 (x INT)").unwrap();
        execute("CREATE TABLE u4 (x INT)").unwrap();
        execute("INSERT INTO u3 VALUES (1), (2)").unwrap();
        execute("INSERT INTO u4 VALUES (2), (3)").unwrap();
        let r = execute("SELECT x FROM u3 UNION SELECT x FROM u4 ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 3); // 1, 2, 3 (deduped)
    }

    #[test]
    #[serial_test::serial]
    fn intersect_basic() {
        setup();
        execute("CREATE TABLE i1 (x INT)").unwrap();
        execute("CREATE TABLE i2 (x INT)").unwrap();
        execute("INSERT INTO i1 VALUES (1), (2), (3)").unwrap();
        execute("INSERT INTO i2 VALUES (2), (3), (4)").unwrap();
        let r = execute("SELECT x FROM i1 INTERSECT SELECT x FROM i2 ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 2); // 2, 3
    }

    #[test]
    #[serial_test::serial]
    fn except_basic() {
        setup();
        execute("CREATE TABLE e1 (x INT)").unwrap();
        execute("CREATE TABLE e2 (x INT)").unwrap();
        execute("INSERT INTO e1 VALUES (1), (2), (3)").unwrap();
        execute("INSERT INTO e2 VALUES (2), (3), (4)").unwrap();
        let r = execute("SELECT x FROM e1 EXCEPT SELECT x FROM e2 ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 1); // 1
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    // ── Subquery Enhancements ───────────────────────────────────

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

    // ── Expression Enhancements ─────────────────────────────────

    #[test]
    #[serial_test::serial]
    #[ignore = "ORDER BY alias resolution not yet implemented"]
    fn expression_aliases_in_order_by() {
        setup();
        execute("CREATE TABLE ea (price INT, qty INT)").unwrap();
        execute("INSERT INTO ea VALUES (10, 5), (20, 2), (5, 10)").unwrap();
        let r = execute("SELECT price * qty AS total FROM ea ORDER BY total").unwrap();
        assert_eq!(r.rows[0][0], Some("40".into()));
        assert_eq!(r.rows[1][0], Some("50".into()));
        assert_eq!(r.rows[2][0], Some("50".into()));
    }

    #[test]
    #[serial_test::serial]
    fn negative_literal() {
        setup();
        let r = execute("SELECT -5").unwrap();
        assert_eq!(r.rows[0][0], Some("-5".into()));
    }

    #[test]
    #[serial_test::serial]
    fn modulo_operator() {
        setup();
        let r = execute("SELECT 10 % 3").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    // ── INSERT ... SELECT ───────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn insert_select() {
        setup();
        execute("CREATE TABLE src (x INT)").unwrap();
        execute("CREATE TABLE dst (x INT)").unwrap();
        execute("INSERT INTO src VALUES (1), (2), (3)").unwrap();
        execute("INSERT INTO dst SELECT x FROM src WHERE x > 1").unwrap();
        let r = execute("SELECT x FROM dst ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("2".into()));
        assert_eq!(r.rows[1][0], Some("3".into()));
    }

    // ── UPDATE with JOIN / FROM ─────────────────────────────────

    #[test]
    #[serial_test::serial]
    #[ignore = "UPDATE FROM not yet implemented"]
    fn update_from_join() {
        setup();
        execute("CREATE TABLE prices (id INT, price INT)").unwrap();
        execute("CREATE TABLE discounts (id INT, discount INT)").unwrap();
        execute("INSERT INTO prices VALUES (1, 100), (2, 200)").unwrap();
        execute("INSERT INTO discounts VALUES (1, 10), (2, 20)").unwrap();
        execute("UPDATE prices SET price = prices.price - discounts.discount FROM discounts WHERE prices.id = discounts.id").unwrap();
        let r = execute("SELECT id, price FROM prices ORDER BY id").unwrap();
        assert_eq!(r.rows[0][1], Some("90".into()));
        assert_eq!(r.rows[1][1], Some("180".into()));
    }

    // ── DELETE with USING ───────────────────────────────────────

    #[test]
    #[serial_test::serial]
    #[ignore = "DELETE USING not yet implemented"]
    fn delete_using() {
        setup();
        execute("CREATE TABLE items (id INT, name TEXT)").unwrap();
        execute("CREATE TABLE blacklist (name TEXT)").unwrap();
        execute("INSERT INTO items VALUES (1, 'good'), (2, 'bad'), (3, 'ugly')").unwrap();
        execute("INSERT INTO blacklist VALUES ('bad'), ('ugly')").unwrap();
        execute("DELETE FROM items USING blacklist WHERE items.name = blacklist.name").unwrap();
        let r = execute("SELECT name FROM items").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("good".into()));
    }

    // ── CREATE TABLE AS / SELECT INTO ───────────────────────────

    #[test]
    #[serial_test::serial]
    fn create_table_as_select() {
        setup();
        execute("CREATE TABLE ctas_src (id INT, name TEXT)").unwrap();
        execute("INSERT INTO ctas_src VALUES (1, 'alice'), (2, 'bob')").unwrap();
        execute("CREATE TABLE ctas_dst AS SELECT * FROM ctas_src WHERE id = 1").unwrap();
        let r = execute("SELECT id, name FROM ctas_dst").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1], Some("alice".into()));
    }

    // ── IF NOT EXISTS / IF EXISTS ───────────────────────────────

    #[test]
    #[serial_test::serial]
    fn create_table_if_not_exists() {
        setup();
        execute("CREATE TABLE ine (id INT)").unwrap();
        // Should not error
        execute("CREATE TABLE IF NOT EXISTS ine (id INT)").unwrap();
        execute("INSERT INTO ine VALUES (1)").unwrap();
        let r = execute("SELECT id FROM ine").unwrap();
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    #[serial_test::serial]
    fn drop_table_if_exists() {
        setup();
        // Should not error even if table doesn't exist
        execute("DROP TABLE IF EXISTS nonexistent").unwrap();
    }

    // ── Multiple DEFAULT expressions ────────────────────────────

    #[test]
    #[serial_test::serial]
    fn insert_default_keyword() {
        setup();
        execute("CREATE TABLE def1 (id SERIAL, name TEXT DEFAULT 'unnamed')").unwrap();
        execute("INSERT INTO def1 (name) VALUES (DEFAULT)").unwrap();
        let r = execute("SELECT id, name FROM def1").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("unnamed".into()));
    }

    // ── Column aliases in SELECT ────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn column_alias() {
        setup();
        execute("CREATE TABLE ca (first_name TEXT)").unwrap();
        execute("INSERT INTO ca VALUES ('alice')").unwrap();
        let r = execute("SELECT first_name AS name FROM ca").unwrap();
        assert_eq!(r.columns[0].0, "name");
        assert_eq!(r.rows[0][0], Some("alice".into()));
    }

    // ---- Window function tests ----

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

    // ---- Window value function tests ----

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
        // LAST_VALUE for id=2 and id=3 should both be id=3's value (20)
        let r = execute(
            "SELECT id, LAST_VALUE(id) OVER (ORDER BY val) AS lv FROM wlastp ORDER BY id",
        ).unwrap();
        assert_eq!(r.rows.len(), 4);
        assert_eq!(r.rows[0][1], Some("1".into())); // val=10, no peers, last=self
        assert_eq!(r.rows[1][1], Some("3".into())); // val=20, peer group [2,3], last=3
        assert_eq!(r.rows[2][1], Some("3".into())); // val=20, peer group [2,3], last=3
        assert_eq!(r.rows[3][1], Some("4".into())); // val=30, no peers, last=self
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
        // Row 1: frame [1], 2nd value doesn't exist -> NULL
        // Row 2: frame [1,2], 2nd value = 20
        // Row 3: frame [1,2,3], 2nd value = 20
        assert_eq!(r.rows[0][1], None);
        assert_eq!(r.rows[1][1], Some("20".into()));
        assert_eq!(r.rows[2][1], Some("20".into()));
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

    // ---- Aggregate window function tests ----

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

    // ---- CTE tests ----

    #[test]
    #[serial_test::serial]
    fn cte_basic() {
        setup();
        let r = execute("WITH cte AS (SELECT 1 AS x) SELECT * FROM cte").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.columns[0].0, "x");
    }

    #[test]
    #[serial_test::serial]
    fn cte_from_table() {
        setup();
        execute("CREATE TABLE cte_t (id int, name text)").unwrap();
        execute("INSERT INTO cte_t VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO cte_t VALUES (2, 'bob')").unwrap();
        let r = execute(
            "WITH active AS (SELECT * FROM cte_t WHERE id = 1) SELECT * FROM active",
        ).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn cte_with_where() {
        setup();
        execute("CREATE TABLE cte_w (id int, val int)").unwrap();
        execute("INSERT INTO cte_w VALUES (1, 10)").unwrap();
        execute("INSERT INTO cte_w VALUES (2, 20)").unwrap();
        execute("INSERT INTO cte_w VALUES (3, 30)").unwrap();
        let r = execute(
            "WITH data AS (SELECT * FROM cte_w) SELECT * FROM data WHERE val > 15 ORDER BY id",
        ).unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("2".into()));
        assert_eq!(r.rows[1][0], Some("3".into()));
    }

    #[test]
    #[serial_test::serial]
    fn cte_multiple() {
        setup();
        let r = execute(
            "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a, b",
        ).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("2".into()));
    }

    #[test]
    #[serial_test::serial]
    fn cte_with_join() {
        setup();
        execute("CREATE TABLE cte_users (id int, name text)").unwrap();
        execute("CREATE TABLE cte_orders (id int, user_id int, amount int)").unwrap();
        execute("INSERT INTO cte_users VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO cte_orders VALUES (1, 1, 100)").unwrap();
        let r = execute(
            "WITH u AS (SELECT * FROM cte_users) \
             SELECT u.name, o.amount FROM u JOIN cte_orders o ON u.id = o.user_id",
        ).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("alice".into()));
        assert_eq!(r.rows[0][1], Some("100".into()));
    }

    #[test]
    #[serial_test::serial]
    fn cte_column_alias() {
        setup();
        execute("CREATE TABLE cte_ca (id int, name text)").unwrap();
        execute("INSERT INTO cte_ca VALUES (1, 'alice')").unwrap();
        let r = execute(
            "WITH cte(x, y) AS (SELECT id, name FROM cte_ca) SELECT x, y FROM cte",
        ).unwrap();
        assert_eq!(r.columns[0].0, "x");
        assert_eq!(r.columns[1].0, "y");
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn cte_referenced_twice() {
        setup();
        let r = execute(
            "WITH nums AS (SELECT 1 AS n UNION ALL SELECT 2) \
             SELECT a.n, b.n FROM nums a, nums b ORDER BY a.n, b.n",
        ).unwrap();
        assert_eq!(r.rows.len(), 4); // 2x2 cross join
    }

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

    // ---- UPSERT tests ----

    #[test]
    #[serial_test::serial]
    fn upsert_do_nothing() {
        setup();
        execute("CREATE TABLE udn (id int PRIMARY KEY, name text)").unwrap();
        execute("INSERT INTO udn VALUES (1, 'alice')").unwrap();
        // Conflicting insert should be silently skipped
        let r = execute("INSERT INTO udn VALUES (1, 'bob') ON CONFLICT DO NOTHING").unwrap();
        assert_eq!(r.tag, "INSERT 0 0");
        // Original row unchanged
        let r = execute("SELECT * FROM udn").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn upsert_do_nothing_no_conflict() {
        setup();
        execute("CREATE TABLE udnn (id int PRIMARY KEY, name text)").unwrap();
        execute("INSERT INTO udnn VALUES (1, 'alice')").unwrap();
        // No conflict - should insert normally
        let r = execute("INSERT INTO udnn VALUES (2, 'bob') ON CONFLICT DO NOTHING").unwrap();
        assert_eq!(r.tag, "INSERT 0 1");
        let r = execute("SELECT * FROM udnn ORDER BY id").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn upsert_do_update() {
        setup();
        execute("CREATE TABLE udu (id int PRIMARY KEY, name text, val int)").unwrap();
        execute("INSERT INTO udu VALUES (1, 'alice', 10)").unwrap();
        let r = execute(
            "INSERT INTO udu VALUES (1, 'bob', 20) \
             ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, val = EXCLUDED.val",
        ).unwrap();
        assert_eq!(r.tag, "INSERT 0 1");
        let r = execute("SELECT * FROM udu").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1], Some("bob".into()));
        assert_eq!(r.rows[0][2], Some("20".into()));
    }

    #[test]
    #[serial_test::serial]
    fn upsert_do_update_partial() {
        setup();
        execute("CREATE TABLE udup (id int PRIMARY KEY, name text, val int)").unwrap();
        execute("INSERT INTO udup VALUES (1, 'alice', 10)").unwrap();
        // Only update val, keep existing name
        let r = execute(
            "INSERT INTO udup VALUES (1, 'bob', 20) \
             ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val",
        ).unwrap();
        assert_eq!(r.tag, "INSERT 0 1");
        let r = execute("SELECT * FROM udup").unwrap();
        assert_eq!(r.rows[0][1], Some("alice".into())); // unchanged
        assert_eq!(r.rows[0][2], Some("20".into()));    // updated
    }

    #[test]
    #[serial_test::serial]
    fn upsert_do_update_expression() {
        setup();
        execute("CREATE TABLE udue (id int PRIMARY KEY, counter int)").unwrap();
        execute("INSERT INTO udue VALUES (1, 10)").unwrap();
        // Increment counter using existing value + excluded value
        let r = execute(
            "INSERT INTO udue VALUES (1, 5) \
             ON CONFLICT (id) DO UPDATE SET counter = udue.counter + EXCLUDED.counter",
        ).unwrap();
        assert_eq!(r.tag, "INSERT 0 1");
        let r = execute("SELECT * FROM udue").unwrap();
        assert_eq!(r.rows[0][1], Some("15".into())); // 10 + 5
    }

    #[test]
    #[serial_test::serial]
    fn upsert_batch_mixed() {
        setup();
        execute("CREATE TABLE ubm (id int PRIMARY KEY, val int)").unwrap();
        execute("INSERT INTO ubm VALUES (1, 10)").unwrap();
        // Batch: id=1 conflicts (update), id=2 is new (insert)
        let r = execute(
            "INSERT INTO ubm VALUES (1, 100), (2, 200) \
             ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val",
        ).unwrap();
        assert_eq!(r.tag, "INSERT 0 2");
        let r = execute("SELECT * FROM ubm ORDER BY id").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][1], Some("100".into())); // updated
        assert_eq!(r.rows[1][1], Some("200".into())); // inserted
    }

    #[test]
    #[serial_test::serial]
    fn upsert_unique_constraint() {
        setup();
        execute("CREATE TABLE uuc (id int, email text UNIQUE, name text)").unwrap();
        execute("INSERT INTO uuc VALUES (1, 'a@b.com', 'alice')").unwrap();
        let r = execute(
            "INSERT INTO uuc VALUES (2, 'a@b.com', 'bob') ON CONFLICT DO NOTHING",
        ).unwrap();
        assert_eq!(r.tag, "INSERT 0 0");
        let r = execute("SELECT * FROM uuc").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][2], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn upsert_returning() {
        setup();
        execute("CREATE TABLE ur (id int PRIMARY KEY, val int)").unwrap();
        execute("INSERT INTO ur VALUES (1, 10)").unwrap();
        let r = execute(
            "INSERT INTO ur VALUES (1, 20) \
             ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val \
             RETURNING id, val",
        ).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("20".into()));
    }

