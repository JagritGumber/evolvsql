use super::*;

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
