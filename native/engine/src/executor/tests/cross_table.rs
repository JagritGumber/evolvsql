use super::*;

#[test]
#[serial_test::serial]
#[ignore = "UPDATE FROM not yet implemented"]
fn update_from_join() {
    setup();
    execute("CREATE TABLE prices (id INT, price INT)").unwrap();
    execute("CREATE TABLE discounts (id INT, discount INT)").unwrap();
    execute("INSERT INTO prices VALUES (1, 100), (2, 200)").unwrap();
    execute("INSERT INTO discounts VALUES (1, 10), (2, 20)").unwrap();
    execute(
        "UPDATE prices SET price = prices.price - discounts.discount FROM discounts WHERE prices.id = discounts.id",
    )
    .unwrap();
    let r = execute("SELECT id, price FROM prices ORDER BY id").unwrap();
    assert_eq!(r.rows[0][1], Some("90".into()));
    assert_eq!(r.rows[1][1], Some("180".into()));
}

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
