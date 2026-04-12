use super::super::*;

// ── PR #35: left()/right() with negative n wrapped to huge usize ──────
// PostgreSQL semantics: negative n means "all but the last |n| chars"
// (for left) or "all but the first |n| chars" (for right). Our initial
// `*i as usize` cast wrapped negative to usize::MAX, making take() return
// the whole string.
#[test]
#[serial_test::serial]
fn left_negative_n_drops_from_end() {
    setup();
    let r = execute("SELECT LEFT('hello', -2)").unwrap();
    assert_eq!(r.rows[0][0], Some("hel".into()));
}

#[test]
#[serial_test::serial]
fn right_negative_n_drops_from_start() {
    setup();
    let r = execute("SELECT RIGHT('hello', -2)").unwrap();
    assert_eq!(r.rows[0][0], Some("llo".into()));
}
