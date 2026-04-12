//! Three-valued logic and NULL edge case tests.
//!
//! PostgreSQL uses SQL's three-valued logic (TRUE/FALSE/NULL). These
//! tests cover the non-obvious cases where intuition disagrees with
//! the spec. Missing coverage here has historically been the source
//! of correctness bugs.

mod in_not_in;
mod aggregates;
mod grouping;
mod bool_logic;
