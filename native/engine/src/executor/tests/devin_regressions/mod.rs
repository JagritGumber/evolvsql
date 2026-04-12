//! Regression tests for bugs that shipped with passing tests but were
//! caught by static review. Each test here is a direct counterexample
//! for a test that existed and passed before the bug was fixed.
//!
//! Keep this directory as a forcing function: any bug caught in review
//! should have its repro added here before merge.

mod aggregates;
mod set_ops;
mod strings;
mod misc;
