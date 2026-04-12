//! Memtable unit tests.

use super::*;
use crate::types::Value;

mod basic;
mod mutations;
mod drain;

pub(super) fn row(id: i64, name: &str) -> Vec<Value> {
    vec![Value::Int(id), Value::Text(std::sync::Arc::from(name))]
}
