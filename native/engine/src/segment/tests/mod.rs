//! Segment test modules. Helpers for file paths and row construction.

use super::*;
use crate::types::Value;

mod basic;
mod zone_maps;
mod large;
mod edge_types;

pub(super) fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("evolvsql_seg_{}_{}.bin", name, std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

pub(super) fn users_schema() -> Vec<(String, i32)> {
    vec![
        ("id".into(), 23),   // int4
        ("name".into(), 25), // text
    ]
}

pub(super) fn user_row(id: i64, name: &str) -> Vec<Value> {
    vec![
        Value::Int(id),
        Value::Text(std::sync::Arc::from(name)),
    ]
}
