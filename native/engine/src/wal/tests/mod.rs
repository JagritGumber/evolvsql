//! WAL test modules. Helpers shared across test files.

use super::*;
use crate::types::Value;

mod basic;
mod corruption;
mod payload;
mod manager_tests;
mod storage_integration;
mod recovery;
mod writer_concurrent;
mod writer_rollback;
mod concurrent_writes;

pub(super) fn tmp_wal_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("evolvsql_wal_test_{}_{}.log", name, std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

pub(super) fn insert_op(id: i64, name: &str) -> WalOp {
    WalOp::Insert {
        schema: "public".into(),
        table: "users".into(),
        row: vec![Value::Int(id), Value::Text(std::sync::Arc::from(name))],
    }
}
