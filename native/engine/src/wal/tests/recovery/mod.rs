//! End-to-end recovery tests: simulate a crash by resetting storage,
//! then replay the WAL and verify all durable rows are restored.

mod basic;
mod errors;
mod vector;
mod mutations;
mod ddl;

pub(super) fn tmp_recovery_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("evolvsql_rec_{}_{}.log", name, std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}
