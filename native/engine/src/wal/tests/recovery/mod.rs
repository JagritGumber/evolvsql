//! End-to-end recovery tests: simulate a crash by resetting storage,
//! then replay the WAL and verify all durable rows are restored.

mod basic;
mod errors;
mod vector;
mod mutations;
mod ddl;
mod continuation;
mod unique_constraints;
mod bulk_ops;
mod alter_columns;
mod alter_rename;
mod sequences;
mod vector_knn;
mod torn_write;
mod multi_cycle;
mod composite_pk;
mod update_pk;
mod alter_defaults;
mod null_updates;
mod alter_preserves_unique;
mod alter_preserves_pk;
mod alter_then_mutate;
mod vector_alter;

pub(super) fn tmp_recovery_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("evolvsql_rec_{}_{}.log", name, std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}
