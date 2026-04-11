use super::*;
use crate::catalog;
use crate::storage;

fn setup() {
    catalog::reset();
    storage::reset();
}

fn setup_test_table() {
    setup();
    execute("CREATE TABLE t (id integer, name text)").unwrap();
    execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
    execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
    execute("INSERT INTO t VALUES (3, 'carol')").unwrap();
}

mod crud;
mod where_clause;
mod update;
mod delete;
mod order_limit;
mod expressions;
mod constraints;
mod aggregate;
mod aggregate_ext;
mod join;
mod join_null;
mod join_regression;
mod returning;
mod serial;
mod subquery;
mod subquery_ext;
mod vector;
mod vector_hnsw;
mod vector_hnsw_recall;
mod like;
mod case_when;
mod distinct;
mod ddl;
mod ddl_ext;
mod cast;
mod between;
mod coalesce;
mod string_func;
mod math_func;
mod set_ops;
mod insert_select;
mod cross_table;
mod misc;
mod window;
mod window_value;
mod window_aggregate;
mod window_running;
mod window_partition;
mod cte;
mod cte_ext;
mod upsert;
mod upsert_ext;
