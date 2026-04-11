// TODO(#5): Describe Portal sends NoData -- requires parsing SQL to determine
//   column types without execution. Needs a type-inference pass over the AST
//   to return RowDescription for prepared statements. Phase 2 work.
//
// TODO(#8): O(N) uniqueness check on INSERT/UPDATE -- needs hash indexes on
//   unique columns for O(1) constraint validation. Phase 2 work (index engine).

mod types;
mod resolve;
mod helpers;
mod like;
mod expr;
mod expr_cast;
mod expr_ops;
mod expr_ops_ext;
mod expr_logic;
mod sublink;
mod func;
mod func_str;
mod func_math;
mod filter;
mod join;
mod from;
mod knn;
mod sort;
mod select;
mod select_set;
mod select_nofrom;
mod select_fast;
mod select_general;
mod select_post;
mod aggregate;
mod aggregate_compute;
mod having;
mod returning;
mod ddl;
mod ddl_alter;
mod insert;
mod insert_conflict;
mod delete;
mod update;

use std::num::NonZeroUsize;
use std::sync::LazyLock;

use lru::LruCache;
use parking_lot::Mutex;
use pg_query::NodeEnum;

pub use types::QueryResult;
pub(crate) use types::{JoinContext, SortKey};
pub(crate) use resolve::{resolve_column, extract_func_name};
pub(crate) use expr::eval_expr;
pub(crate) use helpers::eval_const_i64;
pub(crate) use sort::compare_rows;

const MAX_PARSE_CACHE: usize = 1024;
static PARSE_CACHE: LazyLock<Mutex<LruCache<String, pg_query::protobuf::ParseResult>>> =
    LazyLock::new(|| Mutex::new(LruCache::new(NonZeroUsize::new(MAX_PARSE_CACHE).unwrap())));

pub fn execute(sql: &str) -> Result<QueryResult, String> {
    let protobuf = {
        let mut cache = PARSE_CACHE.lock();
        if let Some(cached) = cache.get(sql) {
            cached.clone()
        } else {
            let parsed = pg_query::parse(sql).map_err(|e| e.to_string())?;
            let proto = parsed.protobuf;
            cache.put(sql.to_string(), proto.clone());
            proto
        }
    };
    let raw_stmt = protobuf.stmts.first().ok_or("empty query")?;
    let stmt = raw_stmt.stmt.as_ref().ok_or("missing statement")?;
    let node = stmt.node.as_ref().ok_or("missing node")?;
    match node {
        NodeEnum::CreateStmt(create) => ddl::exec_create_table(create),
        NodeEnum::DropStmt(drop) => ddl_alter::exec_drop(drop),
        NodeEnum::InsertStmt(ins) => insert::exec_insert(ins),
        NodeEnum::SelectStmt(sel) => select::exec_select(sel),
        NodeEnum::DeleteStmt(del) => delete::exec_delete(del),
        NodeEnum::UpdateStmt(upd) => update::exec_update(upd),
        NodeEnum::TruncateStmt(trunc) => update::exec_truncate(trunc),
        NodeEnum::VariableSetStmt(_) => Ok(QueryResult { tag: "SET".into(), columns: vec![], rows: vec![] }),
        NodeEnum::VariableShowStmt(_) => Ok(QueryResult { tag: "SHOW".into(), columns: vec![], rows: vec![] }),
        NodeEnum::TransactionStmt(_) => Ok(QueryResult { tag: "OK".into(), columns: vec![], rows: vec![] }),
        NodeEnum::AlterTableStmt(alter) => ddl_alter::exec_alter_table(alter),
        NodeEnum::RenameStmt(rename) => ddl_alter::exec_rename(rename),
        NodeEnum::CreateTableAsStmt(ctas) => ddl_alter::exec_create_table_as(ctas),
        _ => Err("unsupported statement type".into()),
    }
}

#[cfg(test)]
mod tests;
