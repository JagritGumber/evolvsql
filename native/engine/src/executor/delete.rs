use crate::arena::QueryArena;
use crate::catalog;
use crate::storage;

use super::filter::eval_where_value;
use super::returning::eval_returning;
use super::types::{JoinContext, QueryResult};

/// Execute DELETE with optional WHERE and RETURNING.
pub(crate) fn exec_delete(delete: &pg_query::protobuf::DeleteStmt) -> Result<QueryResult, String> {
    let rel = delete.relation.as_ref().ok_or("DELETE missing relation")?;
    let table_name = &rel.relname;
    let schema = if rel.schemaname.is_empty() { "public" } else { &rel.schemaname };
    let table_def = catalog::get_table(schema, table_name)
        .ok_or_else(|| format!("relation \"{}\" does not exist", table_name))?;
    let has_returning = !delete.returning_list.is_empty();

    let (count, deleted_rows) = if delete.where_clause.is_some() {
        let ctx = JoinContext::single(schema, table_name, table_def.clone());
        let mut validate_arena = QueryArena::new();
        storage::scan_with(schema, table_name, |all_rows| {
            for row in all_rows { eval_where_value(&delete.where_clause, row, &ctx, &mut validate_arena)?; }
            Ok(())
        })?;
        if has_returning {
            let wc = delete.where_clause.clone();
            let mut del_arena = QueryArena::new();
            let rows = storage::delete_where_returning(schema, table_name, |row| {
                eval_where_value(&wc, row, &ctx, &mut del_arena).unwrap_or(false)
            })?;
            let n = rows.len() as u64;
            (n, rows)
        } else {
            let wc = delete.where_clause.clone();
            let mut del_arena = QueryArena::new();
            let n = storage::delete_where(schema, table_name, |row| {
                eval_where_value(&wc, row, &ctx, &mut del_arena).unwrap_or(false)
            })?;
            (n, vec![])
        }
    } else {
        if has_returning {
            let rows = storage::delete_all_returning(schema, table_name)?;
            (rows.len() as u64, rows)
        } else {
            (storage::delete_all(schema, table_name)?, vec![])
        }
    };

    let tag = format!("DELETE {}", count);
    if has_returning { eval_returning(&delete.returning_list, &deleted_rows, &table_def, schema, table_name, &tag) }
    else { Ok(QueryResult { tag, columns: vec![], rows: vec![] }) }
}
