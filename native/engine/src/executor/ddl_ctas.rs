//! CREATE TABLE AS SELECT. Builds a table from the result columns of
//! a SELECT and materializes rows via the checked insert path so the
//! whole operation is durable.

use pg_query::NodeEnum;

use crate::arena::QueryArena;
use crate::catalog::{self, Column};
use crate::storage;
use crate::types::TypeOid;

use super::select::exec_select_raw;
use super::types::QueryResult;

pub(crate) fn exec_create_table_as(ctas: &pg_query::protobuf::CreateTableAsStmt) -> Result<QueryResult, String> {
    let into = ctas.into.as_ref().ok_or("CREATE TABLE AS missing INTO")?;
    let rel = into.rel.as_ref().ok_or("CREATE TABLE AS missing relation")?;
    let table_name = &rel.relname;
    let schema = if rel.schemaname.is_empty() { "public" } else { &rel.schemaname };
    let select_node = ctas.query.as_ref().and_then(|n| n.node.as_ref()).ok_or("CREATE TABLE AS missing query")?;
    let NodeEnum::SelectStmt(sel) = select_node else { return Err("CREATE TABLE AS requires SELECT".into()); };
    let mut arena = QueryArena::new();
    let (columns, raw_rows) = exec_select_raw(sel, None, &mut arena)?;
    let table_columns: Vec<Column> = columns.iter().map(|(name, oid)| Column {
        name: name.clone(),
        type_oid: TypeOid::from_oid(*oid),
        nullable: true,
        primary_key: false,
        unique: false,
        default_expr: None,
    }).collect();
    let table = catalog::Table { name: table_name.clone(), schema: schema.to_string(), columns: table_columns };
    // WAL-first: the INSERTs below already log themselves via the
    // checked batch path, so just the CREATE TABLE step needs reordering.
    crate::wal::manager::append_create_table(&table)?;
    catalog::create_table(table.clone())?;
    storage::create_table(schema, table_name);
    let rows: Vec<Vec<crate::types::Value>> = raw_rows.iter()
        .map(|row| row.iter().map(|v| v.to_value(&arena)).collect())
        .collect();
    if !rows.is_empty() {
        storage::insert_batch_checked(schema, table_name, rows, &[], &[])?;
    }
    Ok(QueryResult { tag: format!("SELECT {}", raw_rows.len()), columns: vec![], rows: vec![] })
}
