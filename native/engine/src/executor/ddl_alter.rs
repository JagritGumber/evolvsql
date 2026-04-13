use pg_query::NodeEnum;

use crate::arena::QueryArena;
use crate::catalog::{self, Column};
use crate::storage;
use crate::types::TypeOid;

use super::ddl::extract_type_name;
use super::helpers::eval_const;
use super::select::exec_select_raw;
use super::types::QueryResult;

/// Execute DROP TABLE.
pub(crate) fn exec_drop(drop: &pg_query::protobuf::DropStmt) -> Result<QueryResult, String> {
    for obj in &drop.objects {
        if let Some(NodeEnum::List(list)) = obj.node.as_ref() {
            let parts: Vec<String> = list.items.iter().filter_map(|i| i.node.as_ref())
                .filter_map(|n| if let NodeEnum::String(s) = n { Some(s.sval.clone()) } else { None }).collect();
            let (schema, name) = if parts.len() >= 2 { (parts[0].as_str(), parts[1].as_str()) }
                else if parts.len() == 1 { ("public", parts[0].as_str()) } else { continue; };
            if drop.missing_ok && catalog::get_table(schema, name).is_none() { continue; }
            catalog::drop_table(schema, name)?;
            storage::drop_table(schema, name);
            // WAL: log drop so recovery doesn't replay the table creation
            crate::wal::manager::append_drop_table(schema, name)?;
        }
    }
    Ok(QueryResult { tag: "DROP TABLE".into(), columns: vec![], rows: vec![] })
}

/// Execute ALTER TABLE (ADD/DROP COLUMN).
pub(crate) fn exec_alter_table(alter: &pg_query::protobuf::AlterTableStmt) -> Result<QueryResult, String> {
    let rel = alter.relation.as_ref().ok_or("ALTER TABLE missing relation")?;
    let table_name = &rel.relname;
    let schema = if rel.schemaname.is_empty() { "public" } else { &rel.schemaname };
    for cmd_node in &alter.cmds {
        let cmd = match cmd_node.node.as_ref() { Some(NodeEnum::AlterTableCmd(c)) => c, _ => continue };
        use pg_query::protobuf::AlterTableType;
        if cmd.subtype == AlterTableType::AtAddColumn as i32 {
            let col_def = match cmd.def.as_ref().and_then(|n| n.node.as_ref()) {
                Some(NodeEnum::ColumnDef(cd)) => cd,
                _ => return Err("ALTER TABLE ADD COLUMN missing column definition".into()),
            };
            let type_name = extract_type_name(col_def);
            let mut default_expr = None;
            for cnode in &col_def.constraints {
                if let Some(NodeEnum::Constraint(c)) = cnode.node.as_ref() {
                    if c.contype == pg_query::protobuf::ConstrType::ConstrDefault as i32 {
                        if let Some(raw) = c.raw_expr.as_ref().and_then(|n| n.node.as_ref()) {
                            default_expr = Some(catalog::DefaultExpr::Literal(eval_const(Some(raw))));
                        }
                    }
                }
            }
            let col = Column { name: col_def.colname.clone(), type_oid: TypeOid::from_name(&type_name), nullable: !col_def.is_not_null, primary_key: false, unique: false, default_expr: default_expr.clone() };
            catalog::alter_table_add_column(schema, table_name, col)?;
            let default_val = match &default_expr { Some(catalog::DefaultExpr::Literal(v)) => v.clone(), _ => crate::types::Value::Null };
            storage::alter_add_column(schema, table_name, default_val);
        } else if cmd.subtype == AlterTableType::AtDropColumn as i32 {
            let col_idx = catalog::get_column_index(schema, table_name, &cmd.name)?;
            catalog::alter_table_drop_column(schema, table_name, &cmd.name)?;
            storage::alter_drop_column(schema, table_name, col_idx);
        } else { return Err(format!("unsupported ALTER TABLE subcommand: {}", cmd.subtype)); }
    }
    Ok(QueryResult { tag: "ALTER TABLE".into(), columns: vec![], rows: vec![] })
}

/// Execute ALTER TABLE RENAME.
pub(crate) fn exec_rename(rename: &pg_query::protobuf::RenameStmt) -> Result<QueryResult, String> {
    let rel = rename.relation.as_ref().ok_or("RENAME missing relation")?;
    let table_name = &rel.relname;
    let schema = if rel.schemaname.is_empty() { "public" } else { &rel.schemaname };
    use pg_query::protobuf::ObjectType;
    if rename.rename_type == ObjectType::ObjectTable as i32 {
        catalog::rename_table(schema, table_name, &rename.newname)?;
        storage::rename_table(schema, table_name, &rename.newname);
    } else if rename.rename_type == ObjectType::ObjectColumn as i32 {
        catalog::rename_column(schema, table_name, &rename.subname, &rename.newname)?;
    } else { return Err("unsupported RENAME type".into()); }
    Ok(QueryResult { tag: "ALTER TABLE".into(), columns: vec![], rows: vec![] })
}

/// Execute CREATE TABLE AS SELECT.
pub(crate) fn exec_create_table_as(ctas: &pg_query::protobuf::CreateTableAsStmt) -> Result<QueryResult, String> {
    let into = ctas.into.as_ref().ok_or("CREATE TABLE AS missing INTO")?;
    let rel = into.rel.as_ref().ok_or("CREATE TABLE AS missing relation")?;
    let table_name = &rel.relname;
    let schema = if rel.schemaname.is_empty() { "public" } else { &rel.schemaname };
    let select_node = ctas.query.as_ref().and_then(|n| n.node.as_ref()).ok_or("CREATE TABLE AS missing query")?;
    let NodeEnum::SelectStmt(sel) = select_node else { return Err("CREATE TABLE AS requires SELECT".into()); };
    let mut arena = QueryArena::new();
    let (columns, raw_rows) = exec_select_raw(sel, None, &mut arena)?;
    let table_columns: Vec<Column> = columns.iter().map(|(name, oid)| Column { name: name.clone(), type_oid: TypeOid::from_oid(*oid), nullable: true, primary_key: false, unique: false, default_expr: None }).collect();
    let table = crate::catalog::Table { name: table_name.clone(), schema: schema.to_string(), columns: table_columns };
    catalog::create_table(table)?;
    storage::create_table(schema, table_name);
    let rows: Vec<Vec<crate::types::Value>> = raw_rows.iter().map(|row| row.iter().map(|v| v.to_value(&arena)).collect()).collect();
    if !rows.is_empty() { storage::insert_batch(schema, table_name, rows); }
    Ok(QueryResult { tag: format!("SELECT {}", raw_rows.len()), columns: vec![], rows: vec![] })
}
