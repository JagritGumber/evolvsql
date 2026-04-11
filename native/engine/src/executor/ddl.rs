use pg_query::NodeEnum;

use crate::catalog::{self, Column, Table};
use crate::storage;
use crate::types::TypeOid;

use super::helpers::eval_const;
use super::types::QueryResult;

/// Execute CREATE TABLE.
pub(crate) fn exec_create_table(create: &pg_query::protobuf::CreateStmt) -> Result<QueryResult, String> {
    let rel = create.relation.as_ref().ok_or("CREATE TABLE missing relation")?;
    let table_name = &rel.relname;
    let schema = if rel.schemaname.is_empty() { "public" } else { &rel.schemaname };
    let mut columns = Vec::new();
    let mut created_sequences: Vec<(String, String)> = Vec::new();
    for elt in &create.table_elts {
        let node = elt.node.as_ref().ok_or_else(|| { for (s, n) in &created_sequences { crate::sequence::drop_sequence(s, n); } "missing table element".to_string() })?;
        if let NodeEnum::ColumnDef(col) = node {
            let (column, seqs) = parse_column_def(col, table_name, schema, &created_sequences)?;
            created_sequences.extend(seqs);
            columns.push(column);
        }
    }
    parse_table_constraints(&create.table_elts, &mut columns);
    let table = Table { name: table_name.clone(), schema: schema.to_string(), columns };
    if create.if_not_exists && catalog::get_table(schema, table_name).is_some() {
        return Ok(QueryResult { tag: "CREATE TABLE".into(), columns: vec![], rows: vec![] });
    }
    catalog::create_table(table.clone())?;
    storage::create_table(schema, table_name);
    setup_indexes(&table, schema, table_name)?;
    Ok(QueryResult { tag: "CREATE TABLE".into(), columns: vec![], rows: vec![] })
}

fn parse_column_def(col: &pg_query::protobuf::ColumnDef, table_name: &str, schema: &str, prev_seqs: &[(String, String)]) -> Result<(Column, Vec<(String, String)>), String> {
    let type_name = extract_type_name(col);
    let (mut nullable, mut primary_key, mut unique, mut default_expr) = (!col.is_not_null, false, false, None);
    let mut new_seqs = Vec::new();
    let is_serial = matches!(type_name.to_lowercase().as_str(), "serial" | "bigserial");
    if is_serial {
        let seq_name = format!("{}_{}_seq", table_name, col.colname);
        crate::sequence::create_sequence(schema, &seq_name, 1, 1).map_err(|e| { for (s, n) in prev_seqs { crate::sequence::drop_sequence(s, n); } e })?;
        new_seqs.push((schema.to_string(), seq_name.clone()));
        default_expr = Some(catalog::DefaultExpr::NextVal(format!("{}.{}", schema, seq_name)));
        nullable = false;
    }
    for cnode in &col.constraints {
        if let Some(NodeEnum::Constraint(c)) = cnode.node.as_ref() {
            match c.contype {
                x if x == pg_query::protobuf::ConstrType::ConstrPrimary as i32 => { primary_key = true; nullable = false; unique = true; }
                x if x == pg_query::protobuf::ConstrType::ConstrUnique as i32 => { unique = true; }
                x if x == pg_query::protobuf::ConstrType::ConstrNotnull as i32 => { nullable = false; }
                x if x == pg_query::protobuf::ConstrType::ConstrDefault as i32 => {
                    if let Some(raw) = c.raw_expr.as_ref().and_then(|n| n.node.as_ref()) {
                        let func_node = match raw {
                            NodeEnum::FuncCall(_) => Some(raw),
                            NodeEnum::TypeCast(tc) => tc.arg.as_ref().and_then(|a| a.node.as_ref()).filter(|n| matches!(n, NodeEnum::FuncCall(_))),
                            _ => None,
                        };
                        if let Some(NodeEnum::FuncCall(fc)) = func_node {
                            let func_name: String = fc.funcname.iter().filter_map(|n| n.node.as_ref()).filter_map(|n| if let NodeEnum::String(s) = n { Some(s.sval.as_str()) } else { None }).collect::<Vec<_>>().join(".");
                            for (s, n) in prev_seqs { crate::sequence::drop_sequence(s, n); }
                            return Err(format!("DEFAULT function expressions are not yet supported: {}", func_name));
                        }
                        default_expr = Some(catalog::DefaultExpr::Literal(eval_const(Some(raw))));
                    }
                }
                _ => {}
            }
        }
    }
    Ok((Column { name: col.colname.clone(), type_oid: TypeOid::from_name(&type_name), nullable, primary_key, unique, default_expr }, new_seqs))
}

fn parse_table_constraints(table_elts: &[pg_query::protobuf::Node], columns: &mut [Column]) {
    for elt in table_elts {
        if let Some(NodeEnum::Constraint(c)) = elt.node.as_ref() {
            let key_cols: Vec<String> = c.keys.iter().filter_map(|k| k.node.as_ref()).filter_map(|n| if let NodeEnum::String(s) = n { Some(s.sval.clone()) } else { None }).collect();
            match c.contype {
                x if x == pg_query::protobuf::ConstrType::ConstrPrimary as i32 => { for col in columns.iter_mut() { if key_cols.contains(&col.name) { col.primary_key = true; col.nullable = false; col.unique = true; } } }
                x if x == pg_query::protobuf::ConstrType::ConstrUnique as i32 => { for col in columns.iter_mut() { if key_cols.contains(&col.name) { col.unique = true; } } }
                _ => {}
            }
        }
    }
}

fn setup_indexes(table: &Table, schema: &str, table_name: &str) -> Result<(), String> {
    let pk_cols: Vec<usize> = table.columns.iter().enumerate().filter(|(_, c)| c.primary_key).map(|(i, _)| i).collect();
    for (i, col) in table.columns.iter().enumerate() {
        if col.primary_key && pk_cols.len() == 1 { storage::add_unique_index(schema, table_name, i)?; }
        else if col.unique { storage::add_unique_index(schema, table_name, i)?; }
    }
    if pk_cols.len() > 1 { storage::add_pk_index(schema, table_name, &pk_cols)?; }
    Ok(())
}

/// Extract type name from ColumnDef.
pub(crate) fn extract_type_name(col: &pg_query::protobuf::ColumnDef) -> String {
    col.type_name.as_ref().map(|tn| {
        tn.names.iter().filter_map(|n| n.node.as_ref()).filter_map(|node| if let NodeEnum::String(s) = node { Some(s.sval.clone()) } else { None }).last().unwrap_or_else(|| "text".into())
    }).unwrap_or_else(|| "text".into())
}
