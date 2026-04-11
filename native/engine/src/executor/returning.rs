use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};
use crate::catalog::Table;
use crate::types::Value;

use super::expr::eval_expr;
use super::resolve::{resolve_column, extract_string_fields, column_type_oid};
use super::types::{JoinContext, QueryResult, ReturningTarget};

/// Evaluate RETURNING clause expressions for affected rows.
pub(crate) fn eval_returning(
    returning_list: &[pg_query::protobuf::Node], affected_rows: &[Vec<Value>],
    table: &Table, schema: &str, table_name: &str, tag: &str,
) -> Result<QueryResult, String> {
    if returning_list.is_empty() {
        return Ok(QueryResult { tag: tag.into(), columns: vec![], rows: vec![] });
    }
    let ctx = JoinContext::single(schema, table_name, table.clone());
    let mut columns = Vec::new();
    let mut col_exprs: Vec<ReturningTarget> = Vec::new();
    for node in returning_list {
        if let Some(NodeEnum::ResTarget(rt)) = node.node.as_ref() {
            let val_node = rt.val.as_ref().and_then(|v| v.node.as_ref());
            match val_node {
                Some(NodeEnum::ColumnRef(cref)) => {
                    let has_star = cref.fields.iter().any(|f| matches!(f.node.as_ref(), Some(NodeEnum::AStar(_))));
                    if has_star {
                        for (i, col) in table.columns.iter().enumerate() {
                            columns.push((col.name.clone(), col.type_oid.oid()));
                            col_exprs.push(ReturningTarget::Column(i));
                        }
                    } else {
                        let fields = extract_string_fields(cref);
                        let idx = resolve_column(cref, &ctx)?;
                        let alias = if rt.name.is_empty() { fields.last().cloned().unwrap_or("?column?".into()) } else { rt.name.clone() };
                        columns.push((alias, column_type_oid(idx, &ctx)?));
                        col_exprs.push(ReturningTarget::Column(idx));
                    }
                }
                Some(expr) => {
                    let alias = if rt.name.is_empty() { "?column?".into() } else { rt.name.clone() };
                    columns.push((alias, crate::types::TypeOid::Text.oid()));
                    col_exprs.push(ReturningTarget::Expr(expr.clone()));
                }
                None => return Err("RETURNING clause contains an invalid expression".into()),
            }
        }
    }
    let mut rows = Vec::new();
    let mut ret_arena = QueryArena::new();
    for row in affected_rows {
        let mut result_row = Vec::new();
        for target in &col_exprs {
            let val = match target {
                ReturningTarget::Column(idx) => {
                    if *idx < row.len() { row[*idx].clone() }
                    else { return Err(format!("internal error: RETURNING column index {} out of range for row of width {}", idx, row.len())); }
                }
                ReturningTarget::Expr(expr) => {
                    let arena_row: Vec<ArenaValue> = row.iter().map(|v| ArenaValue::from_value(v, &mut ret_arena)).collect();
                    eval_expr(expr, &arena_row, &ctx, &mut ret_arena)?.to_value(&ret_arena)
                }
            };
            result_row.push(val.to_text());
        }
        rows.push(result_row);
    }
    Ok(QueryResult { tag: tag.into(), columns, rows })
}
