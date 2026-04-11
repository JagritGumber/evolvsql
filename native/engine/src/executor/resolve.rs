use pg_query::NodeEnum;

use crate::types::TypeOid;
use super::types::{JoinContext, SelectTarget};

/// Extract string fields from a ColumnRef.
pub(crate) fn extract_string_fields(cref: &pg_query::protobuf::ColumnRef) -> Vec<String> {
    cref.fields
        .iter()
        .filter_map(|f| f.node.as_ref())
        .filter_map(|n| {
            if let NodeEnum::String(s) = n {
                Some(s.sval.clone())
            } else {
                None
            }
        })
        .collect()
}

/// Resolve a column reference to an index into the joined row.
pub(crate) fn resolve_column(
    cref: &pg_query::protobuf::ColumnRef,
    ctx: &JoinContext,
) -> Result<usize, String> {
    if ctx.sources.len() == 1 {
        if let Some(last_field) = cref.fields.last().and_then(|f| f.node.as_ref()) {
            if let NodeEnum::String(s) = last_field {
                let src = &ctx.sources[0];
                if let Some(pos) = src.table_def.columns.iter().position(|c| c.name == s.sval) {
                    return Ok(src.col_offset + pos);
                }
                if cref.fields.len() == 2 {
                    return Err(format!("column \"{}\" does not exist", s.sval));
                }
            }
        }
    }
    let fields = extract_string_fields(cref);
    match fields.len() {
        1 => {
            let col_name = &fields[0];
            let mut found = Vec::new();
            for src in &ctx.sources {
                if let Some(pos) = src.table_def.columns.iter().position(|c| c.name == *col_name) {
                    found.push(src.col_offset + pos);
                }
            }
            match found.len() {
                0 => Err(format!("column \"{}\" does not exist", col_name)),
                1 => Ok(found[0]),
                _ => Err(format!("column reference \"{}\" is ambiguous", col_name)),
            }
        }
        2 => {
            let (qualifier, col_name) = (&fields[0], &fields[1]);
            let matches: Vec<_> = ctx.sources.iter().filter(|s| s.alias == *qualifier).collect();
            let src = if matches.len() == 1 {
                matches[0]
            } else if matches.is_empty() {
                let by_name: Vec<_> = ctx.sources.iter().filter(|s| s.table_name == *qualifier).collect();
                match by_name.len() {
                    0 => return Err(format!("missing FROM-clause entry for table \"{}\"", qualifier)),
                    1 => by_name[0],
                    _ => return Err(format!("table reference \"{}\" is ambiguous", qualifier)),
                }
            } else {
                return Err(format!("table reference \"{}\" is ambiguous", qualifier));
            };
            let pos = src.table_def.columns.iter().position(|c| c.name == *col_name)
                .ok_or_else(|| format!("column \"{}.{}\" does not exist", qualifier, col_name))?;
            Ok(src.col_offset + pos)
        }
        _ => Err("unsupported column reference".into()),
    }
}

/// Get the type OID for a column index in a JoinContext.
pub(crate) fn column_type_oid(idx: usize, ctx: &JoinContext) -> Result<i32, String> {
    for src in &ctx.sources {
        let end = src.col_offset + src.table_def.columns.len();
        if idx >= src.col_offset && idx < end {
            return Ok(src.table_def.columns[idx - src.col_offset].type_oid.oid());
        }
    }
    if ctx.sources.is_empty() {
        Ok(TypeOid::Text.oid())
    } else {
        Err(format!(
            "internal error: column index {} not found in any source (total: {})",
            idx, ctx.total_columns
        ))
    }
}

/// Extract function name from FuncCall node.
pub(crate) fn extract_func_name(fc: &pg_query::protobuf::FuncCall) -> String {
    fc.funcname.iter()
        .filter_map(|n| n.node.as_ref())
        .filter_map(|n| if let NodeEnum::String(s) = n { Some(s.sval.to_lowercase()) } else { None })
        .last()
        .unwrap_or_default()
}

/// Extract column name from ColumnRef.
pub(crate) fn extract_col_name(cref: &pg_query::protobuf::ColumnRef) -> String {
    cref.fields.iter()
        .filter_map(|f| f.node.as_ref())
        .filter_map(|n| if let NodeEnum::String(s) = n { Some(s.sval.clone()) } else { None })
        .last()
        .unwrap_or_default()
}

/// Resolve SELECT target list to Column or Expr variants.
pub(crate) fn resolve_targets(
    select: &pg_query::protobuf::SelectStmt, ctx: &JoinContext,
) -> Result<Vec<SelectTarget>, String> {
    let mut targets = Vec::new();
    for target in &select.target_list {
        if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
            let val_node = rt.val.as_ref().and_then(|v| v.node.as_ref());
            match val_node {
                Some(NodeEnum::ColumnRef(cref)) => {
                    let has_star = cref.fields.iter().any(|f| matches!(f.node.as_ref(), Some(NodeEnum::AStar(_))));
                    if has_star {
                        let string_fields = extract_string_fields(cref);
                        if string_fields.is_empty() {
                            for src in &ctx.sources {
                                for (i, col) in src.table_def.columns.iter().enumerate() {
                                    targets.push(SelectTarget::Column { name: col.name.clone(), idx: src.col_offset + i });
                                }
                            }
                        } else {
                            let qualifier = &string_fields[0];
                            let matches: Vec<_> = ctx.sources.iter().filter(|s| s.alias == *qualifier || s.table_name == *qualifier).collect();
                            let src = match matches.len() {
                                0 => return Err(format!("missing FROM-clause entry for table \"{}\"", qualifier)),
                                1 => matches[0],
                                _ => return Err(format!("table reference \"{}\" is ambiguous", qualifier)),
                            };
                            for (i, col) in src.table_def.columns.iter().enumerate() {
                                targets.push(SelectTarget::Column { name: col.name.clone(), idx: src.col_offset + i });
                            }
                        }
                    } else {
                        let idx = resolve_column(cref, ctx)?;
                        let alias = if rt.name.is_empty() { extract_col_name(cref) } else { rt.name.clone() };
                        targets.push(SelectTarget::Column { name: alias, idx });
                    }
                }
                Some(expr) => {
                    let alias = if rt.name.is_empty() { "?column?".to_string() } else { rt.name.clone() };
                    targets.push(SelectTarget::Expr { name: alias, expr: expr.clone() });
                }
                None => { targets.push(SelectTarget::Column { name: "?column?".into(), idx: 0 }); }
            }
        }
    }
    Ok(targets)
}
