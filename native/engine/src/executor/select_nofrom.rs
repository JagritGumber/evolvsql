use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};
use crate::types::TypeOid;

use super::expr::eval_expr;
use super::types::{JoinContext, JoinSource};

/// Handle SELECT with no FROM clause, returning raw Values.
pub(crate) fn exec_select_raw_no_from(
    select: &pg_query::protobuf::SelectStmt,
    outer: Option<(&[ArenaValue], &JoinContext)>,
    arena: &mut QueryArena,
) -> Result<(Vec<(String, i32)>, Vec<Vec<ArenaValue>>), String> {
    let (eval_row, eval_ctx): (Vec<ArenaValue>, JoinContext) = if let Some((outer_row, outer_ctx)) = outer {
        let sources: Vec<JoinSource> = outer_ctx.sources.iter()
            .map(|src| JoinSource {
                alias: src.alias.clone(), table_name: src.table_name.clone(),
                schema: src.schema.clone(), table_def: src.table_def.clone(), col_offset: src.col_offset,
            })
            .collect();
        (outer_row.to_vec(), JoinContext { total_columns: outer_ctx.total_columns, sources })
    } else {
        (vec![], JoinContext { sources: vec![], total_columns: 0 })
    };

    let mut columns = Vec::new();
    let mut row = Vec::new();

    for target in &select.target_list {
        if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
            let alias = if rt.name.is_empty() { "?column?".to_string() } else { rt.name.clone() };
            let val = match rt.val.as_ref().and_then(|v| v.node.as_ref()) {
                Some(expr) => eval_expr(expr, &eval_row, &eval_ctx, arena)?,
                None => ArenaValue::Null,
            };
            columns.push((alias, TypeOid::Text.oid()));
            row.push(val);
        }
    }

    Ok((columns, vec![row]))
}
