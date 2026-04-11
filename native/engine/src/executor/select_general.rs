use crate::arena::{ArenaValue, QueryArena};

use super::filter::eval_where;
use super::from::execute_from_clause;
use super::select_post::exec_select_raw_post_filter;
use super::types::{JoinContext, JoinSource};

/// General path: JOINs, implicit joins, subqueries, correlated.
pub(crate) fn exec_general_select(
    select: &pg_query::protobuf::SelectStmt,
    outer: Option<(&[ArenaValue], &JoinContext)>,
    arena: &mut QueryArena,
) -> Result<(Vec<(String, i32)>, Vec<Vec<ArenaValue>>), String> {
    let (all_rows, inner_ctx) = execute_from_clause(&select.from_clause, arena)?;

    let (eval_rows, merged_ctx, outer_width) = if let Some((outer_row, outer_ctx)) = outer {
        let outer_width = outer_ctx.total_columns;
        let mut sources: Vec<JoinSource> = Vec::new();
        for src in &outer_ctx.sources {
            sources.push(JoinSource {
                alias: src.alias.clone(), table_name: src.table_name.clone(),
                schema: src.schema.clone(), table_def: src.table_def.clone(), col_offset: src.col_offset,
            });
        }
        for src in inner_ctx.sources {
            sources.push(JoinSource {
                alias: src.alias, table_name: src.table_name,
                schema: src.schema, table_def: src.table_def, col_offset: src.col_offset + outer_width,
            });
        }
        let merged = JoinContext { total_columns: outer_width + inner_ctx.total_columns, sources };
        let rows: Vec<Vec<ArenaValue>> = all_rows.into_iter()
            .map(|inner_row| { let mut combined = outer_row.to_vec(); combined.extend(inner_row); combined })
            .collect();
        (rows, merged, outer_width)
    } else {
        (all_rows, inner_ctx, 0)
    };

    let mut rows = Vec::new();
    for row in eval_rows {
        if eval_where(&select.where_clause, &row, &merged_ctx, arena)? { rows.push(row); }
    }

    exec_select_raw_post_filter(select, merged_ctx, rows, outer_width, arena)
}
