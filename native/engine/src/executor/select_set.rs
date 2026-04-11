use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};

use super::filter::dedup_distinct;
use super::helpers::eval_const_i64;
use super::select::exec_select_raw;
use super::sort::compare_rows;
use super::types::{JoinContext, SortKey};

/// Execute UNION / INTERSECT / EXCEPT set operations.
pub(crate) fn exec_set_operation(
    select: &pg_query::protobuf::SelectStmt,
    outer: Option<(&[ArenaValue], &JoinContext)>,
    arena: &mut QueryArena,
    set_op: i32,
) -> Result<(Vec<(String, i32)>, Vec<Vec<ArenaValue>>), String> {
    let larg = select.larg.as_ref().ok_or("set operation missing left SELECT")?;
    let rarg = select.rarg.as_ref().ok_or("set operation missing right SELECT")?;
    let (lcols, lrows) = exec_select_raw(larg, outer, arena)?;
    let (_, rrows) = exec_select_raw(rarg, outer, arena)?;

    let mut result_rows = if set_op == pg_query::protobuf::SetOperation::SetopUnion as i32 {
        let mut combined = lrows;
        combined.extend(rrows);
        if !select.all {
            combined = dedup_distinct(&[pg_query::protobuf::Node { node: None }], combined, arena);
        }
        combined
    } else if set_op == pg_query::protobuf::SetOperation::SetopIntersect as i32 {
        lrows.into_iter().filter(|lrow| {
            rrows.iter().any(|rrow| lrow.len() == rrow.len() && lrow.iter().zip(rrow.iter()).all(|(a, b)| a.eq_with(b, arena)))
        }).collect()
    } else {
        lrows.into_iter().filter(|lrow| {
            !rrows.iter().any(|rrow| lrow.len() == rrow.len() && lrow.iter().zip(rrow.iter()).all(|(a, b)| a.eq_with(b, arena)))
        }).collect()
    };

    if !select.sort_clause.is_empty() || select.limit_count.is_some() || select.limit_offset.is_some() {
        if !select.sort_clause.is_empty() {
            let mut sort_keys = Vec::new();
            for sort_node in &select.sort_clause {
                if let Some(NodeEnum::SortBy(sb)) = sort_node.node.as_ref() {
                    let ascending = sb.sortby_dir != pg_query::protobuf::SortByDir::SortbyDesc as i32;
                    let nulls_first = sb.sortby_nulls == pg_query::protobuf::SortByNulls::SortbyNullsFirst as i32;
                    if let Some(ref node) = sb.node {
                        if let Some(NodeEnum::ColumnRef(cref)) = node.node.as_ref() {
                            let col_name = cref.fields.iter()
                                .filter_map(|f| f.node.as_ref())
                                .filter_map(|n| if let NodeEnum::String(s) = n { Some(s.sval.as_str()) } else { None })
                                .last().unwrap_or("");
                            let idx = lcols.iter().position(|(n, _)| n == col_name)
                                .ok_or_else(|| format!("column \"{}\" does not exist in UNION result", col_name))?;
                            sort_keys.push(SortKey { col_idx: idx, ascending, nulls_first });
                        } else if let Some(NodeEnum::AConst(ac)) = node.node.as_ref() {
                            if let Some(pg_query::protobuf::a_const::Val::Ival(iv)) = &ac.val {
                                sort_keys.push(SortKey { col_idx: (iv.ival as usize) - 1, ascending, nulls_first });
                            }
                        }
                    }
                }
            }
            result_rows.sort_by(|a, b| compare_rows(&sort_keys, a, b, arena));
        }
        if let Some(ref offset_node) = select.limit_offset {
            if let Some(n) = eval_const_i64(offset_node.node.as_ref()) {
                let n = n.max(0) as usize;
                if n >= result_rows.len() { result_rows.clear(); } else { result_rows.drain(0..n); }
            }
        }
        if let Some(ref limit_node) = select.limit_count {
            if let Some(n) = eval_const_i64(limit_node.node.as_ref()) { result_rows.truncate(n.max(0) as usize); }
        }
    }
    Ok((lcols, result_rows))
}
