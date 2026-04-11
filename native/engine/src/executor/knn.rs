use pg_query::NodeEnum;

use crate::storage;

use super::helpers::{eval_const_i64, extract_op_name};
use super::resolve::resolve_column;
use super::types::{JoinContext, KnnPlan};

/// Detect ORDER BY col <->/'<=>'/<#> '[...]' LIMIT K with HNSW index.
pub(crate) fn try_detect_knn(
    select: &pg_query::protobuf::SelectStmt, ctx: &JoinContext, schema: &str, table_name: &str,
) -> Option<KnnPlan> {
    if select.sort_clause.len() != 1 { return None; }
    let limit_node = select.limit_count.as_ref()?;
    let k = eval_const_i64(limit_node.node.as_ref())? as usize;
    if k == 0 { return None; }
    let hnsw_col_idx = storage::has_hnsw_index(schema, table_name)?;
    let sort_node = select.sort_clause[0].node.as_ref()?;
    let sb = if let NodeEnum::SortBy(sb) = sort_node { sb } else { return None; };
    let inner = sb.node.as_ref()?.node.as_ref()?;
    let a_expr = if let NodeEnum::AExpr(expr) = inner { expr } else { return None; };
    let op = extract_op_name(&a_expr.name).ok()?;
    let metric = match op.as_str() {
        "<->" => crate::hnsw::DistanceMetric::L2,
        "<=>" => crate::hnsw::DistanceMetric::Cosine,
        "<#>" => crate::hnsw::DistanceMetric::InnerProduct,
        _ => return None,
    };
    let left = a_expr.lexpr.as_ref()?.node.as_ref()?;
    let right = a_expr.rexpr.as_ref()?.node.as_ref()?;
    let (col_node, vec_node) = if matches!(left, NodeEnum::ColumnRef(_)) { (left, right) }
        else if matches!(right, NodeEnum::ColumnRef(_)) { (right, left) }
        else { return None; };
    if let NodeEnum::ColumnRef(cref) = col_node {
        let col_idx = resolve_column(cref, ctx).ok()?;
        if col_idx != hnsw_col_idx { return None; }
    } else { return None; }
    let query_vector = extract_const_vector(vec_node)?;
    Some(KnnPlan { query_vector, k, metric })
}

/// Extract a constant vector from an AST node.
pub(crate) fn extract_const_vector(node: &NodeEnum) -> Option<Vec<f32>> {
    match node {
        NodeEnum::AConst(ac) => {
            if let Some(pg_query::protobuf::a_const::Val::Sval(s)) = &ac.val {
                let trimmed = s.sval.trim();
                if trimmed.starts_with('[') && trimmed.ends_with(']') {
                    let inner = &trimmed[1..trimmed.len() - 1];
                    return inner.split(',').map(|p| p.trim().parse::<f32>()).collect::<Result<Vec<_>, _>>().ok();
                }
            }
            None
        }
        NodeEnum::TypeCast(tc) => extract_const_vector(tc.arg.as_ref()?.node.as_ref()?),
        _ => None,
    }
}
