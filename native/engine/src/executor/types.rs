use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};
use crate::catalog::Table;
use crate::types::Value;

/// Query execution result returned to callers.
#[derive(Debug, serde::Serialize)]
pub struct QueryResult {
    pub tag: String,
    pub columns: Vec<(String, i32)>,
    pub rows: Vec<Vec<Option<String>>>,
}

/// A table source in a multi-table join context.
pub(crate) struct JoinSource {
    pub(crate) alias: String,
    pub(crate) table_name: String,
    #[allow(dead_code)]
    pub(crate) schema: String,
    pub(crate) table_def: Table,
    pub(crate) col_offset: usize,
}

/// Context for column resolution across potentially multiple tables.
pub(crate) struct JoinContext {
    pub(crate) sources: Vec<JoinSource>,
    pub(crate) total_columns: usize,
}

impl JoinContext {
    /// Build a single-table context (backward compat for DELETE/UPDATE).
    pub(crate) fn single(schema: &str, table_name: &str, table_def: Table) -> Self {
        let ncols = table_def.columns.len();
        JoinContext {
            sources: vec![JoinSource {
                alias: table_name.to_string(),
                table_name: table_name.to_string(),
                schema: schema.to_string(),
                table_def,
                col_offset: 0,
            }],
            total_columns: ncols,
        }
    }
}

/// ORDER BY key specification.
pub(crate) struct SortKey {
    pub(crate) col_idx: usize,
    pub(crate) ascending: bool,
    pub(crate) nulls_first: bool,
}

/// Target column/expression in SELECT.
pub(crate) enum SelectTarget {
    Column { name: String, idx: usize },
    Expr { name: String, expr: NodeEnum },
}

/// Targets for RETURNING clause evaluation.
pub(crate) enum ReturningTarget {
    Column(usize),
    Expr(NodeEnum),
}

/// Bypass eval_expr for `column = constant` WHERE patterns.
pub(crate) struct FastEqualityFilter {
    pub(crate) col_idx: usize,
    pub(crate) value: ArenaValue,
}

impl FastEqualityFilter {
    #[inline(always)]
    pub(crate) fn matches(&self, row: &[ArenaValue], arena: &QueryArena) -> bool {
        if self.col_idx >= row.len() { return false; }
        let v = &row[self.col_idx];
        v.eq_with(&self.value, arena) || v.compare(&self.value, arena) == Some(std::cmp::Ordering::Equal)
    }

    #[inline(always)]
    pub(crate) fn matches_value(&self, row: &[Value], arena: &QueryArena) -> bool {
        if self.col_idx >= row.len() { return false; }
        let v = &row[self.col_idx];
        match (v, &self.value) {
            (Value::Int(a), ArenaValue::Int(b)) => *a == *b,
            (Value::Float(a), ArenaValue::Float(b)) => *a == *b,
            (Value::Bool(a), ArenaValue::Bool(b)) => *a == *b,
            (Value::Int(a), ArenaValue::Float(b)) => (*a as f64) == *b,
            (Value::Float(a), ArenaValue::Int(b)) => *a == (*b as f64),
            (Value::Null, _) | (_, ArenaValue::Null) => false,
            (Value::Text(a), ArenaValue::Text(b)) => a.as_ref() == arena.get_str(*b),
            (Value::Vector(a), ArenaValue::Vector(b)) => a.as_slice() == arena.get_vec(*b),
            (Value::Bytea(a), ArenaValue::Bytea(b)) => {
                let bs = b.offset as usize;
                a.as_slice() == &arena.bytes_ref()[bs..bs + b.len as usize]
            }
            _ => false,
        }
    }
}

/// Detected KNN query plan for HNSW acceleration.
pub(crate) struct KnnPlan {
    pub(crate) query_vector: Vec<f32>,
    pub(crate) k: usize,
    pub(crate) metric: crate::hnsw::DistanceMetric,
}
