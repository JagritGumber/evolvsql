use std::collections::HashMap;
use std::hash::{DefaultHasher, Hasher};

use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};
use crate::executor::{
    JoinContext, SortKey, compare_rows, eval_const_i64, extract_func_name, resolve_column,
};

pub(crate) struct WindowSpec {
    pub partition_keys: Vec<usize>,
    pub sort_keys: Vec<SortKey>,
}

pub(crate) enum WindowFuncKind {
    RowNumber,
    Rank,
    DenseRank,
    Ntile(i64),
}

pub(crate) struct WindowTarget {
    pub kind: WindowFuncKind,
    pub alias: String,
    pub spec: WindowSpec,
}

/// Parse a pg_query WindowDef into a WindowSpec with resolved column indices.
fn parse_window_spec(
    wd: &pg_query::protobuf::WindowDef,
    ctx: &JoinContext,
) -> Result<WindowSpec, String> {
    let mut partition_keys = Vec::new();
    for node in &wd.partition_clause {
        if let Some(NodeEnum::ColumnRef(cref)) = node.node.as_ref() {
            partition_keys.push(resolve_column(cref, ctx)?);
        } else {
            return Err("PARTITION BY supports column references only".into());
        }
    }

    let mut sort_keys = Vec::new();
    for node in &wd.order_clause {
        if let Some(NodeEnum::SortBy(sb)) = node.node.as_ref() {
            let inner = sb.node.as_ref().and_then(|n| n.node.as_ref());
            let col_idx = match inner {
                Some(NodeEnum::ColumnRef(cref)) => resolve_column(cref, ctx)?,
                _ => return Err("window ORDER BY supports column references only".into()),
            };
            let ascending =
                sb.sortby_dir != pg_query::protobuf::SortByDir::SortbyDesc as i32;
            let nulls_first = match sb.sortby_nulls {
                x if x == pg_query::protobuf::SortByNulls::SortbyNullsFirst as i32 => true,
                x if x == pg_query::protobuf::SortByNulls::SortbyNullsLast as i32 => false,
                _ => !ascending,
            };
            sort_keys.push(SortKey { col_idx, ascending, nulls_first });
        }
    }

    Ok(WindowSpec { partition_keys, sort_keys })
}

/// Extract window function targets from a SELECT target list.
pub(crate) fn extract_window_targets(
    select: &pg_query::protobuf::SelectStmt,
    ctx: &JoinContext,
    arena: &mut QueryArena,
) -> Result<Vec<WindowTarget>, String> {
    let mut targets = Vec::new();
    for node in &select.target_list {
        if let Some(NodeEnum::ResTarget(rt)) = node.node.as_ref() {
            if let Some(NodeEnum::FuncCall(fc)) = rt.val.as_ref().and_then(|v| v.node.as_ref()) {
                if let Some(ref wd) = fc.over {
                    let name = extract_func_name(fc);
                    let spec = parse_window_spec(wd, ctx)?;
                    let alias = if rt.name.is_empty() {
                        name.clone()
                    } else {
                        rt.name.clone()
                    };
                    let kind = match name.as_str() {
                        "row_number" => WindowFuncKind::RowNumber,
                        "rank" => WindowFuncKind::Rank,
                        "dense_rank" => WindowFuncKind::DenseRank,
                        "ntile" => {
                            let n = fc.args.first()
                                .and_then(|a| a.node.as_ref())
                                .and_then(|n| eval_const_i64(Some(n)))
                                .ok_or("NTILE requires an integer argument")?;
                            if n <= 0 {
                                return Err("NTILE argument must be positive".into());
                            }
                            WindowFuncKind::Ntile(n)
                        }
                        _ => {
                            let _ = arena;
                            return Err(format!(
                                "window function \"{}\" is not yet supported", name
                            ));
                        }
                    };
                    targets.push(WindowTarget { kind, alias, spec });
                }
            }
        }
    }
    Ok(targets)
}

/// Group rows into partitions by partition key values.
/// Returns Vec of partitions, each a Vec of original row indices.
fn partition_rows(
    rows: &[Vec<ArenaValue>],
    partition_keys: &[usize],
    arena: &QueryArena,
) -> Vec<Vec<usize>> {
    if partition_keys.is_empty() {
        return vec![(0..rows.len()).collect()];
    }

    let mut groups: Vec<(Vec<ArenaValue>, Vec<usize>)> = Vec::new();
    let mut index: HashMap<u64, Vec<usize>> = HashMap::new();

    for (row_idx, row) in rows.iter().enumerate() {
        let key: Vec<ArenaValue> = partition_keys.iter().map(|&i| row[i]).collect();
        let mut hasher = DefaultHasher::new();
        for v in &key {
            v.hash_with(arena, &mut hasher);
        }
        let h = hasher.finish();

        let mut found = None;
        if let Some(candidates) = index.get(&h) {
            for &ci in candidates {
                let existing = &groups[ci].0;
                if existing.len() == key.len()
                    && existing.iter().zip(key.iter()).all(|(a, b)| a.eq_with(b, arena))
                {
                    found = Some(ci);
                    break;
                }
            }
        }

        if let Some(gi) = found {
            groups[gi].1.push(row_idx);
        } else {
            let gi = groups.len();
            index.entry(h).or_default().push(gi);
            groups.push((key, vec![row_idx]));
        }
    }

    groups.into_iter().map(|(_, indices)| indices).collect()
}

/// Sort row indices within a partition by the window ORDER BY keys.
fn sort_partition(
    indices: &mut [usize],
    sort_keys: &[SortKey],
    rows: &[Vec<ArenaValue>],
    arena: &QueryArena,
) {
    if sort_keys.is_empty() {
        return;
    }
    indices.sort_by(|&a, &b| compare_rows(sort_keys, &rows[a], &rows[b], arena));
}

/// Evaluate all window functions, returning a Vec<Vec<ArenaValue>>
/// where result[row_idx][window_fn_idx] is the computed value.
pub(crate) fn evaluate_window_functions(
    targets: &[WindowTarget],
    rows: &[Vec<ArenaValue>],
    arena: &QueryArena,
) -> Result<Vec<Vec<ArenaValue>>, String> {
    let n = rows.len();
    let m = targets.len();
    let mut results = vec![vec![ArenaValue::Null; m]; n];

    for (wf_idx, wt) in targets.iter().enumerate() {
        let mut partitions = partition_rows(rows, &wt.spec.partition_keys, arena);
        for partition in &mut partitions {
            sort_partition(partition, &wt.spec.sort_keys, rows, arena);
            compute_ranking(&wt.kind, partition, &wt.spec.sort_keys, rows, arena, wf_idx, &mut results);
        }
    }

    Ok(results)
}

/// Compute ranking function values for one partition.
fn compute_ranking(
    kind: &WindowFuncKind,
    partition: &[usize],
    sort_keys: &[SortKey],
    rows: &[Vec<ArenaValue>],
    arena: &QueryArena,
    wf_idx: usize,
    results: &mut [Vec<ArenaValue>],
) {
    let plen = partition.len();
    match kind {
        WindowFuncKind::RowNumber => {
            for (pos, &row_idx) in partition.iter().enumerate() {
                results[row_idx][wf_idx] = ArenaValue::Int((pos + 1) as i64);
            }
        }
        WindowFuncKind::Rank => {
            let mut rank = 1i64;
            for (pos, &row_idx) in partition.iter().enumerate() {
                if pos > 0 {
                    let prev = partition[pos - 1];
                    if compare_rows(sort_keys, &rows[prev], &rows[row_idx], arena)
                        != std::cmp::Ordering::Equal
                    {
                        rank = (pos + 1) as i64;
                    }
                }
                results[row_idx][wf_idx] = ArenaValue::Int(rank);
            }
        }
        WindowFuncKind::DenseRank => {
            let mut rank = 1i64;
            for (pos, &row_idx) in partition.iter().enumerate() {
                if pos > 0 {
                    let prev = partition[pos - 1];
                    if compare_rows(sort_keys, &rows[prev], &rows[row_idx], arena)
                        != std::cmp::Ordering::Equal
                    {
                        rank += 1;
                    }
                }
                results[row_idx][wf_idx] = ArenaValue::Int(rank);
            }
        }
        WindowFuncKind::Ntile(n) => {
            let n = *n as usize;
            let base_size = plen / n;
            let extra = plen % n;
            let mut bucket = 1usize;
            let mut count = 0usize;
            let bucket_size = if extra > 0 { base_size + 1 } else { base_size };
            let mut threshold = bucket_size;
            for (pos, &row_idx) in partition.iter().enumerate() {
                if pos >= threshold && bucket < n {
                    bucket += 1;
                    let bs = if bucket <= extra { base_size + 1 } else { base_size };
                    threshold += bs;
                }
                results[row_idx][wf_idx] = ArenaValue::Int(bucket as i64);
                count = pos + 1;
            }
            let _ = count;
        }
    }
}
