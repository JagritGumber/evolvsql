use std::collections::HashMap;
use std::hash::{DefaultHasher, Hasher};

use pg_query::NodeEnum;

use crate::arena::{ArenaValue, QueryArena};
use crate::executor::{
    JoinContext, SortKey, compare_rows, eval_const_i64, eval_expr, extract_func_name,
    resolve_column,
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
    Lag { expr: NodeEnum, offset: i64, default: Option<NodeEnum> },
    Lead { expr: NodeEnum, offset: i64, default: Option<NodeEnum> },
    FirstValue { expr: NodeEnum },
    LastValue { expr: NodeEnum },
    NthValue { expr: NodeEnum, n: i64 },
    AggregateOver { name: String, expr: Option<NodeEnum>, agg_star: bool },
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
                        "lag" | "lead" => {
                            let expr_node = fc.args.first()
                                .and_then(|a| a.node.as_ref())
                                .ok_or("LAG/LEAD requires an expression argument")?
                                .clone();
                            let offset = fc.args.get(1)
                                .and_then(|a| a.node.as_ref())
                                .and_then(|n| eval_const_i64(Some(n)))
                                .unwrap_or(1);
                            let default = fc.args.get(2)
                                .and_then(|a| a.node.as_ref())
                                .cloned();
                            if name == "lag" {
                                WindowFuncKind::Lag { expr: expr_node, offset, default }
                            } else {
                                WindowFuncKind::Lead { expr: expr_node, offset, default }
                            }
                        }
                        "first_value" => {
                            let expr_node = fc.args.first()
                                .and_then(|a| a.node.as_ref())
                                .ok_or("FIRST_VALUE requires an expression argument")?
                                .clone();
                            WindowFuncKind::FirstValue { expr: expr_node }
                        }
                        "last_value" => {
                            let expr_node = fc.args.first()
                                .and_then(|a| a.node.as_ref())
                                .ok_or("LAST_VALUE requires an expression argument")?
                                .clone();
                            WindowFuncKind::LastValue { expr: expr_node }
                        }
                        "nth_value" => {
                            let expr_node = fc.args.first()
                                .and_then(|a| a.node.as_ref())
                                .ok_or("NTH_VALUE requires an expression argument")?
                                .clone();
                            let n = fc.args.get(1)
                                .and_then(|a| a.node.as_ref())
                                .and_then(|n| eval_const_i64(Some(n)))
                                .ok_or("NTH_VALUE requires an integer second argument")?;
                            if n <= 0 {
                                return Err("NTH_VALUE argument must be positive".into());
                            }
                            WindowFuncKind::NthValue { expr: expr_node, n }
                        }
                        "count" | "sum" | "avg" | "min" | "max" => {
                            let expr_node = fc.args.first()
                                .and_then(|a| a.node.as_ref())
                                .cloned();
                            WindowFuncKind::AggregateOver {
                                name: name.clone(),
                                expr: expr_node,
                                agg_star: fc.agg_star,
                            }
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
    ctx: &JoinContext,
    arena: &mut QueryArena,
) -> Result<Vec<Vec<ArenaValue>>, String> {
    let n = rows.len();
    let m = targets.len();
    let mut results = vec![vec![ArenaValue::Null; m]; n];

    for (wf_idx, wt) in targets.iter().enumerate() {
        let mut partitions = partition_rows(rows, &wt.spec.partition_keys, arena);
        for partition in &mut partitions {
            sort_partition(partition, &wt.spec.sort_keys, rows, arena);
            match &wt.kind {
                WindowFuncKind::RowNumber
                | WindowFuncKind::Rank
                | WindowFuncKind::DenseRank
                | WindowFuncKind::Ntile(_) => {
                    compute_ranking(&wt.kind, partition, &wt.spec.sort_keys, rows, arena, wf_idx, &mut results);
                }
                WindowFuncKind::AggregateOver { .. } => {
                    compute_aggregate_window(&wt.kind, partition, &wt.spec.sort_keys, rows, ctx, arena, wf_idx, &mut results)?;
                }
                _ => {
                    compute_value(&wt.kind, partition, &wt.spec.sort_keys, rows, ctx, arena, wf_idx, &mut results)?;
                }
            }
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
            let bucket_size = if extra > 0 { base_size + 1 } else { base_size };
            let mut threshold = bucket_size;
            for (pos, &row_idx) in partition.iter().enumerate() {
                if pos >= threshold && bucket < n {
                    bucket += 1;
                    let bs = if bucket <= extra { base_size + 1 } else { base_size };
                    threshold += bs;
                }
                results[row_idx][wf_idx] = ArenaValue::Int(bucket as i64);
            }
        }
        _ => {}
    }
}

/// Find the last peer position for RANGE frame semantics.
/// Peers are rows with equal ORDER BY values.
fn last_peer_pos(
    pos: usize,
    partition: &[usize],
    sort_keys: &[SortKey],
    rows: &[Vec<ArenaValue>],
    arena: &QueryArena,
) -> usize {
    if sort_keys.is_empty() {
        return partition.len() - 1;
    }
    let mut end = pos;
    while end + 1 < partition.len() {
        if compare_rows(sort_keys, &rows[partition[pos]], &rows[partition[end + 1]], arena)
            != std::cmp::Ordering::Equal
        {
            break;
        }
        end += 1;
    }
    end
}

/// Compute value function results (LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE)
/// for one partition.
fn compute_value(
    kind: &WindowFuncKind,
    partition: &[usize],
    sort_keys: &[SortKey],
    rows: &[Vec<ArenaValue>],
    ctx: &JoinContext,
    arena: &mut QueryArena,
    wf_idx: usize,
    results: &mut [Vec<ArenaValue>],
) -> Result<(), String> {
    match kind {
        WindowFuncKind::Lag { expr, offset, default } => {
            for (pos, &row_idx) in partition.iter().enumerate() {
                let target_pos = pos as i64 - offset;
                let val = if target_pos >= 0 && (target_pos as usize) < partition.len() {
                    let target_row = partition[target_pos as usize];
                    eval_expr(expr, &rows[target_row], ctx, arena)?
                } else if let Some(def_expr) = default {
                    eval_expr(def_expr, &rows[row_idx], ctx, arena)?
                } else {
                    ArenaValue::Null
                };
                results[row_idx][wf_idx] = val;
            }
        }
        WindowFuncKind::Lead { expr, offset, default } => {
            for (pos, &row_idx) in partition.iter().enumerate() {
                let target_pos = pos as i64 + offset;
                let val = if target_pos >= 0 && (target_pos as usize) < partition.len() {
                    let target_row = partition[target_pos as usize];
                    eval_expr(expr, &rows[target_row], ctx, arena)?
                } else if let Some(def_expr) = default {
                    eval_expr(def_expr, &rows[row_idx], ctx, arena)?
                } else {
                    ArenaValue::Null
                };
                results[row_idx][wf_idx] = val;
            }
        }
        WindowFuncKind::FirstValue { expr } => {
            if partition.is_empty() {
                return Ok(());
            }
            // Default frame: UNBOUNDED PRECEDING to CURRENT ROW
            // FIRST_VALUE always returns the first row in the frame
            let first_row = partition[0];
            let val = eval_expr(expr, &rows[first_row], ctx, arena)?;
            for &row_idx in partition {
                results[row_idx][wf_idx] = val;
            }
        }
        WindowFuncKind::LastValue { expr } => {
            // Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            // In RANGE mode, CURRENT ROW means the last peer row
            for (pos, &row_idx) in partition.iter().enumerate() {
                let peer_end = last_peer_pos(pos, partition, sort_keys, rows, arena);
                let target_row = partition[peer_end];
                let val = eval_expr(expr, &rows[target_row], ctx, arena)?;
                results[row_idx][wf_idx] = val;
            }
        }
        WindowFuncKind::NthValue { expr, n } => {
            // Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            // In RANGE mode, frame end is the last peer row
            let target_pos = (*n - 1) as usize; // 1-indexed to 0-indexed
            for (pos, &row_idx) in partition.iter().enumerate() {
                let peer_end = last_peer_pos(pos, partition, sort_keys, rows, arena);
                let val = if target_pos <= peer_end {
                    let target_row = partition[target_pos];
                    eval_expr(expr, &rows[target_row], ctx, arena)?
                } else {
                    ArenaValue::Null
                };
                results[row_idx][wf_idx] = val;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Compute aggregate window function (SUM/COUNT/AVG/MIN/MAX OVER).
/// Uses default RANGE frame: UNBOUNDED PRECEDING to CURRENT ROW with peer groups.
fn compute_aggregate_window(
    kind: &WindowFuncKind,
    partition: &[usize],
    sort_keys: &[SortKey],
    rows: &[Vec<ArenaValue>],
    ctx: &JoinContext,
    arena: &mut QueryArena,
    wf_idx: usize,
    results: &mut [Vec<ArenaValue>],
) -> Result<(), String> {
    let (name, expr, agg_star) = match kind {
        WindowFuncKind::AggregateOver { name, expr, agg_star } => (name.as_str(), expr, *agg_star),
        _ => return Ok(()),
    };

    // Evaluate expressions for all rows in partition order
    let vals: Vec<ArenaValue> = if agg_star {
        vec![ArenaValue::Int(1); partition.len()]
    } else if let Some(expr_node) = expr {
        let mut v = Vec::with_capacity(partition.len());
        for &row_idx in partition.iter() {
            v.push(eval_expr(expr_node, &rows[row_idx], ctx, arena)?);
        }
        v
    } else {
        return Err(format!("{} requires an argument", name.to_uppercase()));
    };

    // Compute running aggregate with RANGE peer group handling.
    // All peers share the same accumulated value (inclusive of all peers).
    let mut pos = 0usize;
    while pos < partition.len() {
        let peer_end = last_peer_pos(pos, partition, sort_keys, rows, arena);
        // Frame: all values from 0..=peer_end
        let frame_vals = &vals[0..=peer_end];
        let agg_val = compute_frame_aggregate(name, frame_vals, arena)?;
        for i in pos..=peer_end {
            results[partition[i]][wf_idx] = agg_val;
        }
        pos = peer_end + 1;
    }

    Ok(())
}

/// Compute an aggregate over a frame slice of pre-evaluated values.
fn compute_frame_aggregate(
    name: &str,
    vals: &[ArenaValue],
    arena: &QueryArena,
) -> Result<ArenaValue, String> {
    match name {
        "count" => {
            let count = vals.iter().filter(|v| !v.is_null()).count();
            Ok(ArenaValue::Int(count as i64))
        }
        "sum" => {
            let mut si: i64 = 0;
            let mut sf: f64 = 0.0;
            let mut is_float = false;
            let mut any = false;
            for v in vals {
                match v {
                    ArenaValue::Int(n) => { si = si.wrapping_add(*n); any = true; }
                    ArenaValue::Float(f) => { sf += f; is_float = true; any = true; }
                    ArenaValue::Null => {}
                    _ => return Err("SUM requires numeric input".into()),
                }
            }
            if !any { return Ok(ArenaValue::Null); }
            Ok(if is_float { ArenaValue::Float(sf + si as f64) } else { ArenaValue::Int(si) })
        }
        "avg" => {
            let mut sum: f64 = 0.0;
            let mut count: i64 = 0;
            for v in vals {
                match v {
                    ArenaValue::Int(n) => { sum += *n as f64; count += 1; }
                    ArenaValue::Float(f) => { sum += f; count += 1; }
                    ArenaValue::Null => {}
                    _ => return Err("AVG requires numeric input".into()),
                }
            }
            if count == 0 { Ok(ArenaValue::Null) } else { Ok(ArenaValue::Float(sum / count as f64)) }
        }
        "min" => {
            let mut best: Option<ArenaValue> = None;
            for v in vals {
                if v.is_null() { continue; }
                best = Some(match best {
                    None => *v,
                    Some(cur) => {
                        if v.compare(&cur, arena) == Some(std::cmp::Ordering::Less) { *v } else { cur }
                    }
                });
            }
            Ok(best.unwrap_or(ArenaValue::Null))
        }
        "max" => {
            let mut best: Option<ArenaValue> = None;
            for v in vals {
                if v.is_null() { continue; }
                best = Some(match best {
                    None => *v,
                    Some(cur) => {
                        if v.compare(&cur, arena) == Some(std::cmp::Ordering::Greater) { *v } else { cur }
                    }
                });
            }
            Ok(best.unwrap_or(ArenaValue::Null))
        }
        _ => Err(format!("aggregate window function \"{}\" not supported", name)),
    }
}
