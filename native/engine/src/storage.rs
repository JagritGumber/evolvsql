use crate::hnsw::{DistanceMetric, HnswIndex};
use crate::types::Value;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

/// In-memory row storage. Each table maps to a Vec of rows.
/// This is the Phase 1 storage — will be replaced with a persistent
/// B-tree engine with undo-log MVCC and direct I/O in Phase 2.
///
/// Per-table RwLock: the outer HashMap RwLock is only held during
/// table creation/deletion. Individual table locks allow concurrent
/// reads on different tables and concurrent same-table reads via
/// parking_lot's reader-writer fairness.
static STORE: LazyLock<RwLock<HashMap<String, Arc<RwLock<TableStore>>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

type Row = Vec<Value>;

struct TableStore {
    rows: Vec<Row>,
    /// Hash indexes for unique constraints: column_index -> value -> row_index.
    /// O(1) constraint checks and O(1) conflicting row lookup.
    unique_indexes: HashMap<usize, HashMap<Value, usize>>,
    /// Composite PK column indices (stored for index rebuild).
    pk_cols: Vec<usize>,
    /// Composite PK index: composite key -> row_index.
    pk_index: Option<HashMap<Vec<Value>, usize>>,
    /// Optional HNSW index for vector KNN queries.
    hnsw_index: Option<HnswIndex>,
}

fn key(schema: &str, name: &str) -> String {
    format!("{}.{}", schema, name)
}

/// Fast table lookup: acquires outer read lock briefly, clones the Arc, releases.
/// The caller then works with the per-table lock independently.
fn get_table(schema: &str, name: &str) -> Result<Arc<RwLock<TableStore>>, String> {
    let store = STORE.read();
    store
        .get(&key(schema, name))
        .cloned()
        .ok_or_else(|| format!("table \"{}.{}\" not found in storage", schema, name))
}

pub fn create_table(schema: &str, name: &str) {
    let mut store = STORE.write();
    store.insert(
        key(schema, name),
        Arc::new(RwLock::new(TableStore {
            rows: Vec::new(),
            unique_indexes: HashMap::new(),
            pk_cols: Vec::new(),
            pk_index: None,
            hnsw_index: None,
        })),
    );
}

/// Register a hash index for a unique/PK column. O(1) constraint checks.
pub fn add_unique_index(schema: &str, name: &str, col_idx: usize) -> Result<(), String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();
    let mut idx = HashMap::new();
    for (row_idx, row) in table.rows.iter().enumerate() {
        if col_idx < row.len() && !matches!(row[col_idx], Value::Null) {
            idx.insert(row[col_idx].clone(), row_idx);
        }
    }
    table.unique_indexes.insert(col_idx, idx);
    Ok(())
}

/// Register a composite PK hash index. O(1) constraint checks.
pub fn add_pk_index(schema: &str, name: &str, pk_cols: &[usize]) -> Result<(), String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();
    let mut idx = HashMap::new();
    for (row_idx, row) in table.rows.iter().enumerate() {
        let key: Vec<Value> = pk_cols.iter().map(|&i| row[i].clone()).collect();
        idx.insert(key, row_idx);
    }
    table.pk_cols = pk_cols.to_vec();
    table.pk_index = Some(idx);
    Ok(())
}

pub fn drop_table(schema: &str, name: &str) {
    let mut store = STORE.write();
    store.remove(&key(schema, name));
}

#[allow(dead_code)]
pub fn insert(schema: &str, name: &str, row: Row) -> Result<(), String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();
    table.rows.push(row);
    Ok(())
}

pub fn scan(schema: &str, name: &str) -> Result<Vec<Row>, String> {
    let tbl = get_table(schema, name)?;
    let table = tbl.read();
    Ok(table.rows.clone())
}

/// Zero-copy scan: runs callback with borrowed rows, avoids cloning.
pub fn scan_with<F, R>(schema: &str, name: &str, f: F) -> Result<R, String>
where
    F: FnOnce(&[Row]) -> Result<R, String>,
{
    let tbl = get_table(schema, name)?;
    let table = tbl.read();
    f(&table.rows)
}

pub fn delete_all(schema: &str, name: &str) -> Result<u64, String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();
    let count = table.rows.len() as u64;
    table.rows.clear();
    for idx in table.unique_indexes.values_mut() {
        idx.clear();
    }
    if let Some(ref mut pk_idx) = table.pk_index {
        pk_idx.clear();
    }
    table.hnsw_index = None;
    Ok(count)
}

pub fn delete_where(
    schema: &str,
    name: &str,
    mut predicate: impl FnMut(&Row) -> bool,
) -> Result<u64, String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();
    let before = table.rows.len();
    table.rows.retain_mut(|row| !predicate(row));
    let deleted = before - table.rows.len();
    if deleted > 0 {
        rebuild_indexes(&mut table);
    }
    Ok(deleted as u64)
}

/// Delete matching rows and return the deleted rows (for RETURNING clause).
/// Uses retain + side-channel to avoid 2x memory allocation (#10).
pub fn delete_where_returning(
    schema: &str,
    name: &str,
    mut predicate: impl FnMut(&Row) -> bool,
) -> Result<Vec<Row>, String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();
    let mut deleted = Vec::new();
    table.rows.retain_mut(|row| {
        if predicate(row) {
            deleted.push(row.clone()); // clone only deleted rows
            false
        } else {
            true
        }
    });
    if !deleted.is_empty() {
        rebuild_indexes(&mut table);
    }
    Ok(deleted)
}



// ── Index maintenance helpers ─────────────────────────────────────────

/// Add a row's values to all applicable indexes.
fn add_to_indexes(table: &mut TableStore, row: &Row, row_idx: usize) {
    for (&col_idx, idx) in table.unique_indexes.iter_mut() {
        if col_idx < row.len() && !matches!(row[col_idx], Value::Null) {
            idx.insert(row[col_idx].clone(), row_idx);
        }
    }
    if table.pk_cols.len() > 1 {
        if let Some(pk_idx) = &mut table.pk_index {
            let key: Vec<Value> = table.pk_cols.iter().map(|&i| row[i].clone()).collect();
            pk_idx.insert(key, row_idx);
        }
    }
}

/// Rebuild all indexes from current rows. Used after UPDATE and DELETE
/// to ensure composite PK index consistency.
fn rebuild_indexes(table: &mut TableStore) {
    for (&col_idx, idx) in table.unique_indexes.iter_mut() {
        idx.clear();
        for (row_idx, row) in table.rows.iter().enumerate() {
            if col_idx < row.len() && !matches!(row[col_idx], Value::Null) {
                idx.insert(row[col_idx].clone(), row_idx);
            }
        }
    }
    if let Some(pk_idx) = &mut table.pk_index {
        pk_idx.clear();
        for (row_idx, row) in table.rows.iter().enumerate() {
            let key: Vec<Value> = table.pk_cols.iter().map(|&i| row[i].clone()).collect();
            pk_idx.insert(key, row_idx);
        }
    }
    // Rebuild HNSW index — row_ids are positional, so any row shift invalidates them
    if let Some(ref old_hnsw) = table.hnsw_index {
        let col_idx = old_hnsw.col_idx();
        let metric = old_hnsw.metric();
        let mut new_hnsw = crate::hnsw::HnswIndex::new(metric, col_idx);
        for (i, row) in table.rows.iter().enumerate() {
            if col_idx < row.len() {
                if let Value::Vector(v) = &row[col_idx] {
                    new_hnsw.insert(i, v.clone());
                }
            }
        }
        table.hnsw_index = Some(new_hnsw);
    }
}

/// Insert with uniqueness check under a single write lock (no TOCTOU race).
/// `unique_checks` is a list of (column_index, constraint_name) pairs.
/// `pk_cols` is a list of column indices forming the composite primary key (if any).
/// Uses O(1) hash index lookups instead of O(N) full table scans.
#[allow(dead_code)]
pub fn insert_checked(
    schema: &str,
    name: &str,
    row: Row,
    unique_checks: &[(usize, String)],
    pk_cols: &[usize],
) -> Result<(), String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();

    // Reject NULL in any PK column (PK implies NOT NULL)
    for &pk_col in pk_cols {
        if pk_col < row.len() && matches!(row[pk_col], Value::Null) {
            return Err(
                "null value in column violates not-null constraint".to_string(),
            );
        }
    }

    // Composite PK check - O(1) via hash index
    if pk_cols.len() > 1 {
        if let Some(ref pk_idx) = table.pk_index {
            let key: Vec<Value> = pk_cols.iter().map(|&i| row[i].clone()).collect();
            if pk_idx.contains_key(&key) {
                return Err(format!(
                    "duplicate key value violates unique constraint \"{}.{}_pkey\"",
                    schema, name
                ));
            }
        }
    }

    // Per-column unique checks - O(1) via hash index
    for &(col_idx, ref cname) in unique_checks {
        if matches!(row[col_idx], Value::Null) {
            continue; // NULLs don't violate UNIQUE
        }
        if let Some(idx) = table.unique_indexes.get(&col_idx) {
            if idx.contains_key(&row[col_idx]) {
                return Err(format!(
                    "duplicate key value violates unique constraint \"{}\"",
                    cname
                ));
            }
        }
    }

    // Update indexes after successful validation
    let row_idx = table.rows.len();
    add_to_indexes(&mut table, &row, row_idx);
    table.rows.push(row);
    Ok(())
}

/// Insert multiple rows atomically with uniqueness checks.
/// All rows are validated against existing data AND each other before any are committed.
/// If any row fails validation, no rows are inserted.
/// Uses O(1) hash index lookups plus temporary batch sets for intra-batch checks.
pub fn insert_batch_checked(
    schema: &str,
    name: &str,
    rows: Vec<Row>,
    unique_checks: &[(usize, String)],
    pk_cols: &[usize],
) -> Result<usize, String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();

    // Temporary batch sets for intra-batch duplicate detection
    let mut batch_unique: HashMap<usize, HashSet<Value>> = HashMap::new();
    let mut batch_pk: HashSet<Vec<Value>> = HashSet::new();

    for row in rows.iter() {
        // Reject NULL in any PK column (PK implies NOT NULL)
        for &pk_col in pk_cols {
            if pk_col < row.len() && matches!(row[pk_col], Value::Null) {
                return Err(
                    "null value in column violates not-null constraint".to_string(),
                );
            }
        }

        // Composite PK check - O(1) against index + batch set
        if pk_cols.len() > 1 {
            let key: Vec<Value> = pk_cols.iter().map(|&ci| row[ci].clone()).collect();
            if let Some(ref pk_idx) = table.pk_index {
                if pk_idx.contains_key(&key) {
                    return Err(format!(
                        "duplicate key value violates unique constraint \"{}.{}_pkey\"",
                        schema, name
                    ));
                }
            }
            if !batch_pk.insert(key) {
                return Err(format!(
                    "duplicate key value violates unique constraint \"{}.{}_pkey\"",
                    schema, name
                ));
            }
        }

        // Per-column unique checks - O(1) against index + batch set
        for &(col_idx, ref cname) in unique_checks {
            if matches!(row[col_idx], Value::Null) {
                continue;
            }
            if let Some(idx) = table.unique_indexes.get(&col_idx) {
                if idx.contains_key(&row[col_idx]) {
                    return Err(format!(
                        "duplicate key value violates unique constraint \"{}\"",
                        cname
                    ));
                }
            }
            let batch_set = batch_unique.entry(col_idx).or_default();
            if !batch_set.insert(row[col_idx].clone()) {
                return Err(format!(
                    "duplicate key value violates unique constraint \"{}\"",
                    cname
                ));
            }
        }
    }

    // All validated - push all atomically and update indexes
    let base_row_id = table.rows.len();
    for (i, row) in rows.into_iter().enumerate() {
        add_to_indexes(&mut table, &row, base_row_id + i);
        table.rows.push(row);
    }
    Ok(base_row_id)
}

/// Insert rows with ON CONFLICT handling. For each row:
/// - If no conflict on specified columns: insert normally
/// - If conflict + DO NOTHING: skip
/// - If conflict + DO UPDATE: call updater on conflicting row
/// conflict_cols: column indices to check for conflicts (from ON CONFLICT (col) clause).
/// If empty, checks all unique/PK constraints.
/// Returns (inserted_count, updated_count, all affected rows for RETURNING).
pub fn insert_upsert(
    schema: &str,
    name: &str,
    rows: Vec<Row>,
    unique_checks: &[(usize, String)],
    pk_cols: &[usize],
    conflict_cols: &[usize],
    do_update: bool,
    mut updater: impl FnMut(&Row, &Row) -> Result<Row, String>,
) -> Result<(u64, u64, Vec<Row>), String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();

    let mut inserted: u64 = 0;
    let mut updated: u64 = 0;
    let mut affected_rows: Vec<Row> = Vec::new();

    for row in rows {
        let conflict_idx = find_conflict(&table, &row, unique_checks, pk_cols, conflict_cols);

        match conflict_idx {
            None => {
                let row_idx = table.rows.len();
                affected_rows.push(row.clone());
                add_to_indexes(&mut table, &row, row_idx);
                table.rows.push(row);
                inserted += 1;
            }
            Some(idx) if do_update => {
                let existing = &table.rows[idx];
                let new_row = updater(existing, &row)?;
                affected_rows.push(new_row.clone());
                // Targeted index update: remove old values, set new row, add new values
                remove_from_indexes(&mut table, idx);
                table.rows[idx] = new_row;
                let row_ref = table.rows[idx].clone();
                add_to_indexes(&mut table, &row_ref, idx);
                updated += 1;
            }
            Some(_) => {
                // Conflict + DO NOTHING
            }
        }
    }

    // Rebuild HNSW if any mutations (HNSW needs full rebuild since row positions matter)
    if (inserted > 0 || updated > 0) && table.hnsw_index.is_some() {
        rebuild_hnsw(&mut table);
    }

    Ok((inserted, updated, affected_rows))
}

/// Remove a row's values from unique/PK indexes (before update or delete).
fn remove_from_indexes(table: &mut TableStore, row_idx: usize) {
    let row = &table.rows[row_idx];
    for (&col_idx, idx) in table.unique_indexes.iter_mut() {
        if col_idx < row.len() && !matches!(row[col_idx], Value::Null) {
            idx.remove(&row[col_idx]);
        }
    }
    if table.pk_cols.len() > 1 {
        if let Some(pk_idx) = &mut table.pk_index {
            let key: Vec<Value> = table.pk_cols.iter().map(|&i| row[i].clone()).collect();
            pk_idx.remove(&key);
        }
    }
}

/// Rebuild only the HNSW vector index (row positions are positional).
fn rebuild_hnsw(table: &mut TableStore) {
    if let Some(ref old_hnsw) = table.hnsw_index {
        let col_idx = old_hnsw.col_idx();
        let metric = old_hnsw.metric();
        let mut new_hnsw = crate::hnsw::HnswIndex::new(metric, col_idx);
        for (i, row) in table.rows.iter().enumerate() {
            if col_idx < row.len() {
                if let Value::Vector(v) = &row[col_idx] {
                    new_hnsw.insert(i, v.clone());
                }
            }
        }
        table.hnsw_index = Some(new_hnsw);
    }
}

/// Find the index of a conflicting row, if any.
/// Uses O(1) hash index lookup for both detection and row position.
/// conflict_cols: only check these columns. If empty, check all unique/PK.
fn find_conflict(
    table: &TableStore,
    row: &Row,
    unique_checks: &[(usize, String)],
    pk_cols: &[usize],
    conflict_cols: &[usize],
) -> Option<usize> {
    // Check composite PK (only if conflict_cols is empty or matches PK)
    if pk_cols.len() > 1 {
        let pk_matches = conflict_cols.is_empty()
            || conflict_cols.iter().all(|c| pk_cols.contains(c));
        if pk_matches {
            let key: Vec<Value> = pk_cols.iter().map(|&ci| row[ci].clone()).collect();
            if let Some(ref pk_idx) = table.pk_index {
                if let Some(&row_idx) = pk_idx.get(&key) {
                    return Some(row_idx);
                }
            }
        }
    }

    // Check per-column unique constraints
    for &(col_idx, _) in unique_checks {
        if !conflict_cols.is_empty() && !conflict_cols.contains(&col_idx) {
            continue; // skip constraints not in the conflict target
        }
        if matches!(row[col_idx], Value::Null) {
            continue;
        }
        if let Some(idx) = table.unique_indexes.get(&col_idx) {
            if let Some(&row_idx) = idx.get(&row[col_idx]) {
                return Some(row_idx);
            }
        }
    }

    None
}

/// Update matching rows with validation. Returns error if any updater fails.
/// Fixes: intra-batch uniqueness check (#3) and panic-safe atomic swap (#6).
pub fn update_rows_checked(
    schema: &str,
    name: &str,
    mut predicate: impl FnMut(&Row) -> bool,
    updater: impl FnMut(&Row) -> Result<Row, String>,
    validator: impl Fn(&Row, &[Row], usize) -> Result<(), String>,
) -> Result<u64, String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();

    // First pass: compute new rows and validate against existing rows
    let mut updates: Vec<(usize, Row)> = Vec::new();
    let mut updater = updater;
    for (idx, row) in table.rows.iter().enumerate() {
        if predicate(row) {
            let new_row = updater(row)?;
            // Validate against all OTHER existing rows (excluding current)
            validator(&new_row, &table.rows, idx)?;
            updates.push((idx, new_row));
        }
    }

    // Intra-batch uniqueness: build the prospective final state and validate
    // that no two updated rows collide with each other on unique columns.
    if updates.len() > 1 {
        // Check each pair of new rows for uniqueness violations
        for i in 0..updates.len() {
            for j in (i + 1)..updates.len() {
                let row_a = &updates[i].1;
                let row_b = &updates[j].1;
                // Check all columns for equality — the caller's validator
                // handles per-column unique constraints against existing rows,
                // but we need to check new rows against each other too.
                // We re-use the validator: validate row_a against a slice
                // containing only row_b (with skip_idx that won't match).
                validator(row_a, &[row_b.clone()], usize::MAX)?;
            }
        }
    }

    // Second pass: atomic swap — build new rows vec, then replace all at once (#6)
    let count = updates.len() as u64;
    let mut new_rows = table.rows.clone();
    for (idx, new_row) in updates {
        new_rows[idx] = new_row;
    }
    table.rows = new_rows; // atomic replacement — if panic occurs during clone, originals are untouched

    // Rebuild indexes from final row state
    rebuild_indexes(&mut table);

    Ok(count)
}

/// Delete all rows and return them (for DELETE ... RETURNING without WHERE).
/// Single write lock — no TOCTOU race.
pub fn delete_all_returning(schema: &str, name: &str) -> Result<Vec<Row>, String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();
    for idx in table.unique_indexes.values_mut() {
        idx.clear();
    }
    if let Some(ref mut pk_idx) = table.pk_index {
        pk_idx.clear();
    }
    table.hnsw_index = None;
    Ok(std::mem::take(&mut table.rows))
}

#[allow(dead_code)]
pub fn row_count(schema: &str, name: &str) -> Result<u64, String> {
    let tbl = get_table(schema, name)?;
    let table = tbl.read();
    Ok(table.rows.len() as u64)
}

/// Ensure the table has an HNSW index on the given column.
/// Creates the index if it doesn't exist yet, and bulk-inserts existing rows.
pub fn ensure_hnsw_index(
    schema: &str,
    name: &str,
    col_idx: usize,
    metric: DistanceMetric,
) -> Result<(), String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();
    if let Some(ref existing) = table.hnsw_index {
        if existing.metric() == metric {
            return Ok(()); // already indexed with matching metric
        }
        // Metric mismatch — rebuild with new metric
    }
    let mut idx = HnswIndex::new(metric, col_idx);
    // Bulk-insert existing rows
    for (row_id, row) in table.rows.iter().enumerate() {
        if let Some(Value::Vector(v)) = row.get(col_idx) {
            idx.insert(row_id, v.clone());
        }
    }
    table.hnsw_index = Some(idx);
    Ok(())
}

/// Add a single vector to the HNSW index (called after row insertion).
pub fn hnsw_insert(
    schema: &str,
    name: &str,
    row_id: usize,
    vector: Vec<f32>,
) -> Result<(), String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();
    if let Some(ref mut idx) = table.hnsw_index {
        idx.insert(row_id, vector);
    }
    Ok(())
}

/// Search the HNSW index. Returns (distance, row_id) pairs sorted ascending.
pub fn hnsw_search(
    schema: &str,
    name: &str,
    query: &[f32],
    k: usize,
) -> Result<Vec<(f32, usize)>, String> {
    let tbl = get_table(schema, name)?;
    let table = tbl.read();
    match &table.hnsw_index {
        Some(idx) => Ok(idx.search(query, k, k.max(64))),
        None => Err("no HNSW index on this table".into()),
    }
}

/// Check if a table has an HNSW index and return the indexed column index.
pub fn has_hnsw_index(schema: &str, name: &str) -> Option<usize> {
    let tbl = get_table(schema, name).ok()?;
    let table = tbl.read();
    table.hnsw_index.as_ref().map(|idx| idx.col_idx())
}

/// Fetch rows by their row IDs (indices into the internal rows vec).
/// Returns rows in the order of the provided IDs.
pub fn get_rows_by_ids(schema: &str, name: &str, ids: &[usize]) -> Result<Vec<Row>, String> {
    let tbl = get_table(schema, name)?;
    let table = tbl.read();
    let mut result = Vec::with_capacity(ids.len());
    for &id in ids {
        if id < table.rows.len() {
            result.push(table.rows[id].clone());
        }
    }
    Ok(result)
}

#[allow(dead_code)]
/// Add a column to all existing rows (appends default_val to each row).
pub fn alter_add_column(schema: &str, name: &str, default_val: Value) {
    if let Ok(table) = get_table(schema, name) {
        let mut t = table.write();
        for row in &mut t.rows {
            row.push(default_val.clone());
        }
    }
}

/// Drop a column from all existing rows by index.
pub fn alter_drop_column(schema: &str, name: &str, col_idx: usize) {
    if let Ok(table) = get_table(schema, name) {
        let mut t = table.write();
        for row in &mut t.rows {
            if col_idx < row.len() {
                row.remove(col_idx);
            }
        }
        // Fix all index references after column removal
        t.unique_indexes.clear();
        t.pk_cols.retain(|c| *c != col_idx);
        for c in t.pk_cols.iter_mut() {
            if *c > col_idx { *c -= 1; }
        }
        t.pk_index = None; // force rebuild on next insert
        t.hnsw_index = None; // invalidate - will be lazily recreated
    }
}

/// Rename a table in storage.
pub fn rename_table(schema: &str, old_name: &str, new_name: &str) {
    let mut store = STORE.write();
    let old_key = key(schema, old_name);
    let new_key = key(schema, new_name);
    if let Some(table) = store.remove(&old_key) {
        store.insert(new_key, table);
    }
}

/// Batch insert without constraint checks (for CTAS, INSERT...SELECT).
pub fn insert_batch(schema: &str, name: &str, rows: Vec<Row>) {
    if let Ok(table) = get_table(schema, name) {
        let mut t = table.write();
        t.rows.extend(rows);
    }
}

pub fn reset() {
    let mut store = STORE.write();
    store.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[serial_test::serial]
    fn insert_and_scan() {
        reset();
        create_table("public", "t");
        insert(
            "public",
            "t",
            vec![Value::Int(1), Value::Text(Arc::from("hello"))],
        )
        .unwrap();
        insert(
            "public",
            "t",
            vec![Value::Int(2), Value::Text(Arc::from("world"))],
        )
        .unwrap();
        let rows = scan("public", "t").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int(1));
    }

    #[test]
    #[serial_test::serial]
    fn delete_all_works() {
        reset();
        create_table("public", "t");
        insert("public", "t", vec![Value::Int(1)]).unwrap();
        insert("public", "t", vec![Value::Int(2)]).unwrap();
        let count = delete_all("public", "t").unwrap();
        assert_eq!(count, 2);
        assert_eq!(scan("public", "t").unwrap().len(), 0);
    }
}
