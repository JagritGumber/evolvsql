use super::*;

#[test]
#[serial_test::serial]
fn hnsw_recall_test() {
    setup();
    execute("CREATE TABLE recall_t (id int, emb vector)").unwrap();
    let dim = 8;
    let n = 200;
    let mut vectors: Vec<Vec<f32>> = Vec::new();
    for i in 0..n {
        let v: Vec<f32> = (0..dim)
            .map(|d| ((i * 13 + d * 7) as f32 * 0.005) % 1.0)
            .collect();
        let vstr = format!(
            "[{}]",
            v.iter()
                .map(|f| format!("{:.6}", f))
                .collect::<Vec<_>>()
                .join(",")
        );
        execute(&format!("INSERT INTO recall_t VALUES ({}, '{}')", i, vstr)).unwrap();
        vectors.push(v);
    }
    let query = vec![0.5f32; dim];
    let k = 10;

    // Brute force ground truth
    let mut brute: Vec<(f32, usize)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let d: f32 = v
                .iter()
                .zip(query.iter())
                .map(|(a, b)| (a - b).powi(2))
                .sum::<f32>()
                .sqrt();
            (d, i)
        })
        .collect();
    brute.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    let truth: std::collections::HashSet<String> = brute
        .iter()
        .take(k)
        .map(|(_, id)| id.to_string())
        .collect();

    // HNSW result
    let qstr = format!(
        "[{}]",
        query
            .iter()
            .map(|f| format!("{:.6}", f))
            .collect::<Vec<_>>()
            .join(",")
    );
    let r = execute(&format!(
        "SELECT id FROM recall_t ORDER BY emb <-> '{}' LIMIT {}",
        qstr, k
    ))
    .unwrap();
    let hnsw_ids: std::collections::HashSet<String> = r
        .rows
        .iter()
        .filter_map(|row| row[0].clone())
        .collect();

    let overlap = truth.intersection(&hnsw_ids).count();
    let recall = overlap as f32 / k as f32;
    assert!(
        recall >= 0.7,
        "HNSW recall {:.0}% ({}/{}) is below 70% threshold",
        recall * 100.0,
        overlap,
        k
    );
}
