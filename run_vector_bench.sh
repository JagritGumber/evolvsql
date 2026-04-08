#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
export PGPASSWORD=postgres
CLI="/home/jagrit/evolvsql/native/cli/target/release/evolvsql"
cd /home/jagrit/evolvsql

pkill -9 -f beam.smp 2>/dev/null; sleep 1
mix run --no-halt &
sleep 6

echo "============================================"
echo "  Vector Search Benchmark: evolvsql vs pgvector"
echo "============================================"
echo ""

# Setup evolvsql
$CLI -c "CREATE TABLE vec_bench (id int PRIMARY KEY, embedding vector);" 2>/dev/null

# Insert 1000 vectors (128-dim, random-ish)
echo "Inserting 1000 × 128-dim vectors into evolvsql..."
for i in $(seq 1 10); do
  VALS=""
  for j in $(seq 1 100); do
    N=$(( (i-1)*100 + j ))
    # Generate a simple 128-dim vector
    VEC="["
    for d in $(seq 1 128); do
      V=$(echo "scale=4; ($N * $d * 0.001) % 1.0" | bc 2>/dev/null || echo "0.$((N*d % 1000))")
      VEC="${VEC}${V}"
      [ $d -lt 128 ] && VEC="${VEC},"
    done
    VEC="${VEC}]"
    VALS="${VALS}(${N}, '${VEC}')"
    [ $j -lt 100 ] && VALS="${VALS},"
  done
  $CLI -c "INSERT INTO vec_bench VALUES ${VALS};" 2>/dev/null
done
echo "Done."
echo ""

# Build query vector
QVEC="["
for d in $(seq 1 128); do
  V="0.$((500*d % 1000))"
  QVEC="${QVEC}${V}"
  [ $d -lt 128 ] && QVEC="${QVEC},"
done
QVEC="${QVEC}]"

echo "=== evolvsql: KNN search (1 client, 128-dim, 1000 vectors) ==="
$CLI --bench 100 --clients 1 -c "SELECT id FROM vec_bench ORDER BY embedding <-> '${QVEC}' LIMIT 10;" 2>&1 | grep -E "Throughput|Avg|P99"

echo ""
echo "=== evolvsql: KNN search (10 clients) ==="
$CLI --bench 100 --clients 10 -c "SELECT id FROM vec_bench ORDER BY embedding <-> '${QVEC}' LIMIT 10;" 2>&1 | grep -E "Throughput|Avg|P99"

echo ""
# Compare with pgvector on PostgreSQL
echo "Setting up pgvector on PostgreSQL..."
psql -h 127.0.0.1 -p 5432 -U postgres -c "CREATE EXTENSION IF NOT EXISTS vector;" 2>/dev/null
psql -h 127.0.0.1 -p 5432 -U postgres -c "DROP TABLE IF EXISTS vec_bench; CREATE TABLE vec_bench (id int PRIMARY KEY, embedding vector(128));" 2>/dev/null

echo "Inserting 1000 × 128-dim vectors into PostgreSQL..."
for i in $(seq 1 1000); do
  VEC="["
  for d in $(seq 1 128); do
    V=$(echo "scale=4; ($i * $d * 0.001) % 1.0" | bc 2>/dev/null || echo "0.$((i*d % 1000))")
    VEC="${VEC}${V}"
    [ $d -lt 128 ] && VEC="${VEC},"
  done
  VEC="${VEC}]"
  psql -h 127.0.0.1 -p 5432 -U postgres -c "INSERT INTO vec_bench VALUES ($i, '${VEC}');" 2>/dev/null
done
echo "Done."

echo ""
echo "=== pgvector: KNN search (1 client, 128-dim, 1000 vectors, no index) ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 100 --clients 1 -c "SELECT id FROM vec_bench ORDER BY embedding <-> '${QVEC}' LIMIT 10;" 2>&1 | grep -E "Throughput|Avg|P99"

echo ""
echo "=== pgvector: KNN search (10 clients, no index) ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 100 --clients 10 -c "SELECT id FROM vec_bench ORDER BY embedding <-> '${QVEC}' LIMIT 10;" 2>&1 | grep -E "Throughput|Avg|P99"

kill %1 2>/dev/null; wait 2>/dev/null
