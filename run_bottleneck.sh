#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
export PGPASSWORD=postgres
CLI="/home/jagrit/pgrx/native/cli/target/release/pgrx"
cd /home/jagrit/pgrx

pkill -9 -f beam.smp 2>/dev/null; sleep 1
mix run --no-halt &
sleep 6

echo "============================================"
echo "  Bottleneck Analysis: Where Time Goes"
echo "============================================"
echo ""

# Test 1: Pure overhead (no table access)
echo "=== SELECT 1 (pure protocol + parse overhead) ==="
$CLI --bench 10000 --clients 1 -c "SELECT 1;" 2>&1 | grep -E "Throughput|Avg"

echo ""
# Test 2: Empty table scan (scan overhead, no rows)
$CLI -c "CREATE TABLE empty_t (id int, name text);" 2>/dev/null
echo "=== SELECT * FROM empty_t (scan overhead, 0 rows) ==="
$CLI --bench 10000 --clients 1 -c "SELECT * FROM empty_t;" 2>&1 | grep -E "Throughput|Avg"

echo ""
# Test 3: 10-row table (minimal scan)
$CLI -c "CREATE TABLE t10 (id int, name text);" 2>/dev/null
for i in $(seq 1 10); do $CLI -c "INSERT INTO t10 VALUES ($i, 'n$i');" 2>/dev/null; done
echo "=== WHERE id=5 on 10 rows (scan cost @ 10 rows) ==="
$CLI --bench 10000 --clients 1 -c "SELECT * FROM t10 WHERE id = 5;" 2>&1 | grep -E "Throughput|Avg"

echo ""
# Test 4: 100-row table
$CLI -c "CREATE TABLE t100 (id int, name text);" 2>/dev/null
VALS=""; for i in $(seq 1 100); do VALS="${VALS}($i, 'name_$i')"; [ $i -lt 100 ] && VALS="${VALS},"; done
$CLI -c "INSERT INTO t100 VALUES ${VALS};" 2>/dev/null
echo "=== WHERE id=50 on 100 rows (scan cost @ 100 rows) ==="
$CLI --bench 10000 --clients 1 -c "SELECT * FROM t100 WHERE id = 50;" 2>&1 | grep -E "Throughput|Avg"

echo ""
# Test 5: 1000-row table (our benchmark table)
echo "=== WHERE id=42 on 1000 rows (scan cost @ 1000 rows) ==="
$CLI --bench 10000 --clients 1 -c "SELECT * FROM bench WHERE id = 42;" 2>&1 | grep -E "Throughput|Avg"

echo ""
# Test 6: Same query, PostgreSQL (has B-tree index on PK)
echo "=== PostgreSQL: WHERE id=42 on 1000 rows (B-tree indexed) ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 10000 --clients 1 -c "SELECT * FROM bench WHERE id = 42;" 2>&1 | grep -E "Throughput|Avg"

echo ""
echo "=== Scaling: QPS vs Table Size ==="
echo "  0 rows:    $(echo 'see above')"
echo "  10 rows:   $(echo 'see above')"
echo "  100 rows:  $(echo 'see above')"
echo "  1000 rows: $(echo 'see above')"

echo ""
# Test 7: 50 clients on different query types
echo "=== 50 clients: SELECT 1 (max protocol throughput) ==="
$CLI --bench 5000 --clients 50 -c "SELECT 1;" 2>&1 | grep -E "Throughput|Avg"

echo ""
echo "=== 50 clients: WHERE id=5 on 10-row table ==="
$CLI --bench 5000 --clients 50 -c "SELECT * FROM t10 WHERE id = 5;" 2>&1 | grep -E "Throughput|Avg"

echo ""
echo "=== 50 clients: WHERE id=42 on 1000-row table ==="
$CLI --bench 5000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" 2>&1 | grep -E "Throughput|Avg"

kill %1 2>/dev/null; wait 2>/dev/null
