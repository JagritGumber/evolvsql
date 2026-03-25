#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
export PGPASSWORD=postgres
CLI="/home/jagrit/pgrx/native/cli/target/release/pgrx"
cd /home/jagrit/pgrx

mix compile --force 2>&1 | tail -3
pkill -9 -f beam.smp 2>/dev/null; sleep 1
elixir --erl "+SDcpu 8:8 +sbwt none +sbwtdcpu none +sbwtdio none" -S mix run --no-halt &
sleep 6

$CLI -c "CREATE TABLE bench (id int PRIMARY KEY, name text, val int);" 2>/dev/null
V=""; for i in $(seq 1 1000); do V="${V}($i,'n$i',$((i*10)))"; [ $i -lt 1000 ] && V="${V},"; done
$CLI -c "INSERT INTO bench VALUES ${V};" 2>/dev/null

$CLI -c "CREATE TABLE users (id int PRIMARY KEY, name text);" 2>/dev/null
V=""; for i in $(seq 1 1000); do V="${V}($i,'user_$i')"; [ $i -lt 1000 ] && V="${V},"; done
$CLI -c "INSERT INTO users VALUES ${V};" 2>/dev/null
$CLI -c "CREATE TABLE orders (id int, user_id int, total int);" 2>/dev/null
for b in $(seq 1 5); do
  V=""; for i in $(seq 1 1000); do N=$(((b-1)*1000+i)); BID=$(((N%1000)+1)); V="${V}($N,$BID,$((N*5)))"; [ $i -lt 1000 ] && V="${V},"; done
  $CLI -c "INSERT INTO orders VALUES ${V};" 2>/dev/null
done

QVEC=$(python3 -c "import random; random.seed(99); print('[' + ','.join([f'{random.random():.4f}' for _ in range(32)]) + ']')")
$CLI -c "CREATE TABLE vecs (id int, embedding vector);" 2>/dev/null
SQL=$(python3 -c "
import random; random.seed(42)
vals = []
for i in range(100):
    vec = ','.join([f'{random.random():.4f}' for _ in range(32)])
    vals.append(f\"({i+1}, '[{vec}]')\")
print('INSERT INTO vecs VALUES ' + ','.join(vals) + ';')
")
$CLI -c "$SQL" 2>/dev/null

echo "============================================"
echo "  VM Philosophy Overhaul — Full Benchmark"
echo "============================================"
echo ""
echo "=== Point query 1 client ==="
$CLI --bench 10000 --clients 1 -c "SELECT * FROM bench WHERE id = 42;" 2>&1 | grep -E "Throughput|Avg"
echo ""
echo "=== Point query 50 clients ==="
$CLI --bench 5000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" 2>&1 | grep -E "Throughput|Avg"
echo ""
echo "=== JOIN + GROUP BY + ORDER BY ==="
$CLI --bench 50 --clients 1 -c "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name ORDER BY COUNT(*) DESC LIMIT 5;" 2>&1 | grep -E "Throughput|Avg"
echo ""
echo "=== Vector KNN 10 clients ==="
$CLI --bench 200 --clients 10 -c "SELECT id FROM vecs ORDER BY embedding <-> '${QVEC}' LIMIT 5;" 2>&1 | grep -E "Throughput|Avg"
echo ""
echo "--- PostgreSQL 17 ---"
echo ""
echo "=== PG: Point 50 clients ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 5000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" 2>&1 | grep -E "Throughput|Avg"
echo ""
echo "=== PG: JOIN + GROUP BY + ORDER BY ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 50 --clients 1 -c "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name ORDER BY COUNT(*) DESC LIMIT 5;" 2>&1 | grep -E "Throughput|Avg"

kill %1 2>/dev/null; wait 2>/dev/null
