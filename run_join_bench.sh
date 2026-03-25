#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
export PGPASSWORD=postgres
CLI="/home/jagrit/pgrx/native/cli/target/release/pgrx"
cd /home/jagrit/pgrx

pkill -9 -f beam.smp 2>/dev/null; sleep 1
elixir --erl "+SDcpu 8:8 +sbwt none +sbwtdcpu none +sbwtdio none" -S mix run --no-halt &
sleep 6

$CLI -c "CREATE TABLE users (id int PRIMARY KEY, name text);" 2>/dev/null
$CLI -c "CREATE TABLE orders (id int, user_id int, total int);" 2>/dev/null
VALS=""; for i in $(seq 1 1000); do VALS="${VALS}($i,'user_$i')"; [ $i -lt 1000 ] && VALS="${VALS},"; done
$CLI -c "INSERT INTO users VALUES ${VALS};" 2>/dev/null
for b in $(seq 1 5); do
  VALS=""; for i in $(seq 1 1000); do N=$(((b-1)*1000+i)); UID=$(((N%1000)+1)); VALS="${VALS}($N,$UID,$((N*5)))"; [ $i -lt 1000 ] && VALS="${VALS},"; done
  $CLI -c "INSERT INTO orders VALUES ${VALS};" 2>/dev/null
done

echo "=== pgrx: JOIN + GROUP BY (1000×5000, 1 client) ==="
$CLI --bench 50 --clients 1 -c "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name LIMIT 10;" 2>&1 | grep -E "Throughput|Avg"

echo ""
echo "=== pgrx: Simple JOIN WHERE (1 client) ==="
$CLI --bench 500 --clients 1 -c "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 42;" 2>&1 | grep -E "Throughput|Avg"

echo ""
echo "=== pgrx: Simple JOIN WHERE (10 clients) ==="
$CLI --bench 200 --clients 10 -c "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 42;" 2>&1 | grep -E "Throughput|Avg"

echo ""
echo "--- PostgreSQL 17 ---"
psql -h 127.0.0.1 -p 5432 -U postgres -c "DROP TABLE IF EXISTS orders, users;" 2>/dev/null
psql -h 127.0.0.1 -p 5432 -U postgres -c "CREATE TABLE users (id int PRIMARY KEY, name text); INSERT INTO users SELECT g, 'user_' || g FROM generate_series(1,1000) g;" 2>/dev/null
psql -h 127.0.0.1 -p 5432 -U postgres -c "CREATE TABLE orders (id int, user_id int, total int); INSERT INTO orders SELECT g, (g%1000)+1, g*5 FROM generate_series(1,5000) g;" 2>/dev/null

echo ""
echo "=== PG: JOIN + GROUP BY (1 client) ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 50 --clients 1 -c "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name LIMIT 10;" 2>&1 | grep -E "Throughput|Avg"

echo ""
echo "=== PG: Simple JOIN WHERE (1 client) ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 500 --clients 1 -c "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 42;" 2>&1 | grep -E "Throughput|Avg"

kill %1 2>/dev/null; wait 2>/dev/null
