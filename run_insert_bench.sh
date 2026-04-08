#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
export PGPASSWORD=postgres
CLI="/home/jagrit/evolvsql/native/cli/target/release/evolvsql"
cd /home/jagrit/evolvsql

pkill -9 -f beam.smp 2>/dev/null; sleep 1
mix run --no-halt &
sleep 6

echo "============================================"
echo "  INSERT Benchmark: Hash Index vs PostgreSQL"
echo "============================================"
echo ""

# Test 1: Insert into empty table with PK (measures constraint check overhead)
$CLI -c "CREATE TABLE ins_test (id int PRIMARY KEY, name text);" 2>/dev/null

echo "=== evolvsql: INSERT 1000 rows into empty table (1 client) ==="
START=$(date +%s%N)
for i in $(seq 1 10); do
  V=""
  for j in $(seq 1 100); do
    N=$(( (i-1)*100 + j ))
    V="${V}(${N},'name_${N}')"
    [ $j -lt 100 ] && V="${V},"
  done
  $CLI -c "INSERT INTO ins_test VALUES ${V};" 2>/dev/null
done
END=$(date +%s%N)
MS=$(( (END - START) / 1000000 ))
echo "  1000 rows in ${MS}ms ($(( 1000000 / (MS + 1) )) rows/s)"

echo ""
echo "=== evolvsql: INSERT 1000 more into 1000-row table ==="
START=$(date +%s%N)
for i in $(seq 1 10); do
  V=""
  for j in $(seq 1 100); do
    N=$(( 1000 + (i-1)*100 + j ))
    V="${V}(${N},'name_${N}')"
    [ $j -lt 100 ] && V="${V},"
  done
  $CLI -c "INSERT INTO ins_test VALUES ${V};" 2>/dev/null
done
END=$(date +%s%N)
MS=$(( (END - START) / 1000000 ))
echo "  1000 rows in ${MS}ms ($(( 1000000 / (MS + 1) )) rows/s)"
echo "  Table now has 2000 rows"

echo ""
echo "=== evolvsql: INSERT 1000 more into 2000-row table ==="
START=$(date +%s%N)
for i in $(seq 1 10); do
  V=""
  for j in $(seq 1 100); do
    N=$(( 2000 + (i-1)*100 + j ))
    V="${V}(${N},'name_${N}')"
    [ $j -lt 100 ] && V="${V},"
  done
  $CLI -c "INSERT INTO ins_test VALUES ${V};" 2>/dev/null
done
END=$(date +%s%N)
MS=$(( (END - START) / 1000000 ))
echo "  1000 rows in ${MS}ms ($(( 1000000 / (MS + 1) )) rows/s)"
echo "  Table now has 3000 rows"

echo ""
echo "--- PostgreSQL 17 ---"
echo ""

psql -h 127.0.0.1 -p 5432 -U postgres -c "DROP TABLE IF EXISTS ins_test; CREATE TABLE ins_test (id int PRIMARY KEY, name text);" 2>/dev/null

echo "=== PG: INSERT 1000 rows into empty table ==="
START=$(date +%s%N)
for i in $(seq 1 10); do
  V=""
  for j in $(seq 1 100); do
    N=$(( (i-1)*100 + j ))
    V="${V}(${N},'name_${N}')"
    [ $j -lt 100 ] && V="${V},"
  done
  psql -h 127.0.0.1 -p 5432 -U postgres -c "INSERT INTO ins_test VALUES ${V};" 2>/dev/null
done
END=$(date +%s%N)
MS=$(( (END - START) / 1000000 ))
echo "  1000 rows in ${MS}ms ($(( 1000000 / (MS + 1) )) rows/s)"

echo ""
echo "=== PG: INSERT 1000 more into 1000-row table ==="
START=$(date +%s%N)
for i in $(seq 1 10); do
  V=""
  for j in $(seq 1 100); do
    N=$(( 1000 + (i-1)*100 + j ))
    V="${V}(${N},'name_${N}')"
    [ $j -lt 100 ] && V="${V},"
  done
  psql -h 127.0.0.1 -p 5432 -U postgres -c "INSERT INTO ins_test VALUES ${V};" 2>/dev/null
done
END=$(date +%s%N)
MS=$(( (END - START) / 1000000 ))
echo "  1000 rows in ${MS}ms ($(( 1000000 / (MS + 1) )) rows/s)"

echo ""
echo "=== PG: INSERT 1000 more into 2000-row table ==="
START=$(date +%s%N)
for i in $(seq 1 10); do
  V=""
  for j in $(seq 1 100); do
    N=$(( 2000 + (i-1)*100 + j ))
    V="${V}(${N},'name_${N}')"
    [ $j -lt 100 ] && V="${V},"
  done
  psql -h 127.0.0.1 -p 5432 -U postgres -c "INSERT INTO ins_test VALUES ${V};" 2>/dev/null
done
END=$(date +%s%N)
MS=$(( (END - START) / 1000000 ))
echo "  1000 rows in ${MS}ms ($(( 1000000 / (MS + 1) )) rows/s)"

kill %1 2>/dev/null; wait 2>/dev/null
