#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
export PGPASSWORD=postgres
CLI="/home/jagrit/evolvsql/native/cli/target/release/evolvsql"
cd /home/jagrit/evolvsql

pkill -9 -f beam.smp 2>/dev/null; sleep 1
mix run --no-halt &
sleep 6

echo "============================================"
echo "  INSERT Deep Analysis: Scaling Behavior"
echo "============================================"
echo ""

# Test INSERT speed at different table sizes
# Insert 100 rows at a time, measure each batch

$CLI -c "CREATE TABLE scale_test (id int PRIMARY KEY, name text);" 2>/dev/null

echo "=== evolvsql: INSERT 100 rows at various table sizes ==="
for SIZE in 0 1000 2000 5000 10000; do
  # Fill up to SIZE if needed
  CURRENT=$($CLI -c "SELECT COUNT(*) FROM scale_test;" 2>/dev/null | grep -oP '^\s*\d+' | tr -d ' ' || echo "0")
  while [ "${CURRENT:-0}" -lt "$SIZE" ]; do
    NEXT=$((CURRENT + 100))
    V=""
    for j in $(seq $((CURRENT+1)) $NEXT); do
      V="${V}($j,'n$j')"
      [ $j -lt $NEXT ] && V="${V},"
    done
    $CLI -c "INSERT INTO scale_test VALUES ${V};" 2>/dev/null
    CURRENT=$NEXT
  done

  # Now benchmark: insert 100 rows
  NEXT_START=$((SIZE + 1))
  NEXT_END=$((SIZE + 100))
  V=""
  for j in $(seq $NEXT_START $NEXT_END); do
    V="${V}($j,'n$j')"
    [ $j -lt $NEXT_END ] && V="${V},"
  done

  START=$(date +%s%N)
  $CLI -c "INSERT INTO scale_test VALUES ${V};" 2>/dev/null
  END=$(date +%s%N)
  US=$(( (END - START) / 1000 ))
  echo "  At ${SIZE} rows: insert 100 in ${US}us ($(( 100000000 / (US + 1) )) rows/s)"
done

$CLI -c "DROP TABLE scale_test;" 2>/dev/null

echo ""
echo "=== evolvsql: INSERT without PK (no constraint check) ==="
$CLI -c "CREATE TABLE no_pk (id int, name text);" 2>/dev/null

for SIZE in 0 1000 5000 10000; do
  CURRENT=$($CLI -c "SELECT COUNT(*) FROM no_pk;" 2>/dev/null | grep -oP '^\s*\d+' | tr -d ' ' || echo "0")
  while [ "${CURRENT:-0}" -lt "$SIZE" ]; do
    NEXT=$((CURRENT + 100))
    V=""
    for j in $(seq $((CURRENT+1)) $NEXT); do
      V="${V}($j,'n$j')"
      [ $j -lt $NEXT ] && V="${V},"
    done
    $CLI -c "INSERT INTO no_pk VALUES ${V};" 2>/dev/null
    CURRENT=$NEXT
  done

  NEXT_START=$((SIZE + 1))
  NEXT_END=$((SIZE + 100))
  V=""
  for j in $(seq $NEXT_START $NEXT_END); do
    V="${V}($j,'n$j')"
    [ $j -lt $NEXT_END ] && V="${V},"
  done

  START=$(date +%s%N)
  $CLI -c "INSERT INTO no_pk VALUES ${V};" 2>/dev/null
  END=$(date +%s%N)
  US=$(( (END - START) / 1000 ))
  echo "  At ${SIZE} rows: insert 100 in ${US}us ($(( 100000000 / (US + 1) )) rows/s)"
done

echo ""
echo "--- PostgreSQL 17 comparison ---"
echo ""
psql -h 127.0.0.1 -p 5432 -U postgres -c "DROP TABLE IF EXISTS scale_test; CREATE TABLE scale_test (id int PRIMARY KEY, name text);" 2>/dev/null

echo "=== PG: INSERT 100 rows at various table sizes ==="
for SIZE in 0 1000 2000 5000 10000; do
  if [ "$SIZE" -gt 0 ]; then
    psql -h 127.0.0.1 -p 5432 -U postgres -c "INSERT INTO scale_test SELECT g, 'n' || g FROM generate_series((SELECT COALESCE(MAX(id),0)+1 FROM scale_test), $SIZE) g;" 2>/dev/null
  fi

  NEXT_START=$((SIZE + 1))
  NEXT_END=$((SIZE + 100))
  V=""
  for j in $(seq $NEXT_START $NEXT_END); do
    V="${V}($j,'n$j')"
    [ $j -lt $NEXT_END ] && V="${V},"
  done

  START=$(date +%s%N)
  psql -h 127.0.0.1 -p 5432 -U postgres -c "INSERT INTO scale_test VALUES ${V};" 2>/dev/null
  END=$(date +%s%N)
  US=$(( (END - START) / 1000 ))
  echo "  At ${SIZE} rows: insert 100 in ${US}us ($(( 100000000 / (US + 1) )) rows/s)"
done

kill %1 2>/dev/null; wait 2>/dev/null
