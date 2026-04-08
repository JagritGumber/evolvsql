#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
CLI="/home/jagrit/evolvsql/native/cli/target/release/evolvsql"
cd /home/jagrit/evolvsql

pkill -9 -f beam.smp 2>/dev/null; sleep 1
mix run --no-halt &
sleep 6

# Setup
$CLI -c "CREATE TABLE cpu_bench (id int PRIMARY KEY, name text, val int);" 2>/dev/null
for i in $(seq 1 10); do
  VALS=""; for j in $(seq 1 100); do N=$(((i-1)*100+j)); VALS="${VALS}($N,'n$N',$((N*10)))"; [ $j -lt 100 ] && VALS="${VALS},"; done
  $CLI -c "INSERT INTO cpu_bench VALUES ${VALS};" 2>/dev/null
done

BEAM_PID=$(pgrep -f beam.smp | head -1)

echo "============================================"
echo "  CPU Usage: evolvsql under various loads"
echo "  Machine: $(nproc) cores available"
echo "============================================"
echo ""

# Idle
sleep 2
echo "=== IDLE (0 queries) ==="
for i in 1 2 3; do
  CPU=$(ps -p $BEAM_PID -o %cpu= 2>/dev/null | tr -d ' ')
  echo "  CPU: ${CPU}%"
  sleep 1
done

echo ""
echo "=== 1 CLIENT sustained ==="
$CLI --bench 20000 --clients 1 -c "SELECT * FROM cpu_bench WHERE id = 42;" &
B=$!; sleep 2
for i in 1 2 3; do
  CPU=$(ps -p $BEAM_PID -o %cpu= 2>/dev/null | tr -d ' ')
  echo "  CPU: ${CPU}%"
  sleep 1
done
wait $B 2>/dev/null

echo ""
echo "=== 10 CLIENTS sustained ==="
$CLI --bench 5000 --clients 10 -c "SELECT * FROM cpu_bench WHERE id = 42;" &
B=$!; sleep 2
for i in 1 2 3; do
  CPU=$(ps -p $BEAM_PID -o %cpu= 2>/dev/null | tr -d ' ')
  echo "  CPU: ${CPU}%"
  sleep 1
done
wait $B 2>/dev/null

echo ""
echo "=== 50 CLIENTS sustained ==="
$CLI --bench 5000 --clients 50 -c "SELECT * FROM cpu_bench WHERE id = 42;" &
B=$!; sleep 2
for i in 1 2 3; do
  CPU=$(ps -p $BEAM_PID -o %cpu= 2>/dev/null | tr -d ' ')
  echo "  CPU: ${CPU}%"
  sleep 1
done
wait $B 2>/dev/null

echo ""
echo "=== 100 CLIENTS sustained ==="
$CLI --bench 2000 --clients 100 -c "SELECT * FROM cpu_bench WHERE id = 42;" &
B=$!; sleep 2
for i in 1 2 3; do
  CPU=$(ps -p $BEAM_PID -o %cpu= 2>/dev/null | tr -d ' ')
  echo "  CPU: ${CPU}%"
  sleep 1
done
wait $B 2>/dev/null

echo ""
echo "=== PostgreSQL CPU comparison: 50 clients ==="
export PGPASSWORD=postgres
PG_PIDS_BEFORE=$(pgrep -f "postgres:" | wc -l)
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 5000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" &
B=$!; sleep 2
# Sum CPU of all postgres processes
PG_CPU=0
for pid in $(pgrep -f "postgres:"); do
  C=$(ps -p $pid -o %cpu= 2>/dev/null | tr -d ' ')
  PG_CPU=$(echo "$PG_CPU + ${C:-0}" | bc 2>/dev/null || echo "$PG_CPU")
done
echo "  Total PG CPU: ${PG_CPU}%"
wait $B 2>/dev/null

echo ""
echo "============================================"
echo "  Summary: CPU% = percentage of 1 core"
echo "  100% = 1 full core, 200% = 2 cores"
echo "  This machine has $(nproc) cores"
echo "============================================"

kill %1 2>/dev/null; wait 2>/dev/null
