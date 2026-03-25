#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
CLI="/home/jagrit/pgrx/native/cli/target/release/pgrx"
cd /home/jagrit/pgrx

measure_idle() {
  sleep 3
  PID=$(pgrep -f beam.smp | head -1)
  echo "  Threads: $(ls /proc/$PID/task/ 2>/dev/null | wc -l)"
  C1=$(ps -p $PID -o %cpu= 2>/dev/null | tr -d ' ')
  sleep 2
  C2=$(ps -p $PID -o %cpu= 2>/dev/null | tr -d ' ')
  sleep 2
  C3=$(ps -p $PID -o %cpu= 2>/dev/null | tr -d ' ')
  echo "  Idle CPU: ${C1}%, ${C2}%, ${C3}%"
}

echo "=== CONFIG 1: Default dev (12 schedulers, busy-wait ON) ==="
pkill -9 -f beam.smp 2>/dev/null; sleep 1
mix run --no-halt &
measure_idle
kill %1 2>/dev/null; wait 2>/dev/null; sleep 1

echo ""
echo "=== CONFIG 2: +sbwt none (busy-wait OFF, still 12 schedulers) ==="
pkill -9 -f beam.smp 2>/dev/null; sleep 1
elixir --erl "+sbwt none +sbwtdcpu none +sbwtdio none" -S mix run --no-halt &
measure_idle
# Quick speed test
$CLI -c "CREATE TABLE t(id int PRIMARY KEY, name text);" 2>/dev/null
VALS=""; for i in $(seq 1 1000); do VALS="${VALS}($i,'n$i')"; [ $i -lt 1000 ] && VALS="${VALS},"; done
$CLI -c "INSERT INTO t VALUES ${VALS};" 2>/dev/null
echo "  50-client QPS:"
$CLI --bench 5000 --clients 50 -c "SELECT * FROM t WHERE id = 42;" 2>&1 | grep Throughput
PID=$(pgrep -f beam.smp | head -1)
$CLI --bench 5000 --clients 50 -c "SELECT * FROM t WHERE id = 42;" &
B=$!; sleep 2
echo "  Load CPU: $(ps -p $PID -o %cpu= | tr -d ' ')%"
wait $B 2>/dev/null
kill %1 2>/dev/null; wait 2>/dev/null; sleep 1

echo ""
echo "=== CONFIG 3: 2 schedulers + no busy-wait ==="
pkill -9 -f beam.smp 2>/dev/null; sleep 1
elixir --erl "+S 2:2 +SDcpu 2:2 +SDio 1 +sbwt none +sbwtdcpu none +sbwtdio none" -S mix run --no-halt &
measure_idle
$CLI -c "CREATE TABLE t(id int PRIMARY KEY, name text);" 2>/dev/null
$CLI -c "INSERT INTO t VALUES ${VALS};" 2>/dev/null
echo "  50-client QPS:"
$CLI --bench 5000 --clients 50 -c "SELECT * FROM t WHERE id = 42;" 2>&1 | grep Throughput
PID=$(pgrep -f beam.smp | head -1)
$CLI --bench 5000 --clients 50 -c "SELECT * FROM t WHERE id = 42;" &
B=$!; sleep 2
echo "  Load CPU: $(ps -p $PID -o %cpu= | tr -d ' ')%"
wait $B 2>/dev/null
kill %1 2>/dev/null; wait 2>/dev/null; sleep 1

echo ""
echo "=== CONFIG 4: 4 schedulers + no busy-wait (sweet spot?) ==="
pkill -9 -f beam.smp 2>/dev/null; sleep 1
elixir --erl "+S 4:4 +SDcpu 2:2 +SDio 1 +sbwt none +sbwtdcpu none +sbwtdio none" -S mix run --no-halt &
measure_idle
$CLI -c "CREATE TABLE t(id int PRIMARY KEY, name text);" 2>/dev/null
$CLI -c "INSERT INTO t VALUES ${VALS};" 2>/dev/null
echo "  50-client QPS:"
$CLI --bench 5000 --clients 50 -c "SELECT * FROM t WHERE id = 42;" 2>&1 | grep Throughput
PID=$(pgrep -f beam.smp | head -1)
$CLI --bench 5000 --clients 50 -c "SELECT * FROM t WHERE id = 42;" &
B=$!; sleep 2
echo "  Load CPU: $(ps -p $PID -o %cpu= | tr -d ' ')%"
wait $B 2>/dev/null
kill %1 2>/dev/null; wait 2>/dev/null
