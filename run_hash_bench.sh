#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
export PGPASSWORD=postgres
CLI="/home/jagrit/pgrx/native/cli/target/release/pgrx"
cd /home/jagrit/pgrx

pkill -9 -f beam.smp 2>/dev/null; sleep 1
mix run --no-halt &
sleep 6

$CLI -c "CREATE TABLE bench (id int PRIMARY KEY, name text, val int);" 2>/dev/null
V=""; for i in $(seq 1 1000); do V="${V}($i,'n$i',$((i*10)))"; [ $i -lt 1000 ] && V="${V},"; done
$CLI -c "INSERT INTO bench VALUES ${V};" 2>/dev/null

echo "=== Point 1 client ==="
$CLI --bench 5000 --clients 1 -c "SELECT * FROM bench WHERE id = 42;" 2>&1
echo ""
echo "=== Point 50 clients ==="
$CLI --bench 3000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" 2>&1

kill %1 2>/dev/null; wait 2>/dev/null
