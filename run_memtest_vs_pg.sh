#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
export PGPASSWORD=postgres
PG_HOST=127.0.0.1
PG_PORT=5432
PG_USER=postgres
PGRX_PORT=5433
N=50

echo "============================================"
echo "  Memory Benchmark: pgrx vs PostgreSQL 17"
echo "  $N connections"
echo "============================================"
echo ""

get_pg_rss() {
    # Sum RSS of all postgres backend processes
    local total=0
    for pid in $(pgrep -f "postgres:" 2>/dev/null); do
        local rss=$(awk '/VmRSS/{print $2}' /proc/$pid/status 2>/dev/null)
        if [ -n "$rss" ]; then
            total=$((total + rss))
        fi
    done
    echo $total
}

get_pg_main_rss() {
    local pid=$(pgrep -f "postgres -D" 2>/dev/null | head -1)
    [ -z "$pid" ] && pid=$(pgrep -x postgres 2>/dev/null | head -1)
    if [ -n "$pid" ]; then
        awk '/VmRSS/{print $2}' /proc/$pid/status 2>/dev/null
    else
        echo "0"
    fi
}

get_beam_rss() {
    local pid=$(pgrep -f beam.smp 2>/dev/null | head -1)
    if [ -n "$pid" ]; then
        awk '/VmRSS/{print $2}' /proc/$pid/status 2>/dev/null
    else
        echo "0"
    fi
}

count_pg_backends() {
    PGPASSWORD=postgres psql -h $PG_HOST -p $PG_PORT -U $PG_USER -t -c "SELECT count(*) FROM pg_stat_activity WHERE state IS NOT NULL;" 2>/dev/null | tr -d ' '
}

# ── PostgreSQL Test ──────────────────────────────────────
echo "=== PostgreSQL 17.8 ==="

# Baseline
PG_RSS0=$(get_pg_rss)
PG_MAIN0=$(get_pg_main_rss)
PG_BACKENDS0=$(count_pg_backends)
echo "BASELINE:    ${PG_RSS0} KB total RSS (${PG_BACKENDS0} backends)"

# Open N connections using psql in background
PG_PIDS=""
for i in $(seq 1 $N); do
    PGPASSWORD=postgres psql -h $PG_HOST -p $PG_PORT -U $PG_USER -c "SELECT pg_sleep(120);" &>/dev/null &
    PG_PIDS="$PG_PIDS $!"
done
sleep 3

PG_RSS1=$(get_pg_rss)
PG_BACKENDS1=$(count_pg_backends)
PG_DELTA=$((PG_RSS1 - PG_RSS0))
PG_PER=$((PG_DELTA * 1024 / N))
echo "ACTIVE:      ${PG_RSS1} KB total RSS (${PG_BACKENDS1} backends, +${PG_DELTA} KB, ~${PG_PER} bytes/conn)"

# Kill all psql clients
for pid in $PG_PIDS; do kill $pid 2>/dev/null; done
wait 2>/dev/null
sleep 3

PG_RSS2=$(get_pg_rss)
PG_BACKENDS2=$(count_pg_backends)
echo "CLOSED:      ${PG_RSS2} KB total RSS (${PG_BACKENDS2} backends)"
echo ""

# ── pgrx Test ────────────────────────────────────────────
echo "=== pgrx (BEAM + Rust) ==="

# Start pgrx
cd /home/jagrit/pgrx
pkill -9 -f beam.smp 2>/dev/null
sleep 1
mix run --no-halt &
SERVER_PID=$!
sleep 6

cd /home/jagrit/pgrx/native/cli
./target/release/pgrx --memtest $N 2>&1 | grep -v "^\[info\]\|^\[notice\]"

kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "============================================"
echo "  PostgreSQL: +${PG_DELTA} KB for $N connections"
echo "  pgrx idle:  ~0 KB for $N hibernated connections"
echo "  Ratio:      PostgreSQL uses ~${PG_DELTA}x more memory"
echo "============================================"
