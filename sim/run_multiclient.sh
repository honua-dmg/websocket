#!/usr/bin/env bash
# Starts the feeder, then connects N clients at staggered times and reports
# whether they all received identical data despite different history/live splits.
#
# Usage: sim/run_multiclient.sh <csv_path> <EXCHANGE:SYMBOL> [clients] [stagger_secs]
# Env:   INTERVAL (feeder seconds per tick), IDLE (client quiet-stop seconds)
#        SERVER=host  run uvicorn natively instead of using the Docker container.
#                     Docker Desktop on macOS caches the bind-mounted history CSV, so a
#                     containerised server never sees the feeder's appends and every
#                     client reads the same frozen snapshot.
set -euo pipefail

CSV="${1:-}"
STOCK="${2:-}"
CLIENTS="${3:-3}"
STAGGER="${4:-10}"
INTERVAL="${INTERVAL:-0.01}"
IDLE="${IDLE:-8}"
KILL_AFTER="${KILL_AFTER:-12}"   # SIGKILL the victim client this long after it connects; 0 disables
START_DELAY=3

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
OUT="$SCRIPT_DIR/output"
SERVER_PORT="${PORT:-8765}"

die()  { echo "[MULTI] ERROR: $*" >&2; exit 1; }
info() { echo "[MULTI] $*"; }

[ -n "$CSV" ] && [ -n "$STOCK" ] || die "Usage: sim/run_multiclient.sh <csv_path> <EXCHANGE:SYMBOL> [clients] [stagger_secs]"
[ -f "$CSV" ] || die "CSV not found: $CSV"
[[ "$STOCK" == *:* ]] || die "stock must be EXCHANGE:SYMBOL, got '$STOCK'"

EXCHANGE="${STOCK%%:*}"
SYMBOL="${STOCK#*:}"

PYTHON="$PROJECT_DIR/.venv/bin/python"
[ -x "$PYTHON" ] || PYTHON="python3"

cd "$PROJECT_DIR"
mkdir -p "$OUT"

SERVER_PID=""
if [ "${SERVER:-docker}" = "host" ]; then
    docker rm -f stonks-ws-server >/dev/null 2>&1 || true   # free the port
    info "Starting uvicorn on the host (sees file appends immediately)..."
    "$PYTHON" -m uvicorn main:app --host 0.0.0.0 --port "$SERVER_PORT" > "$OUT/server.log" 2>&1 &
    SERVER_PID=$!
    for _ in $(seq 1 20); do
        nc -z localhost "$SERVER_PORT" 2>/dev/null && break
        sleep 0.5
    done
fi

nc -z localhost "$SERVER_PORT" 2>/dev/null || die "No server on port $SERVER_PORT. Run ./sim/run_testbench.sh first, or pass SERVER=host."

# ── Reset state ───────────────────────────────────────────────────────────────
info "Resetting Redis stream and today's history CSV..."
pkill -f "sim/feeder.py" 2>/dev/null || true
pkill -f "sim/client.py" 2>/dev/null || true
redis-cli -h localhost -p 6379 del "$SYMBOL" >/dev/null 2>&1 || true
rm -f "$OUT"/sample_"$SYMBOL"_*.csv "$OUT"/client_*.log "$OUT"/feeder.log
# feeder names the history file by UTC date
rm -f "data/$EXCHANGE/$SYMBOL/$(date -u +%F).csv"

# ── Feeder ────────────────────────────────────────────────────────────────────
START_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"   # so we only read this run's server logs

info "Starting feeder (interval ${INTERVAL}s)..."
"$PYTHON" -u sim/feeder.py "$CSV" "$STOCK" --interval "$INTERVAL" > "$OUT/feeder.log" 2>&1 &
FEEDER_PID=$!

cleanup() {
    kill "$FEEDER_PID" 2>/dev/null || true
    [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# ── Clients, staggered ────────────────────────────────────────────────────────
PIDS=()
LABELS=()
OFFSETS=()
VICTIM_LABEL=""
VICTIM_PID=""
elapsed=0
for i in $(seq 1 "$CLIENTS"); do
    offset=$(( START_DELAY + (i - 1) * STAGGER ))
    sleep $(( offset - elapsed ))
    elapsed=$offset
    label="c${i}"
    info "t+${offset}s: starting client ${label}"
    "$PYTHON" -u sim/client.py "$STOCK" \
        --original "$CSV" \
        --label "$label" \
        --idle-timeout "$IDLE" > "$OUT/client_${label}.log" 2>&1 &
    PIDS+=("$!")
    LABELS+=("$label")
    OFFSETS+=("$offset")

    # One extra client alongside the first, SIGKILLed mid-stream. Survivors must be
    # unaffected: the server has to notice the dead socket and cancel that connection's
    # stream task without touching anyone else's.
    if [ "$i" = "1" ] && [ "$KILL_AFTER" != "0" ]; then
        VICTIM_LABEL="victim"
        info "t+${offset}s: starting client ${VICTIM_LABEL} (SIGKILL at t+$(( offset + KILL_AFTER ))s)"
        "$PYTHON" -u sim/client.py "$STOCK" \
            --original "$CSV" \
            --label "$VICTIM_LABEL" \
            --idle-timeout "$IDLE" > "$OUT/client_${VICTIM_LABEL}.log" 2>&1 &
        VICTIM_PID=$!
        ( sleep "$KILL_AFTER"; kill -9 "$VICTIM_PID" 2>/dev/null ) &
    fi
done

info "All clients connected. Waiting for the stream to drain..."
for pid in "${PIDS[@]}"; do
    wait "$pid" || true   # client exits 1 on its own integrity check; not our verdict
done
[ -n "$VICTIM_PID" ] && { wait "$VICTIM_PID" 2>/dev/null || true; }
kill "$FEEDER_PID" 2>/dev/null || true

# ── Report ────────────────────────────────────────────────────────────────────
if [ "${SERVER:-docker}" != "host" ]; then
    docker logs --since "$START_TS" stonks-ws-server > "$OUT/server.log" 2>&1 || true
fi

echo ""
"$PYTHON" "$SCRIPT_DIR/report_multiclient.py" \
    --symbol "$SYMBOL" \
    --output-dir "$OUT" \
    --source "$CSV" \
    --labels "${LABELS[@]}" \
    --offsets "${OFFSETS[@]}" \
    --server-log "$OUT/server.log" \
    ${VICTIM_LABEL:+--victim "$VICTIM_LABEL"}
