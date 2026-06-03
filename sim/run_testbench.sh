#!/usr/bin/env bash
set -euo pipefail

REDIS_CONTAINER="stonks-redis"
SERVER_CONTAINER="stonks-ws-server"
SERVER_PORT="${PORT:-8765}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

die()  { echo "[TESTBENCH] ERROR: $*" >&2; exit 1; }
info() { echo "[TESTBENCH] $*"; }

# ── Check Docker ──────────────────────────────────────────────────────────────
docker info >/dev/null 2>&1 || die "Docker is not running. Start Docker Desktop and try again."

# ── Redis ─────────────────────────────────────────────────────────────────────
if docker ps --format '{{.Names}}' | grep -q "^${REDIS_CONTAINER}$"; then
    info "Redis container already running — skipping."
else
    info "Starting Redis container..."
    docker run -d --name "$REDIS_CONTAINER" -p 6379:6379 redis:alpine >/dev/null
fi

info "Waiting for Redis to be ready..."
for i in $(seq 1 10); do
    if docker exec "$REDIS_CONTAINER" redis-cli ping 2>/dev/null | grep -q PONG; then
        info "Redis is ready."
        break
    fi
    [ "$i" -eq 10 ] && die "Redis did not respond after 10 attempts. Check: docker logs $REDIS_CONTAINER"
    sleep 1
done

# ── WebSocket server ──────────────────────────────────────────────────────────
info "Building WebSocket server image..."
docker build -t "$SERVER_CONTAINER" "$PROJECT_DIR" -f "$PROJECT_DIR/Dockerfile" \
    --quiet || die "Docker build failed."

if docker ps --format '{{.Names}}' | grep -q "^${SERVER_CONTAINER}$"; then
    info "Stopping existing server container..."
    docker rm -f "$SERVER_CONTAINER" >/dev/null
fi

info "Starting WebSocket server container..."
mkdir -p "$PROJECT_DIR/data"
docker run -d \
    --name "$SERVER_CONTAINER" \
    --env-file "$PROJECT_DIR/.env.docker" \
    -p "${SERVER_PORT}:${SERVER_PORT}" \
    --add-host=host.docker.internal:host-gateway \
    -v "$PROJECT_DIR/data:/data" \
    "$SERVER_CONTAINER" >/dev/null

info "Waiting for WebSocket server on port ${SERVER_PORT}..."
for i in $(seq 1 15); do
    if nc -z localhost "$SERVER_PORT" 2>/dev/null; then
        info "Server is ready."
        break
    fi
    [ "$i" -eq 15 ] && die "Server did not open port ${SERVER_PORT} after 15 attempts. Check: docker logs $SERVER_CONTAINER"
    sleep 1
done

# ── Instructions ──────────────────────────────────────────────────────────────
echo ""
info "Testbench ready. Run in separate terminals:"
echo ""
echo "  Terminal 1 (feed data):"
echo "    python sim/feeder.py <path/to/file.csv> EXCHANGE:SYMBOL"
echo ""
echo "  Terminal 2 (connect client):"
echo "    python sim/client.py EXCHANGE:SYMBOL --original <path/to/file.csv>"
echo ""
echo "  To tear down:"
echo "    docker rm -f $REDIS_CONTAINER $SERVER_CONTAINER"
echo ""
