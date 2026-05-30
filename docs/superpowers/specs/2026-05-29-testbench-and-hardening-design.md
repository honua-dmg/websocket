# Testbench & Hardening Design

**Date:** 2026-05-29  
**Status:** Approved

## Overview

Build a 3-process simulation testbench that mirrors production as closely as possible, and harden the existing WebSocket server against the failure modes the testbench will exercise.

The testbench consists of:
1. A shell script that boots Redis and the WebSocket server as Docker containers
2. A local Python feeder that splits a CSV and streams the live half to Redis
3. A local Python client that connects to the WebSocket, saves all received rows, and validates end-to-end data integrity

---

## Architecture

```
sim/
  run_testbench.sh       boots Redis + WebSocket server containers, waits for health
  feeder.py              splits CSV → writes history half to disk, streams live half to Redis
  client.py              connects to WebSocket, saves rows to sample file, integrity check
  output/                gitignored — sample_<SYMBOL>.csv files written here

Hardened (existing files):
  config.py              validates required env vars on import
  csv_reader.py          validates file exists, has rows, handles malformed rows
  redis_consumer.py      connection retry with backoff, handles missing stream key
  main.py                timeout on initial message, validates JSON shape
```

### Process startup order

1. `run_testbench.sh` starts Redis container → polls until `PING` responds (10 attempts, 1s apart)
2. Builds and starts WebSocket server container → polls until port 8765 is open (15 attempts, 1s apart)
3. Prints instructions: run feeder, then client in separate terminals

The feeder and client are intentionally manual — matches production where pipeline and consumers start independently.

---

## Components

### `sim/run_testbench.sh`

- Checks Docker daemon is running; exits with clear message if not
- Starts Redis: `docker run -d --name stonks-redis -p 6379:6379 redis:alpine` (skips if container already running)
- Health-polls Redis with `redis-cli ping` — exits with error if never responds
- Builds WebSocket server image from existing `Dockerfile`
- Starts server container with `--env-file .env.docker`, port 8765 exposed
- Health-polls server port (TCP connect) — exits with error if never responds
- Prints next-step instructions on success

---

### `sim/feeder.py`

```
usage: feeder.py <csv_path> <EXCHANGE:SYMBOL> [--interval 0.1] [--redis-url redis://localhost:6379]
```

**Responsibilities:**
- Validate CSV exists, has at least 2 rows, contains expected columns
- Split rows into two equal halves (first half = history, second half = live)
- Write first half to `DATA_ROOT/{exchange}/{symbol}/YYYY-MM-DD.csv` (creates dirs if needed)
- Connect to Redis with exponential backoff (3 retries: 1s → 2s → 4s) — exits with clear error if unreachable
- Stream second half to Redis at `--interval` seconds per row via `XADD {symbol} * data <json>`
- Print progress per row: `[FEEDER] row 51/100 → IBM`
- Handle `KeyboardInterrupt` cleanly — print row count sent before exit

**No integrity logic** — the feeder's job is only to split and stream correctly.

---

### `sim/client.py`

```
usage: client.py <EXCHANGE:SYMBOL> [--host localhost] [--port 8765] [--timeout 30]
```

**Responsibilities:**
- Validate `EXCHANGE:SYMBOL` format before attempting connection
- Connect with retry — polls up to `--timeout` seconds (1s between attempts)
- Send `{"stock": "EXCHANGE:SYMBOL"}` subscription message
- Print received messages with prefixes:
  ```
  [HISTORY] {"price": "142.50", "volume": "1200", ...}
  [LIVE]    {"price": "143.10", "volume": "800", ...}
  [ERROR]   server sent: no CSV history for NYSE:IBM today
  ```
- Save every received row (history + live, in arrival order) to `sim/output/sample_<SYMBOL>.csv`

**Integrity check (runs on stream end or KeyboardInterrupt):**
- Load original CSV (passed as required `--original <csv_path>` arg) and `sample_<SYMBOL>.csv`
- Compare row count — flag mismatch
- Compare row-by-row in order — report first N mismatches
- Check for duplicates within sample (by row hash)
- Print: `[INTEGRITY] ✓ 200/200 rows matched, 0 duplicates` or detailed diff
- Exit code 1 on any mismatch

**Error handling:**
- Server not yet up → retries silently, prints "waiting for server..." once
- Unexpected mid-stream disconnect → print counts received so far, exit cleanly
- Server sends `{"error": ...}` → print clearly, exit with code 1
- `KeyboardInterrupt` → clean exit with summary and integrity check

---

## Hardening (existing files)

### `config.py`
- Validate `REDIS_URL` and `DATA_ROOT` are present on import
- Raise `RuntimeError` naming the missing variable — no silent `KeyError` later

### `csv_reader.py`
- Check file exists before opening — raise `FileNotFoundError` with full resolved path
- Check CSV has at least one data row after header — raise `ValueError` if empty
- Skip and log malformed rows (wrong column count) rather than crashing mid-stream

### `redis_consumer.py`
- `get_client()`: test connection with `PING` on first use, retry with exponential backoff (3 attempts: 1s → 2s → 4s), raise `ConnectionError` with `REDIS_URL` in message if all fail
- `tail_stream()`: if stream key doesn't exist yet, wait and retry; log "waiting for stream `{symbol}`..." so it's clear it's not hung
- Wrap `xread` in try/except for mid-stream Redis disconnects — attempt reconnect once before propagating

### `main.py`
- Add 10s timeout waiting for initial subscription message — close with clear error if client connects but never sends
- Validate received JSON has a `stock` key — send `{"error": "expected {\"stock\": \"EXCHANGE:SYMBOL\"}"}` if not
- Catch `json.JSONDecodeError` for non-JSON client messages

---

## Data Flow

```
Original CSV (200 rows)
        │
        ├── first 100 rows ──► DATA_ROOT/EXCHANGE/SYMBOL/YYYY-MM-DD.csv
        │                              │
        │                        WebSocket server reads via csv_reader.py
        │                              │
        │                        Client receives as [HISTORY]
        │
        └── last 100 rows ───► Redis stream "SYMBOL" (via feeder.py, 100ms/row)
                                       │
                                 WebSocket server tails via redis_consumer.py
                                       │
                                 Client receives as [LIVE]
                                       │
                               sim/output/sample_SYMBOL.csv (200 rows)
                                       │
                               Integrity check vs original ──► pass/fail
```

---

## Out of Scope

- Multi-symbol simulation (one symbol per testbench run)
- Persistent Redis data between runs (containers are ephemeral)
- CI integration (though exit code 1 on integrity failure makes it possible later)
