# Testbench & Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden the existing WebSocket server against failure modes and build a 3-process simulation testbench (shell bootstrapper, CSV feeder, WebSocket client) that validates end-to-end data integrity.

**Architecture:** Existing server files (`config.py`, `csv_reader.py`, `redis_consumer.py`, `main.py`) are hardened in-place with TDD. A new `sim/` directory holds the testbench scripts. The client saves all received rows to `sim/output/sample_<SYMBOL>.csv` and compares them against the original CSV to verify no rows were dropped, duplicated, or reordered through the full pipeline.

**Tech Stack:** Python 3.13, FastAPI, redis-py asyncio, aiofiles, websockets, pytest, pytest-asyncio

---

## File Map

| Action | Path | Responsibility |
|--------|------|---------------|
| Modify | `config.py` | Validate required env vars on import |
| Modify | `csv_reader.py` | File existence, empty CSV, malformed row handling |
| Modify | `redis_consumer.py` | Connection retry, stream key wait, reconnect on error |
| Modify | `main.py` | Subscription timeout, JSON validation, stock key check |
| Modify | `requirements.txt` | Add `websockets` |
| Create | `requirements-dev.txt` | `pytest`, `pytest-asyncio` |
| Create | `pytest.ini` | `asyncio_mode = auto` |
| Create | `conftest.py` | Set env vars before any test import |
| Create | `tests/__init__.py` | Make tests a package |
| Create | `tests/test_config.py` | Tests for `_require` helper |
| Create | `tests/test_csv_reader.py` | Tests for file validation + malformed rows |
| Create | `tests/test_redis_consumer.py` | Tests for retry logic and stream waiting |
| Create | `tests/test_main.py` | Tests for WS timeout + JSON validation |
| Create | `sim/__init__.py` | Make sim importable in tests |
| Create | `sim/feeder.py` | CSV split, history write, Redis stream |
| Create | `sim/client.py` | WS client, row saving, integrity check |
| Create | `sim/run_testbench.sh` | Docker container bootstrapper |
| Create | `sim/output/.gitkeep` | Track output dir, ignore CSV files |
| Create | `tests/test_feeder.py` | Tests for feeder logic |
| Create | `tests/test_client.py` | Tests for integrity check logic |

---

## Task 1: Test infrastructure

**Files:**
- Create: `requirements-dev.txt`
- Create: `pytest.ini`
- Create: `conftest.py`
- Create: `tests/__init__.py`

- [ ] **Step 1: Create `requirements-dev.txt`**

```
pytest
pytest-asyncio
```

- [ ] **Step 2: Create `pytest.ini`**

```ini
[pytest]
asyncio_mode = auto
```

- [ ] **Step 3: Create `conftest.py` at project root**

This file must set env vars before any module-level import runs — config.py validates them at import time.

```python
import os

os.environ.setdefault("REDIS_URL", "redis://localhost:6379")
os.environ.setdefault("DATA_ROOT", "/tmp/test_stonks")
os.environ.setdefault("PORT", "8765")
```

- [ ] **Step 4: Create `tests/__init__.py`**

Empty file:
```python
```

- [ ] **Step 5: Install dev dependencies and verify pytest runs**

```bash
pip install -r requirements-dev.txt
pytest --collect-only
```

Expected: `no tests ran` with exit code 5 (no tests collected yet — that's fine).

- [ ] **Step 6: Add `websockets` to `requirements.txt`**

```
fastapi
uvicorn[standard]
redis[asyncio]
aiofiles
python-dotenv
websockets
```

- [ ] **Step 7: Install updated requirements**

```bash
pip install -r requirements.txt
```

Expected: websockets installs without error.

- [ ] **Step 8: Commit**

```bash
git add requirements.txt requirements-dev.txt pytest.ini conftest.py tests/__init__.py
git commit -m "chore: add test infrastructure and websockets dependency"
```

---

## Task 2: Harden `config.py`

**Files:**
- Modify: `config.py`
- Create: `tests/test_config.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_config.py
import os
import pytest


def test_require_raises_runtime_error_when_var_missing(monkeypatch):
    monkeypatch.delenv("MISSING_VAR_XYZ", raising=False)
    # Import after env is patched
    import importlib
    import config as cfg
    with pytest.raises(RuntimeError, match="MISSING_VAR_XYZ"):
        cfg._require("MISSING_VAR_XYZ")


def test_require_returns_value_when_var_is_set(monkeypatch):
    monkeypatch.setenv("SOME_TEST_VAR", "hello")
    import config as cfg
    assert cfg._require("SOME_TEST_VAR") == "hello"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_config.py -v
```

Expected: `AttributeError: module 'config' has no attribute '_require'`

- [ ] **Step 3: Implement `_require` in `config.py`**

Replace the entire file:

```python
import os
from dotenv import load_dotenv

load_dotenv(".env.local" if os.path.exists(".env.local") else ".env.docker")


def _require(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(
            f"Required environment variable '{name}' is not set. "
            f"Check .env.local or .env.docker."
        )
    return value


REDIS_URL = _require("REDIS_URL")
DATA_ROOT = _require("DATA_ROOT")
PORT = int(os.getenv("PORT", "8765"))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_config.py -v
```

Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add config.py tests/test_config.py
git commit -m "feat: validate required env vars in config with clear RuntimeError"
```

---

## Task 3: Harden `csv_reader.py`

**Files:**
- Modify: `csv_reader.py`
- Create: `tests/test_csv_reader.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_csv_reader.py
import csv
import pytest
from pathlib import Path


@pytest.fixture
def tmp_csv(tmp_path):
    """Helper: write a CSV file and return its path."""
    def _make(rows: list[dict], filename="test.csv") -> Path:
        path = tmp_path / filename
        if rows:
            fieldnames = list(rows[0].keys())
            with open(path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        else:
            path.write_text("price,volume\n")  # header only, no rows
        return path
    return _make


@pytest.fixture(autouse=True)
def patch_data_root(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    import importlib, config
    importlib.reload(config)
    import csv_reader
    importlib.reload(csv_reader)


async def collect(gen) -> list:
    return [item async for item in gen]


async def test_stream_csv_raises_file_not_found(tmp_path):
    from csv_reader import stream_csv
    with pytest.raises(FileNotFoundError, match=str(tmp_path)):
        await collect(stream_csv("NYSE", "IBM", day="2020-01-01"))


async def test_stream_csv_raises_value_error_on_empty_csv(tmp_path, monkeypatch):
    day = "2020-01-01"
    out_dir = tmp_path / "NYSE" / "IBM"
    out_dir.mkdir(parents=True)
    (out_dir / f"{day}.csv").write_text("price,volume\n")  # header only

    from csv_reader import stream_csv
    with pytest.raises(ValueError, match="empty"):
        await collect(stream_csv("NYSE", "IBM", day=day))


async def test_stream_csv_skips_malformed_rows(tmp_path):
    import csv_reader
    day = "2020-01-01"
    out_dir = tmp_path / "NYSE" / "IBM"
    out_dir.mkdir(parents=True)
    csv_path = out_dir / f"{day}.csv"
    # Row 2 has an extra field (malformed)
    csv_path.write_text("price,volume\n100,200\n150,300,EXTRA\n120,250\n")

    results = await collect(csv_reader.stream_csv("NYSE", "IBM", day=day))
    # Malformed row skipped, two good rows returned
    assert len(results) == 2
    assert results[0] == {"price": "100", "volume": "200"}
    assert results[1] == {"price": "120", "volume": "250"}


async def test_stream_csv_yields_all_rows_for_valid_file(tmp_path):
    import csv_reader
    day = "2020-01-01"
    out_dir = tmp_path / "NYSE" / "IBM"
    out_dir.mkdir(parents=True)
    (out_dir / f"{day}.csv").write_text("price,volume\n100,200\n150,300\n")

    results = await collect(csv_reader.stream_csv("NYSE", "IBM", day=day))
    assert results == [{"price": "100", "volume": "200"}, {"price": "150", "volume": "300"}]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_csv_reader.py -v
```

Expected: multiple failures — `FileNotFoundError` is raised but without the path, `ValueError` not raised, malformed rows crash instead of skip.

- [ ] **Step 3: Implement hardening in `csv_reader.py`**

```python
import csv
import io
import os
import sys
from datetime import date
from typing import AsyncIterator

import aiofiles

from config import DATA_ROOT


async def stream_csv(exchange: str, symbol: str, day: str | None = None) -> AsyncIterator[dict]:
    day = day or date.today().isoformat()
    path = f"{DATA_ROOT}/{exchange}/{symbol}/{day}.csv"

    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found: {os.path.abspath(path)}")

    async with aiofiles.open(path, mode="r") as f:
        content = await f.read()

    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)

    if not rows:
        raise ValueError(f"CSV is empty (no data rows): {path}")

    for i, row in enumerate(rows):
        # csv.DictReader puts extra fields under None key; missing fields have None values
        if None in row or any(v is None for v in row.values()):
            print(f"[csv_reader] skipping malformed row {i + 1} in {path}", file=sys.stderr)
            continue
        yield dict(row)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_csv_reader.py -v
```

Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add csv_reader.py tests/test_csv_reader.py
git commit -m "feat: harden csv_reader with file validation and malformed row handling"
```

---

## Task 4: Harden `redis_consumer.py`

**Files:**
- Modify: `redis_consumer.py`
- Create: `tests/test_redis_consumer.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_redis_consumer.py
import json
import pytest
from unittest.mock import AsyncMock, patch


@pytest.fixture(autouse=True)
def reset_redis_client():
    import redis_consumer
    redis_consumer._client = None
    yield
    redis_consumer._client = None


async def test_ping_with_retry_raises_connection_error_after_all_attempts():
    mock_client = AsyncMock()
    mock_client.ping.side_effect = Exception("connection refused")

    with patch("redis_consumer.get_client", return_value=mock_client), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        from redis_consumer import _ping_with_retry
        with pytest.raises(ConnectionError, match="redis://localhost:6379"):
            await _ping_with_retry(max_attempts=3)

    assert mock_client.ping.call_count == 3


async def test_ping_with_retry_succeeds_on_third_attempt():
    mock_client = AsyncMock()
    mock_client.ping.side_effect = [Exception("refused"), Exception("refused"), None]

    with patch("redis_consumer.get_client", return_value=mock_client), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        from redis_consumer import _ping_with_retry
        result = await _ping_with_retry(max_attempts=3)

    assert result is mock_client
    assert mock_client.ping.call_count == 3


async def test_get_stream_tip_returns_zero_for_empty_stream():
    mock_client = AsyncMock()
    mock_client.ping.return_value = True
    mock_client.xrevrange.return_value = []

    with patch("redis_consumer.get_client", return_value=mock_client):
        from redis_consumer import get_stream_tip
        tip = await get_stream_tip("IBM")

    assert tip == "0"


async def test_get_stream_tip_returns_latest_id():
    mock_client = AsyncMock()
    mock_client.ping.return_value = True
    mock_client.xrevrange.return_value = [("1700000000000-0", {"data": "{}"})]

    with patch("redis_consumer.get_client", return_value=mock_client):
        from redis_consumer import get_stream_tip
        tip = await get_stream_tip("IBM")

    assert tip == "1700000000000-0"


async def test_tail_stream_yields_parsed_json():
    mock_client = AsyncMock()
    mock_client.ping.return_value = True
    mock_client.xread.side_effect = [
        [("IBM", [("1-1", {"data": json.dumps({"price": "100", "volume": "500"})})])],
        [("IBM", [("1-2", {"data": json.dumps({"price": "101", "volume": "600"})})])],
    ]

    with patch("redis_consumer.get_client", return_value=mock_client):
        from redis_consumer import tail_stream
        results = []
        async for tick in tail_stream("IBM", "0"):
            results.append(tick)
            if len(results) == 2:
                break

    assert results == [{"price": "100", "volume": "500"}, {"price": "101", "volume": "600"}]


async def test_tail_stream_logs_waiting_when_stream_empty(capsys):
    mock_client = AsyncMock()
    mock_client.ping.return_value = True
    # First call empty, second call has data
    mock_client.xread.side_effect = [
        [],
        [("IBM", [("1-1", {"data": json.dumps({"price": "100"})})])],
    ]

    with patch("redis_consumer.get_client", return_value=mock_client):
        from redis_consumer import tail_stream
        async for _ in tail_stream("IBM", "0"):
            break

    captured = capsys.readouterr()
    assert "Waiting for stream" in captured.err or "Waiting for stream" in captured.out
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_redis_consumer.py -v
```

Expected: failures — `_ping_with_retry` doesn't exist, `tail_stream` doesn't log waiting.

- [ ] **Step 3: Implement hardening in `redis_consumer.py`**

```python
import asyncio
import json
import logging
from typing import AsyncIterator

import redis.asyncio as aioredis

from config import REDIS_URL

logger = logging.getLogger(__name__)

_client: aioredis.Redis | None = None


def get_client() -> aioredis.Redis:
    global _client
    if _client is None:
        _client = aioredis.from_url(REDIS_URL, decode_responses=True)
    return _client


async def _ping_with_retry(max_attempts: int = 3) -> aioredis.Redis:
    client = get_client()
    delay = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            await client.ping()
            return client
        except Exception as exc:
            if attempt == max_attempts:
                raise ConnectionError(
                    f"Cannot reach Redis at {REDIS_URL} after {max_attempts} attempts: {exc}"
                ) from exc
            logger.warning(
                "Redis unavailable (attempt %d/%d), retrying in %.0fs...",
                attempt, max_attempts, delay,
            )
            await asyncio.sleep(delay)
            delay *= 2
    return client  # unreachable


async def get_stream_tip(symbol: str) -> str:
    client = await _ping_with_retry()
    entries = await client.xrevrange(symbol, count=1)
    return entries[0][0] if entries else "0"


async def tail_stream(symbol: str, last_id: str) -> AsyncIterator[dict]:
    client = await _ping_with_retry()
    current_id = last_id
    waiting_logged = False

    while True:
        try:
            results = await client.xread({symbol: current_id}, count=100, block=100)
        except Exception as exc:
            logger.warning("Redis read error, attempting reconnect: %s", exc)
            client = await _ping_with_retry()
            continue

        if not results:
            if not waiting_logged:
                logger.info("Waiting for stream '%s'...", symbol)
                waiting_logged = True
            continue

        waiting_logged = False
        for _stream, messages in results:
            for msg_id, fields in messages:
                current_id = msg_id
                yield json.loads(fields["data"])
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_redis_consumer.py -v
```

Expected: `6 passed`

- [ ] **Step 5: Commit**

```bash
git add redis_consumer.py tests/test_redis_consumer.py
git commit -m "feat: harden redis_consumer with connection retry and stream key wait"
```

---

## Task 5: Harden `main.py`

**Files:**
- Modify: `main.py`
- Create: `tests/test_main.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_main.py
import json
import pytest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    import main
    return TestClient(main.app)


def test_invalid_json_returns_error(client):
    with client.websocket_connect("/ws") as ws:
        ws.send_text("not-json")
        data = ws.receive_json()
    assert "error" in data
    assert "JSON" in data["error"]


def test_missing_stock_key_returns_error(client):
    with client.websocket_connect("/ws") as ws:
        ws.send_json({"wrong_key": "NYSE:IBM"})
        data = ws.receive_json()
    assert "error" in data
    assert "stock" in data["error"]


def test_invalid_stock_format_returns_error(client):
    with client.websocket_connect("/ws") as ws:
        ws.send_json({"stock": "NODIVIDER"})
        data = ws.receive_json()
    assert "error" in data
    assert "EXCHANGE:SYMBOL" in data["error"]


def test_subscription_timeout_returns_error(client, monkeypatch):
    import main
    monkeypatch.setattr(main, "SUBSCRIPTION_TIMEOUT", 0.05)
    with client.websocket_connect("/ws") as ws:
        # send nothing — server should time out and send error
        data = ws.receive_json()
    assert "error" in data
    assert "subscription" in data["error"].lower()


def test_valid_subscription_streams_history_then_live(client):
    mock_rows = [{"price": "100", "volume": "500"}]

    async def mock_stream_csv(exchange, symbol, day=None):
        for row in mock_rows:
            yield row

    async def mock_tail_stream(symbol, last_id):
        # yield one live tick then stop (simulate by raising StopAsyncIteration)
        yield {"price": "101", "volume": "600"}
        return

    with patch("main.stream_csv", mock_stream_csv), \
         patch("main.tail_stream", mock_tail_stream), \
         patch("main.get_stream_tip", new_callable=AsyncMock, return_value="0"):
        with client.websocket_connect("/ws") as ws:
            ws.send_json({"stock": "NYSE:IBM"})
            history_msg = ws.receive_json()
            live_msg = ws.receive_json()

    assert history_msg == {"source": "history", "data": {"price": "100", "volume": "500"}}
    assert live_msg == {"source": "live", "data": {"price": "101", "volume": "600"}}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_main.py -v
```

Expected: `test_invalid_json_returns_error` fails (currently crashes), timeout test fails (no timeout logic).

- [ ] **Step 3: Implement hardening in `main.py`**

```python
import asyncio
import json
import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from csv_reader import stream_csv
from redis_consumer import get_stream_tip, tail_stream

app = FastAPI()

SUBSCRIPTION_TIMEOUT = float(os.getenv("WS_SUBSCRIPTION_TIMEOUT", "10"))


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    try:
        try:
            raw = await asyncio.wait_for(
                websocket.receive_text(), timeout=SUBSCRIPTION_TIMEOUT
            )
        except asyncio.TimeoutError:
            await websocket.send_json(
                {"error": f"no subscription received within {SUBSCRIPTION_TIMEOUT}s"}
            )
            await websocket.close()
            return

        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            await websocket.send_json({"error": "expected JSON message"})
            await websocket.close()
            return

        if not isinstance(msg, dict) or "stock" not in msg:
            await websocket.send_json({"error": 'expected {"stock": "EXCHANGE:SYMBOL"}'})
            await websocket.close()
            return

        stock = msg["stock"]
        if ":" not in stock:
            await websocket.send_json(
                {"error": f"invalid stock format '{stock}', expected EXCHANGE:SYMBOL"}
            )
            await websocket.close()
            return

        exchange, symbol = stock.split(":", 1)

        bookmark = await get_stream_tip(symbol)

        try:
            async for row in stream_csv(exchange, symbol):
                await websocket.send_json({"source": "history", "data": row})
        except FileNotFoundError:
            await websocket.send_json({"error": f"no CSV history for {stock} today"})

        async for tick in tail_stream(symbol, last_id=bookmark):
            await websocket.send_json({"source": "live", "data": tick})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"error": str(e)})
        except Exception:
            pass
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_main.py -v
```

Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add main.py tests/test_main.py
git commit -m "feat: harden main.py with subscription timeout and JSON validation"
```

---

## Task 6: Build `sim/feeder.py`

**Files:**
- Create: `sim/__init__.py`
- Create: `sim/feeder.py`
- Create: `tests/test_feeder.py`

- [ ] **Step 1: Create `sim/__init__.py`**

Empty file:
```python
```

- [ ] **Step 2: Write the failing tests**

```python
# tests/test_feeder.py
import csv
import json
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, patch


# ── load_and_validate_csv ────────────────────────────────────────────────────

def test_load_validates_csv_must_exist(tmp_path):
    from sim.feeder import load_and_validate_csv
    with pytest.raises(SystemExit):
        load_and_validate_csv(str(tmp_path / "missing.csv"))


def test_load_validates_csv_must_have_two_rows(tmp_path):
    p = tmp_path / "one_row.csv"
    p.write_text("price,volume\n100,200\n")
    from sim.feeder import load_and_validate_csv
    with pytest.raises(SystemExit):
        load_and_validate_csv(str(p))


def test_load_returns_fieldnames_and_rows(tmp_path):
    p = tmp_path / "good.csv"
    p.write_text("price,volume\n100,200\n150,300\n120,250\n")
    from sim.feeder import load_and_validate_csv
    fieldnames, rows = load_and_validate_csv(str(p))
    assert fieldnames == ["price", "volume"]
    assert len(rows) == 3
    assert rows[0] == {"price": "100", "volume": "200"}


# ── write_history ────────────────────────────────────────────────────────────

def test_write_history_creates_file_with_correct_rows(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    import importlib, config
    importlib.reload(config)
    import sim.feeder as feeder_mod
    importlib.reload(feeder_mod)

    rows = [{"price": "100", "volume": "200"}, {"price": "150", "volume": "300"}]
    out_path = feeder_mod.write_history("NYSE", "IBM", ["price", "volume"], rows)

    assert out_path.exists()
    with open(out_path, newline="") as f:
        written = list(csv.DictReader(f))
    assert written == rows


def test_write_history_creates_parent_directories(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    import importlib, config
    importlib.reload(config)
    import sim.feeder as feeder_mod
    importlib.reload(feeder_mod)

    rows = [{"price": "100"}, {"price": "150"}]
    out_path = feeder_mod.write_history("NASDAQ", "AAPL", ["price"], rows)
    assert out_path.exists()


# ── stream_to_redis ──────────────────────────────────────────────────────────

async def test_stream_to_redis_sends_all_rows():
    mock_client = AsyncMock()
    rows = [{"price": "100", "volume": "200"}, {"price": "150", "volume": "300"}]

    from sim.feeder import stream_to_redis
    with patch("asyncio.sleep", new_callable=AsyncMock):
        sent = await stream_to_redis(mock_client, "IBM", rows, interval=0.0)

    assert sent == 2
    calls = mock_client.xadd.call_args_list
    assert len(calls) == 2
    assert json.loads(calls[0].args[1]["data"]) == rows[0]
    assert json.loads(calls[1].args[1]["data"]) == rows[1]


async def test_stream_to_redis_returns_partial_count_on_interrupt():
    mock_client = AsyncMock()
    # Raise KeyboardInterrupt on second xadd
    mock_client.xadd.side_effect = [None, KeyboardInterrupt()]
    rows = [{"price": "100"}, {"price": "150"}, {"price": "120"}]

    from sim.feeder import stream_to_redis
    with patch("asyncio.sleep", new_callable=AsyncMock):
        sent = await stream_to_redis(mock_client, "IBM", rows, interval=0.0)

    assert sent == 1
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
pytest tests/test_feeder.py -v
```

Expected: `ImportError: cannot import name 'load_and_validate_csv' from 'sim.feeder'`

- [ ] **Step 4: Implement `sim/feeder.py`**

```python
#!/usr/bin/env python3
"""
Splits a CSV into history (first half → disk) and live (second half → Redis stream).

Usage:
    python sim/feeder.py <csv_path> <EXCHANGE:SYMBOL> [--interval 0.1] [--redis-url redis://localhost:6379]

Run from project root.
"""
import argparse
import asyncio
import csv
import json
import sys
from datetime import date
from pathlib import Path

import redis.asyncio as aioredis

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATA_ROOT  # noqa: E402


def load_and_validate_csv(csv_path: str) -> tuple[list[str], list[dict]]:
    path = Path(csv_path)
    if not path.exists():
        print(f"[FEEDER] ERROR: CSV not found: {path.resolve()}", file=sys.stderr)
        sys.exit(1)

    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    if not fieldnames:
        print(f"[FEEDER] ERROR: CSV has no header: {csv_path}", file=sys.stderr)
        sys.exit(1)

    if len(rows) < 2:
        print(
            f"[FEEDER] ERROR: CSV must have at least 2 rows to split, got {len(rows)}",
            file=sys.stderr,
        )
        sys.exit(1)

    return fieldnames, rows


def write_history(exchange: str, symbol: str, fieldnames: list[str], rows: list[dict]) -> Path:
    today = date.today().isoformat()
    out_dir = Path(DATA_ROOT) / exchange / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{today}.csv"

    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return out_path


async def connect_redis(redis_url: str, max_attempts: int = 3) -> aioredis.Redis:
    client = aioredis.from_url(redis_url, decode_responses=True)
    delay = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            await client.ping()
            return client
        except Exception as exc:
            if attempt == max_attempts:
                print(
                    f"[FEEDER] ERROR: Cannot connect to Redis at {redis_url} "
                    f"after {max_attempts} attempts: {exc}",
                    file=sys.stderr,
                )
                sys.exit(1)
            print(
                f"[FEEDER] Redis unavailable (attempt {attempt}/{max_attempts}), "
                f"retrying in {delay:.0f}s...",
                file=sys.stderr,
            )
            await asyncio.sleep(delay)
            delay *= 2
    return client  # unreachable


async def stream_to_redis(
    client: aioredis.Redis, symbol: str, rows: list[dict], interval: float
) -> int:
    sent = 0
    total = len(rows)
    try:
        for row in rows:
            await client.xadd(symbol, {"data": json.dumps(row)})
            sent += 1
            print(f"[FEEDER] row {sent}/{total} → {symbol}")
            await asyncio.sleep(interval)
    except KeyboardInterrupt:
        pass
    return sent


async def main() -> None:
    parser = argparse.ArgumentParser(description="Split CSV and stream live half to Redis")
    parser.add_argument("csv_path", help="Path to the source CSV file")
    parser.add_argument("stock", help="Stock in EXCHANGE:SYMBOL format")
    parser.add_argument("--interval", type=float, default=0.1, help="Seconds between rows (default: 0.1)")
    parser.add_argument("--redis-url", default="redis://localhost:6379")
    args = parser.parse_args()

    if ":" not in args.stock:
        print(f"[FEEDER] ERROR: stock must be EXCHANGE:SYMBOL, got '{args.stock}'", file=sys.stderr)
        sys.exit(1)

    exchange, symbol = args.stock.split(":", 1)
    fieldnames, rows = load_and_validate_csv(args.csv_path)
    mid = len(rows) // 2
    history_rows, live_rows = rows[:mid], rows[mid:]

    print(f"[FEEDER] {len(rows)} total rows → {len(history_rows)} history, {len(live_rows)} live")

    history_path = write_history(exchange, symbol, fieldnames, history_rows)
    print(f"[FEEDER] History written to {history_path}")

    client = await connect_redis(args.redis_url)
    print(f"[FEEDER] Connected to Redis, streaming {len(live_rows)} rows to '{symbol}'...")

    sent = await stream_to_redis(client, symbol, live_rows, args.interval)
    print(f"[FEEDER] Done — {sent}/{len(live_rows)} rows sent to Redis stream '{symbol}'")
    await client.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
pytest tests/test_feeder.py -v
```

Expected: `7 passed`

- [ ] **Step 6: Commit**

```bash
git add sim/__init__.py sim/feeder.py tests/test_feeder.py
git commit -m "feat: add sim/feeder.py — CSV splitter and Redis live streamer"
```

---

## Task 7: Build `sim/client.py`

**Files:**
- Create: `sim/client.py`
- Create: `sim/output/.gitkeep`
- Create: `tests/test_client.py`
- Modify: `.gitignore` (add `sim/output/*.csv`)

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_client.py
import csv
import pytest
from pathlib import Path


@pytest.fixture
def write_csv(tmp_path):
    def _make(filename: str, rows: list[dict]) -> Path:
        path = tmp_path / filename
        if rows:
            fieldnames = list(rows[0].keys())
            with open(path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        return path
    return _make


def test_integrity_check_passes_for_matching_rows(write_csv):
    rows = [{"price": str(i), "volume": str(i * 10)} for i in range(10)]
    original = write_csv("original.csv", rows)

    from sim.client import run_integrity_check
    passed = run_integrity_check(str(original), rows)
    assert passed is True


def test_integrity_check_fails_on_row_count_mismatch(write_csv):
    original_rows = [{"price": str(i)} for i in range(10)]
    received_rows = [{"price": str(i)} for i in range(8)]  # 2 missing
    original = write_csv("original.csv", original_rows)

    from sim.client import run_integrity_check
    passed = run_integrity_check(str(original), received_rows)
    assert passed is False


def test_integrity_check_fails_on_row_content_mismatch(write_csv):
    original_rows = [{"price": "100"}, {"price": "200"}]
    received_rows = [{"price": "100"}, {"price": "999"}]  # second row differs
    original = write_csv("original.csv", original_rows)

    from sim.client import run_integrity_check
    passed = run_integrity_check(str(original), received_rows)
    assert passed is False


def test_integrity_check_fails_on_duplicates(write_csv):
    original_rows = [{"price": "100"}, {"price": "200"}, {"price": "300"}]
    original = write_csv("original.csv", original_rows)
    # Same count as original but row 1 is duplicated — tests purely the duplicate check
    received_with_dupe = [{"price": "100"}, {"price": "100"}, {"price": "300"}]

    from sim.client import run_integrity_check
    passed = run_integrity_check(str(original), received_with_dupe)
    assert passed is False


def test_row_hash_is_deterministic():
    from sim.client import row_hash
    row = {"price": "100", "volume": "500"}
    assert row_hash(row) == row_hash(row)
    assert row_hash(row) != row_hash({"price": "101", "volume": "500"})
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_client.py -v
```

Expected: `ImportError: cannot import name 'run_integrity_check' from 'sim.client'`

- [ ] **Step 3: Implement `sim/client.py`**

```python
#!/usr/bin/env python3
"""
Connects to the WebSocket server, saves all received rows to sim/output/sample_<SYMBOL>.csv,
then runs an integrity check against the original CSV.

Usage:
    python sim/client.py <EXCHANGE:SYMBOL> --original <csv_path> [--host localhost] [--port 8765] [--timeout 30]

Run from project root.
"""
import argparse
import asyncio
import csv
import hashlib
import json
import sys
from pathlib import Path

import websockets
import websockets.exceptions


def row_hash(row: dict) -> str:
    return hashlib.md5(json.dumps(row, sort_keys=True).encode()).hexdigest()


def run_integrity_check(original_path: str, received_rows: list[dict]) -> bool:
    print("\n[INTEGRITY] Running integrity check...")

    with open(original_path, newline="") as f:
        original_rows = list(csv.DictReader(f))

    passed = True

    if len(received_rows) != len(original_rows):
        print(
            f"[INTEGRITY] FAIL: row count mismatch — "
            f"got {len(received_rows)}, expected {len(original_rows)}"
        )
        passed = False

    mismatches = 0
    for i, (got, expected) in enumerate(zip(received_rows, original_rows)):
        if got != expected:
            if mismatches < 5:
                print(f"[INTEGRITY] FAIL: row {i + 1} mismatch")
                print(f"  expected: {expected}")
                print(f"  got:      {got}")
            mismatches += 1
    if mismatches > 5:
        print(f"[INTEGRITY] ... and {mismatches - 5} more mismatches")
    if mismatches:
        passed = False

    hashes = [row_hash(r) for r in received_rows]
    seen: set[str] = set()
    dupes = 0
    for h in hashes:
        if h in seen:
            dupes += 1
        seen.add(h)
    if dupes:
        print(f"[INTEGRITY] FAIL: {dupes} duplicate row(s) detected")
        passed = False

    if passed:
        print(
            f"[INTEGRITY] ✓ {len(received_rows)}/{len(original_rows)} rows matched, 0 duplicates"
        )

    return passed


async def connect_with_retry(uri: str, timeout: int) -> websockets.WebSocketClientProtocol:
    printed_waiting = False
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            return await websockets.connect(uri)
        except (OSError, websockets.exceptions.WebSocketException):
            if loop.time() >= deadline:
                print(
                    f"[CLIENT] ERROR: Could not connect to {uri} within {timeout}s",
                    file=sys.stderr,
                )
                sys.exit(1)
            if not printed_waiting:
                print(f"[CLIENT] Waiting for server at {uri}...")
                printed_waiting = True
            await asyncio.sleep(1.0)


async def run(args: argparse.Namespace) -> None:
    _, symbol = args.stock.split(":", 1)
    uri = f"ws://{args.host}:{args.port}/ws"

    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)
    sample_path = output_dir / f"sample_{symbol}.csv"

    ws = await connect_with_retry(uri, args.timeout)

    history_count = 0
    live_count = 0
    received_rows: list[dict] = []
    fieldnames: list[str] | None = None

    try:
        await ws.send(json.dumps({"stock": args.stock}))

        async for raw_msg in ws:
            msg = json.loads(raw_msg)

            if "error" in msg:
                print(f"[ERROR]   server sent: {msg['error']}")
                sys.exit(1)

            source = msg.get("source")
            data: dict = msg.get("data", {})

            if fieldnames is None and data:
                fieldnames = list(data.keys())

            if source == "history":
                history_count += 1
                print(f"[HISTORY] {data}")
            elif source == "live":
                live_count += 1
                print(f"[LIVE]    {data}")

            received_rows.append(data)

    except websockets.exceptions.ConnectionClosed:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        await ws.close()

    print(f"\n[CLIENT] Received {history_count} history rows, {live_count} live ticks")

    if received_rows and fieldnames:
        with open(sample_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(received_rows)
        print(f"[CLIENT] Saved to {sample_path}")

        ok = run_integrity_check(args.original, received_rows)
        sys.exit(0 if ok else 1)
    else:
        print("[CLIENT] No rows received — skipping integrity check")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="WebSocket client with end-to-end integrity check")
    parser.add_argument("stock", help="Stock in EXCHANGE:SYMBOL format")
    parser.add_argument("--original", required=True, help="Path to original CSV for integrity check")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--timeout", type=int, default=30, help="Seconds to wait for server")
    args = parser.parse_args()

    if ":" not in args.stock:
        print(
            f"[CLIENT] ERROR: stock must be EXCHANGE:SYMBOL, got '{args.stock}'",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Create `sim/output/.gitkeep`**

Empty file to track the directory.

- [ ] **Step 5: Update `.gitignore` (create if not present)**

```
sim/output/*.csv
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
pytest tests/test_client.py -v
```

Expected: `5 passed`

- [ ] **Step 7: Commit**

```bash
git add sim/client.py sim/output/.gitkeep tests/test_client.py
git commit -m "feat: add sim/client.py — WebSocket client with integrity check"
```

---

## Task 8: Build `sim/run_testbench.sh`

**Files:**
- Create: `sim/run_testbench.sh`

- [ ] **Step 1: Create `sim/run_testbench.sh`**

```bash
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
docker run -d \
    --name "$SERVER_CONTAINER" \
    --env-file "$PROJECT_DIR/.env.docker" \
    -p "${SERVER_PORT}:${SERVER_PORT}" \
    --add-host=host.docker.internal:host-gateway \
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
```

- [ ] **Step 2: Make executable**

```bash
chmod +x sim/run_testbench.sh
```

- [ ] **Step 3: Verify script syntax**

```bash
bash -n sim/run_testbench.sh
```

Expected: no output (syntax is clean).

- [ ] **Step 4: Commit**

```bash
git add sim/run_testbench.sh
git commit -m "feat: add sim/run_testbench.sh — Docker bootstrapper for Redis and WebSocket server"
```

---

## Task 9: Full test suite verification

- [ ] **Step 1: Run all tests**

```bash
pytest -v
```

Expected: all tests pass. The count should be: 2 (config) + 4 (csv_reader) + 6 (redis_consumer) + 5 (main) + 7 (feeder) + 5 (client) = **29 passed**

- [ ] **Step 2: Commit if any fixes were needed**

```bash
git add -p
git commit -m "fix: address any issues found during full test suite run"
```

---

## Manual smoke test (post-implementation)

Once all tasks are complete, run the full simulation end-to-end:

```bash
# Terminal 0: boot containers
bash sim/run_testbench.sh

# Terminal 1: start feeder with one of your local CSVs
python sim/feeder.py /path/to/your/stock.csv NYSE:IBM --interval 0.1

# Terminal 2: start client
python sim/client.py NYSE:IBM --original /path/to/your/stock.csv

# Expected client output:
# [HISTORY] {...}  (100 rows)
# [LIVE]    {...}  (100 rows, arriving at 100ms intervals)
# [INTEGRITY] ✓ 200/200 rows matched, 0 duplicates
```
