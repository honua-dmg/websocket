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


async def test_stream_to_redis_appends_rows_to_history_csv(tmp_path):
    mock_client = AsyncMock()
    history_path = tmp_path / "history.csv"
    history_path.write_text("price,volume\n90,100\n")
    rows = [{"price": "100", "volume": "200"}, {"price": "150", "volume": "300"}]

    from sim.feeder import stream_to_redis
    with patch("asyncio.sleep", new_callable=AsyncMock):
        sent = await stream_to_redis(
            mock_client,
            "IBM",
            rows,
            interval=0.0,
            history_path=history_path,
            fieldnames=["price", "volume"],
        )

    assert sent == 2
    with open(history_path, newline="") as f:
        written = list(csv.DictReader(f))
    assert written == [{"price": "90", "volume": "100"}, *rows]


async def test_stream_to_redis_returns_partial_count_on_interrupt():
    mock_client = AsyncMock()
    # Raise KeyboardInterrupt on second xadd
    mock_client.xadd.side_effect = [None, KeyboardInterrupt()]
    rows = [{"price": "100"}, {"price": "150"}, {"price": "120"}]

    from sim.feeder import stream_to_redis
    with patch("asyncio.sleep", new_callable=AsyncMock):
        sent = await stream_to_redis(mock_client, "IBM", rows, interval=0.0)

    assert sent == 1
