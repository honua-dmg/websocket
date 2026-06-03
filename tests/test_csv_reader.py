import csv
import pytest
from pathlib import Path


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
