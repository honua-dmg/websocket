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
