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
