import csv
import io
from datetime import date
from typing import AsyncIterator

import aiofiles

from config import DATA_ROOT


async def stream_csv(exchange: str, symbol: str, day: str | None = None) -> AsyncIterator[dict]:
    day = day or date.today().isoformat()
    path = f"{DATA_ROOT}/{exchange}/{symbol}/{day}.csv"

    async with aiofiles.open(path, mode="r") as f:
        content = await f.read()

    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        yield dict(row)
