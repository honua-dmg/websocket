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
