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
from datetime import datetime, timezone
from pathlib import Path
import time

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
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

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


def write_history(
    exchange: str, symbol: str, fieldnames: list[str], rows: list[dict], stream_offset: str
) -> Path:
    today = datetime.now(timezone.utc).date().isoformat()
    out_dir = Path(DATA_ROOT) / exchange / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{today}.csv"

    # every row says where the live stream resumes after it; without that a client
    # reading this file has to guess, and "guess" means jumping to the tip and
    # silently dropping everything published in between
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows({**row, "stream_offset": stream_offset} for row in rows)

    print(f"[FEEDER] Wrote {len(rows)} history rows to {out_path.resolve()}")
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


def _to_raw_tick(row: dict) -> dict:
    """Reshape a flat CSV row into the nested KiteTicker-style payload transform_tick expects."""
    tick = {
        "instrument_token": row.get("stonk"),
        "last_price": row.get("last_price"),
        "last_traded_quantity": row.get("last_traded_quantity"),
        "average_traded_price": row.get("average_traded_price"),
        "volume_traded": row.get("volume_traded"),
        "total_buy_quantity": row.get("total_buy_quantity"),
        "total_sell_quantity": row.get("total_sell_quantity"),
        "ohlc": {
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
        },
        "change": row.get("change"),
        "oi": row.get("oi"),
        "oi_day_high": row.get("oi_day_high"),
        "oi_day_low": row.get("oi_day_low"),
        "depth": {"buy": [], "sell": []},
    }
    for i in range(1, 6):
        tick["depth"]["buy"].append({
            "price": row.get(f"buy_price_{i}"),
            "quantity": row.get(f"buy_qty_{i}"),
            "orders": row.get(f"buy_orders_{i}"),
        })
        tick["depth"]["sell"].append({
            "price": row.get(f"sell_price_{i}"),
            "quantity": row.get(f"sell_qty_{i}"),
            "orders": row.get(f"sell_orders_{i}"),
        })
    return tick


async def stream_to_redis(
    client: aioredis.Redis,
    symbol: str,
    rows: list[dict],
    interval: float,
    history_path: Path | None = None,
    fieldnames: list[str] | None = None,
) -> int:
    sent = 0
    total = len(rows)
    try:
        if history_path is not None:
            with open(history_path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                for row in rows:
                    msg_id = await client.xadd(symbol, {"data": json.dumps(_to_raw_tick(row))})
                    row["stream_offset"] = msg_id
                    writer.writerow(row)
                    f.flush()  # a connecting client must see the offset, not a buffered file
                    sent += 1
                    print(f"[FEEDER] row {sent}/{total} → {symbol} (id={msg_id})")
                    await asyncio.sleep(interval)
        else:
            for row in rows:
                await client.xadd(symbol, {"data": json.dumps(_to_raw_tick(row))})
                sent += 1
                print(f"[FEEDER] row {sent}/{total} → {symbol} ")
                await asyncio.sleep(interval)
    except KeyboardInterrupt:
        pass
    return sent


async def main() -> None:
    parser = argparse.ArgumentParser(description="Split CSV and stream live half to Redis")
    parser.add_argument("csv_path", help="Path to the source CSV file")
    parser.add_argument("stock", help="Stock in EXCHANGE:SYMBOL format")
    parser.add_argument("--interval", type=float, default=0.01, help="Seconds between rows (default: 0.1)")
    parser.add_argument("--redis-url", default="redis://localhost:6379")
    args = parser.parse_args()

    if ":" not in args.stock:
        print(f"[FEEDER] ERROR: stock must be EXCHANGE:SYMBOL, got '{args.stock}'", file=sys.stderr)
        sys.exit(1)

    exchange, symbol = args.stock.split(":", 1)
    fieldnames, rows = load_and_validate_csv(args.csv_path)
    mid = len(rows) // 4  # 25% history, 75% live for more interesting demo
    history_rows, live_rows = rows[:mid], rows[mid:]

    print(f"[FEEDER] {len(rows)} total rows → {len(history_rows)} history, {len(live_rows)} live")

    # the real producer tags each streamed row with its Redis offset so a connecting
    # client can resume exactly where the file ends — the column must exist from the start
    csv_fields = [*fieldnames, "stream_offset"]
    client = await connect_redis(args.redis_url)
    entries = await client.xrevrange(symbol, count=1)
    stream_tip = entries[0][0] if entries else "0"

    history_path = write_history(exchange, symbol, csv_fields, history_rows, stream_tip)
    print(f"[FEEDER] History written to {history_path} (live resumes after {stream_tip})")
    time.sleep(5)  # give user time to see history output before streaming live
    print(f"[FEEDER] Connected to Redis, streaming {len(live_rows)} rows to '{symbol}'...")

    sent = await stream_to_redis(
        client, symbol, live_rows, args.interval, history_path, csv_fields
    )
    print(f"[FEEDER] Done — {sent}/{len(live_rows)} rows sent to Redis stream '{symbol}'")
    await client.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
