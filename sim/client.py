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
