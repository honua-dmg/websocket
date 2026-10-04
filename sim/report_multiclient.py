#!/usr/bin/env python3
"""Compares what each staggered client received and prints a pass/fail report."""
import argparse
import csv
import hashlib
import re
import sys
from pathlib import Path

# timestamp is restamped by transform_tick on the live path; stream_offset only exists on
# rows the feeder appended after xadd; stonk/instrument_token are the same field under the
# history and live schemas. None of them distinguish the underlying tick.
DROP = {"timestamp", "stream_offset", "stonk", "instrument_token"}


def canonical_rows(path: Path) -> list[tuple]:
    rows = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            token = row.get("stonk") or row.get("instrument_token") or ""
            rest = tuple(sorted((k, v) for k, v in row.items() if k not in DROP))
            rows.append((token,) + rest)
    return rows


def digest(rows: list[tuple]) -> str:
    h = hashlib.md5()
    for row in rows:
        h.update(repr(row).encode())
    return h.hexdigest()[:10]


def split_counts(log_path: Path) -> tuple[int, int]:
    text = log_path.read_text() if log_path.exists() else ""
    m = re.search(r"Received (\d+) history rows, (\d+) live ticks", text)
    return (int(m.group(1)), int(m.group(2))) if m else (0, 0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-client comparison report")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--source", required=True, help="Original CSV the feeder replayed")
    parser.add_argument("--labels", required=True, nargs="+")
    parser.add_argument("--offsets", required=True, nargs="+", type=int)
    parser.add_argument("--victim", help="Label of the client killed mid-stream")
    parser.add_argument("--server-log", type=Path)
    args = parser.parse_args()

    with open(args.source, newline="") as f:
        expected = sum(1 for _ in csv.DictReader(f))

    print("=" * 72)
    print(f"MULTI-CLIENT REPORT — {args.symbol}")
    print("=" * 72)
    print(f"Source CSV: {args.source} ({expected} rows)")
    print("Every client should end with all {0} rows, split differently.\n".format(expected))

    print(f"{'client':<8}{'connected':>11}{'history':>10}{'live':>8}{'total':>8}{'digest':>13}")
    print("-" * 72)

    results = []
    for label, offset in zip(args.labels, args.offsets):
        sample = args.output_dir / f"sample_{args.symbol}_{label}.csv"
        if not sample.exists():
            print(f"{label:<8}{'t+' + str(offset) + 's':>11}{'— no output file —':>39}")
            results.append((label, offset, 0, 0, 0, "MISSING"))
            continue

        history, live = split_counts(args.output_dir / f"client_{label}.log")
        rows = canonical_rows(sample)
        d = digest(rows)
        print(f"{label:<8}{'t+' + str(offset) + 's':>11}{history:>10}{live:>8}{len(rows):>8}{d:>13}")
        results.append((label, offset, history, live, len(rows), d))

    victim_rows = 0
    if args.victim:
        log = args.output_dir / f"client_{args.victim}.log"
        text = log.read_text() if log.exists() else ""
        victim_rows = text.count("[HISTORY]") + text.count("[LIVE]")
        print(f"{args.victim:<8}{'t+3s':>11}{'killed mid-stream after ' + str(victim_rows) + ' rows':>42}")

    print("-" * 72)

    totals = {r[4] for r in results}
    digests = {r[5] for r in results}
    splits = {(r[2], r[3]) for r in results}

    complete = all(r[4] == expected for r in results)
    identical = len(digests) == 1 and "MISSING" not in digests
    staggered = len(splits) == len(results)

    def verdict(ok: bool) -> str:
        return "PASS" if ok else "FAIL"

    print(f"[{verdict(complete)}] every client received all {expected} rows", end="")
    print("" if complete else f" — got {sorted(totals)}")
    print(f"[{verdict(identical)}] all clients received identical data (ignoring {', '.join(sorted(DROP))})")
    print(f"[{'OK  ' if staggered else 'NOTE'}] history/live split differs per client", end="")
    print("" if staggered else " — some clients share a split; increase stagger")

    disconnect_ok = True
    if args.victim:
        # a victim that died before receiving anything would make the survivors' PASS
        # meaningless — nothing was torn down mid-stream
        meaningful = victim_rows > 0
        disconnect_ok = meaningful and complete
        print(f"[{verdict(meaningful)}] killed client was mid-stream when it died ({victim_rows} rows in)")
        print(f"[{verdict(complete)}] survivors unaffected by the disconnect")

    traceback_ok = True
    if args.server_log and args.server_log.exists():
        tracebacks = args.server_log.read_text().count("Traceback (most recent call last)")
        traceback_ok = tracebacks == 0
        print(f"[{verdict(traceback_ok)}] server logged no unhandled exceptions", end="")
        print("" if traceback_ok else f" — {tracebacks} traceback(s) in {args.server_log}")

    print("=" * 72)

    sys.exit(0 if complete and identical and disconnect_ok and traceback_ok else 1)


if __name__ == "__main__":
    main()
