#!/usr/bin/env python3
"""Keeps raw latency small enough for Git.

The harness writes a nanosecond arrival time per row. A millisecond offset from the
first arrival holds the same distribution in about a third of the space. This writes
latency.csv.gz beside the originals. The originals stay on disk, are ignored by Git,
and are what the manifest hashes describe.

  python3 scripts/compact_raw.py results/published
"""
import csv
import gzip
import io
import sys
from pathlib import Path


def rows_in(path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", newline="") as source:
        yield from csv.DictReader(source)


def compact(path, target):
    rows = list(rows_in(path))
    if not rows:
        return
    arrivals = [int(r["arrival_unix_ns"]) for r in rows]
    start = min(arrivals)
    with gzip.open(target, "wt", compresslevel=9, newline="") as output:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(["arrival_offset_ms", "latency_us", "attempt", "outcome"])
        for row, arrival in zip(rows, arrivals):
            latency = row.get("latency_us") or (int(row["time_unix_ns"]) - arrival) // 1000
            writer.writerow([(arrival - start) // 1_000_000, latency, row["attempt"], row["outcome"]])


def main():
    root = Path(sys.argv[1])
    for path in list(root.glob("**/effects-*.latency.csv.gz")) + list(root.glob("**/effects-*.csv")):
        tenant = path.name.split(".")[0].removeprefix("effects-")
        compact(path, path.parent / f"latency-{tenant}.csv.gz")


if __name__ == "__main__":
    main()
