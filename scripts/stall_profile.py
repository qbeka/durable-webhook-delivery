#!/usr/bin/env python3
"""Finds short stalls that a whole-run percentile hides. Standard library only.

A window is a stall when its p99 from arrival to effect is over the budget. The gaps
between stalls show whether they are periodic.

  python3 scripts/stall_profile.py <directory> ...

Each directory is searched for latency-healthy.csv.gz files.
"""
import csv
import gzip
import statistics
import sys
from pathlib import Path

WINDOW_SECONDS = 5
BUDGET_MS = 250
WARMUP_SECONDS = 10


def windows(path):
    latency_by_window = {}
    with gzip.open(path, "rt") as rows:
        for row in csv.DictReader(rows):
            seconds = int(row["arrival_offset_ms"]) / 1000
            if seconds >= WARMUP_SECONDS:
                latency_by_window.setdefault(int(seconds // WINDOW_SECONDS), []).append(int(row["latency_us"]) / 1000)
    return {key: sorted(values)[int(len(values) * 0.99)] for key, values in sorted(latency_by_window.items())}


def describe(path):
    p99 = windows(path)
    stalls = [key * WINDOW_SECONDS for key, value in p99.items() if value > BUDGET_MS]
    # Neighbouring windows are one stall, so only a gap over one window separates two.
    starts = [t for i, t in enumerate(stalls) if i == 0 or t - stalls[i - 1] > WINDOW_SECONDS]
    gaps = [b - a for a, b in zip(starts, starts[1:])]
    gap = f"median gap {statistics.median(gaps):.0f} s" if gaps else "no gaps"
    worst = max(p99.values())
    return f"{len(p99) * WINDOW_SECONDS:>4} s, {len(starts):>3} stalls, {gap}, worst {WINDOW_SECONDS} s p99 {worst:,.0f} ms"


def main():
    for directory in sys.argv[1:]:
        for path in sorted(Path(directory).glob("**/latency-healthy.csv.gz")):
            print(f"{path.parent}: {describe(path)}")


if __name__ == "__main__":
    main()
