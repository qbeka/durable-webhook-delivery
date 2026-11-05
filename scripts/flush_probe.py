#!/usr/bin/env python3
"""Records how long a small write and a full drive flush take, four times a second.

It runs beside a benchmark, on the same drive, and touches no database. If commits stall
and this stalls at the same moment, the drive flush itself was slow. macOS only.

  python3 scripts/flush_probe.py <output.csv> <scratch file>
"""
import fcntl
import os
import sys
import time

INTERVAL_SECONDS = 0.25
BLOCK = b"\0" * 4096


def main():
    output_path, scratch_path = sys.argv[1:3]
    descriptor = os.open(scratch_path, os.O_CREAT | os.O_WRONLY, 0o600)
    with open(output_path, "w", buffering=1) as output:
        output.write("unix_ms,flush_us\n")
        while True:
            started = time.monotonic()
            os.pwrite(descriptor, BLOCK, 0)
            fcntl.fcntl(descriptor, fcntl.F_FULLFSYNC)
            elapsed = time.monotonic() - started
            output.write(f"{int(time.time() * 1000)},{int(elapsed * 1e6)}\n")
            time.sleep(max(0, INTERVAL_SECONDS - elapsed))


if __name__ == "__main__":
    main()
