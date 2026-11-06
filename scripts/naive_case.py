#!/usr/bin/env python3
"""Writes results/site-naive.json, the one delivery the website shows from the naive control.

The control writes no trace.json. Its record of each delivery is the receiver's
effect log, which stays local because of its size, so this script pulls one
delivery out of that log. It takes the first event in the log whose attempts ran
applied, dropped_response, applied. Rerun it only after a new final run.

  python3 scripts/naive_case.py
"""
import csv
import hashlib
import json
import struct
from collections import defaultdict
from pathlib import Path

RUN = Path("results/final/faults")
SEED = 2026
TENANT = "healthy"
SHAPE = ["applied", "dropped_response", "applied"]


def amount_cents(index: int) -> int:
    """workload.Stream.AmountCents. The effect log has no amounts, the ledger did."""
    block = struct.pack("<QQ", SEED, index) + TENANT.encode().ljust(16, b"\0")
    return 1 + struct.unpack("<Q", hashlib.sha256(block).digest()[:8])[0] % 100_000


def check_amounts() -> None:
    """The port must match every amount a run did record."""
    for name in ("kill_after_commit_before_ack", "control_idempotent"):
        trace = json.loads((RUN / name / "trace.json").read_text())
        index = int(trace["delivery"]["event_id"].rsplit("-", 1)[1])
        if amount_cents(index) != trace["ledger_amount_cents"]:
            raise SystemExit(f"amount for {name} does not match the recorded ledger")


def main() -> None:
    check_amounts()
    log = RUN / "control_naive" / "effects-healthy.csv"
    events = defaultdict(list)
    with log.open() as source:
        for row in csv.DictReader(source):
            events[row["event_id"]].append(row)
    event_id, rows = next((e, r) for e, r in events.items() if [x["outcome"] for x in r] == SHAPE)
    arrival = int(rows[0]["arrival_unix_ns"])
    case = {
        "source": "results/final/faults/control_naive/effects-healthy.csv",
        "event_id": event_id,
        "delivery_key": rows[0]["delivery_key"],
        "amount_cents": amount_cents(int(event_id.rsplit("-", 1)[1])),
        "moments": [
            {"offset_ms": (int(r["time_unix_ns"]) - arrival) / 1e6, "attempt": int(r["attempt"]), "outcome": r["outcome"]}
            for r in rows
        ],
    }
    Path("results/site-naive.json").write_text(json.dumps(case, indent=2) + "\n")
    print(f"wrote results/site-naive.json from {event_id}")


if __name__ == "__main__":
    main()
