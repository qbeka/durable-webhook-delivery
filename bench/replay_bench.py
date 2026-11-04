"""Run with python3 replay_bench.py. Standard library, temporary local storage."""
import hashlib
import json
import os
import platform
import random
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

UNIQUE = 100_000
RETRIES = 20_000
BATCH = 100
SEED = 291


def connect(path):
    db = sqlite3.connect(path)
    db.execute("pragma journal_mode=wal")
    db.execute("pragma synchronous=full")
    return db


def amount(i):
    return 1 + (i * 7919) % 10000


def worker(root, fault):
    queue = connect(root / "queue.db")
    sink = connect(root / "sink.db")
    for step in range(10**6):
        batch = queue.execute("select id, cents from pending order by id limit ?", (BATCH,)).fetchall()
        if not batch:
            return
        with sink:
            for key, cents in batch:
                inserted = sink.execute("insert into receipts values (?) on conflict do nothing", (key,))
                if inserted.rowcount:
                    sink.execute("insert into effects values (?,?)", (key, cents))
            if step == 4 and fault == "before_commit":
                os._exit(73)
        if step == 4 and fault == "after_commit":
            os._exit(73)
        with queue:
            queue.executemany("delete from pending where id=?", [(k,) for k, _ in batch])
        if step == 4 and fault == "after_ack":
            os._exit(73)


def benchmark(root):
    rng = random.Random(SEED)
    ids = list(range(UNIQUE)) + rng.sample(range(UNIQUE), RETRIES)
    rng.shuffle(ids)
    queue = connect(root / "queue.db")
    queue.execute("create table pending(id integer primary key, cents integer not null)")
    with queue:
        queue.executemany("insert into pending values (?,?) on conflict do nothing",
                          ((i, amount(i)) for i in ids))
    accepted = queue.execute("select count(*) from pending").fetchone()[0]
    assert accepted == UNIQUE
    sink = connect(root / "sink.db")
    sink.execute("create table receipts(id integer primary key)")
    # No uniqueness constraint here. Duplicate business effects must remain observable.
    sink.execute("create table effects(id integer not null, cents integer not null)")
    queue.close()
    sink.close()
    faults = ["before_commit", "after_commit", "after_ack"] * 10
    start = time.perf_counter()
    for fault in [*faults, "finish"]:
        result = subprocess.run([sys.executable, __file__, str(root), fault])
        assert result.returncode == (0 if fault == "finish" else 73), (fault, result.returncode)
    elapsed = time.perf_counter() - start
    sink = connect(root / "sink.db")
    effects = sink.execute("select id,cents from effects order by id").fetchall()
    assert effects == [(i, amount(i)) for i in range(UNIQUE)]
    assert connect(root / "queue.db").execute("select count(*) from pending").fetchone()[0] == 0
    print(json.dumps(dict(unique_events=accepted, input_attempts=len(ids), duplicate_inputs=RETRIES,
        worker_exits=len(faults), fault_sites=faults[:3], runs_per_site=10, lost_effects=0,
        duplicate_effects=0, batch_size=BATCH, seed=SEED, drain_seconds=elapsed,
        scope="local batch drain including worker restarts; excludes bulk enqueue; no HTTP",
        python=sys.version.split()[0], sqlite=sqlite3.sqlite_version, platform=platform.platform(),
        source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()), indent=2))


if __name__ == "__main__":
    if len(sys.argv) == 3:
        worker(Path(sys.argv[1]), sys.argv[2])
    else:
        with tempfile.TemporaryDirectory() as folder:
            benchmark(Path(folder))
