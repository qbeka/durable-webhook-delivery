# Reference run

`bench/replay_bench.py` run unchanged three times on 2026-11-04.

| Run | Drain seconds | Lost effects | Duplicate effects |
|---|---:|---:|---:|
| `run-local-1.json` | 1.005 | 0 | 0 |
| `run-local-2.json` | 1.063 | 0 | 0 |
| `run-local-3.json` | 1.014 | 0 | 0 |

The median is 1.014 seconds. `bench/run.json` holds the supplied run: 3.359
seconds on Linux x86-64.

What these numbers cover: one worker, no network, 100,000 distinct keys, 20,000
duplicate inputs and 30 abrupt worker exits. Drain time excludes the bulk enqueue
and includes the worker restarts. SQLite uses WAL with `synchronous=full`, and
`fullfsync` is off, so this Mac does not flush its drive cache on commit.

These are local prototype evidence. Do not compare them with the HTTP service
benchmarks, and do not compare the two machines with each other: they use
different hardware and different SQLite versions.
