# Durable webhook delivery

A webhook engine in Go and PostgreSQL that survives crashes without losing an event or
recording one twice. It comes with the fault harness that proves it.

If a worker records an expense and crashes before saying so, a retry must recover without
recording the expense again. No sender can guarantee that alone. This project defines what
the receiver must do, builds both sides, and tries to break them on purpose.

| | |
|---|---|
| **Sustained load** | 2,000 events per second with a 164 ms p99, from arrival to durable effect, flushing the drive cache on every commit |
| **Crash safety** | 75 workers killed and 6 databases crashed across 185,200 deliveries: 0 repeated, 0 missing |
| **Failover** | 5 primary failovers: 0 acknowledged events lost. The asynchronous control lost 214 to 422. |
| **Isolation** | During a slow destination, a healthy tenant's p99 was 62 ms. With limits off it was 38,056 ms. |
| **Formal model** | 9,484,492 TLA+ states with no violation. 4 of 4 deliberately broken designs were caught. |
| **Test quality** | 30 of 30 mutants caught |

Measured on 2026-11-04 on one Apple M5 Pro Mac with PostgreSQL 17. The load generator, engine,
receiver and databases shared that machine and one drive, and power loss was not tested.
[What these numbers leave out](#what-the-numbers-leave-out) has the details.

## Run it

You need Go 1.27 and PostgreSQL 17.

```bash
brew install go postgresql@17
```

Start everything: two local databases, the API, a worker and a receiver.

```bash
make run
```

Send a synthetic receipt and see its row in the ledger.

```bash
make send
```

Stop everything.

```bash
make stop
```

## Check it

Run the correctness suite against real PostgreSQL, with the race detector. It takes a few
minutes.

```bash
make up && make test
```

Run a fault smoke test. It runs the negative control, the crash window and an outage in
about 90 seconds.

```bash
make smoke
```

| Command | What it does | Time |
|---|---|---:|
| `make mutants` | Breaks each rule in a copy of the code and requires a test to catch it | 5 min |
| `make faults` | Runs every fault scenario at full scale | 10 min |
| `make spec` | Model checks the protocol. Needs `brew install --cask tla+-toolbox`. | 1 min |
| `make site` | Rebuilds the report and website from recorded runs | Seconds |
| `scripts/run-benchmarks.sh` | Reruns every published benchmark. Close other work first. | 3 hours |
| `scripts/run-failover.sh` | Reruns the failover experiment | 15 min |

Open `site/index.html` for an offline replay of one lost acknowledgement.

## How it works

1. **Ingress.** A signed POST commits the event and its deliveries in one transaction. It
   answers 202 only after that commit. A repeated request converges, and a conflicting body
   gets a visible 409.
2. **Claim.** A worker commits a lease before it sends anything. Each claim gets a new token.
3. **Send.** The request carries one delivery key, the same on every attempt and replay.
4. **Effect.** The receiver commits the key and the expense in one transaction, and answers
   only after that. A resend finds the key and writes nothing.
5. **Finish.** A worker records the outcome only if its lease token is still current.

A crash on any of these lines leaves the system recoverable. The worst case, and the reason
for all of this, is a worker killed after step 4 and before step 5. Its lease expires, another
worker resends, and the receiver answers `duplicate`. This trace is from a recorded run:

```
     0 ms  engine    event and delivery committed, ingress answers 202
    24 ms  worker    worker-5 commits a lease for attempt 1
    58 ms  receiver  delivery key and expense committed
           worker    killed before it can record the result
  3.15 s   engine    lease expires with no report
  6.92 s   worker    worker-18 commits a lease for attempt 2
  6.94 s   receiver  key already present, no new effect
  6.94 s   worker    attempt 2 recorded as success. One ledger row.
```

The file is `results/published/faults/kill_after_commit_before_ack/trace.json`.

## Why you can trust the numbers

A clean test proves nothing if it could not have failed, so each claim has a control that
must fail.

| Claim | Control |
|---|---|
| 0 repeated effects under crashes | A naive receiver under the same fault showed exactly 50 of 50 planted repeats |
| 0 acknowledged events lost in failover | Asynchronous replication under the same fault lost 214, 320 and 422 |
| Limits protect a healthy tenant | With limits off, its p99 rose about 610 times |
| The protocol is safe | Removing any one of 4 rules gives a TLA+ counterexample |
| The tests catch mistakes | 30 of 30 deliberately broken versions failed a test |

- Expected effects are rebuilt from a seed, so a bug cannot hide by writing the wrong thing.
- A scenario is invalid unless its fault really happened and left evidence.
- Load is open-loop and latency counts from scheduled arrival, so a stall cannot hide itself.
- The hypotheses and decision rules were committed before the runs. See
  [docs/preregistration.md](docs/preregistration.md).
- Every result has a manifest with its commit, seeds, hardware and settings.

## Benchmark summary

Measured with power-loss-safe flushing, `wal_sync_method=fsync_writethrough`, on a quiet
machine. A rate is sustained when all three repetitions meet the rule: no errors, p99 under
250 ms, no backlog growth, and complete reconciliation.

| Workers | Highest sustained rate | p99 at that rate |
|---:|---:|---:|
| 1 | 1,000 per second | 58 ms |
| 2 | 1,500 per second | 77 ms |
| 4 | 2,000 per second | 164 ms |

- **Target:** three 10 minute runs at 1,000 events per second gave a p99 of 63 to 68 ms against
  a 250 ms budget, with 1,800,000 effects reconciled.
- **Above the limit:** 4 workers at 2,500 per second had a median p99 of 192 ms, but one of three
  repetitions reached 358 ms, so it does not count as sustained.
- **The cost of flushing:** real flushing raises the p50 from 5.5 ms to 41 ms at 1,500 per second.
- **The cost of replication:** synchronous replication raises the p99 at 1,000 per second from
  68 ms to 197 ms, with both clusters on one drive.
- **The generator:** it reached 16,000 per second alone, so it did not limit these numbers.

The full results are in [docs/report.md](docs/report.md).

## What the numbers leave out

- All processes shared one machine and one drive. A real receiver would be elsewhere.
- The fault model is process death, pauses, lost responses and status bursts. Power loss was
  not tested. The flushing setting is what power loss would require, not proof that it works.
- The failovers ran on one machine. The harness triggers promotion after a declared 2 seconds,
  and there is no real failure detector or fencing of the old primary.
- Unavailable time in failover was 3.3 seconds, and 3 of those are the declared freeze and
  detection time.
- The TLA+ model checks the design, not the Go code, and it has a finite bound: 3 workers, 5
  faults and 2 replays.
- No ordering is guaranteed. Secrets are stored in plain text in the local database.

## Where to look

| Path | What it holds |
|---|---|
| [docs/contract.md](docs/contract.md) | The promises, and what a receiver must do |
| [docs/architecture.md](docs/architecture.md) | Each decision and the alternative it rejected |
| [docs/report.md](docs/report.md) | Every result |
| [docs/experiments.md](docs/experiments.md) | What was tried, including what failed |
| [docs/reviews.md](docs/reviews.md) | Findings from independent reviews |
| [docs/api.md](docs/api.md), [docs/openapi.yaml](docs/openapi.yaml) | The API |
| [spec/DeliveryLease.tla](spec/DeliveryLease.tla) | The TLA+ model |
| `internal/store` | Every SQL statement |
| `internal/worker` | The claim, send and finish loop |
| `internal/receiver` | The reference receiver and the naive control |
| `internal/harness` | Fault scenarios, benchmarks and failover |
| `internal/reconcile` | The comparison with the expected ledger |

Built on [pgx](https://github.com/jackc/pgx). The reference benchmark in `bench/` was supplied
with the project brief. MIT licensed.
