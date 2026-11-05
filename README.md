# Durable webhook delivery

A webhook engine in Go and PostgreSQL that survives crashes without losing an event or
recording one twice. It comes with the fault harness that tests that claim.

If a worker records an expense and crashes before saying so, a retry must recover without
recording the expense again. No sender can guarantee that alone. This project defines what
the receiver must do, builds both sides, and tries to break them on purpose.

| | |
|---|---|
| **Sustained load** | 1,000 events per second for 30 minutes, p99 63 to 71 ms, from arrival to durable effect, flushing the drive cache on every commit |
| **Peak load** | 2,000 events per second in 60 second runs, p99 86 to 94 ms. The ten minute test at this rate failed: p99 1,099 to 1,497 ms. |
| **Crash safety** | 75 workers killed and 6 databases crashed across 185,200 deliveries: 0 repeated, 0 missing |
| **Failover** | 5 primary failovers: 0 acknowledged events lost. The asynchronous control lost 196 to 273. |
| **Isolation** | During a slow destination, a healthy tenant's p99 was 58 to 642 ms. With limits off it was 37,592 to 40,089 ms. The 250 ms budget was missed in 2 of 3 runs. |
| **Formal model** | 9,484,492 TLA+ states with no violation. 4 of 4 deliberately broken designs were caught. |
| **Test quality** | 39 of 39 targeted mutants caught |

Measured on 2026-11-04 on one Apple M5 Pro Mac with PostgreSQL 17. The load generator, engine,
receiver and databases shared that machine and one drive, other work ran on it, and power loss
was not tested. [What the numbers leave out](#what-the-numbers-leave-out) has the details.

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

Run the correctness suite against real PostgreSQL, with the race detector. It takes about
one minute.

```bash
make up && make test
```

Run a fault smoke test. It runs the negative control, the crash window and an outage in
about 50 seconds.

```bash
make smoke
```

| Command | What it does | Time |
|---|---|---:|
| `make mutants` | Breaks each rule in a copy of the code and requires a test to catch it | 5 min |
| `make faults` | Runs every fault scenario at full scale | 10 min |
| `make spec` | Model checks the protocol. Needs `brew install --cask tla+-toolbox`. | 1 min |
| `make site` | Rebuilds the report and website from recorded runs | Seconds |
| `scripts/run-final.sh` | Reruns every recorded experiment from one commit. Close other work first. | 3.5 hours |

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
    10 ms  worker    worker-7 commits a lease for attempt 1
    32 ms  receiver  delivery key and expense committed
           worker    killed before it can record the result
  3.09 s   engine    lease expires with no report
  6.70 s   worker    worker-17 commits a lease for attempt 2
  6.72 s   receiver  key already present, no new effect
  6.72 s   worker    attempt 2 recorded as success. One ledger row.
```

The file is `results/final/faults/kill_after_commit_before_ack/trace.json`.

## Why you can trust the numbers

A clean test means nothing if it could not have failed, so each claim has a control that
must fail.

| Claim | Control |
|---|---|
| 0 repeated effects under crashes | A naive receiver under the same fault showed exactly 50 of 50 planted repeats |
| 0 acknowledged events lost in failover | Asynchronous replication under the same fault lost 196, 210 and 273 engine rows, and 6 to 8 expenses |
| Limits protect a healthy tenant | With limits off, its median p99 rose about 95 times |
| The protocol is safe | Removing any one of 4 rules gives a TLA+ counterexample |
| The tests catch mistakes | 39 of 39 deliberately broken versions failed a test. These are targeted, not random. |

- Expected effects are rebuilt from a seed, so a bug cannot hide by writing the wrong thing.
- A scenario is invalid unless its fault really happened and left evidence.
- Load is open-loop and latency counts from scheduled arrival, so a stall cannot hide itself.
- The hypotheses and decision rules were committed before the runs. See
  [docs/preregistration-final.md](docs/preregistration-final.md). Two of them failed, and the
  failures are reported below.
- Every result has a manifest with its commit, seeds, hardware and settings.

## Benchmark summary

Every run flushed the drive cache on each commit, using `wal_sync_method=fsync_writethrough`. All
of it comes from one commit, `7ba5980`. A rate is sustained when all three repetitions meet the
rule: no errors, accepted rate within 1% of offered, p99 under 250 ms, no backlog growth, and
complete reconciliation.

| Test | Result | Decision |
|---|---|---|
| 1,000 per second, 2 workers, three 10 minute runs | p99 63 to 71 ms, 1,800,000 effects | Passed |
| 2,000 per second, 4 workers, three 10 minute runs | p99 1,099 to 1,497 ms, 3,600,000 effects, none lost | **Failed** |
| 1 worker, 60 second ladder | Highest sustained rate 1,000 per second, p99 57 ms | Measured |
| 2 workers, 60 second ladder | No rung passed. See below. | Measured |
| 4 workers, 60 second ladder | Highest sustained rate 2,000 per second, p99 93 ms | Measured |
| 4 workers beat 1 worker | 2,000 against 1,000 per second | Passed |
| Limits hold a healthy tenant under 250 ms | 2 of 3 slow runs and 2 of 3 offline runs missed it | **Failed** |
| 14 fault scenarios | 0 repeated, 0 missing, and the control caught 50 of 50 | Passed |
| 5 failovers with synchronous replication | 0 acknowledged events lost, and the control lost events every time | Passed |

**The two failures share a symptom.** For about an hour in the middle of the run, ingress and
delivery stalled for about one second, every 30 seconds. The ten minute runs at 2,000 per second
had 15 to 17 stalls each. The healthy tenant was hit before the outage as well as during it, so
these runs show a stall, not a limit leak. With limits off, the same tenant waited 37 to 40
seconds.

**The cause is not known.** The stalls did not return in two later exploratory runs at the same
rate on the same code, 9 and 4 minutes long, the second with a checkpoint running. Those p99s were
153 and 117 ms. They are exploratory and do not replace the failed test. Docker and other work
shared the Mac, but nothing recorded ties the stalls to them. The details are in
[docs/experiments.md](docs/experiments.md).

- **Two workers:** at 1,000 per second, 2 of 3 repetitions ran with over 8 cores of other work, and
  one reached 6,979 ms. At 1,500 per second, one repetition reached 533 ms. These 2 are among the 3
  contended repetitions of 77, and the 30 minutes at 1,000 per second used 2 workers cleanly.
- **Above the limit:** 4 workers at 2,500 per second gave p99s of 200, 410 and 1,881 ms, so it does
  not count as sustained.
- **The cost of flushing:** at 1,500 per second, real flushing raised the p50 from 5.1 ms to 43 ms
  and the p99 from 88 ms to 913 ms. Each flushing repetition held one stall, which inflates that
  p99.
- **The cost of replication:** synchronous replication raised the p99 at 1,000 per second from
  61 ms to 211 ms, with both clusters on one drive.
- **The generator:** it reached 16,000 per second alone, so it did not limit these numbers.

The full results are in [docs/report.md](docs/report.md). The first preregistered run, kept in
`results/published`, passed isolation at 62 ms. It is superseded because the code changed after
review, so quote `results/final`.

## What the numbers leave out

- All processes shared one machine and one drive. A real receiver would be elsewhere.
- 60 second runs miss anything slower than a minute. That is how 2,000 per second looked fine
  until the ten minute test.
- Background CPU was sampled every 5 seconds and drive use was not sampled at all, so the run
  cannot say what the stalls were. Bursts of other work reached 12 cores.
- The fault model is process death, pauses, lost responses and status bursts. Power loss was
  not tested. The flushing setting is what power loss would require, not proof that it works.
- The failovers ran on one machine. They exercise PostgreSQL synchronous commit through this
  service, and the harness triggers promotion after a declared 2 seconds. There is no real failure
  detector or fencing of the old primary.
- Unavailable time in failover was 3.3 seconds, and 3 of those are the declared freeze and
  detection time.
- The TLA+ model checks the design, not the Go code. Safety has a finite bound of 3 workers, 5
  faults and 2 replays. Liveness has 2 workers, 3 faults and no replays. The state count follows
  from those bounds, so a larger number would not mean a safer design.
- The mutants are targeted at rules this project names. An independent reviewer's 15 extra mutants
  survived 11 times at first. Tests now cover the survivors it named, but 39 of 39 is not a measure
  of all possible bugs.
- No ordering is guaranteed. Secrets are stored in plain text in the local database.

## Where to look

| Path | What it holds |
|---|---|
| [docs/contract.md](docs/contract.md) | The promises, and what a receiver must do |
| [docs/architecture.md](docs/architecture.md) | Each decision and the alternative it rejected |
| [docs/report.md](docs/report.md) | Every result |
| [docs/experiments.md](docs/experiments.md) | What was tried, including what failed |
| [docs/reviews.md](docs/reviews.md) | Findings from independent reviews |
| [docs/preregistration-final.md](docs/preregistration-final.md) | The plan frozen before the final run |
| [docs/api.md](docs/api.md), [docs/openapi.yaml](docs/openapi.yaml) | The API |
| [spec/DeliveryLease.tla](spec/DeliveryLease.tla) | The TLA+ model |
| `internal/store` | Every SQL statement |
| `internal/worker` | The claim, send and finish loop |
| `internal/receiver` | The reference receiver and the naive control |
| `internal/harness` | Fault scenarios, benchmarks and failover |
| `internal/reconcile` | The comparison with the expected ledger |

Built on [pgx](https://github.com/jackc/pgx). The reference benchmark in `bench/` was supplied
with the project brief. MIT licensed.
