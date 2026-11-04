# Preregistration

This file was committed before any published run. A published result must come
from a commit at or after this file, and the manifest names the commit. If a
result here turns out badly, it is still reported.

Exploratory numbers measured before this file are not results. They chose the
design and are recorded in `docs/experiments.md`.

## Question

How much end-to-end throughput can a durable, multi-worker HTTP implementation
sustain at a declared tail-latency budget, while every expected effect exists
exactly once?

## Fixed workload

- Payload: 1,024 bytes of synthetic receipt JSON.
- Fan-out: 1.
- Arrivals: open-loop Poisson, seeded. Base seed 7000 plus the repetition number
  for benchmarks, 8000 for recovery, 2026 for faults.
- Latency: from scheduled arrival to the moment the receiver's commit returned.
- Unit of replication: one repetition, from empty databases. A configuration has
  three. Percentiles within a repetition use all `applied` rows after warm-up as
  their denominator. Aggregates report the median and the range across repetitions.

## Durability settings, the same in every benchmark

`synchronous_commit=on`, `fsync=on`, `full_page_writes=on`, and
`wal_sync_method=fsync_writethrough`, which is the macOS setting that flushes the
drive cache. The one exception is H5, which measures this setting on purpose.

## Machine conditions

The Mac is shared with other work. Before each repetition, the harness waits up to
30 minutes for background CPU to fall to 500% or less (five of 18 cores), then
records the real figure. A repetition that ran above that is labelled `contended`
and is still reported. A contended result is a lower bound on what the service can
do alone. It is never removed to improve a number.

## Hypotheses and decision rules

A rate is **sustained** when all three repetitions meet every condition:

1. Offered rate is met: error rate 0.
2. p99 arrival-to-effect is under 250 ms.
3. Backlog slope is within 1% of the offered rate, either way.
4. Reconciliation passes: nothing missing, repeated or dead, and nothing unfinished
   after a 120 second drain deadline.

**H1, the target.** The service sustains 1,000 events per second for ten minutes,
with 10 seconds of warm-up left out. Two workers, concurrency 64 each.

**H2, the limit.** A ladder of 1,000, 1,500, 2,000, 2,500 and 3,000 events per second,
60 seconds each with 10 seconds of warm-up, at 1, 2 and 4 workers with concurrency
64 each. The scale number is the highest sustained rung for each worker count.

**H3, workers.** More workers raise the highest sustained rate. Decision: supported
if 4 workers sustain at least one rung more than 1 worker.

**H4, isolation.** With limits on, a slow destination does not hurt a healthy tenant.
Two tenants at 300 events per second each, one destination slow for 60 seconds, with
20 seconds before and after. The first 5 seconds are left out of the "before" window. Compare `limits on` (in-flight limit 64, breaker at 5
failures) with `limits off` (in-flight limit 1,024, breaker effectively off), and
also run `offline` with limits on. Two workers give 128 slots, so the limit of 64
leaves half for other tenants. A dry run showed a limit of 16 throttled a healthy
tenant at this rate, which is why it changed before any published run. Decision: supported if, with limits on, the healthy
tenant's p99 during the outage stays under 250 ms, and with limits off it does not.

**H5, the cost of flushing.** Compare `fsync_writethrough` with the macOS default at
1,500 events per second, 2 workers, three repetitions, reporting completed rate and
p99. This measures a cost. It has no pass or fail.

## Failure run

Declared before the outcome is known.

| Item | Value |
|---|---|
| Fault model | Process death by SIGKILL, immediate database stop, SIGSTOP pauses, lost responses, status bursts, and one destination offline. Power loss is not tested. |
| Worker crashes | 12 at each of 3 sites, plus 30 in the reference workload (10 at each site) |
| Database crashes | 3 engine, with all workers killed each time, and 3 receiver |
| Receiver crashes | 6 after commit |
| Worker pauses | 3, each past the 3 second lease |
| Lost responses | 50 in each control |
| Status bursts | One 4 second burst each of 429 and 500 |
| Outage | 20 seconds in the suite, 60 seconds in H4 |
| Retained events | All. Nothing is pruned during a run. |
| Volume | 6,000 events per scenario, plus 100,000 events with 20,000 duplicates in the reference workload |
| Recovery deadline | 120 seconds after the load ends, 300 for the reference workload and H4 |

**H6, correctness.** Every scenario passes with zero missing and zero repeated
effects, zero dead letters, and zero unfinished jobs, and every declared fault is
delivered with its evidence.

**The control decides validity.** The naive receiver must show exactly one repeated
effect per dropped response. If it does not, the whole failure run is invalid.

## What the report must include

Offered load, accepted ingress, completed deliveries, error rate, backlog slope,
p50, p95 and p99 latency, time to drain after an outage, failed and timed out
events, the generator's own limit, and the raw distributions.
