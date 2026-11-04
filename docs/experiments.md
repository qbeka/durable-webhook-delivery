# Experiments

These were exploratory runs on 2026-11-04, on a Mac shared with other work. They
chose the design. They are not results: the preregistered runs in `results/` are.
Each row is one repetition unless it says otherwise.

## The first bottleneck: durable commits

The first version sustained 1,000 events per second, then collapsed. At 2,000
offered, it completed fewer deliveries than it had at 1,000. Sampling
`pg_stat_activity` showed almost no CPU use. Nearly all time was spent waiting:

- `LWLock:WALWrite`: commits queuing for a flush.
- `LWLock:BufferContent` and `MultiXact`: every ingress locking the same two rows.

Delivery rate is concurrency divided by cycle time, and each claim and each finish
waited in the same flush queue as ingress. So more ingress meant slower delivery.

## What each change did

2,000 events per second offered, one worker, concurrency 64, three repetitions.

| Commit tag | Change | Completed per second | Ack p99 | Decision |
|---|---|---|---:|---|
| `exp/e0-baseline` | None | 736 to 804 | 101 to 187 ms | Baseline |
| `exp/e1-advisory` | Advisory lock in place of a row lock on the source | 933 to 1,033 | 41 to 226 ms | Kept. It also fixed the subscribe starvation in review finding 7. |
| `exp/e2-no-fk` | Dropped the foreign key to destinations | 969 to 1,006 | 37 to 42 ms | **Reverted.** No measured gain, and it gave up referential integrity. |
| `exp/e3-batch-finish` | One commit for many finishes | 1,349 to 1,491 | 41 to 106 ms | Kept. Largest gain. |

## Failed ideas

- **Dropping the foreign key.** The profile pointed at that row, but the throughput
  did not move. A wait in a profile is a reason to run an experiment, not a result.
- **Raising concurrency to 256.** Throughput rose a little to about 1,190 to 1,665 per
  second, and ack p99 got worse by more than ten times, about 40 ms to about 1.6 s.
  More concurrent writers only lengthened the flush queue.
- **A larger receiver connection pool.** It moved completions from 892 to 1,442 per
  second up to 1,526 to 1,665, too close to the noise to call. It stays at 64.

## The remaining limit is the flush

The same commit, the same load, two repetitions, 2,000 events per second:

| `wal_sync_method` | Completed per second | p99 arrival to effect |
|---|---:|---:|
| `fsync_writethrough` | 664 to 837 | 16,869 to 22,293 ms |
| `open_datasync` (macOS default) | 1,631 to 1,877 | 3,176 to 3,474 ms |

This is why tuning stopped. The next limit is how fast this drive can flush while
other work shares it.

## Noise on a shared machine

Other work used about 12 of 18 cores during these runs, with a load average from 25
to over 200. The same `exp/e3` commit gave 1,349 to 1,491 completions per second in one
hour and 878 to 1,243 in the next. That is larger than some effects above, and it is
why the published runs wait for a quiet machine and record what they found.

A rerun of `exp/e3` against the restored foreign key at that noise level gave 878 to
1,243 against 871 to 1,147, which cannot separate them. The foreign key stays.

## Multiple workers

At 1,500 events per second with two workers, one exploratory run gave a p99 of 129 ms
with the machine at a load average near 170. One worker had collapsed at 2,000. H3
in `docs/preregistration.md` tests this properly.
