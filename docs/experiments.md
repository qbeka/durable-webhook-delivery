# Experiments

These were exploratory runs on 2026-11-04, on a Mac shared with other work. They
chose the design. They are not results: the preregistered runs in `results/final` are.
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
| `exp/e1-advisory` | Advisory lock in place of a row lock on the source | 933 to 1,033 | 41 to 226 ms | Kept. It also fixed the subscribe starvation. |
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

## The one second stalls

The final run failed H1b and H4. Binning latency by time shows why, and
`scripts/stall_profile.py` reproduces this from the recorded files.

| Run | Started | Length | Stalls | Median gap |
|---|---|---:|---:|---:|
| Ladder, 4 workers, 2,000 per second, three reps | 19:10 | 60 s each | 0 | None |
| Ten minutes, 1,000 per second, reps 1 and 2 | 19:20 | 10 min each | 0 | None |
| Ten minutes, 1,000 per second, rep 3 | About 19:40 | 10 min | 2 | 455 s |
| Ten minutes, 2,000 per second, three reps | 19:50 | 10 min each | 15 to 17 each | 30 s |
| Isolation, limits on, six reps | 20:20 | 100 s each | 1 to 3 each | 35 to 65 s |
| Flush cost, flushing, three reps | About 20:40 | 60 s each | 1 each | None |
| Exploratory, 2,000 per second | 21:16 | 9 min | 1, 258 ms | None |
| Exploratory, 2,000 per second, checkpoint running | 21:25 | 4 min | 0 | None |

A stall is a 5 second window with a p99 over 250 ms. The worst windows reached 1 to 2 seconds. The
acknowledgement latency rose with it, which means commits were waiting, not only delivery.

What was checked:

- **Not load alone.** The same rate on the same code was clean before 19:40 and after 21:10.
- **Not a checkpoint alone.** A checkpoint began 5.6 minutes into the 21:16 run and lifted its
  worst 5 second p99 to 258 ms, not a second. It was still running through the clean 21:25 run.
- **Not the disk filling up.** The drive was 39% full.
- **Not low power mode.** It was off.

What was not checked: nothing sampled drive latency during the final run, so it cannot show what
commits were waiting for.

## The retest

`docs/preregistration-retest.md` froze one attempt at the two failed tests, with unchanged service
code and a probe timing a small write and a full drive flush four times a second.

| Test | Result | Stalls |
|---|---|---:|
| 2,000 per second, ten minutes, rep 1 | p99 378 ms, failed | 10, median gap 30 s |
| 2,000 per second, ten minutes, rep 2 | p99 3,881 ms, failed | 9, median gap 58 s |
| 2,000 per second, ten minutes, rep 3 | p99 586 ms, failed | 7, median gap 50 s |
| Isolation, limits on, slow and offline, six reps | p99 56 to 68 ms, passed | 0 |

The probe took 10,983 samples: p50 7.9 ms, p99 15.5 ms, max 134.4 ms. It never came near the length
of a stall, so a slow drive flush does not explain the retest stalls. The probe wrote to its own
file, so this says nothing about a queue inside PostgreSQL.

Other work used 3 to 4 cores on median during the ten minute runs, with bursts to 14, and 1 to 3
cores during isolation. The stalls came with the higher load, but the final run stalled at about 2
cores and the exploratory runs were clean at about 2 to 3. Load alone does not explain them.

Decision: 2,000 per second did not pass ten minutes, so 1,000 per second stays the sustained rate.
This was the one attempt the plan allowed. H4 passed, and its failure in the final run stays on the
record. The next step is to sample `pg_stat_activity` wait events through a stall. The exploratory
sampler ran during two runs, and neither had a stall to catch.

The evidence is in `results/exploratory/stall-2026-11-04/`.
