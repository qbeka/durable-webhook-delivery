# Preregistration: replica failover

Committed before any failover run. It extends `docs/preregistration.md`, and the same
rules apply: a bad result is still reported.

## Question

Does an acknowledged event survive the loss of the primary database under load?

## Setup

The engine runs on a primary and one streaming standby, on separate clusters. Clients
connect with a multi-host URL and `target_session_attrs=read-write`, so they find the
new primary without a code change. The receiver and its ledger are not replicated.

Fixed for every run: 1,024 byte payloads, fan-out 1, two workers, seeded open-loop
arrivals, 500 events per second for 40 seconds, `wal_sync_method=fsync_writethrough`
on all clusters. Each repetition starts from a new primary and a new base backup.

## The fault, the same in every run

1. At 15 seconds, freeze the standby's WAL receiver with SIGSTOP for 1 second.
2. Stop the primary with no clean shutdown.
3. Resume the WAL receiver.
4. Wait 2 seconds. This stands in for failure detection, and it is a declared
   constant, not a measurement.
5. Promote the standby.

The freeze is what makes the control deterministic. Without it, an asynchronous
standby may happen to be current, and the control would pass by luck.

## Hypotheses

**H7, no acknowledged event is lost.** With `synchronous_standby_names='*'`, in 5 of 5
repetitions: every acknowledged event exists after failover, every expected effect
exists exactly once, and nothing is dead or unfinished after a 120 second drain. The
freeze blocks commits instead of losing them, because a commit waits for the standby.

**H8, the negative control.** The same fault with asynchronous replication must lose
acknowledged events, in 3 of 3 repetitions. If any repetition loses none, the fault did
not create the danger it claims to, and H7 is invalid.

**H9, the cost.** Compare 1,000 events per second with 4 workers on synchronous
replication with the matching rung in the published ladder. This measures a cost and
has no pass or fail. Both clusters share one drive, so it is an upper bound on what a
standby on its own disk would cost.

## Metrics

Acknowledged events lost, repeated effects, missing effects, unavailable seconds (the
longest gap between acknowledgements around the crash), and p99 arrival to effect.

## Fault model and limits

Process death on one machine. The primary and the standby share a power supply and a
drive, so this says nothing about a real data centre failure. The harness triggers the
promotion. A real deployment needs a failure detector and fencing of the old primary,
and neither is built here. The receiver's database is not part of this test.

## Formal model

`spec/DeliveryLease.tla` models the lease protocol in TLA+. TLC must report no error
for the design. Each of four variants removes one rule, and TLC must find a
counterexample for each. A variant with no counterexample means the model is too weak.
