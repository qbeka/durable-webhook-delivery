# Architecture

This page explains how the service is built and why. `docs/contract.md` gives the
rules. This page gives the reasons.

## The shape of it

```
 source                engine database                    receiver
   |                  (PostgreSQL 17)                  (its own PostgreSQL)
   |  signed POST            |                                |
   |------> hookd api ------>| events + deliveries            |
   |  202 after COMMIT       | in one transaction             |
   |                         |                                |
   |              hookd worker (N processes)                  |
   |                claim:   | lease commits first            |
   |                         |                                |
   |                send:    |---- signed POST, delivery key ---->| key + expense
   |                         |                                |    in one transaction
   |                finish:  |<--- 2xx after COMMIT ---------------|
   |                         | batch, only if the token matches
```

A crash can happen on any line. Each line either has not happened, so it is retried,
or has happened and left a durable record, so the retry is harmless.

## Decisions

Each decision names the alternative that was rejected. Measurements are in
`docs/experiments.md`.

| Decision | Rejected alternative | Why |
|---|---|---|
| Two databases, sharing nothing | One database for engine and ledger | A real receiver is someone else's system. One database would hide the whole problem, because one transaction could cover both sides. |
| Commit the lease before sending | Hold a transaction open during the request | A slow receiver would pin a connection and a row lock for seconds. |
| A lease token on every claim | Check only the worker ID | A worker can be reclaimed by itself after a restart. A token is unique per claim, so a late report can never match. |
| In-flight count derived from leased rows | A counter on the destination | A crash cannot leave a derived count wrong. Its leases expire and the count follows. |
| Stable key from event identity and destination | A new key per attempt or replay | A new key would let a replay apply twice. The stable key is what makes retry safe. |
| Receiver dedupes on a unique key | Sender tries harder to be exactly-once | No sender can do this. If the response is lost, it cannot know. The fix has to be at the receiver. |
| `FOR NO KEY UPDATE SKIP LOCKED` on the destination | `FOR UPDATE` | `FOR UPDATE` conflicts with the foreign key lock that fan-out inserts take, so a worker could block ingress. |
| Capacity check in a second statement | One statement with a CTE | At READ COMMITTED a statement's snapshot is taken when it starts. The lock must be held first, or the count misses claims that just committed. |
| Advisory lock for subscription changes | `FOR SHARE` on the source row | The row lock wrote to the page on every ingress, and writers starved. See review finding 7. |
| Batch finishes into one commit | One commit per delivery | A durable commit is the scarcest resource on this machine. This was the largest measured gain. |
| Open-loop load with latency from scheduled arrival | Closed-loop load | A closed loop hides stalls: it stops sending when the system stops answering. This is coordinated omission. |
| Expected ledger rebuilt from a seed | Compare against what the system wrote | A reconciler that trusts the system cannot catch the system. |

## What makes the evidence trustworthy

A green test proves little if the test cannot fail. Four layers guard against that.

1. **Negative control.** The naive receiver must show exactly one repeated effect per
   dropped response. If it does not, the run is invalid.
2. **Mutation check.** `make mutants` breaks each invariant in a copy of the code and
   requires a test to catch it.
3. **Fault evidence.** Each scenario declares what its fault must leave behind. A run
   that delivers no real fault is invalid, not a pass.
4. **Preregistration.** The hypotheses and decision rules were committed before any
   published run.

## Known limits

- One region and one database. The engine has no replication, so it is not highly
  available. Durability here means surviving a crash, not losing a disk.
- No ordering guarantee.
- Secrets are stored in plain text in the local database.
- Benchmarks run on one machine, so the load generator, the engine and the receiver
  share CPU and one drive. The receiver would normally be elsewhere.
