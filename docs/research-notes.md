# Research notes

Sources were read on 2026-11-04. Each entry gives the version, what this project
adopted and where it differs.

## Stripe webhook guidance

Source: <https://docs.stripe.com/webhooks>. The page has no document version. Its
curl examples use API version `2026-10-28.preview`.

Adopted:

- Duplicates are normal, and receivers dedupe on a stable ID they log.
- Delivery order is not guaranteed.
- Signatures use HMAC-SHA256 over a timestamp and the raw body, with a `v1`
  scheme prefix, a constant-time compare and a 5-minute tolerance.
- A retry gets a new timestamp and signature.
- Redirects count as failures. They are not followed.

Differences:

- Stripe tells receivers to return 2xx before complex work. Here the reference
  receiver returns 2xx only after its transaction commits, because the effect is
  a short database write and the point of the project is a durable effect.
- Stripe's signed string covers the timestamp and body. Ingress here also signs
  the event identity, because those headers decide which event the body becomes.
- Stripe's manual resend can create extra deliveries alongside its automatic
  retries. Here a replay uses the same delivery key and only runs on deliveries
  that have finished.

## PostgreSQL row locks and SKIP LOCKED

Source: <https://www.postgresql.org/docs/current/sql-select.html>, which served
the PostgreSQL 18.6 manual. This project runs PostgreSQL 17.11. The WAL pages
below were read at version 17. **Gap:** the locking section was read at 18.6, not
17. I relied on it only for long-standing behaviour.

Adopted:

- `SKIP LOCKED` gives an inconsistent view by design and suits queue tables. The
  claim query uses it for exactly that and for nothing that needs a consistent
  count.
- `LIMIT` stops locking once enough rows are returned, so a claim locks only the
  rows it takes.
- At READ COMMITTED, each statement gets a new snapshot. The claim locks the
  destination in one statement and counts in-flight rows in the next, so the
  count sees every claim committed before the lock was granted.

Consequence: a foreign key insert takes `FOR KEY SHARE` on the parent row, which
conflicts with `FOR UPDATE`. The claim uses `FOR NO KEY UPDATE`, so a destination
being claimed does not block fan-out inserts that reference it.

## SQLite durability

Source: <https://www.sqlite.org/pragma.html#pragma_synchronous>.

- In WAL mode, `synchronous=FULL` syncs the WAL after each commit.
  `synchronous=NORMAL` stays consistent but can lose recent commits on power loss.
- All levels survive an application crash.
- `fullfsync` defaults to off and only matters on macOS.

The reference bench uses WAL and `synchronous=full`, but leaves `fullfsync` off.
Its faults are process exits, so its result does not depend on that flag. On
macOS it does not show power-loss safety.

## PostgreSQL durability settings

Sources: <https://www.postgresql.org/docs/17/runtime-config-wal.html> and
<https://www.postgresql.org/docs/17/wal-reliability.html>.

- `wal_sync_method` picks how commits reach disk, and its default is not always
  crash-safe.
- On macOS the manual says write caching is prevented by `fsync_writethrough`.
- `commit_delay` can trade latency for larger group commits.
- `synchronous_commit=off` can lose acknowledged commits. This project never uses
  it, because a lost acknowledged event breaks the ingress promise.

## Local measurement that changes the benchmark

`pg_test_fsync` on this machine on 2026-11-04, one 8 kB write, 2 seconds per test:

| Method | Operations per second | Microseconds per operation |
|---|---:|---:|
| `open_datasync` | 25,933 | 39 |
| `fsync` | 48,081 | 21 |
| `fsync_writethrough` | 267 | 3,743 |

Plain `fsync` is about 178 times faster because it does not force the drive cache
to flush. Any commit rate from this Mac must say which method was used. A rate
measured without writethrough says nothing about power-loss safety.

## Assumptions

- The fault model is process death, including `kill -9` and an immediate database
  stop. Power loss is not tested. Writethrough is the setting that would be
  required for it, not evidence that it works.
- All processes share one machine and one clock, so timestamps from different
  processes can be compared in a trace.
