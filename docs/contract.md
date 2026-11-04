# Delivery contract

This document says what the service promises, what it needs from a receiver,
and what happens in each edge case. Code and tests follow this document.

## The guarantee, stated exactly

A sender alone cannot deliver an effect exactly once. If a receiver commits an
effect and the response is lost, the sender cannot tell that case from a
request that never arrived. It must retry, so the receiver can see a delivery
twice.

This service therefore promises **at-least-once delivery with a stable
delivery key**. A receiver that follows the rules below gets
**effectively-once effects**: each delivery key changes its ledger once.

| Part | Promise |
|---|---|
| Ingress | A 2xx response means the event and all its deliveries are committed. |
| Delivery | Each committed delivery is attempted until it succeeds or reaches a dead letter state. |
| Delivery key | One key per event and destination, the same on every attempt and every replay. |
| Order | None. Deliveries can arrive in any order. |
| Effect count | Exactly one, only when the receiver follows the receiver contract. |

## Event identity

An event is identified by `(tenant, source, event_id)`. The source chooses
`event_id`. The same `event_id` from another source or tenant is a different
event. The digest is the SHA-256 of the raw request body.

## Ingress

`POST /v1/sources/{source}/events`

| Header | Meaning |
|---|---|
| `Hook-Tenant` | Tenant ID. |
| `Hook-Event-Id` | Event ID, chosen by the source. |
| `Hook-Event-Type` | Event type, for example `receipt.forwarded`. |
| `Hook-Timestamp` | Unix seconds when the request was signed. |
| `Hook-Signature` | `v1=` followed by a hex HMAC-SHA256. |

The signed string is the following values joined by newlines: timestamp,
tenant, source, event ID, event type and the hex body digest. Newlines cannot
appear in these values, so no two requests produce the same string. A `.` would
not work: `receipt.forwarded` contains one, so an ID and type could trade
characters without changing the string. IDs match `[A-Za-z0-9._:-]{1,200}`.
The key is the source's signing secret. Signatures compare in constant time. A
timestamp more than 300 seconds from the server clock is rejected. The body
limit is 256 KiB.

The signature covers the identity headers because they decide which event the
body becomes. A signature over the body alone would let anyone who saw a request
replay that body under a new event ID.

| Response | Meaning | What the sender does |
|---|---|---|
| `202` `accepted` | First commit of this event. | Stop. |
| `200` `duplicate` | Already committed with the same type and digest. | Stop. |
| `409` `conflict` | Already committed with a different type or digest. The stored event does not change, and the conflict is recorded. | Stop and alert a person. |
| `400`, `401`, `413` | Invalid, unauthenticated or too large. Nothing is stored. | Stop and fix the request. |
| `503` | The database is unavailable. | Retry with the same event ID and body. |

Repeated requests converge. Concurrent requests with the same identity and body
produce one event, one set of deliveries and one `202`.

Ingress acknowledges only after `COMMIT` returns. Event storage and fan-out are
one transaction, so an event with no deliveries cannot be committed by accident.

## Subscriptions and concurrent changes

A subscription routes `(tenant, source, event_type)` to a destination.
`event_type` can be `*`.

Fan-out reads the active subscriptions inside the ingress transaction. Each
ingress transaction holds a shared lock on its source, and each subscription
change holds the matching exclusive lock. Every event is therefore ordered
either wholly before or wholly after each subscription change.

The lock adds one promise that a snapshot alone cannot give: **when unsubscribe
returns, no ingress still in progress can add a delivery for that destination.**
Without it, an ingress that read the old subscriptions could commit a new delivery
after the operator was told the change was done.

| Situation | Result |
|---|---|
| Subscribe returned before the ingress request began | The event fans out to the new destination. |
| Unsubscribe returned before the ingress request began | The event does not fan out to that destination. |
| The two overlap | Either order is valid. |
| Unsubscribe after the event committed | Existing deliveries still run. A subscription controls future fan-out only. |
| Duplicate ingress after a subscription change | No new deliveries. Fan-out happens once, at first commit. |
| Pausing a destination | Its deliveries wait. Nothing is dropped. |

## Delivery states

```
             claim                 2xx
  pending ----------> leased ---------------> succeeded
     ^                  |  |
     |   retryable      |  |  terminal failure, or attempts exhausted
     +------------------+  +--------------------> dead
     |   or lease expired                          |
     +---------------------------------------------+
                 operator replay (also from succeeded)
```

A delivery is claimed in a committed transaction before any request is sent. The
claim writes a new lease token. A worker can finish a delivery only while its
token is still the current one.

### Failure classes

| Outcome | Class | Result |
|---|---|---|
| 2xx | Success | `succeeded` |
| Connection failure, timeout, 408, 425, 429, 5xx | Retryable | Back to `pending` with backoff, or `dead` when attempts run out |
| 3xx | Terminal | `dead`. Redirects are not followed, because a signed request must not go to an address nobody registered. |
| Other 4xx, including 409 | Terminal | `dead`. The receiver has said this request will never succeed. |
| Lease expired | Retryable | The attempt counts, so a delivery that keeps killing workers still ends. |

### Retry timing

The delay before attempt `n + 1` is a uniform random value between zero and
`min(cap, base * 2^(n-1))`. This is "full jitter". A `Retry-After` header raises
the delay to at least that value, up to the cap.

Retries are per delivery. A destination that fails repeatedly also opens a
breaker. While the breaker is open, no requests are sent. When it reopens, one
probe at a time tests the destination. A probe counts in the attempt history but
spends none of the delivery's budget, so an outage does not push healthy
deliveries into dead letters.

A delivery therefore has two limits, and it goes dead at whichever comes first: 12
attempts, or 72 hours. Both count from creation or the last replay. The age limit
is what ends a delivery that only ever gets free probes.

### Slow requests and leases

The request timeout is always shorter than the lease. A worker also shortens
each request so it ends before its own lease does.

A worker can still outlive its lease, for example when its process pauses. No
fence can stop a request that has already left. So:

1. A second worker can claim the delivery and send it while the first request
   is still in flight.
2. The receiver sees both, and its unique key on the delivery key lets one win.
3. The old worker's finish finds a stale token, changes no state, and is
   recorded in the attempt history as stale.

This is why the receiver contract cannot be optional.

## Replay and new work

| Action | Delivery key | Effect at a compliant receiver |
|---|---|---|
| Automatic retry | Same | At most one |
| Operator replay | Same | At most one. The receiver answers `duplicate`. |
| Source resends with the same event ID | Same, no new delivery | None |
| Source sends a new event ID | New | A new effect. This is new logical work. |

Replay never creates a new key, because a replay that could apply twice would be
a dangerous tool to give an operator during an incident. To apply something
again on purpose, the source must send a new event.

Replay works only on `succeeded` and `dead` deliveries. It requires a reason, a
limit and either one key or a destination and state filter. Every replay writes
an audit row. A replayed delivery goes through the normal per-destination limits
and gets a fresh attempt budget. Its history is kept.

## Receiver contract

Each outbound request carries these headers:

| Header | Meaning |
|---|---|
| `Hook-Delivery-Key` | Stable key for this event and destination. |
| `Hook-Attempt` | Attempt number across the life of the delivery. |
| `Hook-Tenant`, `Hook-Source`, `Hook-Event-Id`, `Hook-Event-Type` | Event identity. |
| `Hook-Timestamp` | Unix seconds when this attempt was signed. |
| `Hook-Signature` | `v1=` followed by a hex HMAC-SHA256 of the timestamp, delivery key, tenant, source, event ID, event type and hex body digest joined by newlines, keyed with the destination secret. |

To get one effect per key, a receiver must do all of the following:

1. Verify the signature before reading the body as data.
2. In one transaction, insert the delivery key into a table where it is unique,
   and apply the effect only if that insert added a row.
3. Return 2xx only after that transaction commits.
4. Return 2xx with no new effect when the key already exists with the same
   digest.
5. Return 409 when the key already exists with a different digest.

Steps 2 and 3 are the whole design. If the key and the effect commit separately,
a crash between them loses an effect or repeats one.

## Retention

| Data | Default | What happens after |
|---|---|---|
| Payload | 7 days | Set to null. The digest stays. Replay reports the payload as gone. |
| Event identity and digest | 30 days | Deleted with its deliveries and attempts. |
| Unfinished deliveries | Forever | Pruning never touches an event with a `pending` or `leased` delivery. |

The duplicate-detection window equals the identity retention. After 30 days the
engine no longer remembers an event, so a resend creates a new delivery. The
delivery key comes from the identity, so that delivery has the same key as before.
A receiver that kept its keys answers `duplicate`, or `409` if the body changed. A
receiver that dropped its keys applies it again. Receivers should keep their keys
for at least as long as the identity retention.

## Out of scope

- Ordering, exactly-once at a receiver that ignores this contract, and multiple
  regions.
- Secrets at rest. They are stored in plain text in the local database. This is
  acceptable for a local prototype with synthetic data and for nothing else.
