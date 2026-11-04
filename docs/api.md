# API

The service has one public endpoint. Operators use `hookctl`, which talks to the
database directly. `docs/contract.md` gives the full rules.

## Send an event

```
POST /v1/sources/{source}/events
```

| Header | Value |
|---|---|
| `Hook-Tenant` | Tenant ID |
| `Hook-Event-Id` | Your ID for this event. Reuse it when you retry. |
| `Hook-Event-Type` | For example `receipt.forwarded` |
| `Hook-Timestamp` | Unix seconds |
| `Hook-Signature` | `v1=` and a hex HMAC-SHA256 |

The signed string is six lines: timestamp, tenant, source, event ID, event type
and the hex SHA-256 of the body. Sign it with the source's secret.

| Status | Body `status` | Meaning |
|---|---|---|
| 202 | `accepted` | Committed. `deliveries` gives the fan-out count. |
| 200 | `duplicate` | Already committed with the same type and body. |
| 409 | `conflict` | Already committed with a different type or body. Both digests are returned. |
| 400 | `rejected` | A header is missing or invalid. |
| 401 | `rejected` | The signature, timestamp or source is wrong. |
| 413 | `rejected` | The body is over 256 KiB. |
| 503 | `unavailable` | Retry with the same event ID. |

## Check health

`GET /healthz` returns 200 when the database answers.

## Receive a delivery

Your endpoint gets a `POST` with the event body and the headers listed in
`docs/contract.md`. To get exactly one effect per delivery, commit
`Hook-Delivery-Key` and your effect in one transaction, then return 2xx.

## Operate with hookctl

| Command | What it does |
|---|---|
| `hookctl source` | Register a source and its secret. |
| `hookctl destination` | Register a destination, with `--max-in-flight` and `--rate`. |
| `hookctl subscribe`, `unsubscribe` | Start or stop routing future events. |
| `hookctl pause`, `resume` | Hold or release a destination's deliveries. |
| `hookctl status` | Show delivery counts per destination. |
| `hookctl trace --key K` | Show one delivery with every attempt. |
| `hookctl replay` | Send finished deliveries again under the same keys. Needs `--reason` and `--limit`. Add `--dry-run` to count first. |
| `hookctl prune` | Apply retention. |
| `hookctl send` | Sign and send one event. |
