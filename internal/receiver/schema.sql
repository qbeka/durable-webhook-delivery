-- Receiver schema. It lives in its own database, as a real receiver's would.

CREATE TABLE IF NOT EXISTS processed_deliveries (
    delivery_key   text PRIMARY KEY,
    payload_sha256 bytea       NOT NULL,
    first_seen_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- delivery_key is deliberately not unique here. A repeated effect must be able
-- to exist in this table, or the harness could never observe one.
CREATE TABLE IF NOT EXISTS expense_ledger (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_key   text        NOT NULL,
    tenant_id      text        NOT NULL,
    source_id      text        NOT NULL,
    event_id       text        NOT NULL,
    receipt_id     text        NOT NULL,
    amount_cents   bigint      NOT NULL,
    currency       text        NOT NULL,
    merchant       text        NOT NULL,
    payload_sha256 bytea       NOT NULL,
    committed_at   timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS expense_ledger_event ON expense_ledger (tenant_id, source_id, event_id);
