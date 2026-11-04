-- Engine schema. docs/contract.md explains the rules these tables enforce.

CREATE TABLE IF NOT EXISTS sources (
    tenant_id      text   NOT NULL,
    source_id      text   NOT NULL,
    signing_secret text   NOT NULL,
    PRIMARY KEY (tenant_id, source_id)
);

CREATE TABLE IF NOT EXISTS destinations (
    id                   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id            text             NOT NULL,
    name                 text             NOT NULL,
    url                  text             NOT NULL,
    signing_secret       text             NOT NULL,
    max_in_flight        integer          NOT NULL DEFAULT 16 CHECK (max_in_flight > 0),
    rate_per_second      double precision NOT NULL DEFAULT 0 CHECK (rate_per_second >= 0), -- 0 means no limit
    paused               boolean          NOT NULL DEFAULT false,
    -- Nothing is claimed before this time. Set by the breaker, Retry-After and the rate limit.
    not_before           timestamptz      NOT NULL DEFAULT '-infinity',
    consecutive_failures integer          NOT NULL DEFAULT 0,
    rate_tokens          double precision NOT NULL DEFAULT 0,
    rate_tokens_at       timestamptz      NOT NULL DEFAULT now(),
    last_claimed_at      timestamptz      NOT NULL DEFAULT '-infinity',
    UNIQUE (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id      text        NOT NULL,
    source_id      text        NOT NULL,
    event_type     text        NOT NULL, -- an exact type, or '*'
    destination_id bigint      NOT NULL REFERENCES destinations (id),
    created_at     timestamptz NOT NULL DEFAULT now(),
    ended_at       timestamptz,
    FOREIGN KEY (tenant_id, source_id) REFERENCES sources (tenant_id, source_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS subscriptions_active
    ON subscriptions (tenant_id, source_id, event_type, destination_id)
    WHERE ended_at IS NULL;

CREATE TABLE IF NOT EXISTS events (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id      text        NOT NULL,
    source_id      text        NOT NULL,
    event_id       text        NOT NULL,
    event_type     text        NOT NULL,
    payload        bytea,                -- null after payload retention ends
    payload_sha256 bytea       NOT NULL,
    received_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id, source_id, event_id)
);

-- A conflicting body never changes the stored event. It is recorded here instead.
CREATE TABLE IF NOT EXISTS event_conflicts (
    tenant_id       text        NOT NULL,
    source_id       text        NOT NULL,
    event_id        text        NOT NULL,
    event_type      text        NOT NULL,
    received_sha256 bytea       NOT NULL,
    stored_sha256   bytea       NOT NULL,
    first_seen_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    seen_count      bigint      NOT NULL DEFAULT 1,
    PRIMARY KEY (tenant_id, source_id, event_id, event_type, received_sha256)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_row_id     bigint      NOT NULL REFERENCES events (id),
    -- No foreign key on purpose. A foreign key takes a lock on the destination row
    -- for every insert, and that one hot row capped fan-out. See docs/experiments.md.
    -- Fan-out reads destination_id from subscriptions, which does have a foreign
    -- key, and destinations are never deleted, so the reference cannot dangle.
    destination_id   bigint      NOT NULL,
    delivery_key     text        NOT NULL UNIQUE, -- stable across attempts and replays
    state            text        NOT NULL DEFAULT 'pending'
                     CHECK (state IN ('pending', 'leased', 'succeeded', 'dead')),
    attempt_count    integer     NOT NULL DEFAULT 0, -- every attempt, across replays
    budget_used      integer     NOT NULL DEFAULT 0, -- attempts since creation or the last replay
    next_attempt_at  timestamptz NOT NULL DEFAULT now(),
    lease_token      uuid,
    lease_owner      text,
    lease_expires_at timestamptz,
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at      timestamptz,
    UNIQUE (event_row_id, destination_id)
);
CREATE INDEX IF NOT EXISTS deliveries_due ON deliveries (destination_id, next_attempt_at)
    WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS deliveries_leased ON deliveries (destination_id)
    WHERE state = 'leased';
CREATE INDEX IF NOT EXISTS deliveries_lease_expiry ON deliveries (lease_expires_at)
    WHERE state = 'leased';
CREATE INDEX IF NOT EXISTS deliveries_dead ON deliveries (destination_id)
    WHERE state = 'dead';

-- One row per attempt. Written at claim time so a crash leaves a visible record.
CREATE TABLE IF NOT EXISTS delivery_attempts (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id      bigint      NOT NULL REFERENCES deliveries (id),
    attempt_number   integer     NOT NULL,
    lease_token      uuid        NOT NULL,
    worker_id        text        NOT NULL,
    claimed_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at      timestamptz,
    outcome          text, -- null while in flight
    http_status      integer,
    error            text,
    stale            boolean     NOT NULL DEFAULT false, -- the lease was gone when the worker finished
    lease_expired_at timestamptz
);
CREATE INDEX IF NOT EXISTS delivery_attempts_delivery ON delivery_attempts (delivery_id, lease_token);

CREATE TABLE IF NOT EXISTS replays (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operator   text        NOT NULL,
    reason     text        NOT NULL,
    selector   text        NOT NULL,
    row_limit  integer     NOT NULL,
    requeued   integer     NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
