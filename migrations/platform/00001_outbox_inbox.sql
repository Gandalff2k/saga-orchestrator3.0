-- Platform tables, applied to EVERY service database (version table: goose_platform_version).

-- +goose Up

-- OUTBOX: messages written in the same tx as the business change; the relay publishes them.
CREATE TABLE outbox (
    seq          bigserial   PRIMARY KEY,           -- publish order within a shard
    id           uuid        NOT NULL UNIQUE,       -- message id (= Envelope.ID, inbox key downstream)
    topic        text        NOT NULL,
    key          text        NOT NULL,              -- Kafka key = saga id
    type         text        NOT NULL,
    payload      jsonb       NOT NULL,              -- the full Envelope
    headers      jsonb       NOT NULL DEFAULT '{}', -- traceparent, tracestate, message-id, message-type
    -- 16 shards, fixed here. Changing it = new migration + drain the outbox first.
    shard        int         GENERATED ALWAYS AS ((hashtext(key) & 2147483647) % 16) STORED,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
-- The relay's hot query: unpublished rows of one shard in seq order.
CREATE INDEX outbox_unpublished_idx ON outbox (shard, seq) WHERE published_at IS NULL;
-- Janitor: delete old published rows.
CREATE INDEX outbox_published_at_idx ON outbox (published_at) WHERE published_at IS NOT NULL;

-- One row per shard; a relay replica owns a shard while its lease is valid.
CREATE TABLE outbox_shard_lease (
    shard      int         PRIMARY KEY,
    owner      text,
    expires_at timestamptz NOT NULL DEFAULT '-infinity'
);
INSERT INTO outbox_shard_lease (shard) SELECT g FROM generate_series(0, 15) AS g;

-- Live relay replicas (heartbeats) -> fair share of shards per replica.
CREATE TABLE outbox_relay_member (
    instance_id  text        PRIMARY KEY,
    heartbeat_at timestamptz NOT NULL DEFAULT now()
);

-- INBOX: ids of consumed messages, written in the same tx as their effects.
CREATE TABLE inbox (
    consumer    text        NOT NULL,
    message_id  uuid        NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);
CREATE INDEX inbox_received_at_idx ON inbox (received_at);

-- +goose Down
DROP TABLE inbox;
DROP TABLE outbox_relay_member;
DROP TABLE outbox_shard_lease;
DROP TABLE outbox;
