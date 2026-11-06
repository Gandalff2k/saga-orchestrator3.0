-- +goose Up
CREATE TABLE risk_events (
    saga_id      uuid        PRIMARY KEY,
    customer_id  text        NOT NULL,
    card_hash    text        NOT NULL,
    ip           text        NOT NULL,
    country      text        NOT NULL CHECK (char_length(country) = 2),
    amount_cents bigint      NOT NULL CHECK (amount_cents > 0),
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX risk_events_card_idx ON risk_events (card_hash, created_at);
CREATE INDEX risk_events_customer_idx ON risk_events (customer_id, created_at);
CREATE INDEX risk_events_ip_idx ON risk_events (ip, created_at);

CREATE TABLE risk_decisions (
    saga_id       uuid        PRIMARY KEY REFERENCES risk_events (saga_id),
    decision      text        NOT NULL CHECK (decision IN ('ALLOW', 'REVIEW', 'BLOCK')),
    score         int         NOT NULL,
    hits          jsonb       NOT NULL DEFAULT '[]',
    rules_version text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE review_cases (
    saga_id        uuid        PRIMARY KEY REFERENCES risk_decisions (saga_id),
    status         text        NOT NULL DEFAULT 'PENDING'
                   CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED', 'EXPIRED')),
    recommendation jsonb,
    agent_attempts int         NOT NULL DEFAULT 0,
    locked_by      text,
    locked_until   timestamptz,
    decided_by     text,
    note           text,
    decided_at     timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX review_cases_pending_idx ON review_cases (created_at) WHERE status = 'PENDING';

-- +goose Down
DROP TABLE review_cases;
DROP TABLE risk_decisions;
DROP TABLE risk_events;
