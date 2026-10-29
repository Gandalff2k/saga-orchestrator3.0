-- +goose Up
CREATE TABLE orders (
    order_id        uuid        PRIMARY KEY,
    idempotency_key text        UNIQUE,         -- Idempotency-Key header of POST /orders
    seat_id         int         NOT NULL,
    amount_cents    bigint      NOT NULL CHECK (amount_cents > 0),
    status          text        NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'ABORTED')),
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sagas (
    saga_id      uuid        PRIMARY KEY,
    order_id     uuid        NOT NULL UNIQUE REFERENCES orders (order_id),
    state        text        NOT NULL
                 CHECK (state IN ('RESERVING', 'CHARGING', 'COMPENSATING', 'COMPLETED', 'ABORTED')),
    seat_id      int         NOT NULL,
    amount_cents bigint      NOT NULL,
    version      int         NOT NULL DEFAULT 0, -- optimistic lock: UPDATE ... WHERE version = $v
    deadline_at  timestamptz NOT NULL,           -- step timeout, used by the saga sweeper (phase 5)
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX sagas_active_deadline_idx ON sagas (deadline_at)
    WHERE state NOT IN ('COMPLETED', 'ABORTED');

-- +goose Down
DROP TABLE sagas;
DROP TABLE orders;
