-- +goose Up
CREATE TABLE payments (
    saga_id      uuid        PRIMARY KEY,         -- one payment per saga: makes charge idempotent
    amount_cents bigint      NOT NULL,
    status       text        NOT NULL CHECK (status IN ('CHARGED', 'DECLINED', 'REFUNDED')),
    reason       text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE payments;
