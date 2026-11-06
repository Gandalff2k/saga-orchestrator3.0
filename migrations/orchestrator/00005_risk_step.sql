-- +goose Up
ALTER TABLE orders
    ADD COLUMN customer_id text,
    ADD COLUMN card_hash   text,
    ADD COLUMN ip          text,
    ADD COLUMN country     text;

ALTER TABLE sagas DROP CONSTRAINT sagas_state_check;
ALTER TABLE sagas ADD CONSTRAINT sagas_state_check
    CHECK (state IN ('SCORING', 'IN_REVIEW', 'RESERVING', 'CHARGING', 'REFUNDING', 'COMPENSATING',
                     'COMPLETED', 'ABORTED'));

-- +goose Down
ALTER TABLE sagas DROP CONSTRAINT sagas_state_check;
ALTER TABLE sagas ADD CONSTRAINT sagas_state_check
    CHECK (state IN ('RESERVING', 'CHARGING', 'REFUNDING', 'COMPENSATING', 'COMPLETED', 'ABORTED'));

ALTER TABLE orders
    DROP COLUMN customer_id,
    DROP COLUMN card_hash,
    DROP COLUMN ip,
    DROP COLUMN country;
