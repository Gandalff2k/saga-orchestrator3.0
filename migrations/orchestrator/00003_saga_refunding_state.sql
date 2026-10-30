-- REFUNDING: a payment step timed out and the saga is undoing whatever charge
-- may have happened before releasing the seat.

-- +goose Up
ALTER TABLE sagas DROP CONSTRAINT sagas_state_check;
ALTER TABLE sagas ADD CONSTRAINT sagas_state_check
    CHECK (state IN ('RESERVING', 'CHARGING', 'REFUNDING', 'COMPENSATING', 'COMPLETED', 'ABORTED'));

-- +goose Down
ALTER TABLE sagas DROP CONSTRAINT sagas_state_check;
ALTER TABLE sagas ADD CONSTRAINT sagas_state_check
    CHECK (state IN ('RESERVING', 'CHARGING', 'COMPENSATING', 'COMPLETED', 'ABORTED'));
