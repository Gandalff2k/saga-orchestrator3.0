-- The step timeout is decided by ONE component, the sweeper, from its own
-- config: a saga is overdue when it hasn't moved for SAGA_STEP_TIMEOUT
-- (updated_at is bumped on every transition). deadline_at was computed by
-- whoever wrote the saga (the API for the first step, the worker after), so
-- two deployments with different settings gave sagas different timeouts
-- (found on kind, D-604).

-- +goose Up
DROP INDEX sagas_active_deadline_idx;
ALTER TABLE sagas DROP COLUMN deadline_at;
CREATE INDEX sagas_active_updated_idx ON sagas (updated_at)
    WHERE state NOT IN ('COMPLETED', 'ABORTED');

-- +goose Down
DROP INDEX sagas_active_updated_idx;
ALTER TABLE sagas ADD COLUMN deadline_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX sagas_active_deadline_idx ON sagas (deadline_at)
    WHERE state NOT IN ('COMPLETED', 'ABORTED');
