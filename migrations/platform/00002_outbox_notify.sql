-- Wake the relay as soon as outbox rows are committed, instead of waiting for
-- its next poll. NOTIFY is transactional: it's delivered at COMMIT (exactly
-- when the rows become visible) and not at all on rollback. Statement-level,
-- and Postgres folds identical notifications of one transaction into one, so
-- a batch insert costs one notification.

-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION outbox_notify() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('outbox', '');
    RETURN NULL;
END
$$;
-- +goose StatementEnd

CREATE TRIGGER outbox_notify
    AFTER INSERT ON outbox
    FOR EACH STATEMENT EXECUTE FUNCTION outbox_notify();

-- +goose Down
DROP TRIGGER outbox_notify ON outbox;
DROP FUNCTION outbox_notify();
