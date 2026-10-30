-- Reverts 00002 (LISTEN/NOTIFY wake-up). Measured under load (D-701): a
-- transaction that NOTIFYs takes a database-wide lock at commit to append to
-- the notification queue, so every business transaction writing the outbox
-- committed one at a time. HTTP p95 went 19ms -> 99ms and saga p95 0.7s ->
-- 2.0s. Polling every 200ms is faster AND cheaper here.

-- +goose Up
DROP TRIGGER IF EXISTS outbox_notify ON outbox;
DROP FUNCTION IF EXISTS outbox_notify();

-- +goose Down
-- +goose StatementBegin
CREATE FUNCTION outbox_notify() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('outbox', '');
    RETURN NULL;
END
$$;
-- +goose StatementEnd
CREATE TRIGGER outbox_notify AFTER INSERT ON outbox FOR EACH STATEMENT EXECUTE FUNCTION outbox_notify();
