-- CANCELLED: a refund arrived before (or instead of) the charge. The row is a
-- tombstone: a late charge command for this saga finds it and is refused,
-- so a timed-out, compensated order can never be charged afterwards.

-- +goose Up
ALTER TABLE payments DROP CONSTRAINT payments_status_check;
ALTER TABLE payments ADD CONSTRAINT payments_status_check
    CHECK (status IN ('CHARGED', 'DECLINED', 'REFUNDED', 'CANCELLED'));

-- +goose Down
ALTER TABLE payments DROP CONSTRAINT payments_status_check;
ALTER TABLE payments ADD CONSTRAINT payments_status_check
    CHECK (status IN ('CHARGED', 'DECLINED', 'REFUNDED'));
