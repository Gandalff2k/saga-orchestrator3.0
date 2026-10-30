-- Why an order was aborted ("seat taken", "limit exceeded"), copied from the
-- participant's failure reply so GET /orders/{id} can explain itself.

-- +goose Up
ALTER TABLE orders ADD COLUMN failure_reason text;

-- +goose Down
ALTER TABLE orders DROP COLUMN failure_reason;
