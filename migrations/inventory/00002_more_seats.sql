-- A bigger catalogue so load tests can place many orders without running out
-- of seats (every order needs a distinct free seat to be confirmed).

-- +goose Up
INSERT INTO seats (seat_id) SELECT g FROM generate_series(1001, 100000) AS g;

-- +goose Down
DELETE FROM seats WHERE seat_id > 1000;
