-- +goose Up
CREATE TABLE seats (
    seat_id int  PRIMARY KEY,
    status  text NOT NULL DEFAULT 'FREE' CHECK (status IN ('FREE', 'RESERVED'))
);

CREATE TABLE reservations (
    saga_id    uuid        PRIMARY KEY,          -- one reservation per saga: makes reserve idempotent
    seat_id    int         NOT NULL REFERENCES seats (seat_id),
    status     text        NOT NULL CHECK (status IN ('HELD', 'RELEASED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
-- At most one HELD reservation per seat (the DB, not the code, guarantees no double booking).
CREATE UNIQUE INDEX reservations_one_held_per_seat ON reservations (seat_id) WHERE status = 'HELD';

INSERT INTO seats (seat_id) SELECT g FROM generate_series(1, 1000) AS g;

-- +goose Down
DROP TABLE reservations;
DROP TABLE seats;
