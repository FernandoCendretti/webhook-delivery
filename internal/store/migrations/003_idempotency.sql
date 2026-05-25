-- +goose Up
-- +goose StatementBegin
CREATE TABLE idempotency_records (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_id     UUID        NOT NULL REFERENCES endpoints(id),
    idempotency_key TEXT        NOT NULL,
    payload_hash    TEXT        NOT NULL,
    event_id        UUID        REFERENCES events(id),
    delivery_id     UUID        REFERENCES deliveries(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ NOT NULL,
    UNIQUE (endpoint_id, idempotency_key)
);

CREATE INDEX idx_idempotency_expires
    ON idempotency_records (expires_at);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_idempotency_expires;
DROP TABLE IF EXISTS idempotency_records;
-- +goose StatementEnd
