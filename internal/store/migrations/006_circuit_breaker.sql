-- +goose Up
-- +goose StatementBegin
CREATE TYPE circuit_state AS ENUM ('closed', 'open', 'half_open');

ALTER TABLE endpoints
    ADD COLUMN circuit_state              circuit_state NOT NULL DEFAULT 'closed',
    ADD COLUMN circuit_failure_count      INT           NOT NULL DEFAULT 0,
    ADD COLUMN circuit_suspended_until    TIMESTAMPTZ,
    ADD COLUMN circuit_sensitive_recovery BOOLEAN       NOT NULL DEFAULT FALSE,
    ADD COLUMN circuit_probe_delivery_id  UUID
        REFERENCES deliveries(id) ON DELETE SET NULL;

-- Scheduler uses this to find endpoints with expired suspensions efficiently
CREATE INDEX idx_endpoints_open_suspended
    ON endpoints (circuit_suspended_until)
    WHERE circuit_state = 'open';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_endpoints_open_suspended;
ALTER TABLE endpoints
    DROP COLUMN IF EXISTS circuit_probe_delivery_id,
    DROP COLUMN IF EXISTS circuit_sensitive_recovery,
    DROP COLUMN IF EXISTS circuit_suspended_until,
    DROP COLUMN IF EXISTS circuit_failure_count,
    DROP COLUMN IF EXISTS circuit_state;
DROP TYPE IF EXISTS circuit_state;
-- +goose StatementEnd
