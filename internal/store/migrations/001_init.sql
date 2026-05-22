-- +goose Up
-- +goose StatementBegin
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE endpoints (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url         TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_endpoints_url_scheme CHECK (url ~* '^https?://')
);

CREATE TABLE events (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_id   UUID NOT NULL REFERENCES endpoints(id),
    payload       JSONB NOT NULL,
    submitted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_events_endpoint ON events(endpoint_id);

CREATE TYPE delivery_status AS ENUM (
    'scheduled', 'in_flight', 'delivered', 'permanently_failed'
);

CREATE TABLE deliveries (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id              UUID NOT NULL REFERENCES events(id),
    endpoint_id           UUID NOT NULL REFERENCES endpoints(id),
    status                delivery_status NOT NULL DEFAULT 'scheduled',
    attempt_count         INT NOT NULL DEFAULT 0,
    next_attempt_at       TIMESTAMPTZ NOT NULL,
    in_flight_lease_until TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_deliveries_scheduled
    ON deliveries (next_attempt_at)
    WHERE status = 'scheduled';

CREATE INDEX idx_deliveries_in_flight_lease
    ON deliveries (in_flight_lease_until)
    WHERE status = 'in_flight';

CREATE INDEX idx_deliveries_event ON deliveries (event_id);

CREATE TYPE attempt_outcome AS ENUM (
    'success', 'transient_failure', 'permanent_failure', 'timeout'
);

CREATE TABLE attempts (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id          UUID NOT NULL REFERENCES deliveries(id),
    sequence             INT NOT NULL,
    started_at           TIMESTAMPTZ NOT NULL,
    completed_at         TIMESTAMPTZ,
    response_status_code INT,
    outcome              attempt_outcome NOT NULL,
    error_reason         TEXT,
    UNIQUE (delivery_id, sequence)
);
CREATE INDEX idx_attempts_delivery ON attempts (delivery_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS attempts;
DROP TYPE IF EXISTS attempt_outcome;
DROP TABLE IF EXISTS deliveries;
DROP TYPE IF EXISTS delivery_status;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS endpoints;
-- +goose StatementEnd
