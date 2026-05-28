-- +goose Up
-- +goose StatementBegin
CREATE TABLE tenants (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name       TEXT CHECK (name IS NULL OR (length(name) >= 1 AND length(name) <= 255)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS tenants;
-- +goose StatementEnd
