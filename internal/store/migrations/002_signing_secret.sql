-- +goose Up
-- +goose StatementBegin
ALTER TABLE endpoints
    ADD COLUMN signing_secret BYTEA;

UPDATE endpoints
    SET signing_secret = gen_random_bytes(32)
    WHERE signing_secret IS NULL;

ALTER TABLE endpoints
    ALTER COLUMN signing_secret SET NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE endpoints DROP COLUMN IF EXISTS signing_secret;
-- +goose StatementEnd
