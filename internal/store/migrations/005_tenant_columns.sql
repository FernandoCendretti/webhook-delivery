-- +goose Up
-- +goose StatementBegin

-- Default tenant for pre-existing rows (dev environment; no production data)
INSERT INTO tenants (id, name)
VALUES ('00000000-0000-0000-0000-000000000001', 'system-default-tenant');

-- endpoints: add tenant_id
ALTER TABLE endpoints
    ADD COLUMN tenant_id UUID REFERENCES tenants(id);

UPDATE endpoints
    SET tenant_id = '00000000-0000-0000-0000-000000000001'
    WHERE tenant_id IS NULL;

ALTER TABLE endpoints ALTER COLUMN tenant_id SET NOT NULL;

CREATE INDEX idx_endpoints_tenant ON endpoints (tenant_id);

-- events: add tenant_id
ALTER TABLE events
    ADD COLUMN tenant_id UUID REFERENCES tenants(id);

UPDATE events ev
    SET tenant_id = (SELECT e.tenant_id FROM endpoints e WHERE e.id = ev.endpoint_id);

ALTER TABLE events ALTER COLUMN tenant_id SET NOT NULL;

-- deliveries: add tenant_id (denormalized for ordering subquery)
ALTER TABLE deliveries
    ADD COLUMN tenant_id UUID REFERENCES tenants(id);

UPDATE deliveries d
    SET tenant_id = (SELECT e.tenant_id FROM endpoints e WHERE e.id = d.endpoint_id);

ALTER TABLE deliveries ALTER COLUMN tenant_id SET NOT NULL;

-- Partial index for the per-tenant ordering NOT EXISTS subquery (hot path)
CREATE INDEX idx_deliveries_tenant_ordering
    ON deliveries (tenant_id, created_at)
    WHERE status NOT IN ('delivered', 'permanently_failed');

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_deliveries_tenant_ordering;
ALTER TABLE deliveries DROP COLUMN IF EXISTS tenant_id;
ALTER TABLE events DROP COLUMN IF EXISTS tenant_id;
DROP INDEX IF EXISTS idx_endpoints_tenant;
ALTER TABLE endpoints DROP COLUMN IF EXISTS tenant_id;
DELETE FROM tenants WHERE id = '00000000-0000-0000-0000-000000000001';
-- +goose StatementEnd
