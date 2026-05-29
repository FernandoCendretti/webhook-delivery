ALTER TABLE deliveries ADD COLUMN source_delivery_id UUID REFERENCES deliveries(id);

CREATE UNIQUE INDEX idx_deliveries_one_active_replay
    ON deliveries(source_delivery_id)
    WHERE source_delivery_id IS NOT NULL AND status IN ('scheduled', 'in_flight');

CREATE INDEX idx_deliveries_pf_tenant_endpoint
    ON deliveries(status, tenant_id, endpoint_id, updated_at DESC)
    WHERE status = 'permanently_failed';
