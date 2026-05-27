<!--
  Adapted from specs/templates/plan-template.md
  Feature: 003-order-circuit-breaker
-->

# Implementation Plan: Order & Circuit Breaker

**Date**: 2026-05-26
**Spec**: ./spec.md

## Summary

Feature 003 adds two capabilities to the delivery pipeline. A new `Tenant` entity groups
endpoints under a producer namespace; events carry a mandatory `tenant_id` and the
scheduler enforces per-tenant FIFO ordering — E2 is never dispatched before E1 reaches a
terminal state. A per-endpoint circuit breaker tracks consecutive transient failures and,
on reaching a configurable threshold (default 5), suspends delivery for a configurable
period (default 60 s). At suspension expiry, the oldest non-terminal delivery becomes a
probe; a successful probe closes the circuit and resumes normal delivery; a failed probe
reopens the circuit. All circuit breaker state lives in PostgreSQL, providing restart
durability (FR-013) and sub-500 ms cross-instance consistency without Redis. No new
external libraries are introduced.

## Technical Context

**Language/Version**: Go 1.25 (same as features 001/002)
**Primary Dependencies**: same as feature 002; no additions
**Storage**: PostgreSQL 16 — three new migrations; Redis 7 provisioned but unused in 003
**Messaging**: Apache Kafka 3.7 (unchanged)
**Testing**: stdlib `testing` + testify; integration via testcontainers (Postgres + Kafka)
**Target Platform**: Linux container, same binary (`webhookd api | worker | scheduler`)
**Performance Goals**: scheduler claim query (ordering + circuit filter) must complete
within one tick period (500 ms); partial index on `deliveries(tenant_id, created_at)`
keeps the ordering subquery below 10 ms at target load
**Constraints**: circuit state consistent across instances within 500 ms; must survive
complete restart; no new external libraries

## Project Structure

### Documentation (this feature)

```text
specs/003-order-circuit-breaker/
├── spec.md              # WHAT (approved)
├── plan.md              # this file — HOW
└── tasks.md             # ORDER (created after plan is approved)
```

### Source Code — new and modified files

```text
internal/
├── domain/
│   ├── tenant.go               NEW — Tenant struct
│   └── circuit_breaker.go      NEW — CircuitState enum, CircuitBreakerInfo struct
├── api/
│   ├── handlers_tenant.go      NEW — POST /v1/tenants, GET /v1/tenants/{id}
│   ├── handlers_endpoint.go    MODIFIED — Create accepts mandatory tenant_id;
│   │                                       validates tenant exists
│   ├── handlers_event.go       MODIFIED — Submit requires tenant_id;
│   │                                       validates tenant+endpoint match
│   ├── handlers_circuit.go     NEW — GET /v1/endpoints/{id}/circuit-breaker
│   ├── server.go               MODIFIED — register four new routes
│   └── dto.go                  MODIFIED — add TenantResponse, CreateTenantRequest;
│                                           update CreateEndpointRequest/EventRequest;
│                                           add CircuitBreakerResponse
├── service/
│   ├── tenant_service.go       NEW — Create, GetByID
│   ├── endpoint_service.go     MODIFIED — Create takes tenantID uuid.UUID
│   └── event_service.go        MODIFIED — Submit takes tenantID; validates cross-tenant
├── store/
│   ├── tenant_store.go         NEW — Insert, GetByID
│   ├── endpoint_store.go       MODIFIED — Insert stores tenant_id;
│   │                                       GetByID returns tenant_id
│   ├── delivery_store.go       MODIFIED — ClaimReady adds ordering + circuit filter;
│   │                                       Insert stores tenant_id;
│   │                                       LoadForWorker fetches endpoint circuit_state
│   ├── circuit_store.go        NEW — HandleTransientFailure, HandleSuccess,
│   │                                   HandleProbePermanentFailure,
│   │                                   ProcessExpiredSuspensions, SetProbeDelivery,
│   │                                   GetState
│   └── migrations/
│       ├── 004_tenants.sql         NEW
│       ├── 005_tenant_columns.sql  NEW
│       └── 006_circuit_breaker.sql NEW
├── scheduler/
│   └── scheduler.go            MODIFIED — each tick: (0a) recover orphaned half_open
│                                           endpoints; (0b) ProcessExpiredSuspensions +
│                                           SetProbeDelivery; (1) ClaimReady
├── delivery/
│   └── worker.go               MODIFIED — call circuit_store after each attempt result
└── config/
    └── config.go               MODIFIED — add CircuitBreakerThreshold, SuspensionSeconds
```

**Structure Decision**: All circuit breaker transitions live in `circuit_store.go` as
atomic single-row PostgreSQL UPDATEs — no in-memory state machine. This gives durability
and cross-instance consistency without additional infrastructure. `domain/circuit_breaker.go`
holds the pure enum and read model so handlers and services can depend on domain types
without importing the store. Tenant logic mirrors the existing endpoint/event pattern.

## Technical Design

### Components & responsibilities

```
Scheduler tick (every 500 ms)

  Step 0a — Recovery: fix orphaned half_open endpoints (scheduler crash guard)
    SELECT id FROM endpoints
    WHERE circuit_state = 'half_open' AND circuit_probe_delivery_id IS NULL
    → For each result: circuit_store.SetProbeDelivery(ctx, endpointID)
    (handles the case where a prior scheduler committed the half_open transition
    but crashed before calling SetProbeDelivery — see SetProbeDelivery for the
    empty-queue fallback that also closes the circuit when no delivery remains)

  Step 0b — Transition expired open circuits
    circuit_store.ProcessExpiredSuspensions(ctx, cfg)
      UPDATE endpoints WHERE circuit_state='open' AND circuit_suspended_until<=NOW()
        → 'half_open' if non-terminal deliveries exist for that endpoint
        → 'closed'    if queue is empty (FR-024), reset failure counter
      Returns: []halfOpenEndpointIDs, []closedEndpointIDs

    For each newly half_open endpoint:
      circuit_store.SetProbeDelivery(ctx, endpointID)
        SELECT oldest non-terminal delivery for endpoint
        → if none found: UPDATE endpoints SET circuit_state='closed', circuit_failure_count=0
                         (queue emptied in the race between ProcessExpiredSuspensions and here)
        → if found: UPDATE endpoints SET circuit_probe_delivery_id = probeID
                    UPDATE deliveries SET next_attempt_at=NOW() WHERE id=probeID AND next_attempt_at>NOW()

  Step 1 — Claim eligible deliveries
    delivery_store.ClaimReady(ctx, limit)
      FOR UPDATE SKIP LOCKED with ordering + circuit filter (see claim query below)
      Returns [(deliveryID, endpointID)]
      Publishes delivery IDs to Kafka


Worker process(deliveryID)
  1. LoadForWorker(deliveryID) — fetches url, signing_secret, circuit_state, ev.tenant_id
  2. Skip if delivery.status != 'in_flight' (idempotency guard; FR-023: in-flight continues)
  3. Insert attempt, execute HTTP POST with signing headers (feature 002)
  4. Classify outcome, then:

     OutcomeSuccess:
       circuit_store.HandleSuccess(ctx, endpointID)
       Mark delivery 'delivered'

     OutcomeTransient / OutcomeTimeout:
       circuit_store.HandleTransientFailure(ctx, endpointID, cfg)
       Update delivery retry schedule (or 'permanently_failed' at max attempts)

     OutcomePermanentFailure:
       IF wd.CircuitState == 'half_open':
         circuit_store.HandleProbePermanentFailure(ctx, endpointID, cfg)
       Mark delivery 'permanently_failed'
       (counter NOT incremented — FR-011)


API
  POST /v1/tenants             → tenant_service.Create
  GET  /v1/tenants/{id}        → tenant_service.GetByID
  POST /v1/endpoints           → endpoint_service.Create  (now requires tenant_id)
  POST /v1/events              → event_service.Submit     (now requires tenant_id)
  GET  /v1/endpoints/{id}/circuit-breaker → circuit_store.GetState
```

### Data model

#### Migration 004 — `004_tenants.sql`

```sql
CREATE TABLE tenants (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name       TEXT CHECK (name IS NULL OR (length(name) >= 1 AND length(name) <= 255)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

`name` is NULL when the producer omits it; the DTO omits the field from responses when
NULL. The 255-char and empty-string bounds are enforced both here and in Go validation;
the Unicode Cc category check (no control characters) is enforced only in Go because
PostgreSQL has no built-in Cc predicate.

#### Migration 005 — `005_tenant_columns.sql`

```sql
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
```

`deliveries.tenant_id` is denormalized from `endpoints.tenant_id` to avoid a JOIN in the
scheduler claim query. It is immutable after insert (same guarantee as `endpoint.tenant_id`).

#### Migration 006 — `006_circuit_breaker.sql`

```sql
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
```

`circuit_sensitive_recovery` is an internal implementation flag (never exposed in the API
response) that implements FR-019: after a successful probe, a single transient failure
reopens the circuit regardless of the configured threshold.

#### Domain types

```go
// internal/domain/tenant.go
type Tenant struct {
    ID        uuid.UUID
    Name      *string   // nil when not provided
    CreatedAt time.Time
}

// internal/domain/circuit_breaker.go
type CircuitState string
const (
    CircuitClosed   CircuitState = "closed"
    CircuitOpen     CircuitState = "open"
    CircuitHalfOpen CircuitState = "half_open"
)

type CircuitBreakerInfo struct {
    EndpointID         uuid.UUID
    State              CircuitState
    ConsecutiveFailures int
    SuspendedUntil     *time.Time // non-nil only when State == CircuitOpen
}
```

#### WorkerDelivery extension

```go
// internal/store/delivery_store.go — WorkerDelivery gains two fields:
type WorkerDelivery struct {
    // ... all fields from features 001/002 ...
    SigningSecret        []byte
    EndpointCircuitState string  // 'closed', 'open', 'half_open'
}
```

The worker uses `EndpointCircuitState` solely to determine whether to call
`HandleProbePermanentFailure` on a permanent-failure outcome.

### API contracts

All routes under `/v1`. JSON-only. No authentication.

#### POST /v1/tenants — new (FR-001, FR-002)

```http
POST /v1/tenants
Content-Type: application/json

{ "name": "acme-corp" }    -- name is optional; null and absent are equivalent
```

```json
201 Created
{
  "id": "uuid",
  "name": "acme-corp",
  "created_at": "2026-05-26T10:00:00Z"
}
```

When `name` is absent or null: response omits `name` entirely.

| Status | Body | Condition |
|--------|------|-----------|
| `400 Bad Request` | `{ "error": "invalid_name", "detail": "..." }` | `name` present but empty, >255 chars, or contains Unicode Cc character |

#### GET /v1/tenants/{id} — new (FR-003)

```json
200 OK
{
  "id": "uuid",
  "name": "acme-corp",
  "created_at": "2026-05-26T10:00:00Z"
}
```

`name` absent from response when tenant has no name.

| Status | Body | Condition |
|--------|------|-----------|
| `400 Bad Request` | `{ "error": "invalid_tenant_id" }` | `{id}` not a valid UUID |
| `404 Not Found` | `{ "error": "tenant_not_found" }` | tenant does not exist |

#### POST /v1/endpoints — updated (FR-007)

Request body (updated):
```json
{
  "url": "https://example.com/webhook",
  "tenant_id": "uuid"
}
```

Response (201, updated to include `tenant_id`):
```json
{
  "id": "uuid",
  "url": "https://example.com/webhook",
  "tenant_id": "uuid",
  "created_at": "2026-05-26T10:00:00Z",
  "signing_secret": "a3f1...64-char-hex"
}
```

New error responses:

| Status | Body | Condition |
|--------|------|-----------|
| `400 Bad Request` | `{ "error": "missing_tenant_id" }` | `tenant_id` absent from request |
| `422 Unprocessable Entity` | `{ "error": "tenant_not_found" }` | `tenant_id` does not reference an existing tenant |

#### GET /v1/endpoints/{id} — updated response

Response now includes `tenant_id`:
```json
200 OK
{ "id": "uuid", "url": "https://...", "tenant_id": "uuid", "created_at": "..." }
```

#### POST /v1/events — updated (FR-004, FR-005, FR-006)

Request body (updated):
```json
{
  "endpoint_id": "uuid",
  "tenant_id": "uuid",
  "payload": { ... }
}
```

Response: unchanged (`202 Accepted { "delivery_id": "uuid", "event_id": "uuid" }`)

New error responses:

| Status | Body | Condition |
|--------|------|-----------|
| `400 Bad Request` | `{ "error": "missing_tenant_id" }` | `tenant_id` field absent |
| `422 Unprocessable Entity` | `{ "error": "tenant_not_found" }` | `tenant_id` does not reference an existing tenant |
| `422 Unprocessable Entity` | `{ "error": "tenant_endpoint_mismatch" }` | endpoint's `tenant_id` differs from the supplied `tenant_id` |

#### GET /v1/endpoints/{id}/circuit-breaker — new (FR-021)

```json
200 OK — closed
{ "endpoint_id": "uuid", "state": "closed", "consecutive_failures": 0 }
```

```json
200 OK — open
{
  "endpoint_id": "uuid",
  "state": "open",
  "consecutive_failures": 5,
  "suspended_until": "2026-05-26T10:01:00Z"
}
```

```json
200 OK — half-open
{ "endpoint_id": "uuid", "state": "half-open", "consecutive_failures": 5 }
```

`suspended_until` is absent when state is `closed` or `half-open`. Note: the API uses
`"half-open"` (hyphen) in JSON responses; the PostgreSQL ENUM and Go constants use
`half_open` (underscore); the DTO handles the translation.

| Status | Condition |
|--------|-----------|
| `400 Bad Request` | `{ "error": "invalid_endpoint_id" }` when `{id}` is not a valid UUID |
| `404 Not Found` | `{ "error": "endpoint_not_found" }` |

### Critical flows

#### Flow A — Create tenant (FR-001, FR-002)

```
1. api.tenantHandler.Create receives POST /v1/tenants
2. Parse body:
   a. name absent or null → namePtr = nil
   b. name present and non-null:
      - empty string → 400 invalid_name
      - len(name) > 255 → 400 invalid_name
      - any rune where unicode.Is(unicode.Cc, r) → 400 invalid_name
      - valid → namePtr = &name
3. tenant_service.Create(ctx, namePtr):
     INSERT INTO tenants (name) VALUES ($1)
     RETURNING id, name, created_at
4. Respond 201 TenantResponse (name field omitted when nil)
```

#### Flow B — Create endpoint with tenant (FR-007)

```
1. Validate URL (existing)
2. Validate tenant_id present → else 400 missing_tenant_id
3. endpoint_service.Create(ctx, url, tenantID):
   BEGIN TX
     SELECT id FROM tenants WHERE id=$tenantID
       → 422 tenant_not_found if absent
     crypto/rand.Read(32 bytes) → secret  [feature 002]
     INSERT INTO endpoints (url, tenant_id, signing_secret)
     RETURNING id, created_at
   COMMIT
4. Respond 201 (includes tenant_id, signing_secret)
```

#### Flow C — Submit event with tenant validation (FR-004, FR-005, FR-006)

```
1. [existing] io.ReadAll + json.Unmarshal + idempotency gate (feature 002)
2. Validate tenant_id present in body → else 400 missing_tenant_id
3. event_service.Submit(ctx, endpointID, tenantID, payload, idempotencyKey, rawBody):
   BEGIN TX
     [idempotency gate from feature 002]

     -- FR-005: validate tenant exists
     SELECT id FROM tenants WHERE id=$tenantID
       → 422 tenant_not_found if absent

     -- FR-004 / FR-006: validate endpoint exists and belongs to tenant
     SELECT id, tenant_id FROM endpoints WHERE id=$endpointID
       → 404 endpoint_not_found if absent
       → 422 tenant_endpoint_mismatch if endpoint.tenant_id != tenantID

     INSERT INTO events (endpoint_id, tenant_id, payload) RETURNING id
     INSERT INTO deliveries (event_id, endpoint_id, tenant_id,
                              status='scheduled', next_attempt_at=NOW())
     RETURNING id

     [idempotency complete from feature 002]
   COMMIT
4. Respond 202 { delivery_id, event_id }
```

#### Flow D — Scheduler tick: circuit transitions + claim (FR-008–FR-009, FR-014–FR-015, FR-024)

```
Step 0 — ProcessExpiredSuspensions

  BEGIN TX
    WITH transitioned AS (
      UPDATE endpoints
      SET
        circuit_state = CASE
          WHEN EXISTS (
            SELECT 1 FROM deliveries d
            WHERE d.endpoint_id = endpoints.id
              AND d.status NOT IN ('delivered', 'permanently_failed')
          ) THEN 'half_open'::circuit_state
          ELSE 'closed'::circuit_state
        END,
        circuit_failure_count = CASE
          WHEN EXISTS (...non-terminal...) THEN circuit_failure_count
          ELSE 0
        END,
        circuit_suspended_until  = NULL,
        circuit_probe_delivery_id = NULL
      WHERE circuit_state = 'open'
        AND circuit_suspended_until <= NOW()
      RETURNING id, circuit_state
    )
    SELECT id, circuit_state FROM transitioned
  COMMIT

  For each endpoint where circuit_state = 'half_open':
    SetProbeDelivery(ctx, endpointID):
      BEGIN TX
        SELECT d.id FROM deliveries d
        WHERE d.endpoint_id = $endpointID
          AND d.status NOT IN ('delivered', 'permanently_failed')
        ORDER BY d.created_at ASC
        LIMIT 1
        → probeDeliveryID

        IF probeDeliveryID IS NULL:
          -- No non-terminal deliveries: queue emptied in the race between
          -- ProcessExpiredSuspensions and this call (or recovery step found
          -- an orphaned half_open endpoint). Apply FR-024 semantics.
          UPDATE endpoints
          SET circuit_state = 'closed', circuit_failure_count = 0
          WHERE id = $endpointID AND circuit_state = 'half_open'
          RETURN

        UPDATE endpoints SET circuit_probe_delivery_id=$probeDeliveryID WHERE id=$endpointID

        -- Dispatch probe immediately if retry was scheduled in the future (FR-015)
        UPDATE deliveries SET next_attempt_at=NOW()
        WHERE id=$probeDeliveryID AND next_attempt_at > NOW()
      COMMIT


Step 1 — ClaimReady

  BEGIN TX
    WITH claimed AS (
      SELECT d.id, d.endpoint_id
      FROM deliveries d
      JOIN endpoints e ON e.id = d.endpoint_id
      WHERE d.status = 'scheduled'
        AND d.next_attempt_at <= NOW()

        -- Per-tenant ordering (FR-008, FR-009):
        -- block if any earlier non-terminal delivery exists for the same tenant
        AND NOT EXISTS (
            SELECT 1 FROM deliveries d2
            WHERE d2.tenant_id = d.tenant_id
              AND d2.status NOT IN ('delivered', 'permanently_failed')
              AND d2.created_at < d.created_at
        )

        -- Circuit breaker (FR-012, FR-014):
        -- closed endpoints always eligible;
        -- half_open: only the designated probe delivery
        AND (
            e.circuit_state = 'closed'
            OR (e.circuit_state = 'half_open'
                AND e.circuit_probe_delivery_id = d.id)
        )

      ORDER BY d.next_attempt_at
      LIMIT 100
      FOR UPDATE OF d SKIP LOCKED
    )
    UPDATE deliveries d
    SET status = 'in_flight',
        in_flight_lease_until = NOW() + interval '5 minutes',
        updated_at = NOW()
    FROM claimed WHERE d.id = claimed.id
    RETURNING d.id, d.endpoint_id
  COMMIT

  Publish each delivery_id to Kafka (keyed by endpoint_id)
```

The NOT EXISTS subquery is covered by `idx_deliveries_tenant_ordering` — a partial index
on `(tenant_id, created_at)` excluding terminal rows. Two concurrent scheduler instances
race-safely: `FOR UPDATE SKIP LOCKED` ensures each delivery is claimed by at most one
instance, and the `ProcessExpiredSuspensions` UPDATE's WHERE clause is idempotent (the
second instance finds no matching 'open' rows).

#### Flow E — Worker: attempt + circuit update (FR-010–FR-023)

```
1. LoadForWorker(deliveryID) — extends feature 002 query:
     SELECT d.*, e.url, e.signing_secret, e.circuit_state, ev.payload
     FROM deliveries d
     JOIN endpoints e  ON e.id = d.endpoint_id
     JOIN events    ev ON ev.id = d.event_id
     WHERE d.id = $1

2. Skip if d.status != 'in_flight'

3. [existing] Insert attempt, execute HTTP POST with signing headers

4a. OutcomeSuccess:
    circuit_store.HandleSuccess(ctx, endpointID):
      UPDATE endpoints
      SET circuit_failure_count = 0,
          circuit_state         = 'closed',
          circuit_suspended_until   = NULL,
          circuit_probe_delivery_id = NULL,
          circuit_sensitive_recovery = CASE
              WHEN circuit_state = 'half_open' THEN TRUE  -- FR-019: probe succeeded
              ELSE FALSE
          END
      WHERE id=$endpointID AND circuit_state IN ('closed', 'half_open')
    Mark delivery 'delivered'

4b. OutcomeTransient / OutcomeTimeout:
    circuit_store.HandleTransientFailure(ctx, endpointID, cfg):
      UPDATE endpoints
      SET
        circuit_failure_count = circuit_failure_count + 1,
        circuit_state = CASE
            WHEN circuit_state = 'half_open'            THEN 'open'
            WHEN circuit_sensitive_recovery = TRUE       THEN 'open'
            WHEN circuit_failure_count+1 >= $threshold  THEN 'open'
            ELSE 'closed'
          END,
        circuit_suspended_until = CASE
            WHEN circuit_state = 'half_open'            THEN NOW() + $suspension
            WHEN circuit_sensitive_recovery = TRUE       THEN NOW() + $suspension
            WHEN circuit_failure_count+1 >= $threshold  THEN NOW() + $suspension
            ELSE NULL
          END,
        circuit_probe_delivery_id = CASE
            WHEN circuit_state = 'half_open' THEN NULL
            ELSE circuit_probe_delivery_id
          END,
        circuit_sensitive_recovery = FALSE
      WHERE id=$endpointID AND circuit_state IN ('closed', 'half_open')
      -- WHERE excludes already-open circuit: concurrent worker already opened it (no-op)
    Update delivery retry schedule (or 'permanently_failed' at max attempts)

4c. OutcomePermanentFailure:
    IF wd.EndpointCircuitState == 'half_open':
      circuit_store.HandleProbePermanentFailure(ctx, endpointID, cfg):
        UPDATE endpoints
        SET circuit_state             = 'open',
            circuit_suspended_until   = NOW() + $suspension,
            circuit_probe_delivery_id = NULL
        WHERE id=$endpointID AND circuit_state = 'half_open'
    Mark delivery 'permanently_failed'
    -- counter NOT incremented (FR-011)
```

The three `HandleXxx` methods are single-row UPDATEs with conditional CASE expressions.
Each is atomic and idempotent: concurrent workers racing on the same endpoint produce
correct results because the WHERE clause on `circuit_state` prevents double-transitions.
In-flight attempts at circuit-open time are allowed to complete (FR-023): the scheduler
stops dispatching new work once the circuit is open; the worker continues the already-
running attempt.

#### Flow F — Get circuit breaker state (FR-021)

```
GET /v1/endpoints/{id}/circuit-breaker
1. Parse + validate {id} → 400 if not UUID
2. circuit_store.GetState(ctx, endpointID):
     SELECT id, circuit_state, circuit_failure_count, circuit_suspended_until
     FROM endpoints WHERE id=$endpointID
     → nil → 404 endpoint_not_found
3. Build CircuitBreakerResponse:
   - Always: endpoint_id, state (translating half_open → "half-open"), consecutive_failures
   - Include suspended_until ONLY when state = 'open'
4. Respond 200
```

#### Flow G — FR-020: mid-retry-schedule recovery on circuit close

No special mechanism is required. When a circuit was open, blocked deliveries sat in
`status='scheduled'` with their original `next_attempt_at` values (including overdue
ones). When the circuit closes (either directly from open with empty queue, or via
successful probe), those deliveries' `next_attempt_at <= NOW()` already. On the next
scheduler tick (≤ 500 ms), the claim query picks them up — they pass the circuit check
(state = 'closed') and the ordering check (the probe event is now 'delivered'). The
attempt count is unchanged (FR-020).

### External dependencies

- PostgreSQL 16: all tenant and circuit breaker state stored here; no Redis for this feature.
- Kafka 3.7: delivery dispatch, unchanged.
- Redis 7: provisioned but unused in 003.

## Configuration (new variables)

| Variable | Default | Purpose |
|----------|---------|---------|
| `CIRCUIT_BREAKER_THRESHOLD` | `5` | Consecutive transient failures to open circuit (FR-022) |
| `CIRCUIT_BREAKER_SUSPENSION_SECONDS` | `60` | Suspension period duration in seconds (FR-022) |

Both loaded by `config/config.go` via `caarlos0/env/v11`. A `CircuitConfig` struct
bundles them and is passed into the circuit store methods that need them:

```go
// internal/config/config.go
type CircuitConfig struct {
    Threshold          int           // CIRCUIT_BREAKER_THRESHOLD (default 5)
    SuspensionDuration time.Duration // derived from CIRCUIT_BREAKER_SUSPENSION_SECONDS (default 60s)
}
```

## Testing Strategy

| Layer | Scenario | How |
|-------|----------|-----|
| Unit | Tenant name validation: accept 1 char, 255 chars; reject empty string, 256 chars, NUL byte (Cc), byte 0x01 (Cc), byte 0x7F (DEL — Cc in Go's unicode.Cc), emoji (not Cc) | `api/handlers_tenant_test.go` |
| Unit | Circuit state transitions: table-driven test over `(initial_state, sensitive_recovery, failure_count, threshold, outcome)` tuples; assert resulting state, suspended_until presence, counter value | `store/circuit_store_test.go` |
| Integration | `POST /v1/tenants`: 201 with UUID; with valid name → name present; without name → name absent; with empty name → 400; with 256-char name → 400; with control char → 400 | real Postgres |
| Integration | `GET /v1/tenants/{id}`: 200; non-existent → 404; invalid UUID → 400 | real Postgres |
| Integration | `POST /v1/endpoints` with valid tenant_id → 201 includes tenant_id; with non-existent tenant_id → 422; without tenant_id → 400 | real Postgres |
| Integration | `GET /v1/endpoints/{id}` response includes tenant_id | real Postgres |
| Integration | `POST /v1/events`: without tenant_id → 400; non-existent tenant_id → 422; endpoint belongs to different tenant → 422 | real Postgres |
| Integration | Ordering: submit E1 + E2 under same tenant; verify E2 not in ClaimReady result while E1 is non-terminal; advance E1 to 'delivered' via SQL; verify E2 becomes claimable | real Postgres |
| Integration | Ordering across tenants: E1 under T1, E2 under T2; verify E2 is claimable while E1 is non-terminal | real Postgres |
| Integration | Ordering + in-flight: E1 transitions to 'in_flight'; verify E2 still blocked | real Postgres |
| Integration | Circuit: 5 transient failures → HandleTransientFailure × 5 → circuit_state='open'; `GET .../circuit-breaker` → state=open, consecutive_failures=5, suspended_until present | real Postgres |
| Integration | Circuit: permanent failure does not increment counter — 4 transient + 1 permanent + 1 transient = counter 5, not 6 (FR-011) | real Postgres |
| Integration | Circuit: open → manipulate suspended_until to past → ProcessExpiredSuspensions → endpoint has non-terminal delivery → half_open + probe_delivery_id set | real Postgres |
| Integration | Circuit: open → no non-terminal deliveries → ProcessExpiredSuspensions → closed, counter=0 (FR-024) | real Postgres |
| Integration | Scheduler crash recovery: force endpoint to `half_open` with `circuit_probe_delivery_id=NULL` via SQL; run scheduler tick; verify probe_delivery_id set and delivery dispatched (step 0a recovery) | real Postgres |
| Integration | SetProbeDelivery empty-queue race: force endpoint to `half_open` then mark its last non-terminal delivery as 'delivered' via SQL before calling SetProbeDelivery; verify endpoint transitions to `closed`, counter=0 | real Postgres |
| Integration | Circuit: probe success → HandleSuccess → state=closed, circuit_sensitive_recovery=TRUE | real Postgres |
| Integration | Circuit: probe transient failure → HandleTransientFailure from half_open → state=open, new suspended_until, probe_delivery_id=NULL | real Postgres |
| Integration | Circuit: probe permanent failure → HandleProbePermanentFailure → state=open, suspended_until set (FR-018) | real Postgres |
| Integration | FR-019: circuit_sensitive_recovery=TRUE → single transient failure → state=open immediately | real Postgres |
| Integration | FR-019: after probe success + one successful delivery → circuit_sensitive_recovery=FALSE → normal threshold applies | real Postgres |
| Integration | Restart durability: open circuit → close and reopen Postgres connection → GetState returns open (FR-013) | real Postgres; reconnect pool |
| Integration | Multi-instance: two goroutines call HandleTransientFailure concurrently on same endpoint for the 5th failure; assert circuit_state=open exactly once (no double-open) | parallel goroutines; real Postgres |
| Integration | FR-020: delivery with overdue next_attempt_at during open circuit → after circuit closes, delivery appears in ClaimReady on next tick | real Postgres |
| E2E | Full pipeline: register tenant, two endpoints E_A and E_B; submit E1 targeting E_A, E2 targeting E_B under same tenant; E_A always returns 503; after 5 failures, circuit opens for E_A; verify E2 NOT attempted; suspension expires; probe (E1) → 503 again → circuit reopens; manipulate suspended_until; probe → 200 → E1 delivered, circuit closed; E2 proceeds normally | testcontainers + httptest.Server |
| E2E | `GET /v1/endpoints/{id}/circuit-breaker` reflects correct state at each phase of the E2E test above | same E2E test |

Coverage notes:
- SC-002 (100% ordering): ordering integration tests verify zero claims before predecessor terminal
- SC-003 (500ms open): circuit_state='open' is committed atomically in HandleTransientFailure; GetState reads PG directly — no caching introduces delay
- SC-007 (restart durability): integration test verifies state survives pool reconnect
- SC-008 (multi-instance): concurrent-goroutines integration test
- SC-010 (overdue retry after circuit close): FR-020 integration test
- SC-011 (single failure reopens after probe): FR-019 integration tests

## Trade-offs

| Decision | Chosen | Rejected | Reason |
|----------|--------|----------|--------|
| Circuit state storage | PostgreSQL only | Redis hot-state + PG for durability | PG satisfies FR-013 (restart + 500ms consistency) without synchronization between two stores. At target scale (≤10k endpoints), PG write throughput is not a bottleneck. Redis would add two-store consistency risk on every transition. |
| Ordering enforcement | Scheduler-level NOT EXISTS subquery | Kafka partition ordering | Kafka ordering guarantees message sequence, not delivery-completion order. A circuit breaker stalling message M does not prevent M+1 from being published; Kafka cannot express "wait for prior delivery to complete." The NOT EXISTS subquery on a partial index is authoritative and efficient. |
| Ordering key | `deliveries.created_at` (denormalized `tenant_id`) | JOIN to `events.submitted_at` | `deliveries.created_at` is set in the same INSERT transaction as `events.submitted_at` — semantically equivalent. Denormalizing `tenant_id` onto deliveries enables the partial index `(tenant_id, created_at) WHERE status NOT IN (...)` and eliminates a JOIN in the hot-path claim query. |
| Probe identification | `circuit_probe_delivery_id` FK on endpoints | Dedicated `probe_deliveries` table | One nullable FK per endpoint is sufficient (cardinality is always 0 or 1 probe). A separate table would be over-engineering for a single-value relationship. |
| Sensitive recovery | `circuit_sensitive_recovery BOOLEAN` on endpoints | Fourth public circuit state (e.g. `sensitive_closed`) | A boolean is an implementation detail, not a producer-visible state. Exposing a fourth state would change the API contract without spec justification and confuse producers who only need to distinguish open / not-open. |
| Half-open probe dispatch | `next_attempt_at = NOW()` at SetProbeDelivery | Wait for remaining retry interval | The probe is a deliberate health check after a full suspension period. Waiting up to 24 h (max retry interval) for it would defeat its purpose. Overriding to NOW() is consistent with FR-015's intent and matches FR-020's "whichever comes later: circuit closing or scheduled retry time." |
| Circuit transition atomicity | Single conditional UPDATE per outcome | Read-then-conditional-write | A single UPDATE with CASE expressions is atomic at the row level. Concurrent workers racing on the 5th failure: the first commits, setting circuit_state='open'; the second's WHERE `circuit_state IN ('closed','half_open')` no longer matches → no-op. No advisory lock needed. |
| tenant_id format | UUID (`gen_random_uuid()`) | nanoid, sequential integer | Consistent with all identifiers in the system. Opaque, collision-free at target scale. |
| Migration backfill | Single `system-default-tenant` for existing rows | Require re-registration | Pre-production system; no live data. A single default tenant safely satisfies the FK without disrupting test fixtures. The operator can re-assign endpoints after migration. |

## Open Questions

None. All open items from the spec were resolved before this plan was written.

## Review Checklist

- [x] Every FR from spec has a clear implementation path in this plan
- [x] Every SC from spec has a way to be measured post-implementation
- [x] Error scenarios from spec are covered, not only the happy path
- [x] Library choices are justified (not just "I know this one")
- [x] Testing strategy covers the spec's acceptance scenarios
- [x] No `[NEEDS CLARIFICATION]` markers remain
