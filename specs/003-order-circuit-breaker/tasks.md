<!--
  Adapted from specs/templates/tasks-template.md
  Feature: 003-order-circuit-breaker
-->

# Tasks: Order & Circuit Breaker

**Input**: Design documents from `specs/003-order-circuit-breaker/`
**Prerequisites**: `plan.md` (approved), `spec.md` (approved)

**Tests**: Tests are part of every deliverable. Test tasks appear before implementation
tasks within each User Story phase. Confirm tests FAIL before writing implementation.

**Organization**: Tasks are grouped by User Story so each story can be implemented and
validated independently. Phases 1 and 2 establish shared prerequisites that block all
user stories.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: Can run in parallel (different files, no mutual dependency)
- **[Story]**: Which User Story this task belongs to (US1, US2, US3, US4)
- Every task includes a concrete file path

---

## Phase 1: Migrations

**Purpose**: All three migrations must apply cleanly before any code depends on the
new schema. They are sequential in goose order but can be written in parallel.

- [x] T001 Create `internal/store/migrations/004_tenants.sql` — `CREATE TABLE tenants` with
      `id UUID PRIMARY KEY DEFAULT gen_random_uuid()`, `name TEXT` with CHECK constraint
      (NULL or length 1–255), `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
      (see plan.md §Migration 004)
- [x] T002 [P] Create `internal/store/migrations/005_tenant_columns.sql` — inserts
      system-default-tenant row; adds `tenant_id UUID REFERENCES tenants(id)` to `endpoints`,
      `events`, and `deliveries` (each with UPDATE backfill and `SET NOT NULL`); adds
      `idx_endpoints_tenant` and `idx_deliveries_tenant_ordering` partial index on
      `(tenant_id, created_at) WHERE status NOT IN ('delivered','permanently_failed')`
      (see plan.md §Migration 005)
- [x] T003 [P] Create `internal/store/migrations/006_circuit_breaker.sql` — creates
      `circuit_state` ENUM `('closed','open','half_open')`; adds `circuit_state`,
      `circuit_failure_count`, `circuit_suspended_until`, `circuit_sensitive_recovery`,
      `circuit_probe_delivery_id` columns to `endpoints`; adds `idx_endpoints_open_suspended`
      partial index on `(circuit_suspended_until) WHERE circuit_state='open'`
      (see plan.md §Migration 006)

**Checkpoint**: `make migrate-up` applies all three migrations in order against a fresh
Postgres container without errors.

---

## Phase 2: Domain Types & Configuration

**Purpose**: Pure types and config that all downstream components depend on. No store or
HTTP dependencies at this layer.

- [x] T004 [P] Create `internal/domain/tenant.go` — `Tenant` struct with fields
      `ID uuid.UUID`, `Name *string` (nil when not provided by producer), `CreatedAt time.Time`
      (see plan.md §Domain types)
- [x] T005 [P] Create `internal/domain/circuit_breaker.go` — `CircuitState` type (string)
      with constants `CircuitClosed`, `CircuitOpen`, `CircuitHalfOpen`; `CircuitBreakerInfo`
      struct with `EndpointID uuid.UUID`, `State CircuitState`, `ConsecutiveFailures int`,
      `SuspendedUntil *time.Time` (non-nil only when State == CircuitOpen)
      (see plan.md §Domain types)
- [x] T006 [P] Add `CircuitConfig` struct to `internal/config/config.go` — fields
      `Threshold int` (env `CIRCUIT_BREAKER_THRESHOLD`, default 5) and
      `SuspensionDuration time.Duration` (derived from `CIRCUIT_BREAKER_SUSPENSION_SECONDS`,
      default 60 s) loaded via `caarlos0/env/v11`; must be created before any circuit store
      tasks (see plan.md §Configuration)

**Checkpoint**: `go build ./internal/domain/... ./internal/config/...` succeeds.

---

## Phase 3: User Story 1 — Register Tenant (Priority: P1) 🎯 MVP

**Goal**: Producer can create a tenant via `POST /v1/tenants` and retrieve it via
`GET /v1/tenants/{id}`. Endpoint creation requires a valid `tenant_id`. All subsequent
phases depend on tenants existing.

**Independent Test**: `POST /v1/tenants` with a valid name → 201 with `tenant_id`.
`POST /v1/endpoints` with that `tenant_id` → 201 with `tenant_id` in body.
`POST /v1/endpoints` with a non-existent `tenant_id` → 422.

### Tests for User Story 1

> Write these tests FIRST and confirm they FAIL before any implementation.

- [x] T007 [P] [US1] Unit tests for tenant name validation in `internal/api/handlers_tenant_test.go`:
      accept 1-char name, 255-char name, absent name (nil), null name; reject empty string → 400,
      256-char name → 400, NUL byte (Cc) → 400, byte 0x01 (Cc) → 400, byte 0x7F DEL (Cc) → 400;
      emoji (not Cc) → accepted
- [x] T008 [US1] Integration tests in `tests/integration/api_tenants_test.go` (real Postgres):
      `POST /v1/tenants` → 201 with UUID and `created_at`; with valid name → name present in
      response; without name → name absent from response; empty string name → 400; 256-char
      name → 400; control char name → 400; `GET /v1/tenants/{id}` → 200 with correct attributes;
      non-existent id → 404; invalid UUID in path → 400
- [x] T009 [US1] Integration tests in `tests/integration/api_endpoints_003_test.go` (real Postgres):
      `POST /v1/endpoints` without `tenant_id` → 400 `missing_tenant_id`;
      with non-existent `tenant_id` → 422 `tenant_not_found`;
      with valid existing `tenant_id` → 201 response includes `tenant_id`;
      `GET /v1/endpoints/{id}` response includes `tenant_id`

**Run tests — confirm they FAIL before implementing.**

### Implementation for User Story 1

- [x] T010 [US1] Implement `internal/store/tenant_store.go` — `Insert(ctx, t *domain.Tenant) error`
      and `GetByID(ctx, id uuid.UUID) (*domain.Tenant, error)`; map `pgx.ErrNoRows` to
      `domain.ErrNotFound`
- [x] T011 [P] [US1] Add tenant DTOs to `internal/api/dto.go` — `CreateTenantRequest` with
      optional `Name *string`; `TenantResponse` with `ID`, `Name *string` (json `omitempty`),
      `CreatedAt`
- [x] T012 [US1] Implement `internal/service/tenant_service.go` — `Create(ctx, name *string)
      (*domain.Tenant, error)` and `GetByID(ctx, id uuid.UUID) (*domain.Tenant, error)`
      (depends on T010)
- [x] T013 [US1] Update `internal/store/endpoint_store.go` — `Insert` includes `tenant_id` in
      the INSERT statement and returns it in the result; `GetByID` includes `tenant_id` in SELECT
      (depends on T002 migration)
- [x] T014 [US1] Update `internal/api/dto.go` — add `TenantID uuid.UUID` (json `tenant_id`)
      to `CreateEndpointRequest`; add `TenantID uuid.UUID` to `EndpointResponse` and
      `EndpointCreatedResponse` (depends on T011)
- [x] T015 [US1] Update `internal/service/endpoint_service.go` — `Create(ctx, url string,
      tenantID uuid.UUID)`: validate `tenantID` non-zero; `SELECT id FROM tenants WHERE id=$tenantID`
      inside TX → return error mapped to 422 `tenant_not_found` if absent; pass `tenantID` to
      `endpoint_store.Insert` (depends on T012, T013)
- [x] T016 [US1] Update `internal/api/handlers_endpoint.go` — parse mandatory `tenant_id` from
      request body → 400 `missing_tenant_id` if absent; pass to `endpoint_service.Create`; map
      tenant-not-found sentinel → 422 `tenant_not_found` (depends on T014, T015)
- [x] T017 [US1] Implement `internal/api/handlers_tenant.go` — `Create` handler: parse body,
      validate name (unicode.Cc check per FR-002) → 400 `invalid_name` or 201 `TenantResponse`;
      `GetByID` handler: parse + validate UUID → 400 / 200 / 404 (depends on T011, T012)
- [x] T018 [US1] Register tenant routes in `internal/api/server.go` —
      `POST /v1/tenants → tenantHandler.Create`,
      `GET /v1/tenants/{id} → tenantHandler.GetByID` (depends on T017)

**Checkpoint**: `go test ./tests/integration/...` for T008, T009 passes. `POST /v1/tenants`
and `POST /v1/endpoints` (with tenant) work end-to-end.

---

## Phase 4: User Story 2 — Ordered Delivery (Priority: P1) 🎯 MVP

**Goal**: `POST /v1/events` requires a `tenant_id` matching the target endpoint's tenant.
The scheduler NEVER dispatches E2 for a tenant before E1 (submitted earlier under the
same tenant) has reached a terminal state (`delivered` or `permanently_failed`).

**Independent Test**: Submit E1 then E2 under the same tenant. Verify `ClaimReady` does
not return E2's delivery while E1 is non-terminal. Mark E1 `delivered` via SQL; verify E2
becomes claimable on the next call to `ClaimReady`.

### Tests for User Story 2

> Write these tests FIRST and confirm they FAIL before any implementation.

- [x] T019 [US2] Integration tests in `tests/integration/api_events_003_test.go` (real Postgres):
      `POST /v1/events` without `tenant_id` → 400 `missing_tenant_id`;
      with non-existent `tenant_id` → 422 `tenant_not_found`;
      endpoint belongs to different tenant → 422 `tenant_endpoint_mismatch`;
      valid `tenant_id` matching endpoint → 202
- [x] T020 [US2] Integration tests in `tests/integration/ordering_test.go` (real Postgres):
      same-tenant ordering: E1 + E2 under same tenant; E2 absent from `ClaimReady` while E1
      is non-terminal; advance E1 to `delivered` via SQL; E2 appears in next `ClaimReady`;
      advance E1 to `permanently_failed` via SQL (separate sub-case); E2 also appears in
      next `ClaimReady` — confirms both terminal states unblock the queue (AS4, FR-008);
      cross-tenant: E1 under T1, E2 under T2; E2 claimable while E1 is non-terminal;
      in-flight ordering: E1 at `in_flight`; E2 still absent from `ClaimReady`

**Run tests — confirm they FAIL before implementing.**

### Implementation for User Story 2

- [x] T021 [P] [US2] Update `EventRequest` DTO in `internal/api/dto.go` — add mandatory
      `TenantID uuid.UUID` (json `tenant_id`)
- [x] T022 [US2] Update `internal/service/event_service.go` — `Submit` signature adds
      `tenantID uuid.UUID`; inside TX: validate tenant exists → 422 `tenant_not_found`;
      validate endpoint exists and `endpoint.tenant_id == tenantID` → 422
      `tenant_endpoint_mismatch`; include `tenant_id` in `INSERT INTO events` and
      `INSERT INTO deliveries` (depends on T015)
- [x] T023 [US2] Update `internal/api/handlers_event.go` — parse mandatory `tenant_id` from
      body → 400 `missing_tenant_id` if absent; pass to `event_service.Submit`; map new
      sentinel errors → 422 `tenant_not_found` / `tenant_endpoint_mismatch` (depends on T021, T022)
- [x] T024 [US2] Update `internal/store/delivery_store.go` — add `tenant_id` column to
      `Insert` statement; add `EndpointCircuitState string` field to `WorkerDelivery` struct
      and populate it from `e.circuit_state` in `LoadForWorker` JOIN; update `ClaimReady` to
      add the per-tenant ordering NOT EXISTS subquery from plan.md §Flow D Step 1 (using
      `idx_deliveries_tenant_ordering` — ordering filter only; circuit filter is added in T041)
      (depends on T002 and T003 migrations)

**Checkpoint**: `go test ./tests/integration/...` for T019, T020 passes. US1 tests remain green.

---

## Phase 5: User Story 3 — Circuit Breaker (Priority: P1)

**Goal**: Consecutive transient failures open the circuit for an endpoint, suspending
delivery. At suspension expiry the oldest queued event becomes a probe; a successful
probe closes the circuit and resumes delivery in order. All state lives in PostgreSQL.

**Independent Test**: Submit 6 events to an always-503 endpoint. After the 5th failure the
circuit opens. Verify no 6th attempt is dispatched. Advance `suspended_until` to the past
via SQL. Verify probe is set and dispatched. Probe succeeds → circuit closed, remaining
events delivered in submission order.

### Skeleton (required before tests can compile)

- [ ] T024a [US3] Create `internal/store/circuit_store.go` skeleton — declare all 6 method
      signatures with stub bodies that return zero values and no logic (`HandleSuccess`,
      `HandleTransientFailure`, `HandleProbePermanentFailure`, `ProcessExpiredSuspensions`,
      `SetProbeDelivery`, `GetState`); this unblocks T025–T039 test compilation before T040a/T040b
      implement the real logic (depends on T005, T006)

### Tests for User Story 3

> Write these tests FIRST and confirm they FAIL before any implementation.

- [ ] T025 [US3] Unit tests in `internal/store/circuit_store_test.go` — table-driven test
      over `(initial_state, sensitive_recovery bool, failure_count, threshold, outcome)`
      tuples for `HandleTransientFailure` and `HandleSuccess`; assert resulting
      `circuit_state`, presence/absence of `suspended_until`, counter value,
      `sensitive_recovery` flag after each transition
- [ ] T026 [US3] Integration test in `tests/integration/circuit_breaker_test.go` — 5 calls
      to `HandleTransientFailure` on the same endpoint → `circuit_state='open'`,
      `circuit_failure_count=5`, `circuit_suspended_until` non-null; 6th call is a no-op
      (WHERE clause skips already-open state); `GetState` returns
      `CircuitBreakerInfo{State: CircuitOpen, ConsecutiveFailures: 5, SuspendedUntil: non-nil}`
- [ ] T027 [US3] Integration test in `tests/integration/circuit_breaker_test.go` — permanent
      failure does NOT increment counter (FR-011): 4 `HandleTransientFailure` calls; do NOT
      call `HandleTransientFailure` for the permanent failure (correct worker behaviour);
      call `HandleTransientFailure` one more time → counter = 5, circuit opens; separately
      verify that 4 transient + 1 permanent path leaves counter at 4 (circuit not yet open)
- [ ] T028 [US3] Integration test in `tests/integration/circuit_breaker_test.go` —
      `ProcessExpiredSuspensions` with non-terminal delivery: open endpoint with
      `suspended_until` in the past; non-terminal delivery exists → state transitions to
      `half_open`; subsequent `SetProbeDelivery` sets `circuit_probe_delivery_id`; delivery
      `next_attempt_at` is reset to `NOW()` if it was scheduled in the future
- [ ] T029 [US3] Integration test in `tests/integration/circuit_breaker_test.go` —
      `ProcessExpiredSuspensions` with empty queue (FR-024): open endpoint with
      `suspended_until` in the past; all deliveries are in terminal state → state transitions
      directly to `closed`; `circuit_failure_count = 0`; no probe delivery set
- [ ] T030 [US3] Integration test in `tests/integration/circuit_breaker_test.go` — scheduler
      crash recovery (Step 0a): force endpoint to `half_open` with `circuit_probe_delivery_id=NULL`
      via SQL (simulates scheduler crash between `ProcessExpiredSuspensions` and
      `SetProbeDelivery`); run scheduler tick; verify `circuit_probe_delivery_id` is populated
      and the delivery is now returned by `ClaimReady`
- [ ] T031 [US3] Integration test in `tests/integration/circuit_breaker_test.go` —
      `SetProbeDelivery` empty-queue race (FR-024 fallback): force endpoint to `half_open`
      via SQL; mark its last non-terminal delivery as `delivered` via SQL before calling
      `SetProbeDelivery`; assert endpoint transitions to `closed` with `circuit_failure_count=0`
- [ ] T032 [US3] Integration test in `tests/integration/circuit_breaker_test.go` — probe
      success: endpoint in `half_open`; call `HandleSuccess` → `circuit_state='closed'`,
      `circuit_sensitive_recovery=TRUE`, `circuit_failure_count=0`,
      `circuit_probe_delivery_id=NULL`
- [ ] T033 [US3] Integration test in `tests/integration/circuit_breaker_test.go` — probe
      transient failure: endpoint in `half_open`; call `HandleTransientFailure` →
      `circuit_state='open'`, new `circuit_suspended_until`, `circuit_probe_delivery_id=NULL`
      (FR-017)
- [ ] T034 [US3] Integration test in `tests/integration/circuit_breaker_test.go` — probe
      permanent failure: endpoint in `half_open`; call `HandleProbePermanentFailure` →
      `circuit_state='open'`, `circuit_suspended_until` set for a new full suspension period
      (FR-018); counter NOT incremented
- [ ] T035 [US3] Integration test in `tests/integration/circuit_breaker_test.go` — FR-019
      sensitive recovery: set `circuit_sensitive_recovery=TRUE` on a closed endpoint via SQL;
      single call to `HandleTransientFailure` (count+1 = 1, below default threshold of 5) →
      `circuit_state='open'` immediately; `circuit_sensitive_recovery=FALSE` after transition
- [ ] T036 [US3] Integration test in `tests/integration/circuit_breaker_test.go` — FR-019
      reset: endpoint in `half_open` → `HandleSuccess` → `circuit_sensitive_recovery=TRUE`;
      call `HandleSuccess` again (simulates one subsequent successful delivery) →
      `circuit_sensitive_recovery=FALSE`; verify a single subsequent `HandleTransientFailure`
      does NOT open the circuit (threshold applies normally)
- [ ] T037 [US3] Integration test in `tests/integration/circuit_breaker_test.go` — restart
      durability (FR-013, SC-007): open a circuit; close and re-open the pgx pool connection
      (or query from a fresh pool); `GetState` returns `{State: CircuitOpen}` — circuit state
      survives reconnect
- [ ] T038 [US3] Integration test in `tests/integration/circuit_breaker_test.go` — multi-instance
      concurrency (SC-008): two goroutines call `HandleTransientFailure` concurrently on the
      same endpoint for the 5th failure; assert `circuit_state='open'` exactly once (no
      double-open); verify via `GetState` that `circuit_failure_count` did not exceed threshold
- [ ] T039 [US3] Integration test in `tests/integration/circuit_breaker_test.go` — FR-020
      overdue retry: delivery with `next_attempt_at` in the past sits in `scheduled` while
      circuit is open; close the circuit via direct SQL update to `circuit_state='closed'`;
      verify delivery appears in `ClaimReady` results immediately on the next call
- [ ] T039a [US3] Integration test in `tests/integration/circuit_breaker_test.go` — AS8
      cross-tenant circuit isolation: open the circuit on endpoint A (tenant T1) via SQL
      (`circuit_state='open'`); register endpoint B under a different tenant T2 with a
      `scheduled` delivery; call `ClaimReady`; assert endpoint B's delivery IS returned
      (T1's open circuit does NOT block T2's endpoints, FR-009 + FR-014)
- [ ] T039b [US3] Integration test in `tests/integration/circuit_breaker_test.go` — SC-005
      queue drain: endpoint in `half_open` with probe delivery D1 and two waiting deliveries
      D2, D3; call `HandleSuccess` (probe succeeds) → `circuit_state='closed'`; call
      `ClaimReady` (with ordering filter, D1 now terminal); assert both D2 and D3 appear in
      results — confirms no delivery is silently dropped or permanently blocked after circuit
      close (SC-005)

**Run tests — confirm they FAIL before implementing.**

### Implementation for User Story 3

- [ ] T040a [US3] Implement the three outcome handlers in `internal/store/circuit_store.go`
      (replaces the stub bodies from T024a; depends on T005, T006):
      - `HandleSuccess(ctx, endpointID uuid.UUID)`: single conditional UPDATE — resets
        `circuit_failure_count=0`, `circuit_state='closed'`, `circuit_suspended_until=NULL`,
        `circuit_probe_delivery_id=NULL`; sets `circuit_sensitive_recovery=TRUE` when
        transitioning from `half_open`, else `FALSE`; WHERE `circuit_state IN ('closed','half_open')`
      - `HandleTransientFailure(ctx, endpointID uuid.UUID, cfg CircuitConfig)`: single CASE
        UPDATE — increments counter; opens circuit if `half_open`, if `sensitive_recovery=TRUE`,
        or if `count+1 >= threshold`; sets `suspended_until=NOW()+cfg.SuspensionDuration`;
        clears `probe_delivery_id` when was `half_open`; resets `sensitive_recovery=FALSE`;
        WHERE `circuit_state IN ('closed','half_open')`
      - `HandleProbePermanentFailure(ctx, endpointID uuid.UUID, cfg CircuitConfig)`: UPDATE
        to `open`, set `suspended_until=NOW()+cfg.SuspensionDuration`, clear
        `probe_delivery_id`; WHERE `circuit_state='half_open'`
- [ ] T040b [US3] Implement the scheduler-side methods in `internal/store/circuit_store.go`
      (depends on T040a):
      - `ProcessExpiredSuspensions(ctx) (halfOpenIDs []uuid.UUID, closedIDs []uuid.UUID, error)`:
        single UPDATE with CASE — transitions expired open endpoints to `half_open` (when
        non-terminal deliveries exist) or `closed` (empty queue: FR-024, counter reset to 0);
        returns both ID lists
      - `SetProbeDelivery(ctx, endpointID uuid.UUID)`: selects oldest non-terminal delivery;
        if none found → UPDATE `circuit_state='closed'`, `circuit_failure_count=0` WHERE
        `circuit_state='half_open'` (empty-queue fallback for race with
        `ProcessExpiredSuspensions` and Step 0a recovery — applies FR-024 semantics);
        if found → UPDATE `circuit_probe_delivery_id=$probeID` on endpoint; UPDATE delivery
        `next_attempt_at=NOW()` WHERE `next_attempt_at > NOW()` (dispatch probe immediately)
      - `GetState(ctx, endpointID uuid.UUID) (*domain.CircuitBreakerInfo, error)`: SELECT
        `id`, `circuit_state`, `circuit_failure_count`, `circuit_suspended_until`; nil →
        `domain.ErrNotFound`
- [ ] T041 [US3] Update `ClaimReady` in `internal/store/delivery_store.go` — add circuit
      breaker eligibility AND condition (builds on ordering NOT EXISTS from T024): eligible if
      `e.circuit_state='closed'` OR (`e.circuit_state='half_open'` AND
      `e.circuit_probe_delivery_id=d.id`); `open` endpoints fully excluded (FR-014)
      (depends on T024, T040a)
- [ ] T042 [US3] Update `internal/scheduler/scheduler.go` — add Step 0a before existing tick
      body: `SELECT id FROM endpoints WHERE circuit_state='half_open' AND
      circuit_probe_delivery_id IS NULL` → call `circuit_store.SetProbeDelivery(ctx, id)`
      for each result (scheduler crash guard for orphaned half_open endpoints); add Step 0b:
      call `circuit_store.ProcessExpiredSuspensions(ctx)` → call
      `circuit_store.SetProbeDelivery(ctx, id)` for each `halfOpenID` returned; existing
      claim + Kafka publish logic becomes Step 1 (depends on T040b)
- [ ] T043 [US3] Update `internal/delivery/worker.go` — after outcome classification, dispatch
      to circuit store: `OutcomeSuccess` → `circuit_store.HandleSuccess(ctx, endpointID)`;
      `OutcomeTransient/Timeout` → `circuit_store.HandleTransientFailure(ctx, endpointID, cfg)`;
      `OutcomePermanentFailure` AND `wd.EndpointCircuitState == "half_open"` →
      `circuit_store.HandleProbePermanentFailure(ctx, endpointID, cfg)`;
      permanent failures do NOT call `HandleTransientFailure` (FR-011)
      (depends on T040a, T024 `WorkerDelivery.EndpointCircuitState`)

**Checkpoint**: `go test ./tests/integration/...` for T025–T039 passes. US1 and US2 tests
remain green.

---

## Phase 6: User Story 4 — Inspect Circuit Breaker State (Priority: P2)

**Goal**: `GET /v1/endpoints/{id}/circuit-breaker` returns the current circuit state,
consecutive failure count, and — when open — the `suspended_until` timestamp.
Producers can self-diagnose stalled deliveries without access to internal metrics.

**Independent Test**: Open the circuit for an endpoint by forcing 5 transient failures.
Query `GET /v1/endpoints/{id}/circuit-breaker`. Response includes `state:"open"`,
`consecutive_failures:5`, and a non-null `suspended_until`.

### Tests for User Story 4

> Write these tests FIRST and confirm they FAIL before any implementation.

- [ ] T044 [US4] Integration tests in `tests/integration/api_circuit_test.go` (real Postgres):
      closed endpoint → 200 `{state:"closed", consecutive_failures:0}` (no `suspended_until`);
      open endpoint → 200 `{state:"open", consecutive_failures:5, suspended_until:"..."}`;
      half-open endpoint → 200 `{state:"half-open", consecutive_failures:5}` (no `suspended_until`,
      JSON uses hyphen per plan.md §API contracts); non-existent endpoint UUID → 404;
      invalid UUID in path → 400 `invalid_endpoint_id`

**Run tests — confirm they FAIL before implementing.**

### Implementation for User Story 4

- [ ] T045 [P] [US4] Add `CircuitBreakerResponse` DTO to `internal/api/dto.go` — fields
      `EndpointID uuid.UUID`, `State string`, `ConsecutiveFailures int`,
      `SuspendedUntil *time.Time` (json `omitempty`); DTO converts internal `half_open` →
      JSON `"half-open"` and includes `SuspendedUntil` only when state is `open`
      (see plan.md §Flow F)
- [ ] T046 [US4] Implement `internal/api/handlers_circuit.go` — `GetState` handler for
      `GET /v1/endpoints/{id}/circuit-breaker`: parse and validate `{id}` → 400
      `invalid_endpoint_id` if not UUID; call `circuit_store.GetState` → 404
      `endpoint_not_found` if `ErrNotFound`; build `CircuitBreakerResponse` (translate
      `half_open` → `"half-open"`, omit `SuspendedUntil` unless state is `open`); respond 200
      (depends on T040, T045)
- [ ] T047 [US4] Register circuit-breaker route in `internal/api/server.go` —
      `GET /v1/endpoints/{id}/circuit-breaker → circuitHandler.GetState` (depends on T046)

**Checkpoint**: `go test ./tests/integration/...` for T044 passes. All prior tests remain green.

---

## Phase 7: E2E Tests & Polish

**Purpose**: End-to-end validation of all four user stories working together, lint hygiene,
and API documentation update.

- [ ] T048 [US2,US3,US4] E2E test in `tests/integration/e2e_003_test.go` (testcontainers + `httptest.Server`):
      register tenant; register endpoints E_A and E_B under the same tenant; submit E1
      targeting E_A and E2 targeting E_B; E_A always returns 503; after 5 failures circuit
      opens for E_A; assert E2 is NOT dispatched while circuit is open; advance
      `suspended_until` to the past via SQL; probe (E1) → 503 again → circuit reopens;
      advance `suspended_until` again; probe → 200 → E1 marked `delivered`, circuit closed;
      assert E2 proceeds in order and eventually reaches `delivered`; assert
      `GET /v1/endpoints/{id}/circuit-breaker` reflects the correct state at each phase
      (covers SC-002, SC-003, SC-005, SC-006)
- [ ] T049 [P] Run `go vet ./internal/... ./tests/...` and
      `golangci-lint run ./internal/... ./tests/...`; fix all findings in packages modified
      by this feature: `internal/domain/`, `internal/config/`, `internal/store/`,
      `internal/service/`, `internal/api/`, `internal/scheduler/`, `internal/delivery/`,
      `tests/integration/`
- [ ] T050 [P] Update `docs/api-reference.md` — add `POST /v1/tenants` and
      `GET /v1/tenants/{id}` endpoints; add `GET /v1/endpoints/{id}/circuit-breaker`;
      update `POST /v1/endpoints` with new `tenant_id` field and new error responses
      (400 `missing_tenant_id`, 422 `tenant_not_found`); update `POST /v1/events` with
      new `tenant_id` field and new error responses (400 `missing_tenant_id`,
      422 `tenant_not_found`, 422 `tenant_endpoint_mismatch`)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Migrations)**: No dependencies — start immediately
- **Phase 2 (Domain & Config)**: No dependencies — can run in parallel with Phase 1
- **Phase 3 (US1)**: Depends on Phase 1 (schema) and Phase 2 (domain types) — **blocks Phases 4–6**
- **Phase 4 (US2)**: Depends on Phase 3 (tenant service + store exist; endpoint store updated)
- **Phase 5 (US3)**: Depends on Phase 4 (`WorkerDelivery.EndpointCircuitState` from T024; ordering
  filter in `ClaimReady` from T024); also depends on T006 (`CircuitConfig`)
- **Phase 6 (US4)**: Depends on Phase 5 (`circuit_store.GetState` must exist)
- **Phase 7 (Polish)**: Depends on all prior phases complete

### Within Each User Story Phase

- Test tasks must be written and confirmed FAILING before any implementation task begins
- Store layer before service layer before handler layer
- Domain types and DTOs before any component that uses them
- Route registration (`server.go`) always last within a phase

### Parallel Opportunities

- T001, T002, T003 (migrations) can be written in parallel; goose enforces apply order
- T004, T005, T006 (domain + config) are fully parallel with each other and with Phase 1
- T007, T008, T009 (Phase 3 tests) can be written in parallel
- T010, T011, T013 (tenant_store, DTOs, endpoint_store) are parallel within Phase 3 implementation
- T014 must run after T011 (same file, explicit dependency) — not parallelisable
- T021 (EventRequest DTO) is parallel with T022/T023 within Phase 4
- T024a (circuit_store.go skeleton) is a prerequisite for T025–T039b; write before any test
- T025 (unit test) can be written in parallel with T026–T039b (different test functions,
  same file `circuit_breaker_test.go`)
- T040a and T040b are sequential (T040b depends on T040a's method stubs being complete)
- T041 and T042 can run in parallel within Phase 5 implementation (different files);
  both depend on T040a/T040b being complete
- T049 and T050 (lint + docs) are parallel within Phase 7

---

## Implementation Strategy

### MVP first (US1 + US2 — both P1)

1. Complete Phase 1: Migrations
2. Complete Phase 2: Domain Types & Config
3. Complete Phase 3: US1 — tenant CRUD + endpoint with tenant_id
4. Complete Phase 4: US2 — event submission with tenant_id + ordering
5. **STOP and VALIDATE**: register tenant, register endpoint, submit two events under same
   tenant, confirm E2 never dispatched before E1 reaches terminal state
6. Demo / commit / push as MVP

### Incremental delivery

1. Phase 1 + Phase 2 → schema and types ready
2. + Phase 3 → tenant API + endpoint-with-tenant-id demo (US1 complete)
3. + Phase 4 → ordered delivery demo — MVP (US2 complete)
4. + Phase 5 → circuit breaker demo — resilience (US3 complete)
5. + Phase 6 → circuit breaker observability API (US4 complete)
6. + Phase 7 → E2E validated, production-ready

Each phase adds value without breaking previous phases.

---

## Notes

- `[P]` tasks = different files, no mutual dependency — safe to parallelise
- `[USn]` label maps every task to its User Story for traceability
- No new external libraries — plan.md §Technical Context
- Migrations 004, 005, 006 apply in numeric order; goose enforces this
- `circuit_sensitive_recovery` is an internal column — never exposed in API responses
- `ClaimReady` receives the ordering filter in T024 (Phase 4) and the circuit filter in T041
  (Phase 5); both are required for the final query from plan.md §Flow D Step 1
- Step 0a in T042 handles the crash-recovery case: scheduler committed the `half_open`
  transition but crashed before calling `SetProbeDelivery`; recovery runs on every tick
- `SetProbeDelivery` empty-queue fallback (T040b) is the safety net for the race between
  `ProcessExpiredSuspensions` and Step 0a: if the queue empties between the two calls,
  the endpoint closes cleanly (FR-024 semantics)
- T040a (HandleXxx outcome methods) and T040b (ProcessExpiredSuspensions, SetProbeDelivery,
  GetState) are separate tasks; T042 depends on T040b; T043 depends on T040a
- Confirm tests FAIL before implementing at every User Story phase boundary
- Commit after each completed task or logical group; stop at any Checkpoint to independently
  validate the story before proceeding

---

## Review Checklist

- [x] Every FR from spec has at least one task covering its implementation path
- [x] Every test scenario from plan.md §Testing Strategy is present as an explicit task
- [x] US2 AS4 (permanently_failed unblocks E2) covered in T020
- [x] US3 AS8 (open circuit on A does not block different tenant) covered in T039a
- [x] SC-005 (queue drain after circuit close) covered in T039b (Phase 5) — not only Phase 7
- [x] Integration and E2E tests are standalone tasks, not sub-bullets of implementation tasks
- [x] Tasks are ordered such that no task depends on a task that comes later in the file
- [x] CircuitConfig struct (T006) appears before circuit_store tasks (T024a, T040a, T040b)
- [x] circuit_store.go skeleton (T024a) precedes all test tasks (T025–T039b)
- [x] Scheduler Step 0a orphan recovery is an explicit task (T042)
- [x] SetProbeDelivery empty-queue fallback (FR-024 race) covered by test (T031) and
      implementation (T040b)
- [x] ClaimReady ordering filter (T024) and circuit filter (T041) are separate tasks in
      correct phases
- [x] `WorkerDelivery.EndpointCircuitState` field addition is explicit in T024
- [x] T040 split into T040a (HandleXxx outcome methods) and T040b (ProcessExpiredSuspensions,
      SetProbeDelivery, GetState) — each a manageable commit cluster
- [x] No two [P] tasks within the same phase modify the same file
- [x] No production code planned outside the SDD flow (CLAUDE.md §Execution gates)
