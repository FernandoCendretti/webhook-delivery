<!--
  Feature: 004-dlq-replay
-->

# Tasks: DLQ & Replay

**Input**: Design documents from `specs/004-dlq-replay/`
**Status**: Approved
**Prerequisites**: `plan.md` (approved), `spec.md` (approved)

**Tests**: Each user story writes both integration tests (red → green) and unit tests for
`DLQService` first, placed before the matching service implementation in the same phase.

**Organization**: Tasks are grouped by phase. Within each user story, all test tasks
precede all implementation tasks. Stories are ordered P1 → P2 → P1 → P3 (spec priority
+ dependency order).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no shared write dependency)
- **[Story]**: Traceability to spec user story (US1–US4)

---

## Phase 1: Foundational Changes (Blocking Prerequisites)

**Purpose**: Schema migration and domain/store additions that every story depends on.

**⚠️ CRITICAL**: No user story work can begin until T001–T004 are complete.

- [x] T001 Write migration `internal/store/migrations/007_dlq_replay.sql`: `ALTER TABLE deliveries ADD COLUMN source_delivery_id UUID REFERENCES deliveries(id)`; unique partial index `idx_deliveries_one_active_replay ON deliveries(source_delivery_id) WHERE source_delivery_id IS NOT NULL AND status IN ('scheduled','in_flight')`; partial index `idx_deliveries_pf_tenant_endpoint ON deliveries(status, tenant_id, endpoint_id, updated_at DESC) WHERE status='permanently_failed'`
- [x] T002 [P] Add `SourceDeliveryID *uuid.UUID` field to `domain.Delivery` struct in `internal/domain/delivery.go`
- [x] T003 [P] Add sentinel errors `ErrConflict` and `ErrUnprocessable` in `internal/domain/errors.go`
- [x] T004 Extend `DeliveryStore.Create` in `internal/store/delivery_store.go` to accept an optional `sourceDeliveryID *uuid.UUID` parameter; set the column on INSERT; update all existing call sites in the package to pass `nil`; update `DeliveryStore` interface accordingly (depends on T001, T002)

**Checkpoint**: Migration file written; `domain.Delivery` has `SourceDeliveryID`; new sentinels compile; `DeliveryStore.Create` signature updated and all call sites pass `nil`.

---

## Phase 2: DLQ Service Scaffold

**Purpose**: Define the shared interface and types referenced by all four user story
phases. No business logic yet.

- [x] T005 Declare `DLQService` interface, `DLQFilter`, `DLQEntry`, `DLQDetail`, and `Pagination` types in `internal/service/dlq_service.go`; create a concrete `dlqService` struct embedding the store interfaces it will need (`DeliveryStore`, `AttemptStore`, `EndpointStore`); leave all method bodies as `panic("not implemented")` placeholders

**Checkpoint**: Package compiles; interface and types are importable by subsequent phases.

---

## Phase 3: User Story 1 — List Permanently-Failed Deliveries (Priority: P1)

**Goal**: `GET /v1/dlq` returns a paginated list of `permanently_failed` deliveries with
optional `tenant_id` / `endpoint_id` filters; SC-001 freshness (<1 s) validated.

**Independent Test**: Seed one `permanently_failed` delivery; call `GET /v1/dlq`; verify
the delivery appears with correct `delivery_id`, `endpoint_id`, `attempt_count`, and
`failed_at`.

### Tests for User Story 1

> Write these tests FIRST; run them and confirm they FAIL before touching implementation.

- [x] T006 [US1] Write integration tests for `GET /v1/dlq` in `tests/integration/dlq_test.go` covering: happy-path single item, empty list, pagination (`page=2`, `has_next`), `tenant_id` filter, `endpoint_id` filter, invalid UUID query param → 400, **SC-001 freshness timing** (record timestamp at status transition; call `GET /v1/dlq`; assert delivery appears in response and elapsed time < 1 s), **SC-004 performance** (seed 1 000+ `permanently_failed` deliveries; call `GET /v1/dlq`; assert first page returns in < 1 s); tests must FAIL at this point
- [x] T007 [US1] Write unit tests for `DLQService.List` in `internal/service/dlq_service_test.go`: mock store returns empty list, `page < 1` rejected, `limit` outside 1–100 rejected, filter fields propagated to store call; tests must FAIL

### Implementation for User Story 1

- [x] T008 [P] [US1] Implement `DeliveryStore.ListPermanentlyFailed(ctx, filter DLQFilter, page, limit int)` in `internal/store/delivery_store.go`: `SELECT … FROM deliveries LEFT JOIN LATERAL (SELECT MAX(completed_at) FROM attempts …) WHERE status='permanently_failed'` + optional filters, `ORDER BY updated_at DESC`, `LIMIT/OFFSET`; add method to `DeliveryStore` interface (depends on T001)
- [x] T009 [P] [US1] Add `DLQListResponse`, `DLQItemResponse`, `PaginationResponse` DTO types in `internal/api/dto.go`
- [x] T010 [US1] Implement `DLQService.List` in `internal/service/dlq_service.go`: validate `page ≥ 1` and `1 ≤ limit ≤ 100`, call `ListPermanentlyFailed` with `limit+1` to compute `HasNext`, map rows to `[]DLQEntry` and `Pagination` (depends on T005, T008)
- [x] T011 [US1] Implement `handleListDLQ` in `internal/api/handlers_dlq.go`; add `RegisterDLQ` method and register `GET /v1/dlq` route in `internal/api/server.go` (depends on T009, T010)

**Checkpoint**: `go test ./tests/integration/ -run TestDLQList` and `go test ./internal/service/ -run TestDLQServiceList` both pass green.

---

## Phase 4: User Story 2 — Inspect a Single DLQ Entry (Priority: P2)

**Goal**: `GET /v1/dlq/{delivery_id}` returns full delivery metadata plus attempt history
sorted by sequence; 404 for missing or wrong-status deliveries.

**Independent Test**: Let a delivery reach `permanently_failed` after retries; call
`GET /v1/dlq/{delivery_id}`; assert the `attempts` array has one entry per attempt,
sorted by `sequence` ascending.

### Tests for User Story 2

> Write these tests FIRST; run them and confirm they FAIL before touching implementation.

- [x] T012 [US2] Write integration tests for `GET /v1/dlq/{delivery_id}` in `tests/integration/dlq_test.go` covering: happy path with attempt history sorted by sequence, 404 for non-existent ID, 404 for delivery with status ≠ `permanently_failed`, `response_status_code` present on HTTP-error attempt, timeout attempt has `outcome: "timeout"` and null `response_status_code`; tests must FAIL
- [x] T013 [US2] Write unit tests for `DLQService.Detail` in `internal/service/dlq_service_test.go`: `ErrNotFound` propagated when store returns not found, `FailedAt` equals `MAX(completed_at)` across the returned attempts; tests must FAIL

### Implementation for User Story 2

- [x] T014 [US2] Implement `DeliveryStore.GetPermanentlyFailed(ctx, id uuid.UUID)` in `internal/store/delivery_store.go`: `SELECT … WHERE id=$1 AND status='permanently_failed'`; return `domain.ErrNotFound` when no row; add to `DeliveryStore` interface (depends on T001)
- [x] T015 [P] [US2] Implement `AttemptStore.ListByDelivery(ctx, deliveryID uuid.UUID)` in `internal/store/attempt_store.go`: `SELECT … FROM attempts WHERE delivery_id=$1 ORDER BY sequence ASC`; add to `AttemptStore` interface
- [x] T016 [US2] Implement `DLQService.Detail` in `internal/service/dlq_service.go`: call `GetPermanentlyFailed` (propagate `ErrNotFound`), call `ListByDelivery`, compute `FailedAt` as `MAX(completed_at)` from returned attempts, assemble `DLQDetail` (depends on T005, T014, T015)
- [x] T017 [P] [US2] Add `DLQDetailResponse` and `AttemptResponse` DTO types in `internal/api/dto.go`
- [x] T018 [US2] Implement `handleDLQDetail` in `internal/api/handlers_dlq.go` (parse UUID path param, map `ErrNotFound` → 404); register `GET /v1/dlq/{delivery_id}` route in `internal/api/server.go` (depends on T016, T017)

**Checkpoint**: `go test ./tests/integration/ -run TestDLQDetail` and `go test ./internal/service/ -run TestDLQServiceDetail` both pass green.

---

## Phase 5: User Story 3 — Replay a Single DLQ Entry (Priority: P1)

**Goal**: `POST /v1/dlq/{delivery_id}/replay` creates a new `scheduled` delivery with
`source_delivery_id` set; 409 if a non-terminal replay exists; 422 if endpoint is gone.

**Independent Test**: Let a delivery reach `permanently_failed`; fix the endpoint to
return 200; call `POST /v1/dlq/{delivery_id}/replay`; verify new delivery reaches
`delivered`.

### Tests for User Story 3

> Write these tests FIRST; run them and confirm they FAIL before touching implementation.

- [ ] T019 [US3] Write integration tests for `POST /v1/dlq/{delivery_id}/replay` in `tests/integration/dlq_test.go` covering: 202 happy path (new `delivery_id` returned, original still `permanently_failed`), **SC-002 latency** (with < 10 000 permanently-failed records in DB, assert response time < 500 ms), **SC-003** (assert new delivery eventually reaches `delivered` status after endpoint is healthy), 404 for non-existent delivery, **409 for existing delivery with status ≠ `permanently_failed`** (per spec US3-AS3), 409 for concurrent duplicate replay (SC-005), 422 for deleted endpoint, replay of a replay (chain allowed, returns 202); tests must FAIL
- [ ] T020 [US3] Write unit tests for `DLQService.Replay` in `internal/service/dlq_service_test.go`: `ErrNotFound` when delivery not found, `ErrConflict` when delivery exists but status ≠ `permanently_failed` (US3-AS3), `ErrUnprocessable` when endpoint gone, `ErrConflict` when `pgx.PgError` code `23505` is returned by store, happy path returns new delivery; tests must FAIL

### Implementation for User Story 3

- [ ] T021 [US3] Implement `DeliveryStore.CreateReplay(ctx, eventID, endpointID, sourceID uuid.UUID)` in `internal/store/delivery_store.go`: `INSERT INTO deliveries(…) VALUES(…, 'scheduled', 0, NOW(), sourceID)` where `source_delivery_id=sourceID`; caller handles `pgx.PgError` code `23505`; add to `DeliveryStore` interface (depends on T001, T004)
- [ ] T022 [US3] Implement `DLQService.Replay` in `internal/service/dlq_service.go`: call `GetByID` → `ErrNotFound` only when the delivery does not exist; if it exists but status ≠ `permanently_failed` return `ErrConflict` (US3-AS3); call `EndpointStore.GetByID` → `ErrUnprocessable`; call `CreateReplay`; translate `23505` pgx error to `ErrConflict` (depends on T003, T005, T021; add `GetByID` to the `dlqDeliveryStore` interface)
- [ ] T023 [P] [US3] Add `ReplayResponse` DTO type in `internal/api/dto.go`
- [ ] T024 [US3] Implement `handleReplay` in `internal/api/handlers_dlq.go` (map `ErrNotFound` → 404, `ErrConflict` → 409, `ErrUnprocessable` → 422); register `POST /v1/dlq/{delivery_id}/replay` route in `internal/api/server.go` (depends on T022, T023)

**Checkpoint**: `go test ./tests/integration/ -run TestDLQReplay` and `go test ./internal/service/ -run TestDLQServiceReplay` both pass green.

---

## Phase 6: User Story 4 — Bulk Replay by Filter (Priority: P3)

**Goal**: `POST /v1/dlq/replay` (literal route, registered before the wildcard) replays
all matching `permanently_failed` deliveries; requires at least one filter field;
skips deliveries that already have a non-terminal replay.

**Independent Test**: Seed ten `permanently_failed` deliveries for the same endpoint;
call `POST /v1/dlq/replay` with `{"endpoint_id": "<id>"}`; verify response is
`{"replayed": 10}` and ten new `scheduled` deliveries are created.

### Tests for User Story 4

> Write these tests FIRST; run them and confirm they FAIL before touching implementation.

- [ ] T025 [US4] Write integration tests for `POST /v1/dlq/replay` in `tests/integration/dlq_test.go` covering: 202 with correct count, 202 with `{"replayed":0}` when no matches, 400 when no filter fields provided, 422 for non-existent `endpoint_id`, 422 for non-existent `tenant_id`, skip deliveries that already have a non-terminal replay (response count reflects only new replays); tests must FAIL
- [ ] T026 [US4] Write unit tests for `DLQService.BulkReplay` in `internal/service/dlq_service_test.go`: `ErrUnprocessable` when no filter fields, `ErrUnprocessable` when filter entity not found, IDs with existing non-terminal replay are skipped, `23505` pgx error during `CreateReplay` is treated as skip (not abort), returned count equals successful INSERTs only; tests must FAIL

### Implementation for User Story 4

- [ ] T027 [US4] Implement `DeliveryStore.ListPermanentlyFailedIDs(ctx, filter DLQFilter)` in `internal/store/delivery_store.go`: `SELECT id FROM deliveries WHERE status='permanently_failed' [AND tenant_id=$x] [AND endpoint_id=$y]`; returns `[]uuid.UUID` with no LIMIT/OFFSET (snapshot semantics); add to `DeliveryStore` interface (depends on T001)
- [ ] T028 [US4] Implement `DeliveryStore.HasNonTerminalReplay(ctx, sourceID uuid.UUID)` in `internal/store/delivery_store.go`: `SELECT 1 FROM deliveries WHERE source_delivery_id=$1 AND status IN ('scheduled','in_flight') LIMIT 1`; returns `bool`; add to `DeliveryStore` interface (depends on T001)
- [ ] T029 [US4] Implement `DLQService.BulkReplay` in `internal/service/dlq_service.go`: validate at least one filter field present (else `ErrUnprocessable`); verify filter entities exist via `TenantStore.GetByID`/`EndpointStore.GetByID` (else `ErrUnprocessable`); fetch IDs snapshot via `ListPermanentlyFailedIDs`; iterate: skip if `HasNonTerminalReplay`; call `CreateReplay`; treat `23505` as skip; return accumulated count (depends on T003, T005, T021, T027, T028)
- [ ] T030 [P] [US4] Add `BulkReplayRequest` and `BulkReplayResponse` DTO types in `internal/api/dto.go`
- [ ] T031 [US4] Implement `handleBulkReplay` in `internal/api/handlers_dlq.go` (map `ErrUnprocessable` → 422, missing-filter → 400); **register `POST /v1/dlq/replay` (literal) BEFORE `POST /v1/dlq/{delivery_id}/replay` (pattern) in `RegisterDLQ` in `internal/api/server.go`** (depends on T029, T030)

**Checkpoint**: `go test ./tests/integration/ -run TestDLQBulkReplay` and `go test ./internal/service/ -run TestDLQServiceBulkReplay` both pass green; `make test-integration` full suite passes.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (T001–T004)**: T001 first; then T002 `[P]` and T003 `[P]` in parallel (different files, no deps); then T004 sequentially after T002.
- **Phase 2 (T005)**: Depends on Phase 1 completion.
- **Phase 3–6 (US stories)**: All depend on Phase 2 completion. US3 uses the
  pre-existing `GetByID` (not `GetPermanentlyFailed`), so it has no hard dependency on
  US2; US4 reuses `CreateReplay` from US3.
- **No separate unit-test phase**: unit test tasks are embedded in their story phase,
  before the matching service implementation task.

### Cross-story store method dependencies

| Store method | Introduced | Also used by |
|---|---|---|
| `ListPermanentlyFailed` | T008 (US1) | — |
| `GetPermanentlyFailed` | T014 (US2) | — |
| `GetByID` (pre-existing) | — | T022 (US3) |
| `ListByDelivery` | T015 (US2) | — |
| `CreateReplay` | T021 (US3) | T029 (US4) |
| `ListPermanentlyFailedIDs` | T027 (US4) | — |
| `HasNonTerminalReplay` | T028 (US4) | — |

Phase 5 (US3) uses the pre-existing `GetByID`, so it no longer depends on T014.

### Within Each User Story

1. Write integration tests → confirm FAIL
2. Write unit tests for `DLQService` method → confirm FAIL
3. Implement store methods → service method → DTO types → handler + route
4. Run story-scoped tests → confirm PASS
5. Run full suite: `make test-integration`

### Parallel Opportunities

| Tasks | Files | Condition |
|---|---|---|
| T002, T003, T004 | `delivery.go`, `errors.go`, `delivery_store.go` | After T001 |
| T008, T009 | `delivery_store.go`, `dto.go` | After T006, T007 (tests written) |
| T014, T015 | `delivery_store.go`, `attempt_store.go` | After T012, T013 (tests written) |
| T014, T017 | `delivery_store.go`, `dto.go` | After T012, T013 (tests written) |
| T021, T023 | `delivery_store.go`, `dto.go` | After T019, T020 (tests written) |
| T027, T028, T030 | `delivery_store.go`×2, `dto.go` | After T025, T026 (tests written) — T027 and T028 share a file so are sequential between themselves |
