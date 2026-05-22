<!--
  Adapted from specs/templates/tasks-template.md
  Feature: 002-signature-idempotency
-->

# Tasks: Signature & Idempotency

**Input**: `specs/002-signature-idempotency/spec.md` and `specs/002-signature-idempotency/plan.md`
**Prerequisites**: `plan.md` (approved), `spec.md` (approved)

**Tests**: Tests are part of every deliverable. Test tasks appear before implementation
tasks within each User Story phase. Confirm tests FAIL before writing implementation.

**Organization**: Tasks are grouped by User Story so each story can be implemented and
validated independently.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: Can run in parallel (different files, no mutual dependency)
- **[Story]**: Which User Story this task belongs to (US1, US2, US3)
- Every task includes a concrete file path

---

## Phase 1: Setup & Dependencies

**Purpose**: No new external libraries are introduced in this feature. Phase 1 creates
the two database migrations and the new `internal/signing/` package skeleton — the
earliest shared prerequisites that every subsequent phase depends on.

- [ ] T001 Create migration `internal/store/migrations/002_signing_secret.sql` — adds
      `signing_secret BYTEA NOT NULL` to `endpoints` with a backfill of
      `gen_random_bytes(32)` for any pre-existing rows (see plan.md §Data model)
- [ ] T002 [P] Create migration `internal/store/migrations/003_idempotency.sql` — creates
      `idempotency_records` table with columns `id`, `endpoint_id`, `idempotency_key`,
      `payload_hash`, `event_id`, `delivery_id`, `created_at`, `expires_at`; adds
      `UNIQUE (endpoint_id, idempotency_key)` constraint and
      `idx_idempotency_expires` index (see plan.md §Data model)
- [ ] T003 [P] Create package skeleton `internal/signing/signer.go` — package declaration
      only; no logic yet (unblocks parallel test writing in T004)

**Checkpoint**: Migrations exist and apply cleanly against a local Postgres container.
`internal/signing/` compiles. No production logic written yet.

---

## Phase 2: Foundational (Domain Types & Pure Signing Function)

**Purpose**: Core types and the pure cryptographic primitive that every other component
depends on. No store, HTTP, or Kafka dependencies at this layer.

- [ ] T004 Extend `internal/domain/endpoint.go` — add `SigningSecret []byte` field to the
      `Endpoint` struct (nil on reads, populated only when returned by Insert or
      `LoadForWorker`)
- [ ] T005 [P] Add `ErrConflict` sentinel to `internal/domain/errors.go` — used by
      `event_service` to signal idempotency key collision (FR-007)

### Signing primitive

- [ ] T006 Write unit tests for `signing.Sign` in `internal/signing/signer_test.go`:
      - Known vector: fixed secret + timestamp + body → assert exact hex digest
      - Empty body: `Sign(secret, ts, []byte{})` must produce a 64-char lowercase hex string
      - Output invariants: `len(sig) == 64` and `strings.ToLower(sig) == sig` for any input

**Run tests — confirm they FAIL before implementing `Sign`.**

- [ ] T007 Implement `Sign(secret []byte, ts int64, body []byte) string` in
      `internal/signing/signer.go` using `crypto/hmac`, `crypto/sha256`, `encoding/hex`,
      `strconv` — zero external dependencies (see plan.md §Signing function)

**Checkpoint**: `go test ./internal/signing/...` passes. Domain types compile. All other
phases may now proceed independently.

---

## Phase 3: User Story 1 — Verify Webhook Authenticity (Priority: P1)

**Goal**: Every outgoing delivery POST carries `X-Webhook-Timestamp` and
`X-Webhook-Signature` headers. A consumer holding the `signing_secret` can verify
authenticity using the Signing Scheme Contract. The `POST /v1/endpoints` 201 response
exposes the secret; no other read endpoint does.

**Independent Test**: Register an endpoint, submit an event, inspect the outgoing POST.
Both headers must be present. Apply the consumer verification procedure from spec.md
§Signing Scheme Contract; the computed value must equal `X-Webhook-Signature` exactly.

### Tests for User Story 1

> Write these tests FIRST and confirm they FAIL before any implementation.

- [ ] T008 [P] [US1] Integration test: `POST /v1/endpoints` → 201 response contains
      `signing_secret` as a non-empty 64-char lowercase hex string; subsequent
      `GET /v1/endpoints/{id}` response does NOT contain `signing_secret` field.
      File: `tests/integration/endpoint_signing_test.go`

- [ ] T009 [US1] Integration test: submit an event to a registered endpoint; capture
      the outgoing HTTP POST with an `httptest.Server`; assert both
      `X-Webhook-Timestamp` and `X-Webhook-Signature` headers are present; apply
      consumer verification procedure and assert computed value equals the header value.
      File: `tests/integration/worker_signing_test.go`

- [ ] T010 [US1] Integration test: worker retries a failed delivery; each attempt
      produces a fresh `X-Webhook-Timestamp` (different from the previous attempt's
      value); the new signature is valid against the current secret.
      File: `tests/integration/worker_signing_test.go`

- [ ] T011 [US1] Integration test: empty payload delivery; `X-Webhook-Timestamp` and
      `X-Webhook-Signature` are still present and the signature verifies correctly.
      File: `tests/integration/worker_signing_test.go`

**Run tests — confirm they FAIL before implementing.**

### Implementation for User Story 1

- [ ] T012 [US1] Update `internal/store/endpoint_store.go`:
      - `Insert`: include `signing_secret` in the `INSERT` statement; populate
        `ep.SigningSecret` from the returned row
      - `GetByID`: exclude `signing_secret` from the `SELECT` (leave `SigningSecret` nil)
      - Add `UpdateSecret(ctx, id uuid.UUID, newSecret []byte) error` — executes
        `UPDATE endpoints SET signing_secret=$1 WHERE id=$2 RETURNING id`;
        returns `domain.ErrNotFound` when 0 rows affected

- [ ] T013 [P] [US1] Update `internal/store/delivery_store.go`:
      - Add `SigningSecret []byte` to the `WorkerDelivery` struct
      - Update `LoadForWorker` query to `JOIN endpoints e ON e.id = d.endpoint_id` and
        `SELECT ... e.signing_secret` (populates `WorkerDelivery.SigningSecret`)

- [ ] T014 [P] [US1] Update `internal/api/dto.go`:
      - Add `EndpointCreatedResponse` struct: fields `ID`, `URL`, `CreatedAt`,
        `SigningSecret` (JSON: `"signing_secret"`)
      - Add `RotateSecretResponse` struct: field `SigningSecret` (JSON: `"signing_secret"`)
      - Ensure existing `EndpointResponse` (used by GET) does NOT include `SigningSecret`

- [ ] T015 [US1] Update `internal/service/endpoint_service.go`:
      - `Create`: call `crypto/rand.Read(32)` to generate `rawSecret`; set
        `ep.SigningSecret = rawSecret`; pass to `endpoint_store.Insert`; return `ep`
        (with secret populated) to the handler
      - Add `RotateSecret(ctx, id) ([]byte, error)`: calls `crypto/rand.Read(32)`,
        then `endpoint_store.UpdateSecret(ctx, id, newSecret)`; returns `newSecret`

- [ ] T016 [US1] Update `internal/api/handlers_endpoint.go`:
      - `Create` handler: use `EndpointCreatedResponse` for the 201 body (includes
        `hex.EncodeToString(ep.SigningSecret)`)
      - Add `RotateSecret` handler: parse `{id}` from path → call
        `endpoint_service.RotateSecret`; respond 200 `RotateSecretResponse` or
        404 `{ "error": "endpoint_not_found" }`

- [ ] T017 [US1] Register new route in `internal/api/server.go` (or router file):
      `POST /v1/endpoints/{id}/rotate-secret → endpointHandler.RotateSecret`

- [ ] T018 [US1] Update `internal/delivery/worker.go`:
      - `doHTTP` signature gains `signingSecret []byte` parameter
      - Inside `doHTTP`: compute `ts := time.Now().Unix()`; compute
        `sig := signing.Sign(signingSecret, ts, payload)`; set headers
        `X-Webhook-Timestamp` and `X-Webhook-Signature` on the outgoing request
      - `process`: pass `wd.SigningSecret` from `WorkerDelivery` to `doHTTP`

**Checkpoint**: `go test ./tests/integration/...` for US1 tests passes. Running the
full pipeline locally: register endpoint, submit event, inspect captured POST for both
headers, verify signature with consumer procedure.

---

## Phase 4: User Story 2 — Safe Event Re-submission (Priority: P1)

**Goal**: `POST /v1/events` accepts an optional `Idempotency-Key` header. Duplicate
submissions within 24 hours return the original response without creating new records.
Concurrent duplicate requests serialize safely (exactly one event created).

**Independent Test**: Submit an event with `Idempotency-Key: test-key-1`; record
`delivery_id`. Submit again with identical payload. Response must be identical and exactly
one delivery row must exist in the database.

### Tests for User Story 2

> Write these tests FIRST and confirm they FAIL before any implementation.

- [ ] T019 [P] [US2] Unit tests for `Idempotency-Key` header validation in
      `internal/api/handlers_event_test.go`:
      - Accept: 1-char key, 255-char key, printable ASCII boundary chars (0x21 `!`, 0x7E `~`)
      - Reject with 400: empty value (header present but blank)
      - Reject with 400: 256-char key
      - Reject with 400: key containing space (0x20)
      - Reject with 400: key containing DEL (0x7F)
      - Reject with 400: key containing non-ASCII byte (>0x7F)
      - Accept: no header present (no idempotency, new event always created)

- [ ] T020 [P] [US2] Unit test for `lockKey` determinism in
      `internal/store/idempotency_store_test.go`:
      - Same `(endpointID, key)` always produces the same `int64`
      - Different keys produce different values (sanity check)

- [ ] T021 [US2] Integration test — happy path re-submission in
      `tests/integration/idempotency_test.go`:
      - Submit (key K, payload P) → 202; record `event_id`, `delivery_id`
      - Resubmit (key K, same P) → 202 with same `event_id` and `delivery_id`
      - Assert: exactly 1 row in `events`, 1 row in `deliveries`,
        1 row in `idempotency_records` for that key

- [ ] T022 [US2] Integration test — payload conflict in
      `tests/integration/idempotency_test.go`:
      - Submit (key K, payload P) → 202
      - Resubmit (key K, payload P2 where P2 ≠ P) → 409 Conflict

- [ ] T023 [US2] Integration test — no header → two independent events in
      `tests/integration/idempotency_test.go`:
      - Submit twice without `Idempotency-Key` → two distinct `event_id` values

- [ ] T024 [US2] Integration test — expired record in
      `tests/integration/idempotency_test.go`:
      - Submit (key K, payload P) → 202; manually set `expires_at = NOW() - interval '1s'`
        via SQL; resubmit same key → 202 with a NEW `event_id` (treated as fresh)

- [ ] T025 [US2] Integration test — exact 24-hour boundary in
      `tests/integration/idempotency_test.go` (covers US2 AS5 and FR-006):
      - Still-valid case: insert a record with `expires_at = NOW() + interval '1s'`;
        resubmit immediately → 202 with original `event_id` (window not yet elapsed)
      - Exact-boundary case: insert a record via SQL with `expires_at = NOW()`; resubmit
        immediately → assert 202 with original `event_id`; this pins that the lookup
        predicate must be `expires_at >= NOW()`, not `expires_at > NOW()` (per spec AS5:
        "exactly 24 hours elapsed → still within window")
      - Expired case: insert a record with `expires_at = NOW() - interval '1ms'`;
        resubmit → 202 with NEW `event_id` (window elapsed)

- [ ] T026 [US2] Integration test — concurrent duplicate requests in
      `tests/integration/idempotency_test.go`:
      - Two goroutines submit simultaneously with the same key and identical payload
      - Both must receive 202 with the same `event_id`
      - Assert: exactly 1 row in `events`, 1 row in `idempotency_records`

- [ ] T027 [US2] Integration test — non-2xx path does not create idempotency record in
      `tests/integration/idempotency_test.go`:
      - Submit with `Idempotency-Key` to a non-existent `endpoint_id` → 404
      - Assert: 0 rows in `idempotency_records` for that key

- [ ] T028 [US2] Integration test — key scoping per endpoint in
      `tests/integration/idempotency_test.go` (covers SC-007):
      - Submit key K to endpoint A → 202; submit same key K to endpoint B → 202
      - Assert: two distinct events created, no collision
      - Assert: `idempotency_records` count = 2, each scoped to its `endpoint_id`

- [ ] T029 [US2] Integration test — 400 for invalid key characters in
      `tests/integration/idempotency_test.go`:
      - Submit with key containing 0x00 byte → 400; assert no event or record created
      - Submit with 256-char key → 400; assert no event or record created

**Run tests — confirm they FAIL before implementing.**

### Implementation for User Story 2

- [ ] T030 [US2] Create `internal/store/idempotency_store.go`:
      - Define `IdempotencyRecord` struct: fields `PayloadHash`, `EventID`, `DeliveryID`,
        `ExpiresAt`
      - Implement private `lockKey(endpointID uuid.UUID, key string) int64` using
        `hash/fnv` FNV-64a (see plan.md §Advisory lock key computation)
      - Implement `Lookup(ctx, tx, endpointID, key) (*IdempotencyRecord, error)` — SELECT
        with `expires_at > NOW()`
      - Implement `Claim(ctx, tx, endpointID, key, payloadHash string, expiresAt time.Time) error`
        — INSERT the partial record (no `event_id`/`delivery_id` yet)
      - Implement `Complete(ctx, tx, endpointID, key string, eventID, deliveryID uuid.UUID) error`
        — UPDATE to set `event_id` and `delivery_id`
      - Implement `AcquireAdvisoryLock(ctx, tx, lockKey int64) error` — executes
        `SELECT pg_advisory_xact_lock($1)`

- [ ] T031 [US2] Update `internal/service/event_service.go`:
      - Change `Submit` signature to accept `idempotencyKey string` and `rawBody []byte`
        parameters
      - Implement idempotency check-and-set flow (Flow C from plan.md) inside a
        single database transaction:
        1. If `idempotencyKey != ""`: acquire advisory lock, call
           `idempotency_store.Lookup`
        2. If complete record found with matching hash → return cached
           `(event_id, delivery_id)` (rollback transaction first)
        3. If complete record found with different hash → return `domain.ErrConflict`
        4. If no record → call `idempotency_store.Claim` with
           `hex(sha256(rawBody))` and `NOW() + 24h`
        5. INSERT event and delivery (existing logic)
        6. If `idempotencyKey != ""`: call `idempotency_store.Complete`
        7. COMMIT

- [ ] T032 [US2] Update `internal/api/handlers_event.go`:
      - Replace `json.NewDecoder(r.Body).Decode` with
        `io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))` then `json.Unmarshal`
        (preserves `rawBody` for hashing and passes it to service)
      - Add `Idempotency-Key` header parsing and validation (regex or byte-range loop:
        `len 1–255`, all bytes in `[0x21, 0x7E]`); return 400 `invalid_idempotency_key`
        if invalid
      - Pass `idempotencyKey` and `rawBody` to `event_service.Submit`
      - Map `domain.ErrConflict` → 409 `{ "error": "idempotency_conflict", ... }`

**Checkpoint**: `go test ./tests/integration/...` and `go test ./internal/api/...` for
US2 tests pass. US1 tests remain green.

---

## Phase 5: User Story 3 — Rotate Signing Secret (Priority: P2)

**Goal**: `POST /v1/endpoints/{id}/rotate-secret` replaces the active secret with a new
one. All delivery attempts signed after the rotation response is returned use the new
secret. Old secret is immediately invalidated.

**Independent Test**: Register endpoint, note `signing_secret`. Rotate. Submit event.
Outgoing POST signature must verify against the new secret and must NOT verify against
the old secret.

### Tests for User Story 3

> Write these tests FIRST and confirm they FAIL before any implementation.

- [ ] T033 [US3] Integration test — successful rotation in
      `tests/integration/endpoint_rotation_test.go`:
      - Register endpoint (capture `oldSecret`)
      - Call `POST /v1/endpoints/{id}/rotate-secret` → 200 with new `signing_secret`
        (64-char lowercase hex, differs from `oldSecret`)
      - Submit event; capture outgoing POST headers via `httptest.Server`
      - Assert signature verifies with `newSecret`; assert signature does NOT verify
        with `oldSecret` (SC-004)

- [ ] T034 [US3] Integration test — sequential rotations in
      `tests/integration/endpoint_rotation_test.go`:
      - Rotate three times in sequence; capture `secret1`, `secret2`, `secret3`
      - Submit event; verify signature only against `secret3`

- [ ] T035 [US3] Integration test — rotate non-existent endpoint in
      `tests/integration/endpoint_rotation_test.go`:
      - `POST /v1/endpoints/{random-uuid}/rotate-secret` → 404

- [ ] T036 [US3] Integration test — rotation after failed delivery in
      `tests/integration/endpoint_rotation_test.go`:
      - Enqueue a delivery that fails transiently; rotate secret; trigger retry;
        outgoing POST must use new secret (FR-012, FR-016)

- [ ] T037 [US3] Integration test — concurrent rotation in
      `tests/integration/endpoint_rotation_test.go`:
      - Two goroutines call rotate simultaneously for the same endpoint
      - Both receive 200 with a `signing_secret` value
      - Query DB: exactly one `signing_secret` value is active; it equals one of the
        two returned values

**Run tests — confirm they FAIL before implementing.**

### Implementation for User Story 3

> Note: The handler (`RotateSecret`) and service method (`RotateSecret`) were
> implemented in Phase 3 (T015, T016) as part of the US1 signing infrastructure.
> The following tasks cover store-level verification and an integration-level test
> not already covered.

- [ ] T038 [US3] Add unit test `TestUpdateSecret_NotFound` in
      `internal/store/endpoint_store_test.go`: call `UpdateSecret(ctx, randomUUID, secret)`
      against a real Postgres container; assert `domain.ErrNotFound` is returned
      (verifies that T012's `UpdateSecret` correctly handles the missing-row case)

- [ ] T039 [US3] Add integration test `TestRotateSecret_NotFound` in
      `internal/api/handlers_endpoint_test.go`: `POST /v1/endpoints/{random-uuid}/rotate-secret`
      → assert 404 response body `{ "error": "endpoint_not_found" }`
      (exercises the full path: handler → service → store → ErrNotFound propagation → 404)

**Checkpoint**: `go test ./tests/integration/...` for US3 tests pass. US1 and US2 tests
remain green. Running the full pipeline: rotate, submit, verify new-secret signature and
old-secret rejection.

---

## Phase 6: Polish, Reaper Extension & Integration Tests

**Purpose**: Reaper purge, cross-cutting edge cases, end-to-end verification, cleanup.

- [ ] T040 Update `internal/recovery/reaper.go` — add periodic purge of expired
      idempotency records to `tick()`:
      `DELETE FROM idempotency_records WHERE expires_at <= NOW()`
      (Flow E from plan.md)

- [ ] T041 [P] Integration test for reaper purge in
      `tests/integration/reaper_idempotency_test.go`:
      - Insert an `idempotency_records` row with `expires_at = NOW() - interval '1s'`
      - Run `reaper.tick()` (or the DB statement directly)
      - Assert the row is deleted

- [ ] T042 [P] E2E test covering the full pipeline in
      `tests/integration/e2e_signing_idempotency_test.go`:
      - Register endpoint (capture `signing_secret`)
      - Submit event with `Idempotency-Key` → 202; record `event_id`, `delivery_id`
      - Assert outgoing POST carries correct `X-Webhook-Timestamp` and
        `X-Webhook-Signature` (consumer verification procedure passes)
      - Resubmit with same key → 202 with same `event_id` and `delivery_id`
      - Assert exactly 1 event row, 1 delivery row, 1 idempotency record

- [ ] T043 [P] Edge case test — `Idempotency-Key` exactly 255 chars in
      `tests/integration/idempotency_test.go`:
      - Submit with 255-char key → 202 (accepted normally)

- [ ] T044 Add JSON serialization test `TestEndpointResponse_NoSigningSecret` in
      `internal/api/dto_test.go`: marshal `EndpointResponse{}` and assert that the
      resulting JSON string does NOT contain the substring `"signing_secret"` —
      enforces at test level that the secret never leaks through read responses (SC-005)

- [ ] T045 Run `go vet ./...` and linter (`golangci-lint run ./...`); fix any findings

- [ ] T046 Update `docs/api-reference.md` (create if absent): document the updated
      `POST /v1/endpoints` 201 response body (add `signing_secret` field example),
      the new `POST /v1/endpoints/{id}/rotate-secret` endpoint (request, 200 response,
      404 error), and the `Idempotency-Key` header semantics for `POST /v1/events`
      (valid range, 400/409 error responses)

**Checkpoint**: All tests pass (`go test ./...`). Linter clean. Full pipeline verified
end-to-end. Feature 002 is production-ready.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 (migrations apply; `signer.go` skeleton
  exists) — **blocks all user story phases**
- **Phase 3 (US1)**: Depends on Phase 2 — can start once foundational types and signing
  primitive are done
- **Phase 4 (US2)**: Depends on Phase 2 — can run in parallel with Phase 3 if staffed,
  as idempotency store and event handler modifications are in separate files from Phase 3
- **Phase 5 (US3)**: Depends on Phase 3 (handler and service method already implemented
  there); consists mostly of test writing and verification
- **Phase 6 (Polish)**: Depends on Phases 3, 4, and 5 being complete

### Within Each User Story Phase

- Test tasks must be written and confirmed FAILING before any implementation task begins
- Store layer before service layer before handler layer
- Domain types and pure functions before anything that depends on them
- Core implementation before integration

### Parallel Opportunities

- T001 and T002 (migrations) can run in parallel
- T003 (package skeleton) can run in parallel with migrations
- T004 and T005 (domain types) can run in parallel
- T006 (signing tests) must be confirmed FAIL before T007 (signing impl)
- T008 (US1 endpoint test) can be written in parallel with T009–T011 (different file)
- T009, T010, T011 are sequential (same file: `worker_signing_test.go`)
- T012 and T013 (store modifications) can run in parallel
- T014 (DTO) can run in parallel with T012/T013
- T019 and T020 can be written in parallel (different files)
- T021–T029 (US2 integration tests) are sequential (same file: `idempotency_test.go`)
- T030, T031, T032 must run in order (store → service → handler)
- T033–T037 (US3 tests) are sequential (same file: `endpoint_rotation_test.go`)
- T041–T043 (Phase 6 tasks marked [P]) can run in parallel within Phase 6

---

## Implementation Strategy

### MVP first (US1 + US2 only — both P1)

1. Complete Phase 1: migrations + package skeleton
2. Complete Phase 2: domain types + `signing.Sign`
3. Complete Phase 3: US1 signing end-to-end
4. **STOP and VALIDATE**: register endpoint, submit event, verify outgoing headers
5. Complete Phase 4: US2 idempotency
6. **STOP and VALIDATE**: submit with key, resubmit, confirm single event row
7. Demo / deploy MVP

### Incremental delivery

1. Phase 1 + Phase 2 → foundation ready
2. Phase 3 → signing live (US1 complete, independently testable)
3. Phase 4 → idempotency live (US2 complete, independently testable)
4. Phase 5 → rotation tested end-to-end (US3 complete)
5. Phase 6 → reaper, E2E, polish

Each phase adds value without breaking previous phases.

---

## Notes

- `[P]` tasks = different files, no mutual dependency — safe to parallelise
- `[USn]` label maps every implementation task to its User Story for traceability
- No new external libraries — all Go stdlib
- No production code outside the SDD flow (CLAUDE.md §Execution gates)
- Do not edit `specs/templates/` files
- Confirm tests FAIL before implementing at every User Story phase boundary
- Commit after each completed task or logical group; stop at any Checkpoint to
  independently validate the story before proceeding
