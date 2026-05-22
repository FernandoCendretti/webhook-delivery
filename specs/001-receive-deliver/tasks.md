# Tasks: Receive & Deliver

**Input**: Design documents from `specs/001-receive-deliver/`
**Prerequisites**: `plan.md` (approved), `spec.md` (approved)

**Tests**: Tests are part of the deliverable in this feature. Each phase that produces user-facing behavior includes test tasks; tests are written first wherever possible.

**Organization**: Tasks are grouped by phase. Phases 3–6 each map to one User Story so they can be implemented and validated independently.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2, US3, US4)
- Each task description references a concrete file path

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project scaffolding and local infrastructure

- [x] T001 Initialize Go module (`go mod init github.com/FernandoCendretti/webhook-delivery`) and create `cmd/webhookd/main.go` with subcommand routing skeleton (`api`, `worker`, `scheduler`)
- [x] T002 [P] Configure `golangci-lint` with sensible rule set in `.golangci.yml` (errcheck, gosimple, govet, ineffassign, staticcheck, unused, gofmt, goimports, gosec)
- [x] T003 [P] Add `.editorconfig` enforcing tabs / LF / final newline
- [x] T004 [P] Create `docker-compose.yml` at repo root with Postgres 16, Redis 7, Kafka 3.7 + Zookeeper for local development
- [x] T005 [P] Add `Makefile` with targets: `lint`, `test`, `test-integration`, `run-api`, `run-worker`, `run-scheduler`, `migrate-up`, `migrate-down`, `infra-up`, `infra-down`
- [x] T006 [P] Add CI workflow `.github/workflows/ci.yml` running `make lint && make test` on push and PR (Go 1.23, ubuntu-latest)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Infrastructure that ALL user stories depend on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [x] T007 Setup `pressly/goose` migrations in `internal/store/migrations/`; create `001_init.sql` with full schema from `plan.md` (extensions, `endpoints`, `events`, `delivery_status` enum, `deliveries` with all columns + partial indexes, `attempt_outcome` enum, `attempts`)
- [x] T008 [P] Implement `internal/config/config.go` — env-tagged structs (`APIConfig`, `WorkerConfig`, `SchedulerConfig`) loading via `caarlos0/env/v11`; defaults from plan's env-vars table
- [x] T009 [P] Implement `internal/observability/logger.go` — `slog` JSON handler factory, context-aware with `request_id` propagation
- [x] T010 [P] Implement `internal/observability/metrics.go` — declare all 8 Prometheus collectors from plan, register on default registry, expose `Handler()` for the metrics endpoint
- [x] T011 [P] Define `internal/domain/endpoint.go` — `Endpoint` struct (no infra tags) + `ValidateURL(rawURL string) error` predicate
- [x] T012 [P] Define `internal/domain/event.go` — `Event` struct
- [x] T013 [P] Define `internal/domain/delivery.go` — `Delivery` struct + `DeliveryStatus` enum (`scheduled`, `in_flight`, `delivered`, `permanently_failed`) + transition helpers (`MarkInFlight`, `MarkDelivered`, `MarkPermanentlyFailed`, `RescheduleAfter(time.Duration)`)
- [x] T014 [P] Define `internal/domain/attempt.go` — `Attempt` struct + `AttemptOutcome` enum (`success`, `transient_failure`, `permanent_failure`, `timeout`)
- [x] T015 [P] Define `internal/domain/retry.go` — `MaxAttempts = 9` constant, `Delay(attemptNumber int) time.Duration` with ±15% jitter, exposed as a pure function
- [x] T016 [P] Implement `internal/store/postgres.go` — pgx pool initialization with role-specific `MaxConns`, ping check on startup, transaction helper
- [x] T017 [P] Implement `internal/queue/publisher.go` and `internal/queue/consumer.go` — `kafka-go` wrappers (Publish, Consume, Commit) with structured logging hooks
- [x] T018 Implement `internal/api/middleware.go` — `RequestID`, `Recover`, `Logging`, `Metrics` middleware (depends on T009, T010)
- [x] T019 Implement `internal/api/server.go` — `net/http` server bootstrap with chained middleware and route registration extension point (depends on T018)

### Hardening additions (from review)

- [x] T019a Add `.env.example` template at repo root and `-include .env` in Makefile with `DATABASE_URL` fallback for local dev. Aligns with 12-factor config (binary reads `os.Environ()`; `.env` is a dev convenience only)
- [x] T019b Add `internal/domain/retry_test.go` with `TestProductionScheduleLock` and `TestMaxAttemptsLock` — golden-test pattern that fails CI if the retry schedule or `MaxAttempts` drift from FR-015 without spec updates

**Checkpoint**: Foundation ready — user story implementation can begin in parallel.

---

## Phase 3: User Story 1 - Register a destination endpoint (Priority: P1) 🎯 MVP

**Goal**: Producer can register an HTTP/HTTPS endpoint and retrieve it by ID.

**Independent Test**: Register a valid URL, fetch it back by the returned ID, verify URL matches; submit malformed URL → 400; fetch missing ID → 404.

### Tests for User Story 1

> Write these first; ensure they FAIL before implementing.

- [x] T020 [P] [US1] Unit tests for URL validation in `internal/domain/endpoint_test.go` (valid http, valid https, malformed, ftp scheme, empty)
- [x] T021 [P] [US1] Integration tests in `tests/integration/api_endpoints_test.go` using testcontainers Postgres: register valid → 201 + body; register invalid → 400; fetch existing → 200; fetch unknown UUID → 404 (gated by `//go:build integration`; route wiring TODO at T026 — red until then)

### Implementation for User Story 1

- [x] T022 [US1] Implement `internal/store/endpoint_store.go` — `Insert(ctx, e *domain.Endpoint) error` and `GetByID(ctx, id uuid.UUID) (*domain.Endpoint, error)`; map `pgx.ErrNoRows` to a domain "not found" sentinel (`domain.ErrNotFound` in `internal/domain/errors.go`)
- [x] T023 [P] [US1] Implement endpoint DTOs in `internal/api/dto.go` — `EndpointRequest`, `EndpointResponse` with json tags + bidirectional conversion helpers to `domain.Endpoint`
- [x] T024 [US1] Implement `internal/service/endpoint_service.go` — `Register(ctx, url string) (*domain.Endpoint, error)` and `Get(ctx, id uuid.UUID)` (depends on T022)
- [x] T025 [US1] Implement `internal/api/handlers_endpoint.go` — `POST /v1/endpoints` (201/400) and `GET /v1/endpoints/{id}` (200/404) using stdlib `http.ServeMux` path patterns (depends on T023, T024). Adds `ErrorResponse` DTO + `writeJSON`/`writeError` helpers shared by future handlers.
- [x] T026 [US1] Register endpoint routes in `internal/api/server.go` (depends on T025). Adds `Server.RegisterEndpoints(svc)` helper; integration test `setupAPI` updated to wire pool → store → service → server.
- [x] T027 [US1] Wire `webhookd api` subcommand in `cmd/webhookd/main.go` to start the HTTP server (depends on T026)

**Checkpoint**: `make run-api`. Can register and fetch endpoints. T020 and T021 pass.

---

## Phase 4: User Story 2 - Submit and deliver event (happy path) (Priority: P1) 🎯 MVP

**Goal**: Producer can submit an event for a registered endpoint, get a `delivery_id` synchronously (202), and the system asynchronously delivers via HTTP POST. On 2xx the delivery transitions to `delivered`.

**Independent Test**: Register an endpoint pointing to an `httptest.Server` returning 200, submit an event, assert (a) API returned 202 + `delivery_id`, (b) destination received POST with the exact payload and `Content-Type: application/json`, (c) delivery status eventually `delivered`.

> US2 implements the happy path only. Failure paths are marked `permanently_failed` for now; full retry comes in US3.

### Tests for User Story 2

- [x] T028 [P] [US2] Unit tests for payload size + JSON validation in `internal/api/handlers_event_test.go`
- [x] T029 [P] [US2] Integration tests in `tests/integration/api_events_test.go`: submit valid → 202 + delivery_id; non-existent endpoint → 404; payload >1 MB → 413; malformed JSON → 400
- [x] T030 [P] [US2] Concurrency test in `tests/integration/scheduler_test.go`: insert 100 deliveries, run two scheduler instances claiming concurrently, assert no row claimed twice and no row missed (validates `FOR UPDATE SKIP LOCKED`)
- [x] T031 [P] [US2] E2E pipeline test in `tests/integration/pipeline_test.go`: full happy path with `httptest.Server` as destination, asserts payload received byte-for-byte and delivery transitions `scheduled` → `in_flight` → `delivered`

### Implementation for User Story 2

- [x] T032 [US2] Implement `internal/store/delivery_store.go` — `Create`, `GetByID`, `ClaimReady(ctx, batch int) ([]ClaimedDelivery, error)` (the SQL with `FOR UPDATE SKIP LOCKED` from plan), `MarkInFlight`, `MarkDelivered`, `MarkPermanentlyFailed`, `Reschedule(id, nextAt)`
- [x] T033 [P] [US2] Implement `internal/store/attempt_store.go` — `InsertStarted(deliveryID, sequence) (attemptID, error)`, `UpdateOutcome(attemptID, outcome, statusCode, errorReason)`
- [x] T034 [US2] Implement `internal/service/event_service.go` — `Submit(ctx, endpointID, payload)`: verify endpoint exists, transactional INSERT event + INSERT delivery (status=`scheduled`, `next_attempt_at=NOW()`); return `delivery_id` + `event_id` (depends on T032)
- [x] T035 [P] [US2] Implement event/delivery DTOs in `internal/api/dto.go` (`EventRequest`, `EventAcceptedResponse`)
- [x] T036 [US2] Implement `internal/api/handlers_event.go` — `POST /v1/events` wrapping body with `http.MaxBytesReader(1MB)` (depends on T034, T035)
- [x] T037 [US2] Register event routes in `internal/api/server.go` (depends on T036)
- [x] T038 [US2] Implement `internal/scheduler/scheduler.go` — tick loop (`SCHEDULER_TICK_MS`), call `ClaimReady` in batches of 100, publish `(endpoint_id, delivery_id)` to Kafka topic `webhook.deliveries`; graceful shutdown via context (depends on T032, T017)
- [x] T039 [US2] Wire `webhookd scheduler` subcommand in `cmd/webhookd/main.go` (depends on T038)
- [x] T040 [P] [US2] Implement `internal/delivery/http_client.go` — `*http.Client` configured with 30 s timeout and `CheckRedirect` allowing exactly 1 redirect
- [x] T041 [P] [US2] Implement `internal/delivery/outcome.go` — `Classify(resp, err) AttemptOutcome` per plan's classification matrix
- [x] T042 [US2] Implement `internal/delivery/worker.go` — Kafka consumer loop: load delivery + endpoint + payload (single JOIN query), `INSERT attempt` (started), HTTP POST, `Classify`, transactional UPDATE attempt + UPDATE delivery (success → `delivered`; **anything else → `permanently_failed` for US2**, US3 will refine), commit Kafka offset only after Postgres commit (depends on T032, T033, T040, T041, T017)
- [x] T043 [US2] Wire `webhookd worker` subcommand in `cmd/webhookd/main.go`, spinning up `WORKER_CONCURRENCY` consumer goroutines (depends on T042)
- [x] T044 [P] [US2] Wire metrics emission: `webhook_events_submitted_total`, `webhook_events_rejected_total{reason}`, `webhook_delivery_attempts_total{outcome=success|permanent_failure}`, `webhook_delivery_attempt_duration_seconds`

**Checkpoint**: `make infra-up && make run-api && make run-scheduler && make run-worker`. Submit event via `curl`, destination receives POST. T028–T031 pass.

---

## Phase 5: User Story 3 - Retry on transient failure with exponential backoff (Priority: P2)

**Goal**: When a delivery attempt fails transiently (timeout/connection error/5xx/429), the system schedules a retry on the published backoff sequence. After 9 attempts without success, marks `permanently_failed`. Crashed workers' deliveries are resurrected by the reaper.

**Independent Test**: Destination returns 503 three times then 200 → delivery eventually `delivered` with 4 attempts recorded at intervals ~1s/5s/30s. Destination returns 503 forever → `permanently_failed` after attempt 9. Worker killed mid-attempt → reaper resurrects within `IN_FLIGHT_LEASE_SECONDS` and second worker delivers.

### Tests for User Story 3

- [x] T045 [P] [US3] Unit test for `Delay()` jitter in `internal/domain/retry_test.go` — assert each interval falls within ±15 % of base; assert `Delay(1)` is zero; assert `Delay(10)` is zero (out of schedule)
- [x] T046 [P] [US3] Unit test for `Classify()` in `internal/delivery/outcome_test.go` covering 200/204/301-after-redirect/400/404/429/500/503/`context.DeadlineExceeded`/dial-error
- [x] T047 [P] [US3] Integration test in `tests/integration/retry_test.go`: destination flaky (3× 503 then 200) → eventually delivered with 4 attempts, intervals approximately 1 s/5 s/30 s
- [x] T048 [P] [US3] In same file: destination returns 400 → `permanently_failed` after attempt 1; destination returns 429 → retried per standard schedule (no `Retry-After`)
- [x] T049 [P] [US3] In same file: destination returns 503 forever → `permanently_failed` after attempt 9 (uses test-only short schedule from T051)
- [x] T050 [P] [US3] Crash/recovery integration test in `tests/integration/recovery_test.go`: register endpoint, submit event, kill worker mid-attempt by closing its context, assert reaper resurrects delivery within configured lease and a second worker completes the delivery (validates at-least-once, SC-007 unit case)

### Implementation for User Story 3

- [x] T051 [US3] Add a test-only short schedule in `internal/domain/retry.go` — selectable via package-level variable swap in tests (e.g. `retry.UseShortScheduleForTests()`); production schedule remains the default
- [x] T052 [US3] Extend `internal/delivery/worker.go` failure handling: on `transient_failure` / `timeout`, if `attempt_count+1 < MaxAttempts`, UPDATE delivery `status='scheduled'`, `next_attempt_at = NOW() + retry.Delay(attempt_count+1)`, `in_flight_lease_until=NULL`; otherwise UPDATE `status='permanently_failed'`. On `permanent_failure`, mark `permanently_failed` directly. (modifies T042)
- [x] T053 [US3] Implement `internal/recovery/reaper.go` — periodic UPDATE (`REAPER_TICK_SECONDS` default 60 s) that resets `in_flight` deliveries past `in_flight_lease_until` back to `scheduled` with `in_flight_lease_until=NULL`
- [x] T054 [US3] Wire reaper to run inside `webhookd scheduler` subcommand alongside the scheduler tick (depends on T053, T039)
- [x] T055 [P] [US3] Wire remaining metrics: `webhook_delivery_lease_resurrected_total` (incremented by reaper for each resurrected row), `webhook_endpoint_failure_streak{endpoint_id}`, `webhook_scheduler_queue_depth`, `webhook_scheduler_claimed_total`

**Checkpoint**: T045–T049 and T052 pass. End-to-end demo: destination returns 503 for 30 s then 200; delivery completes after a few retries.

---

## Phase 6: User Story 4 - Inspect delivery attempts (Priority: P3)

**Goal**: Producer fetches a delivery by ID and sees overall status, attempt count, next scheduled attempt (if applicable), and the ordered list of attempts with status codes and error reasons.

**Independent Test**: Submit event to a flaky destination → fetch delivery → response contains attempts list with timestamps and outcomes.

### Tests for User Story 4

- [x] T056 [P] [US4] Integration test in `tests/integration/api_deliveries_test.go`: submit event to flaky destination, fetch delivery → assert response shape per plan (status, attempt_count, next_attempt_at, attempts list); fetch unknown UUID → 404

### Implementation for User Story 4

- [x] T057 [US4] Extend `internal/store/delivery_store.go` with `GetByIDWithAttempts(ctx, id) (*domain.Delivery, []domain.Attempt, error)` (single query with LEFT JOIN ordered by attempts.sequence)
- [x] T058 [US4] Implement `internal/service/delivery_service.go` — `Get(ctx, deliveryID)` returns aggregated view (depends on T057)
- [x] T059 [P] [US4] Implement delivery response DTO in `internal/api/dto.go` (shape per plan: includes ordered `attempts` array)
- [x] T060 [US4] Implement `internal/api/handlers_delivery.go` — `GET /v1/deliveries/{id}` with 200 / 404 (depends on T058, T059)
- [x] T061 [US4] Register delivery routes in `internal/api/server.go` (depends on T060)

**Checkpoint**: T056 passes. Producer can introspect any delivery's history.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Production readiness, validation of full-system success criteria, operational ergonomics

- [x] T062 Run E2E SC-007 test in `tests/integration/sc007_test.go`: submit 1,000 events at 50 events/sec to a healthy endpoint, restart all components mid-run, assert 100 % delivered with no data loss (at-least-once: duplicates allowed but accounted for)
- [x] T063 [P] Tune `DATABASE_POOL_MAX` defaults under load based on T062 metrics
- [x] T064 [P] Add `pprof` endpoint in `cmd/webhookd/main.go` behind `--pprof-addr` flag (empty = disabled)
- [x] T065 [P] Write operational README section: env-vars table from plan, metrics list, common scenarios (lease tuning, scheduler scaling, PgBouncer setup snippet), troubleshooting playbook for "deliveries stuck in scheduled"
- [x] T066 Add `Dockerfile` (multi-stage: Go 1.25 build → distroless runtime) and verify image runs all three subcommands
- [x] T067 [P] Lint pass: ensure all exported types and functions have docstrings; 86 symbols documented; go build + go vet clean

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational
- **User Story 2 (Phase 4)**: Depends on Foundational. Can start in parallel with US1, but the API server bootstrap from Phase 2 must be done by both
- **User Story 3 (Phase 5)**: Depends on US2 (extends the worker and the same delivery flow)
- **User Story 4 (Phase 6)**: Depends on US3 (needs `attempts` populated to be useful), but technically only requires US2 data model
- **Polish (Phase 7)**: Depends on US3 (and ideally US4) being complete

### Within Each User Story

- Tests written first; verify they FAIL before implementing
- Domain types before services
- Services before handlers
- Wiring (`server.go`, `main.go`) last
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked `[P]` after T001
- All Foundational tasks T008–T017 marked `[P]` (only T018 and T019 are sequential at the end of Phase 2)
- US1 and US2 can be developed in parallel by different people once Foundational is complete
- Within each US, all `[P]` tasks (tests, DTOs, pure functions) run in parallel

---

## Implementation Strategy

### MVP first (US1 + US2)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: US1 — register endpoint
4. Complete Phase 4: US2 — submit + deliver happy path
5. **STOP and VALIDATE**: end-to-end demo with one healthy endpoint, single event submitted and delivered
6. Demo / commit / push as MVP

### Incremental delivery

1. Setup + Foundational → infrastructure ready
2. + US1 → register endpoint demo
3. + US2 → MVP demo (submit + deliver happy path)
4. + US3 → resilience demo (retry on flaky endpoint, crash recovery)
5. + US4 → observability demo (inspect any delivery's history)
6. + Polish → production-ready (SC-007 validated, Docker image, README)

Each step is a meaningful demoable increment.

---

## Notes

- `[P]` tasks = different files, no dependencies
- `[Story]` label maps task to a specific user story for traceability
- Each user story is independently completable and testable
- Verify tests fail before implementing each task they cover
- Commit after each task or logical `[P]` group
- Stop at any checkpoint to validate the story independently
- Avoid: vague tasks, same-file conflicts, cross-story dependencies that break independence
- After a phase's checkpoint, run the full test suite to catch regressions before starting the next phase
