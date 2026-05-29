# Feature Specification: Test Architecture — Unit vs Integration Boundary

**Created**: 2026-05-29
**Status**: Draft
**Input**: Separate pure-Go orchestration tests from SQL-layer tests to reduce container startup overhead in the red→green cycle.

---

## Context

The circuit breaker feature introduced two test files that both carry `//go:build integration`
and both spin up a Postgres testcontainer on every run:

| File | Tags today | Tests |
|------|-----------|-------|
| `internal/store/circuit_store_test.go` | `integration` | T025 — 8 tests exercising `CircuitStore` SQL methods |
| `tests/integration/circuit_breaker_test.go` | `integration` | T026-T039b — 16 tests exercising full CB scenarios |

Every `go test -tags integration ./...` invocation therefore starts at least one Postgres
container, adding 3-5 s of startup even when the change under test is in pure Go code.

The root cause is that **two distinct kinds of correctness** are conflated in the same
build tag:

1. **SQL correctness** — does the CASE expression in `HandleTransientFailure` produce the
   right state transition in Postgres?  
2. **Orchestration correctness** — given a known set of outcomes returned by the store, does
   the Go code in `Scheduler.Tick` or `Worker.ProcessOne` call the right store methods?

Only (1) requires a real database. (2) can be tested with a fake/mock implementation of
the already-defined store interfaces.

---

## User Scenarios & Testing

### User Story 1 — Fast feedback on orchestration changes (Priority: P1)

A developer modifies `Scheduler.Tick` (e.g., the crash-recovery logic in Step 0a) or
`Worker.ProcessOne` (e.g., the outcome-to-store-method routing). She runs `go test ./...`
(no build tag) and gets a green/red result in under 5 s — without Colima or Docker running.

**Why this priority**: This is the tightest loop in daily development. The scheduler and
worker orchestration code changes more often than the SQL state machine.

**Independent Test**: Run `go test ./internal/delivery/... ./internal/scheduler/...` with no
`-tags integration` flag on a machine with no Docker. All orchestration tests pass in <1 s.

**Acceptance Scenarios**:

1. **Given** `Scheduler.Tick` is called with a mock `circuitStore` that returns two orphaned
   half_open IDs, **When** no Postgres is present, **Then** `SetProbeDelivery` is called
   exactly once per orphaned ID.

2. **Given** `Worker.ProcessOne` receives a delivery with outcome `transient_failure`,
   **When** no Postgres is present, **Then** `HandleTransientFailure` is called with the
   correct endpoint ID and config, and `HandleSuccess` is NOT called.

3. **Given** `Worker.ProcessOne` receives a delivery with outcome `success`,
   **When** no Postgres is present, **Then** `HandleSuccess` is called and
   `HandleTransientFailure` is NOT called.

4. **Given** `Worker.ProcessOne` receives a delivery with outcome `permanent_failure`,
   **When** no Postgres is present, **Then** neither `HandleSuccess` nor
   `HandleTransientFailure` is called; `HandleProbePermanentFailure` is NOT called either
   (permanent failure on a closed circuit only marks the delivery; probe handling is
   half_open-specific).

---

### User Story 2 — Trustworthy SQL state machine validation (Priority: P1)

A developer modifies a SQL UPDATE in `circuit_store.go` (e.g., the CASE expression for
`sensitive_recovery`). She runs `make test-integration` and gets definitive proof that the
real Postgres accepted or rejected the transition correctly.

**Why this priority**: SQL CASE expressions, enum constraints, and correlated subqueries
cannot be meaningfully tested without the real engine. False positives from mocks would
hide regressions.

**Independent Test**: Run `make test-integration` and all `CircuitStore` SQL tests pass
against a live Postgres container.

**Acceptance Scenarios**:

1. **Given** `HandleTransientFailure` is called 5 times on a closed circuit, **When** the
   SQL executes against Postgres, **Then** `circuit_state` is `open` and
   `circuit_suspended_until` is non-null.

2. **Given** `ProcessExpiredSuspensions` runs on an endpoint whose `circuit_suspended_until`
   is in the past with non-terminal deliveries, **When** the correlated subquery executes,
   **Then** the endpoint transitions to `half_open` (not `closed`) and the `halfOpenIDs`
   slice contains the endpoint ID.

3. **Given** two goroutines both call `HandleTransientFailure` for the 5th failure
   concurrently, **When** the SQL WHERE-clause guard executes under concurrent load,
   **Then** `circuit_failure_count` does not exceed the threshold.

---

### Edge Cases

- What happens when `Worker.ProcessOne` is wired with `CircuitStore = nil`? The routing
  code must skip all circuit-store calls — the unit test must cover this nil-store path
  without a container.
- How does `Scheduler.Tick` behave when `OrphanedHalfOpenEndpoints` returns an error?
  The scheduler must log and continue to Step 0b — testable without a container.

---

## Requirements

### Functional Requirements

- **FR-001**: A test MUST be classified as a **unit test** (no build tag, no container) if
  and only if its correctness depends solely on Go control flow, not on SQL expressions,
  database constraints, or database-level atomicity.

- **FR-002**: A test MUST be classified as an **integration test** (`//go:build integration`,
  requires Postgres) if it validates any of the following:
  - A SQL CASE expression or conditional UPDATE  
  - A database constraint (enum type, FK, NOT NULL)  
  - A correlated subquery across two tables  
  - Concurrent write atomicity / serialization semantics  
  - State persistence across store reconnection  
  - Cross-table filter behavior (e.g., `ClaimReady` join with `endpoints.circuit_state`)

- **FR-003**: The `delivery.Classify` function already has unit tests in
  `internal/delivery/outcome_test.go`. Those tests must not be touched by this feature
  (they already satisfy FR-001 for their scope).

- **FR-004**: New unit tests for `Scheduler.Tick` orchestration MUST live in
  `internal/scheduler/` with no build tag. They MUST use a mock or stub implementation
  of the `circuitStore` and `deliveryStore` interfaces already defined in `scheduler.go`.

- **FR-005**: New unit tests for `Worker.ProcessOne` outcome routing MUST live in
  `internal/delivery/` with no build tag. They MUST use a mock or stub of the
  `workerCircuitStore` interface already defined in `worker.go`.

- **FR-006**: All existing tests in `internal/store/circuit_store_test.go` (T025) MUST
  remain integration tests. They validate SQL CASE expressions embedded in UPDATE queries
  and cannot be made meaningful without Postgres.

- **FR-007**: All existing tests in `tests/integration/circuit_breaker_test.go`
  (T026-T039b) MUST remain integration tests. Every scenario either validates a SQL
  transition, a cross-table query, concurrent write behavior, or persistence durability.

- **FR-008**: The total wall-clock time for `go test ./...` (no build tag) MUST be under
  5 s on a developer laptop with no Docker daemon running. Today's baseline is ~0 s (the
  only no-tag test is `outcome_test.go`); the target is still <5 s after adding new unit
  tests.

- **FR-009**: No mock library may be introduced. Stubs must be plain Go structs
  implementing the existing interfaces. This keeps the dependency graph unchanged.

### Migration Map

| Test | Current layer | Target layer | Reason |
|------|--------------|-------------|--------|
| `TestClassify` (outcome_test.go) | unit (no tag) | unit — no change | already correct |
| T025 in `circuit_store_test.go` | integration | integration — no change | tests SQL CASE expressions |
| T026 `OpenAfterThreshold` | integration | integration — no change | tests SQL threshold logic |
| T027 `PermanentFailureNoCounter` | integration | integration — no change | tests SQL counter isolation |
| T028 `ProcessExpired_NonTerminalDelivery` | integration | integration — no change | correlated subquery |
| T029 `ProcessExpired_EmptyQueue` | integration | integration — no change | correlated subquery |
| T030 `CrashRecovery_HalfOpenNullProbe` | integration | integration — no change | SetProbeDelivery + ClaimReady SQL |
| T031 `SetProbeDelivery_EmptyQueueFallback` | integration | integration — no change | race between two SQL writes |
| T032 `ProbeSuccess` | integration | integration — no change | SQL flag update + read |
| T033 `ProbeTransientFailure` | integration | integration — no change | SQL CASE for half_open |
| T034 `ProbePermanentFailure` | integration | integration — no change | FR-011 counter non-increment |
| T035 `SensitiveRecovery_SingleFailureOpens` | integration | integration — no change | SQL CASE for sensitive flag |
| T036 `SensitiveRecovery_Reset` | integration | integration — no change | multi-step SQL flag lifecycle |
| T037 `RestartDurability` | integration | integration — no change | persistence across store reconnect |
| T038 `ConcurrentTransientFailure` | integration | integration — no change | SQL concurrent write atomicity |
| T039 `CircuitClose_UnblocksDelivery` | integration | integration — no change | ClaimReady cross-table filter |
| T039a `CrossTenantIsolation` | integration | integration — no change | ClaimReady per-endpoint scope |
| T039b `QueueDrainAfterProbeSuccess` | integration | integration — no change | end-to-end pipeline + SQL |
| **NEW** Scheduler.Tick orchestration | — | **unit** | Go control flow only |
| **NEW** Worker.ProcessOne routing | — | **unit** | Go control flow only |

### Cut Criterion (decision rule)

> **"Can I describe the expected behavior without mentioning SQL, a database column, a
> constraint, or a transaction?"**  
> — Yes → unit test.  
> — No → integration test.

Examples:
- "After 5 calls to HandleTransientFailure, `circuit_state` must be `open`" → mentions a
  DB column → **integration**.
- "When the store returns two orphaned IDs, Tick must call SetProbeDelivery twice" →
  pure Go control flow → **unit**.

### Key Entities

- **`circuitStore` interface** (`internal/scheduler/scheduler.go`): the boundary that
  unit tests will stub. Already has methods: `OrphanedHalfOpenEndpoints`,
  `ProcessExpiredSuspensions`, `SetProbeDelivery`.
- **`workerCircuitStore` interface** (`internal/delivery/worker.go`): the boundary for
  worker unit tests. Methods: `HandleSuccess`, `HandleTransientFailure`,
  `HandleProbePermanentFailure`.

---

## Success Criteria

- **SC-001**: `go test ./...` (no build tag, no Docker) completes in under 5 s and all
  tests pass. This is measurable on CI by running without the `integration` tag.

- **SC-002**: `make test-integration` continues to pass all 24 existing circuit breaker
  test cases (8 T025 + 16 T026-T039b) with zero changes to those test files.

- **SC-003**: New unit tests cover at least the following Scheduler paths: Step 0a orphan
  recovery (zero orphans, N orphans, error from store), Step 0b expiry processing (zero
  expired, half_open results, closed results), and the nil-CircuitStore guard.

- **SC-004**: New unit tests cover at least the following Worker paths: success outcome,
  transient_failure outcome, permanent_failure outcome (closed circuit), and nil-
  CircuitStore guard.

- **SC-005**: No new library dependencies are added. The only new files are `*_test.go`
  files and stub types within the existing packages.

---

## Assumptions

- The `workerCircuitStore` interface in `worker.go` is the correct seam for testing
  Worker routing; no refactor of `ProcessOne` is needed beyond what already exists.
- The `circuitStore` interface in `scheduler.go` is the correct seam for testing Tick
  orchestration; no additional interface extraction is needed.
- Plain struct stubs (not generated mocks) are sufficient because the interface methods
  are few and their behavior in tests is trivially controlled by return values.
- The `//go:build integration` tag is the project-wide convention for container-backed
  tests; this feature does not change that convention.
