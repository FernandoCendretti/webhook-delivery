<!--
  Feature: 004-dlq-replay
-->

# Implementation Plan: DLQ & Replay

**Date**: 2026-05-29
**Status**: Approved
**Spec**: [specs/004-dlq-replay/spec.md](spec.md)

## Summary

Expose four HTTP endpoints that let operators inspect permanently-failed deliveries (the
"Dead Letter Queue") and replay them through the standard retry pipeline. A replay
creates a new `Delivery` record referencing the original via `source_delivery_id`;
the original delivery is never mutated. Bulk replay iterates matching records inside a
single request. No new infrastructure components are required — the feature builds on
the existing PostgreSQL schema, delivery pipeline, and HTTP server patterns established
in features 001–003.

## Technical Context

**Language/Version**: Go 1.23  
**Primary Dependencies**: net/http (stdlib), pgx/v5, google/uuid (already in go.mod)  
**Storage**: PostgreSQL 16  
**Messaging**: Apache Kafka 3.7 (indirectly — replayed deliveries enter the scheduler
queue, which the existing `Scheduler` picks up; no direct Kafka writes in this feature)  
**Testing**: Go test + testify; integration via testcontainers-go (existing setup)  
**Target Platform**: Linux container  
**Project Type**: Web service (HTTP API addition)  
**Performance Goals**: `GET /v1/dlq` first page < 1 s with 1 000+ records; `POST
/v1/dlq/{id}/replay` < 500 ms; consistent with SC-001–SC-005  
**Constraints**: Concurrent replay of the same delivery must produce exactly one new
scheduled delivery (SC-005); replayed delivery must follow per-tenant ordering (FR-011)  
**Scale/Scope**: Same as existing system — replayed deliveries share the delivery table
with originals; no separate storage tier

## Project Structure

### Documentation (this feature)

```text
specs/004-dlq-replay/
├── spec.md              # WHAT (already written)
├── plan.md              # this file — HOW
└── tasks.md             # ORDER (created after plan is approved)
```

### Source Code (additions only)

```text
internal/
├── domain/
│   └── delivery.go          # add SourceDeliveryID *uuid.UUID field
├── store/
│   ├── migrations/
│   │   └── 007_dlq_replay.sql   # ALTER + index (see Data Model)
│   ├── delivery_store.go    # add ListPermanentlyFailed, GetPermanentlyFailed,
│   │                        #     CreateReplay, HasNonTerminalReplay
│   └── attempt_store.go     # add ListByDelivery
├── service/
│   └── dlq_service.go       # new: DLQService — orchestrates all DLQ logic
└── api/
    ├── handlers_dlq.go      # new: dlqHandler (List, Detail, Replay, BulkReplay)
    ├── dto.go               # add DLQ request/response types
    └── server.go            # add RegisterDLQ method

tests/
└── integration/
    └── dlq_test.go          # new: integration tests for all four endpoints
```

**Structure Decision**: Follows the existing layered pattern — domain → store → service →
api handler. A new `DLQService` encapsulates all DLQ-specific logic (idempotency guard,
endpoint existence check, bulk iteration) so handlers stay thin.

## Technical Design

### Components & responsibilities

```
HTTP Handler (handlers_dlq.go)
  │  parses and validates HTTP input, serialises responses
  ▼
DLQService (service/dlq_service.go)
  │  enforces business rules:
  │    - delivery must be permanently_failed
  │    - endpoint must still exist (for replay)
  │    - no non-terminal replay in flight (idempotency guard)
  │    - at least one filter required for bulk replay
  ▼
Store layer (delivery_store.go, attempt_store.go, endpoint_store.go)
  │  SQL queries; transactional where required
  ▼
PostgreSQL
```

Replayed deliveries enter the existing scheduler pipeline as ordinary `scheduled`
deliveries; no additional Kafka message is produced at replay time.

#### `DLQService` interface

```go
type DLQService interface {
    List(ctx context.Context, filter DLQFilter, page, limit int) ([]DLQEntry, Pagination, error)
    Detail(ctx context.Context, deliveryID uuid.UUID) (*DLQDetail, error)
    Replay(ctx context.Context, deliveryID uuid.UUID) (*domain.Delivery, error)
    BulkReplay(ctx context.Context, filter DLQFilter) (int, error)
}

// DLQFilter holds the optional filters shared by List and BulkReplay.
type DLQFilter struct {
    TenantID   *uuid.UUID
    EndpointID *uuid.UUID
}

// DLQEntry is a single row in the listing response.
type DLQEntry struct {
    DeliveryID   uuid.UUID
    EventID      uuid.UUID
    EndpointID   uuid.UUID
    TenantID     uuid.UUID
    AttemptCount int
    FailedAt     time.Time
}

// DLQDetail is the full detail response (metadata + attempt history).
type DLQDetail struct {
    DLQEntry
    Attempts []domain.Attempt
}

// Pagination carries paging metadata returned by List.
type Pagination struct {
    Page    int
    Limit   int
    HasNext bool
}
```

Error contract: `Detail` and `Replay` return `domain.ErrNotFound` when the delivery does
not exist or is not `permanently_failed`. `Replay` returns `domain.ErrConflict` (new
sentinel) when a non-terminal replay already exists, and `domain.ErrUnprocessable` when
the endpoint no longer exists. `BulkReplay` returns `domain.ErrUnprocessable` when a
filter entity does not exist.

### Data model

#### Migration `007_dlq_replay.sql`

```sql
-- Track which original delivery triggered a replay
ALTER TABLE deliveries
  ADD COLUMN source_delivery_id UUID REFERENCES deliveries(id);

-- Enforce at most one non-terminal replay per original delivery (SC-005)
CREATE UNIQUE INDEX idx_deliveries_one_active_replay
  ON deliveries (source_delivery_id)
  WHERE source_delivery_id IS NOT NULL
    AND status IN ('scheduled', 'in_flight');

-- Accelerate DLQ listing queries (FR-001, FR-002, SC-004)
CREATE INDEX idx_deliveries_pf_tenant_endpoint
  ON deliveries (status, tenant_id, endpoint_id, updated_at DESC)
  WHERE status = 'permanently_failed';
```

No existing columns are altered; migration is additive and backward-compatible.

#### Existing schema (relevant columns)

`deliveries`: `id`, `event_id`, `endpoint_id`, `tenant_id`, `status`, `attempt_count`,
`next_attempt_at`, `created_at`, `updated_at`, `source_delivery_id` (new)

`attempts`: `id`, `delivery_id`, `sequence`, `started_at`, `completed_at`, `outcome`,
`response_status_code`, `error_reason`

#### New store methods

| Method | SQL summary |
|---|---|
| `DeliveryStore.ListPermanentlyFailed(ctx, filter, page, limit)` | SELECT from `deliveries` WHERE `status='permanently_failed'` + optional filters, ORDER BY `updated_at DESC`, LIMIT/OFFSET — used by the HTTP listing endpoint |
| `DeliveryStore.ListPermanentlyFailedIDs(ctx, filter)` | `SELECT id FROM deliveries WHERE status='permanently_failed' [AND tenant_id=$x] [AND endpoint_id=$y]` — returns all matching IDs as a `[]uuid.UUID` snapshot; no LIMIT/OFFSET; used exclusively by BulkReplay to avoid mid-iteration drift |
| `DeliveryStore.GetPermanentlyFailed(ctx, id)` | SELECT delivery WHERE `id=$1 AND status='permanently_failed'` |
| `DeliveryStore.HasNonTerminalReplay(ctx, sourceID)` | SELECT 1 WHERE `source_delivery_id=$1 AND status IN ('scheduled','in_flight')` — used only by the bulk flow as a pre-filter; single replay relies solely on the unique index conflict |
| `DeliveryStore.CreateReplay(ctx, eventID, endpointID, sourceID)` | INSERT with `status='scheduled'`, `attempt_count=0`, `next_attempt_at=NOW()`, `in_flight_lease_until=NULL`, `source_delivery_id=sourceID`; leverages unique index to reject concurrent duplicates; callers must handle `pgx.PgError` code `23505` |
| `AttemptStore.ListByDelivery(ctx, deliveryID)` | SELECT attempts WHERE `delivery_id=$1` ORDER BY `sequence ASC` |

`DeliveryStore.Create` is extended to accept an optional `sourceDeliveryID` parameter;
the existing call sites pass `nil`.

### API contracts

All responses use `Content-Type: application/json`.

---

#### `GET /v1/dlq`

**Query parameters**:

| Name | Type | Required | Description |
|---|---|---|---|
| `tenant_id` | UUID string | No | Filter to a single tenant |
| `endpoint_id` | UUID string | No | Filter to a single endpoint |
| `page` | int ≥ 1 | No | Page number, default 1 |
| `limit` | int 1–100 | No | Page size, default 20 |

**Response 200**:

```json
{
  "items": [
    {
      "delivery_id": "uuid",
      "event_id":    "uuid",
      "endpoint_id": "uuid",
      "tenant_id":   "uuid",
      "attempt_count": 5,
      "failed_at":   "2026-05-29T10:00:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "has_next": true
  }
}
```

**Response 400**: malformed query parameter (invalid UUID format for `tenant_id` or
`endpoint_id`; `limit` outside 1–100; `page` < 1).

`failed_at` = `MAX(attempts.completed_at)` for the delivery, computed via a sub-select
in the listing query.

---

#### `GET /v1/dlq/{delivery_id}`

**Response 200**:

```json
{
  "delivery_id":  "uuid",
  "event_id":     "uuid",
  "endpoint_id":  "uuid",
  "tenant_id":    "uuid",
  "attempt_count": 5,
  "failed_at":    "2026-05-29T10:00:00Z",
  "attempts": [
    {
      "sequence":             1,
      "started_at":           "2026-05-29T09:55:00Z",
      "completed_at":         "2026-05-29T09:55:01Z",
      "outcome":              "http_error",
      "response_status_code": 503,
      "error_reason":         null
    }
  ]
}
```

**Response 400**: `delivery_id` path parameter is not a valid UUID.  
**Response 404**: delivery does not exist or status ≠ `permanently_failed`.

`failed_at` in the detail response is derived from the `AttemptStore.ListByDelivery`
result: the service layer takes `MAX(completed_at)` across all returned attempts. Since
all attempts for a `permanently_failed` delivery are complete, `completed_at` is never
null and no nullable guard is required.

---

#### `POST /v1/dlq/{delivery_id}/replay`

**Request body**: empty (no body required).

**Response 202**:

```json
{
  "delivery_id": "uuid",
  "status": "scheduled"
}
```

**Response 400**: `delivery_id` path parameter is not a valid UUID.  
**Response 404**: delivery not found or not `permanently_failed`.  
**Response 409**: a non-terminal replay already exists for this delivery.  
**Response 422**: endpoint referenced by the delivery no longer exists.

---

#### `POST /v1/dlq/replay` (bulk)

**Request body**:

```json
{
  "tenant_id":   "uuid",   // at least one of tenant_id / endpoint_id required
  "endpoint_id": "uuid"
}
```

**Response 202**:

```json
{ "replayed": 7 }
```

**Response 400**: no filter field provided, or `tenant_id`/`endpoint_id` value is not a valid UUID format.  
**Response 422**: `tenant_id` or `endpoint_id` is a valid UUID but references a non-existent entity.

---

**Route registration order matters**: `POST /v1/dlq/replay` (literal) must be registered
before `POST /v1/dlq/{delivery_id}/replay` (pattern) in the ServeMux so the stdlib
router prefers the more specific pattern. Go 1.22+ `net/http` pattern matching handles
this correctly when the literal segment is registered first.

### Critical flows

#### Single replay (`POST /v1/dlq/{delivery_id}/replay`)

1. Parse `delivery_id` from path; return 400 on invalid UUID.
2. `DeliveryStore.GetPermanentlyFailed(ctx, id)` → 404 if not found.
3. `EndpointStore.GetByID(ctx, delivery.EndpointID)` → 422 if not found.
4. `DeliveryStore.CreateReplay(ctx, delivery.EventID, delivery.EndpointID, delivery.ID)`
   wrapped in a transaction; the unique partial index
   `idx_deliveries_one_active_replay` causes an INSERT conflict if a concurrent
   replay is in progress. Catch `pgx.PgError` with code `23505` → return 409.
5. Return 202 with `{ delivery_id, status: "scheduled" }`.

The new delivery is immediately visible to the `Scheduler.ClaimReady` loop (same table,
same polling interval). Per-tenant ordering is enforced by the existing
`idx_deliveries_tenant_ordering` check in `ClaimReady`.

#### Bulk replay (`POST /v1/dlq/replay`)

1. Decode and validate body; return 400 if both filter fields are absent.
2. Verify that filter entities exist (`TenantStore.GetByID` / `EndpointStore.GetByID`)
   → 422 if not found.
3. Fetch all matching delivery IDs upfront via a single
   `DeliveryStore.ListPermanentlyFailedIDs(ctx, filter)` query (returns `[]uuid.UUID`,
   no OFFSET pagination — snapshot semantics avoid mid-iteration consistency drift).
4. Iterate the snapshot: for each ID call `CreateReplay`. Treat a `23505` pgx error
   (concurrent replay already inserted) as a skip — do not count it and do not abort
   the loop. Skip IDs where `HasNonTerminalReplay` returns true before attempting
   the INSERT (optimistic pre-filter to avoid unnecessary constraint violations).
5. Accumulate count of successful INSERTs; return 202 with `{ "replayed": N }`.
6. Bulk replay runs within a single HTTP request; there is no background job.

**Known limitation**: if the client disconnects mid-loop, committed INSERTs are not rolled back (each is an independent transaction). The caller receives no count. On retry, `HasNonTerminalReplay` skips already-replayed IDs, so idempotency is preserved — no duplicate deliveries are created.

#### DLQ listing freshness (FR-013 / SC-001)

`GET /v1/dlq` queries PostgreSQL directly with no caching layer. The delivery worker
writes `status='permanently_failed'` synchronously via `DeliveryStore`; the row is
immediately committed and visible to subsequent reads. The partial index
`idx_deliveries_pf_tenant_endpoint` (covering `status`, `tenant_id`, `endpoint_id`,
`updated_at DESC`) ensures the listing query uses an index scan rather than a seq scan,
keeping latency well under the 1 s SLO even with 1 000+ permanently-failed records.
SC-001 is validated in the integration test by recording the `updated_at` timestamp at
status transition, then timing a `GET /v1/dlq` call and asserting both the delivery
appears in the response and the elapsed time is < 1 s.

### External dependencies

- **PostgreSQL**: all state is stored and queried here; no additional services required.
- **Kafka / Scheduler**: replayed deliveries enter the existing scheduler pipeline
  unchanged; no new Kafka topics or consumer groups are introduced.
- **Redis**: not used by this feature.

## Testing Strategy

- **Unit**: `DLQService` methods with a mock store interface — covers all business-rule
  branches: missing delivery, wrong status, non-terminal replay exists, endpoint deleted,
  missing bulk filter, entity-not-found 422 paths.
- **Integration** (`tests/integration/dlq_test.go`): uses the existing testcontainers-go
  Postgres + Kafka setup. Covers all spec acceptance scenarios:
  - US1: list pagination, empty list, `tenant_id`/`endpoint_id` filters.
  - US2: detail with attempt history, 404 for non-existent / wrong-status delivery.
  - US3: single replay happy path (verify new delivery reaches `delivered`), 404, 409
    idempotency guard (concurrent requests), 422 deleted endpoint, replay of a replay.
  - US4: bulk replay with filter, empty result, 400 no-filter guard, 422 entity check,
    skip deliveries that already have a non-terminal replay.
- **No contract or E2E tests** beyond the existing integration suite are added for this
  feature; the integration tests cover the full stack from HTTP request to database.

## Trade-offs

| Decision | Chosen | Rejected | Reason |
|---|---|---|---|
| DLQ listing source | Direct SQL query on `deliveries` table | Materialised view / separate DLQ table | The spec defines DLQ as a projection, not a new entity. A view adds DDL complexity with no correctness benefit at current scale. |
| `failed_at` computation | Sub-select `MAX(attempts.completed_at)` in listing query | Denormalised column on `deliveries` | Avoids write amplification on every attempt; sub-select is fast with the covering index `idx_deliveries_pf_tenant_endpoint`. |
| Concurrent replay guard | Unique partial index on `(source_delivery_id) WHERE status IN (...)` | Application-level check + advisory lock | Database-enforced constraint is simpler, eliminates TOCTOU race, and requires no Redis coordination. |
| Bulk replay execution | Synchronous in-request iteration | Background job / async queue | Spec explicitly states synchronous execution. Async adds operational complexity without a clear benefit at expected bulk sizes. |
| Route conflict resolution | Register literal `/v1/dlq/replay` before wildcard `/{delivery_id}/replay` | chi router | The stdlib `net/http` router in Go 1.22+ selects the most specific match, making a third-party router unnecessary. |

## Open Questions

None — all spec ambiguities were resolved in the spec itself.

## Review Checklist

- [x] Every FR from spec has a clear implementation path in this plan
- [x] Every SC from spec has a way to be measured post-implementation
- [x] Error scenarios from spec are covered, not only the happy path
- [x] Library choices are justified (not just "I know this one")
- [x] Testing strategy covers the spec's acceptance scenarios
- [x] No `[NEEDS CLARIFICATION]` markers remain
