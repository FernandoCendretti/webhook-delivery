<!--
  Adapted from specs/templates/spec-template.md
  Feature: 004-dlq-replay
-->

# Feature Specification: DLQ & Replay

**Created**: 2026-05-29
**Status**: Approved
**Input**: Dead Letter Queue with inspection and manual replay of failed deliveries

## User Scenarios & Testing *(mandatory)*

### User Story 1 - List permanently-failed deliveries (Priority: P1)

An operator needs to inspect all deliveries that exhausted their retry budget and ended
in `permanently_failed` status. They want to see which events failed, for which
endpoints, how many attempts were made, and when the last attempt occurred — without
having to query the database directly.

**Why this priority**: This is the entry point of the DLQ workflow. Without visibility
into what failed, neither diagnosis nor replay is possible.

**Independent Test**: Submit an event whose endpoint always returns 500. Wait until all
retry attempts are exhausted and the delivery reaches `permanently_failed`. Call the DLQ
listing endpoint and verify the delivery appears in the response with the correct
`endpoint_id`, `event_id`, `attempt_count`, and the timestamp of the last attempt.

**Acceptance Scenarios**:

1. **Given** at least one delivery has status `permanently_failed`, **When** an operator
   calls `GET /v1/dlq`, **Then** the system returns 200 with a paginated list where each
   item includes `delivery_id`, `event_id`, `endpoint_id`, `attempt_count`,
   `failed_at` (timestamp of the last attempt), and `tenant_id`.

2. **Given** no deliveries have status `permanently_failed`, **When** an operator calls
   `GET /v1/dlq`, **Then** the system returns 200 with an empty `items` array and
   pagination metadata reflecting zero results.

3. **Given** more permanently-failed deliveries exist than the page size, **When** an
   operator calls `GET /v1/dlq?page=2`, **Then** the system returns the correct page of
   results and `pagination.has_next` reflects whether a third page exists.

4. **Given** a `tenant_id` filter is provided, **When** an operator calls
   `GET /v1/dlq?tenant_id=<id>`, **Then** the system returns only the failed deliveries
   that belong to that tenant; deliveries from other tenants are excluded.

5. **Given** an `endpoint_id` filter is provided, **When** an operator calls
   `GET /v1/dlq?endpoint_id=<id>`, **Then** the system returns only the failed
   deliveries for that endpoint.

---

### User Story 2 - Inspect a single DLQ entry (Priority: P2)

An operator wants to see the full history of a specific permanently-failed delivery:
every attempt with its HTTP status code, error reason, and timing information, so they
can diagnose the root cause before deciding whether to replay.

**Why this priority**: Listing shows existence; detail shows cause. Without the attempt
history, the operator cannot make an informed replay decision.

**Independent Test**: Let a delivery reach `permanently_failed` after several retries.
Call `GET /v1/dlq/{delivery_id}` and verify the response includes the delivery metadata
and an `attempts` array with one entry per attempt, each containing `sequence`,
`started_at`, `completed_at`, `response_status_code` (or null), `outcome`, and
`error_reason` (or null).

**Acceptance Scenarios**:

1. **Given** a delivery with status `permanently_failed` exists, **When** an operator
   calls `GET /v1/dlq/{delivery_id}`, **Then** the system returns 200 with the delivery
   metadata plus an `attempts` array sorted by `sequence` ascending.

2. **Given** a delivery ID that does not exist, **When** an operator calls
   `GET /v1/dlq/{delivery_id}`, **Then** the system returns 404.

3. **Given** a delivery that exists but has status other than `permanently_failed`,
   **When** an operator calls `GET /v1/dlq/{delivery_id}`, **Then** the system returns
   404 (the delivery is not in the DLQ).

4. **Given** a permanently-failed delivery where the last attempt received an HTTP
   response, **When** the detail is fetched, **Then** `response_status_code` is present
   in the relevant attempt entry.

5. **Given** a permanently-failed delivery where the last attempt timed out, **When**
   the detail is fetched, **Then** the relevant attempt entry has `outcome: "timeout"`,
   `response_status_code: null`, and a non-null `error_reason`.

---

### User Story 3 - Replay a single DLQ entry (Priority: P1)

An operator has diagnosed a failed delivery (e.g., the customer endpoint was
temporarily misconfigured) and wants to enqueue it for re-delivery. The replay must
create a new delivery from scratch — with a fresh retry budget — without duplicating the
original event or endpoint records.

**Why this priority**: The whole purpose of the DLQ is to allow recovery. A DLQ that
can only be read but not acted upon provides no business value.

**Independent Test**: Let a delivery reach `permanently_failed`. Fix the target endpoint
to return 200. Call `POST /v1/dlq/{delivery_id}/replay`. A new `delivery_id` is
returned. Verify the new delivery eventually reaches `delivered` status.

**Acceptance Scenarios**:

1. **Given** a delivery with status `permanently_failed`, **When** an operator calls
   `POST /v1/dlq/{delivery_id}/replay`, **Then** the system returns 202 with a new
   `delivery_id` and a `status` of `scheduled`; the original delivery remains in
   `permanently_failed`.

2. **Given** a delivery ID that does not exist, **When** an operator calls
   `POST /v1/dlq/{delivery_id}/replay`, **Then** the system returns 404.

3. **Given** a delivery that exists but has status other than `permanently_failed`,
   **When** an operator calls `POST /v1/dlq/{delivery_id}/replay`, **Then** the system
   returns 409 Conflict.

4. **Given** the same `permanently_failed` delivery is replayed twice concurrently,
   **When** both requests are processed, **Then** exactly one succeeds with 202 and the
   other receives a 409 Conflict (idempotency guard: one pending replay per original
   delivery at a time).

5. **Given** the endpoint referenced by the delivery has been deleted, **When** an
   operator calls `POST /v1/dlq/{delivery_id}/replay`, **Then** the system returns 422
   Unprocessable Entity and no new delivery is created.

6. **Given** a replay delivery is itself in `permanently_failed`, **When** an operator
   calls `POST /v1/dlq/{new_delivery_id}/replay`, **Then** the system returns 202 and
   creates yet another new delivery (replay chains are allowed).

---

### User Story 4 - Bulk replay by filter (Priority: P3)

An operator wants to replay all permanently-failed deliveries for a given endpoint or
tenant in a single operation, without having to iterate delivery IDs manually.

**Why this priority**: Useful for mass recovery after a prolonged outage, but can be
approximated by scripting individual replays. Lower priority than the single-item
workflow.

**Independent Test**: Create ten permanently-failed deliveries for the same endpoint.
Call `POST /v1/dlq/replay` with `{"endpoint_id": "<id>"}`. Verify that ten new
deliveries with status `scheduled` are created and the response contains a count of
replays initiated.

**Acceptance Scenarios**:

1. **Given** 5 permanently-failed deliveries match the filter criteria, **When** an
   operator calls `POST /v1/dlq/replay` with a valid filter body, **Then** the system
   returns 202 with `{"replayed": 5}` and 5 new deliveries with status `scheduled` are
   created.

2. **Given** a bulk replay request with no filter criteria, **When** the request is
   processed, **Then** the system returns 400 Bad Request (at least one filter field is
   required to avoid accidental full-DLQ replay).

3. **Given** no permanently-failed deliveries match the filter, **When** the bulk replay
   is requested, **Then** the system returns 202 with `{"replayed": 0}`.

4. **Given** some deliveries matching the filter already have a non-terminal replay
   delivery (status `scheduled` or `in_flight`) associated with them, **When** the bulk
   replay runs, **Then** those deliveries are skipped and only the ones without a
   non-terminal replay are replayed; the response count reflects only the new replays
   initiated.

5. **Given** a bulk replay request with a `tenant_id` or `endpoint_id` that does not
   exist, **When** the request is processed, **Then** the system returns 422
   Unprocessable Entity and no deliveries are created.

---

### Edge Cases

- What happens when a replayed delivery also fails permanently? It becomes a new
  `permanently_failed` entry in the DLQ and can be replayed again independently of the
  original.
- What is the ordering guarantee for replayed deliveries? Replayed deliveries follow the
  same per-tenant ordering as original deliveries — they are processed in per-tenant
  arrival order, consistent with the ordering guarantee defined in feature 003.
- What happens if the original event payload has been deleted by the time of replay?
  Event payloads are retained indefinitely; replay is always possible as long as the
  delivery record exists.
- What if the endpoint URL has changed since the original failure? The replay uses the
  endpoint as it currently exists (latest URL and secret), not the snapshot at time of
  failure.
- Can a delivery that is currently `scheduled` or `in_flight` be viewed via the DLQ?
  No — only `permanently_failed` deliveries appear in the DLQ.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST expose a `GET /v1/dlq` endpoint that returns all
  deliveries with status `permanently_failed`, paginated.
- **FR-002**: The `GET /v1/dlq` endpoint MUST support filtering by `tenant_id` and
  `endpoint_id` as optional query parameters.
- **FR-003**: The system MUST expose a `GET /v1/dlq/{delivery_id}` endpoint that
  returns the full delivery detail (metadata + attempt history) for any delivery in
  `permanently_failed` status.
- **FR-004**: `GET /v1/dlq/{delivery_id}` MUST return 404 for deliveries that do not
  exist or are not in `permanently_failed` status.
- **FR-005**: The system MUST expose a `POST /v1/dlq/{delivery_id}/replay` endpoint
  that creates a new delivery with status `scheduled` for the same event and endpoint,
  without modifying the original delivery.
- **FR-006**: `POST /v1/dlq/{delivery_id}/replay` MUST return 409 Conflict if a
  delivery created from the same original delivery is already in a non-terminal state
  (`scheduled` or `in_flight`).
- **FR-007**: `POST /v1/dlq/{delivery_id}/replay` MUST return 422 if the referenced
  endpoint no longer exists.
- **FR-008**: The system MUST expose a `POST /v1/dlq/replay` endpoint for bulk replay
  with at least one filter field (`tenant_id` or `endpoint_id`).
- **FR-009**: `POST /v1/dlq/replay` MUST return 400 if no filter field is provided.
- **FR-010**: A replayed delivery MUST go through the standard retry pipeline with a
  full, fresh retry budget identical to that of an original delivery.
- **FR-011**: A replayed delivery MUST be processed in per-tenant arrival order,
  consistent with the ordering guarantee defined in feature 003.
- **FR-012**: The system MUST record a reference from the new replay delivery back to
  the original delivery (`source_delivery_id`) to enable audit traceability.
- **FR-013**: `GET /v1/dlq` MUST reflect a delivery's `permanently_failed` status
  within 1 second of the status transition.
- **FR-014**: `POST /v1/dlq/replay` MUST return 422 Unprocessable Entity if the
  provided `tenant_id` or `endpoint_id` filter references an entity that does not exist.

### Key Entities

- **DLQ Entry**: A projection of a `Delivery` in `permanently_failed` status, enriched
  with the timestamp of the last `Attempt`, the `tenant_id` from the related `Event`,
  and an `attempt_count`. Not a new stored entity — derived from existing tables.
- **Replay**: The act of creating a new `Delivery` for the same `event_id` and
  `endpoint_id` as a permanently-failed delivery. The new delivery carries a
  `source_delivery_id` reference to the original delivery for audit traceability.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: `GET /v1/dlq` reflects a delivery's `permanently_failed` status within 1
  second of the status transition (backed by FR-013).
- **SC-002**: `POST /v1/dlq/{delivery_id}/replay` responds in under 500 ms with fewer
  than 10 000 permanently-failed records in the system.
- **SC-003**: A replayed delivery that encounters a healthy endpoint reaches `delivered`
  status without additional operator intervention.
- **SC-004**: The DLQ listing endpoint handles at least 1 000 permanently-failed records
  and returns the first page in under 1 second.
- **SC-005**: Concurrent replay requests for the same delivery result in at most one new
  scheduled delivery (no duplicate deliveries created).

## Assumptions

- Operators access the DLQ via the same HTTP API used by producers; there is no
  separate admin interface in scope.
- Authentication and authorization are out of scope for this feature (same assumption
  as features 001–003).
- Event payloads are retained indefinitely in PostgreSQL for the duration of this
  feature; a retention/purge policy is out of scope.
- The replay delivery uses the endpoint record as it currently exists in the database;
  snapshotting endpoint state at time of original failure is out of scope.
- Bulk replay operates synchronously within a single HTTP request; asynchronous
  bulk-replay jobs are out of scope.
- The DLQ does not expose a delete/dismiss operation in this version; clearing
  permanently-failed entries from the DLQ is out of scope.
- Page size for `GET /v1/dlq` defaults to 20 items and can be overridden up to a
  maximum of 100 via a `limit` query parameter.
