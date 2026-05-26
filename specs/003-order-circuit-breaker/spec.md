<!--
  Adapted from specs/templates/spec-template.md
  Feature: 003-order-circuit-breaker
-->

# Feature Specification: Order & Circuit Breaker

**Created**: 2026-05-25
**Status**: Draft
**Input**: User description: "Per-tenant ordering and circuit breaker for webhook delivery"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Register a Tenant (Priority: P1)

A producer registers a tenant with the system before submitting any events. The system
assigns a globally unique identifier to the tenant. All subsequent endpoint registrations
and event submissions must reference a valid, existing tenant. The tenant is the unit of
ordering: all events belonging to the same tenant are delivered in the exact order they
were submitted.

**Why this priority**: Without tenant registration, no endpoints can be associated with a
tenant and the ordering guarantee cannot be enforced. This is the prerequisite for every
other story in this feature.

**Independent Test**: Call `POST /v1/tenants`, receive a `tenant_id`. Register an
endpoint using that `tenant_id`. Submit an event with that `tenant_id`. All three
operations must succeed. Attempting to register an endpoint or submit an event with a
non-existent `tenant_id` must be rejected.

**Acceptance Scenarios**:

1. **Given** a producer calls `POST /v1/tenants` without a name, **When** the request is
   processed, **Then** the system returns 201 with a system-generated unique `tenant_id`
   and a `created_at` timestamp.

2. **Given** a producer calls `POST /v1/tenants` with a valid `name` (1–255 Unicode
   characters, none in Unicode general category Cc), **When** the request is processed,
   **Then** the system returns 201 with a unique `tenant_id`, the provided `name`, and
   `created_at`.

3. **Given** a tenant was registered without a `name`, **When** the producer retrieves it
   via `GET /v1/tenants/{id}`, **Then** the system returns 200 with the `tenant_id` and
   `created_at`; no `name` field appears in the response.

4. **Given** a tenant was registered with a valid `name`, **When** the producer retrieves
   it via `GET /v1/tenants/{id}`, **Then** the system returns 200 with the `tenant_id`,
   the exact `name` provided at registration, and `created_at`.

5. **Given** a tenant identifier does not exist, **When** the producer retrieves it,
   **Then** the system returns 404.

6. **Given** a producer calls `POST /v1/tenants` with a `name` that violates any
   constraint (exceeds 255 characters, or contains a Unicode general category Cc
   character), **When** the request is processed, **Then** the system returns 400 Bad
   Request and no tenant is created.

7. **Given** a producer calls `POST /v1/endpoints` with a `tenant_id` that does not
   reference any existing tenant, **When** the request is processed, **Then** the system
   returns 422 Unprocessable Entity and no endpoint is created.

---

### User Story 2 - Ordered delivery per tenant (Priority: P1)

A producer submits multiple events belonging to the same tenant. The system guarantees
that those events are delivered to the consumer in the exact order they were submitted:
no event is attempted before the previous one has reached a terminal state (`delivered`
or `permanently_failed`). Ordering is scoped per tenant — a tenant may have multiple
registered endpoints and the ordering guarantee spans all of them.

**Why this priority**: Without ordering, a downstream consumer can observe
`order.cancelled` before `order.created` for the same resource, corrupting their state
machine. Ordering is the primary motivation for this feature.

**Independent Test**: Register two endpoints under tenant T. Submit events E1, E2, E3
for tenant T in sequence (wait for 202 before submitting each subsequent event). Observe
delivery attempts: the POST for E2 MUST NOT be made before E1 has reached a terminal
state; the POST for E3 MUST NOT be made before E2 has reached terminal state.

**Acceptance Scenarios**:

1. **Given** events E1 and E2 are submitted under the same tenant in sequence (E1's 202
   received before E2 is submitted), **When** the endpoint responds 2xx to E1's delivery
   attempt, **Then** E2's first delivery attempt begins after E1 is marked `delivered`.

2. **Given** events E1 and E2 are submitted under the same tenant in sequence (E1's 202
   received before E2 is submitted), **When** E1's delivery fails transiently and is
   being retried, **Then** E2's first delivery attempt does NOT begin until E1 has
   reached a terminal state.

3. **Given** event E1 is submitted under tenant T1 and event E2 is submitted under tenant
   T2 (T1 ≠ T2), **When** both are in the delivery pipeline simultaneously, **Then** the
   ordering constraint does NOT apply across tenants — E2's first delivery attempt MUST
   NOT be blocked by E1's state; the system initiates E2's attempt independently of E1's
   delivery status.

4. **Given** events E1, E2, E3 are submitted in order under tenant T (each 202 received
   before the next is submitted) and E1 eventually reaches `permanently_failed`, **When**
   E1 reaches that terminal state, **Then** E2's delivery begins; once E2 reaches a
   terminal state, E3's delivery begins — the pipeline advances even when a delivery
   permanently fails.

5. **Given** tenant T has endpoint A (circuit `open`) and endpoint B (circuit `closed`),
   and event E1 targeting endpoint A had its 202 acknowledged before event E2 targeting
   endpoint B was submitted, **When** E1 is blocked by A's open circuit, **Then** E2 is
   also NOT attempted — the per-tenant ordering guarantee holds regardless of which
   endpoint each event targets.

6. **Given** a producer submits `POST /v1/events` without a `tenant_id` field, **When**
   the request is processed, **Then** the system returns 400 Bad Request and no event or
   delivery record is created.

7. **Given** a registered endpoint belonging to tenant T1, **When** a producer submits
   `POST /v1/events` with `tenant_id` = T2 (T2 ≠ T1) referencing that endpoint, **Then**
   the system returns 422 Unprocessable Entity and no event or delivery record is created.

---

### User Story 3 - Circuit breaker suspends delivery to failing endpoints (Priority: P1)

When consecutive delivery attempts to an endpoint all fail transiently, the system
"opens" a circuit breaker for that endpoint and suspends further delivery attempts for a
configurable period. This prevents the system from hammering a down endpoint with
repeated requests and frees delivery capacity for healthy endpoints. After the suspension
period expires, the system selects the oldest queued event as a probe; if the probe
succeeds, the circuit closes and normal delivery resumes; if it fails, the circuit stays
open for another suspension period.

**Why this priority**: Without a circuit breaker, a single down endpoint monopolises
delivery slots and increases latency for every other tenant. This is a fundamental
fairness and resilience requirement.

**Independent Test**: Register an endpoint that always returns 503. Submit 6 events
(exceeding the threshold of 5). Verify that (a) after the 5th failure the circuit
transitions to `open`, (b) no delivery attempt is made for the 6th event while open,
(c) after 60 s, the oldest queued event is used as probe, (d) if the probe fails, the
circuit reopens; if it succeeds, the event is marked `delivered` and normal delivery
resumes.

**Acceptance Scenarios**:

1. **Given** an endpoint has accumulated 5 consecutive transient failures, **When** the
   5th failure is recorded, **Then** the circuit for that endpoint transitions to `open`
   and no further delivery attempts are made for that endpoint until the suspension
   period (60 s by default) elapses.

2. **Given** the circuit for an endpoint is `open` and new events are submitted for that
   endpoint's tenant, **When** those events enter the delivery pipeline, **Then** they
   are NOT attempted; they remain waiting until the circuit closes.

3. **Given** the circuit for an endpoint is `open` and the suspension period has elapsed,
   **When** the suspension expires, **Then** the circuit transitions to `half-open` and
   the oldest queued event for that endpoint is selected and delivered as the probe
   attempt.

4. **Given** the circuit is `half-open` and the probe attempt succeeds (2xx response),
   **When** the probe response is received, **Then** the probe event is marked
   `delivered`, the circuit transitions to `closed`, and the remaining queued events
   begin delivery in submission order.

5. **Given** the circuit is `half-open` and the probe attempt fails transiently,
   **When** the probe response is received, **Then** the circuit transitions back to
   `open` and a new full suspension period begins; when the next suspension expires, the
   same probe event is again selected as the probe attempt.

6. **Given** the circuit is `half-open` and the probe attempt returns a permanent failure
   (4xx other than 429), **When** the response is received, **Then** the probe event is
   marked `permanently_failed`, the circuit transitions to `open` for a new suspension
   period, and when the suspension next expires the next oldest queued event is selected
   as the probe per FR-015.

7. **Given** the circuit for endpoint A is `open` and the queue for that endpoint is
   empty (all prior events reached terminal state during the open period), **When** the
   suspension period expires, **Then** the circuit transitions directly to `closed` and
   the consecutive-failure counter resets to zero — no probe attempt is made.

8. **Given** the circuit is `open` for endpoint A, **When** events for endpoint B
   (belonging to a different tenant) are processed, **Then** endpoint B's deliveries
   proceed normally — the open circuit for A does NOT affect a different tenant's events.

9. **Given** a delivery attempt returns a permanent failure (4xx other than 429),
   **When** the attempt is recorded, **Then** the failure does NOT increment the
   consecutive-failure counter used by the circuit breaker.

10. **Given** the circuit is `closed` following a successful probe, **When** the very
    next delivery attempt for that endpoint fails transiently, **Then** the circuit
    transitions to `open` within 500 ms for a new full suspension period — a single
    transient failure reopens the circuit after a probe recovery.

---

### User Story 4 - Inspect circuit breaker state (Priority: P2)

A producer wants to understand why deliveries to a specific endpoint are not progressing.
They query an API endpoint and see the current circuit breaker state (`open`, `closed`,
or `half-open`), the consecutive failure count, and — when the circuit is open — the
timestamp at which the suspension period expires and the next probe will be attempted.

**Why this priority**: Without observability, a producer cannot distinguish "endpoint is
down and circuit is open" from "events are simply slow to be delivered". This story
enables self-service diagnosis without access to internal metrics or logs.

**Independent Test**: Open the circuit for an endpoint by repeatedly failing. Query
`GET /v1/endpoints/{id}/circuit-breaker`. Verify the response includes state `open`,
the consecutive failure count, and a non-null `suspended_until` timestamp.

**Acceptance Scenarios**:

1. **Given** an endpoint with no delivery failures, **When** the producer queries the
   circuit breaker state, **Then** the response includes state `closed` and a consecutive
   failure count of zero.

2. **Given** the circuit for an endpoint is `open`, **When** the producer queries its
   state, **Then** the response includes state `open`, the consecutive failure count, and
   the `suspended_until` timestamp.

3. **Given** the circuit for an endpoint is `half-open`, **When** the producer queries
   its state, **Then** the response includes state `half-open` and a non-zero consecutive
   failure count; the `suspended_until` field is absent from the response.

4. **Given** an endpoint identifier does not exist, **When** the producer queries its
   circuit breaker state, **Then** the system returns 404.

---

### Edge Cases

- **Ordering and circuit breaker interaction**: if the circuit for an endpoint opens
  while event E1 is still mid-retry-schedule, E1's attempt count and retry schedule are
  preserved. When the circuit closes, if E1's next retry interval has already elapsed,
  E1 is attempted immediately; otherwise it waits for the remaining interval.
- **Cascading tenant stall**: because ordering is per-tenant, an open circuit on endpoint
  A stalls ALL subsequent events for that tenant (including events targeting other
  endpoints of the same tenant). Producers with multiple endpoints under one tenant must
  account for this blast radius.
- **Events queued during an open circuit**: when the circuit closes, queued events are
  delivered in their original submission order.
- **Consecutive failures spanning multiple events**: the counter is per-endpoint across
  all events. If E1 fails twice, E2 fails twice, and E3 fails once in sequence — that is
  5 consecutive failures and the circuit opens.
- **Circuit open with empty queue at probe time**: if all queued events reached terminal
  state during the open period and no new events were submitted, the circuit transitions
  directly to `closed` when the suspension expires — no probe is needed.
- **When the system is running as multiple concurrent instances**: all instances must
  observe the same circuit state; a state transition on one instance must be observable by
  all others within 500 ms.
- **Endpoint that alternates success/failure**: counter resets on each success; the
  circuit never opens if failures are not consecutive.
- **`tenant_id` submitted to `POST /v1/events` but endpoint belongs to a different
  tenant**: the system MUST return 422 Unprocessable Entity.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST allow a producer to create a tenant via `POST /v1/tenants`. The
  system MUST generate a unique, opaque `tenant_id` and return it in a 201 response along
  with `created_at`. An optional `name` field (1–255 Unicode characters, none in Unicode
  general category Cc) MAY be provided by the producer. When `name` is present
  and valid, the system MUST persist it and include it in all subsequent
  `GET /v1/tenants/{id}` responses. When `name` is absent or null, it is treated as not
  provided and MUST NOT appear in the response.

- **FR-002**: System MUST return 400 Bad Request when `POST /v1/tenants` is called with a
  `name` field that is an empty string, exceeds 255 characters, or contains any character
  in Unicode general category Cc, and MUST NOT create a tenant. A null value or absent
  `name` field is valid and treated as "no name provided".

- **FR-003**: System MUST allow retrieving a tenant by its identifier via
  `GET /v1/tenants/{id}`. The system MUST return 200 with the tenant's attributes, and
  MUST return 404 when the identifier does not exist.

- **FR-004**: `POST /v1/events` MUST accept a mandatory `tenant_id` field. If the field
  is absent, the system MUST return 400 Bad Request without creating any event or
  delivery record.

- **FR-005**: System MUST validate that the `tenant_id` supplied in `POST /v1/events`
  references an existing tenant. If no tenant with that identifier exists, the system
  MUST return 422 Unprocessable Entity.

- **FR-006**: System MUST validate that the endpoint referenced in `POST /v1/events`
  belongs to the supplied `tenant_id`. If the endpoint belongs to a different tenant,
  the system MUST return 422 Unprocessable Entity.

- **FR-007**: System MUST allow a producer to register an endpoint with an associated
  `tenant_id`. The referenced tenant MUST exist; if it does not, the system MUST return
  422 Unprocessable Entity. The system MUST NOT allow the `tenant_id` of a registered
  endpoint to be modified after creation.

- **FR-008**: System MUST guarantee that, for any two events E1 and E2 submitted under
  the same `tenant_id`, if the API acknowledged E1's submission with 202 before E2's
  request was received by the API, then E2's first delivery attempt begins only after E1
  has reached a terminal state (`delivered` or `permanently_failed`).

- **FR-009**: The system MUST NOT apply the ordering constraint of FR-008 to events with
  different `tenant_id` values.

- **FR-010**: System MUST track a consecutive transient-failure counter per endpoint,
  incrementing it on each transient failure and resetting it to zero on each successful
  (2xx) delivery.

- **FR-011**: Permanent failures (4xx other than 429) MUST NOT increment the consecutive
  transient-failure counter.

- **FR-012**: When the consecutive transient-failure counter for an endpoint reaches or
  exceeds the configured open threshold (default: 5), the system MUST transition that
  endpoint's circuit to `open` within 500 ms of the counter reaching the threshold, and
  MUST cease delivery attempts for that endpoint until the suspension period elapses.

- **FR-013**: Circuit breaker state MUST be consistent across the entire system — after a
  state transition, any subsequent delivery attempt handled by any system instance and any
  response to `GET /v1/endpoints/{id}/circuit-breaker` from any system instance MUST
  reflect the new state within 500 ms of the transition. Circuit breaker state MUST
  survive a complete restart of any or all system components.

- **FR-014**: When the circuit is `open` and new events arrive for that endpoint's
  tenant, the system MUST hold those events without attempting delivery until the circuit
  transitions to `closed`.

- **FR-015**: When the suspension period of an `open` circuit expires and at least one
  event is queued for that endpoint, the system MUST transition the circuit to `half-open`
  and select the oldest queued event — the event with the earliest API acknowledgement
  timestamp (202 response time) among events not yet in a terminal state — as the probe
  delivery attempt. Exactly one probe attempt MUST be made; the system MUST NOT attempt
  any other events for that endpoint while the circuit is `half-open`.

- **FR-016**: If the probe attempt (in `half-open` state) succeeds (2xx), the system MUST
  mark the probe event as `delivered`, transition the circuit to `closed`, and resume
  delivery of all remaining events for that endpoint's tenant that were waiting, in their
  original submission order per FR-008.

- **FR-017**: If the probe attempt (in `half-open` state) fails transiently, the system
  MUST transition the circuit back to `open` and start a new full suspension period. When
  the suspension expires, the same probe event MUST be selected again as the probe per
  FR-015 (it remains the oldest unresolved event).

- **FR-018**: If the probe attempt (in `half-open` state) returns a permanent failure
  (4xx other than 429), the system MUST mark the probe event as `permanently_failed` and
  MUST transition the circuit to `open` within 500 ms of the permanent failure being
  recorded, for a new full suspension period. The system MUST select the next probe from
  the remaining queue per FR-015 when the new suspension period described above expires.

- **FR-019**: When the circuit transitions from `half-open` to `closed` via a successful
  probe, a single subsequent transient failure for that endpoint MUST transition the
  circuit to `open` within 500 ms of that failure being recorded, for a new full
  suspension period, regardless of the configured open threshold. The system MUST apply
  normal threshold behavior (FR-012) only after the circuit has remained `closed` through
  at least one subsequent successful (2xx) delivery following the probe.

- **FR-020**: When the circuit opens while an event E1 is mid-retry-schedule, E1's next
  delivery attempt MUST occur no later than 500 ms after whichever comes later: the
  circuit closing or E1's originally scheduled retry time. The system MUST NOT reset E1's
  attempt count when the circuit closes.

- **FR-021**: System MUST expose `GET /v1/endpoints/{id}/circuit-breaker` returning the
  current circuit state (`open`, `closed`, or `half-open`), consecutive failure count,
  and — when `open` — the `suspended_until` timestamp. When the circuit is `closed` or
  `half-open`, `suspended_until` MUST be absent from the response. The system MUST return
  404 when the endpoint identifier does not exist.

- **FR-022**: The open threshold and suspension period duration MUST be configurable at
  deployment time without code changes. Defaults: threshold = 5 consecutive transient
  failures; suspension period = 60 seconds (fixed, not exponential).

- **FR-023**: In-flight delivery attempts at the moment the circuit opens MUST be allowed
  to complete; new attempts for that endpoint MUST NOT be initiated after the circuit
  transitions to `open`.

- **FR-024**: When the suspension period of an `open` circuit expires and no events are
  queued for that endpoint, the system MUST transition the circuit directly to `closed`
  and reset the consecutive-failure counter to zero, without sending a probe attempt.

### Key Entities

- **Tenant**: a logical grouping of endpoints owned by the same producer. Attributes:
  `tenant_id` (system-generated opaque identifier, immutable), optional `name` (1–255
  Unicode characters, none in Unicode general category Cc), `created_at` timestamp. A tenant
  has zero or more registered endpoints.

- **Endpoint** (extended from 001/002): gains a mandatory `tenant_id` field referencing
  the Tenant it belongs to. The `tenant_id` is immutable after endpoint creation. Also
  gains circuit-breaker state: current state (`open`, `closed`, `half-open`), consecutive
  transient-failure count, and suspension expiry timestamp (absent from the response when
  the circuit is not `open`).

- **Event** (extended from 001): gains a mandatory `tenant_id` field conveyed by the
  producer in `POST /v1/events`. Used as the ordering key. Must reference an existing
  Tenant.

- **Probe**: the oldest queued event for an endpoint when the circuit transitions to
  `half-open`. The probe is a regular delivery attempt of a real event — not a synthetic
  request. Its outcome determines the next circuit state and, if successful, the event is
  marked `delivered` normally.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A producer can register a tenant and receive a unique `tenant_id` in the
  201 response; 100% of registration attempts to `POST /v1/tenants` with valid input
  return 201.

- **SC-002**: For 100% of event pairs (E1, E2) submitted under the same `tenant_id` in
  sequence (E1's 202 received before E2 is submitted), zero HTTP POSTs are made for E2
  before E1 has reached a terminal state.

- **SC-003**: When an endpoint accumulates 5 consecutive transient failures, the circuit
  transitions to `open` within 500 ms of the 5th failure being recorded, and zero
  additional delivery attempts are made for that endpoint until the suspension period
  elapses.

- **SC-004**: When a probe attempt returns a permanent failure (4xx other than 429), the
  probe event is marked `permanently_failed` — verifiable by querying its delivery status
  — and the circuit transitions to `open` within 500 ms, verifiable via
  `GET /v1/endpoints/{id}/circuit-breaker` returning state `open` with a new
  `suspended_until` timestamp.

- **SC-005**: After the circuit closes (probe succeeds), 100% of events that were queued
  during the open period reach a terminal state (`delivered` or `permanently_failed`)
  within the retry schedule window defined in feature 001 — zero events are silently
  dropped or remain permanently in a waiting state.

- **SC-006**: `GET /v1/endpoints/{id}/circuit-breaker` reflects the current circuit state
  within 500 ms of the last state transition.

- **SC-007**: Circuit breaker state survives a complete restart of all system components —
  a circuit that was `open` before restart is still `open` after restart.

- **SC-008**: With multiple system components running concurrently, a circuit opened by
  any one component prevents new attempts on all other components within 500 ms.

- **SC-009**: An event submitted without a `tenant_id` is rejected 100% of the time with
  400 Bad Request — no event or delivery record is created.

- **SC-010**: When the circuit opens while event E1 has a retry scheduled T seconds in
  the future, and the circuit closes after more than T seconds have elapsed, E1's next
  delivery attempt begins within 500 ms of the circuit closing — not after a fresh retry
  interval restarts from zero.

- **SC-011**: After a circuit closes via a successful probe, 100% of subsequent single
  transient failures for that endpoint cause the circuit to immediately transition to
  `open` — verifiable via `GET /v1/endpoints/{id}/circuit-breaker` reflecting state
  `open` within 500 ms of the single failure, without requiring 5 consecutive failures.

- **SC-012**: 100% of `POST /v1/events` requests referencing a non-existent `tenant_id`
  or an endpoint that belongs to a different tenant return 422 Unprocessable Entity and
  zero event or delivery records are created.

## Assumptions

- `tenant_id` is an opaque identifier generated by the system at tenant creation time. It
  is immutable. The system uses it solely as an ordering key and as a reference for
  endpoint ownership. The specific format is a `plan.md` decision.
- The tenant `name` field is optional. An absent or null `name` in `POST /v1/tenants` is
  treated as "no name provided" and is valid. Only a non-null string that violates the
  1–255 character rule or contains a Unicode general category Cc character triggers a 400
  error.
- There is no API authentication or data isolation between tenants in this feature. Any
  caller who knows a `tenant_id` and an `endpoint_id` can submit events. Access control
  is deferred to a future feature.
- The circuit breaker is per-endpoint (not per-tenant). However, because ordering is
  per-tenant (FR-008), an open circuit on endpoint A within tenant T effectively stalls
  all events for tenant T that were submitted after the first event blocked by A's open
  circuit. This cascading stall is an inherent consequence of combining per-tenant
  ordering with per-endpoint circuit breaking.
- The consecutive-failure counter considers only transient failures. A single successful
  (2xx) delivery resets the counter to zero.
- After a successful probe the circuit enters a sensitive recovery state: a single
  transient failure causes the circuit to reopen within 500 ms. Normal threshold behavior
  resumes only after one subsequent successful delivery that occurs after the probe.
- The suspension period is fixed at 60 seconds (configurable at deployment time) and does
  NOT grow exponentially on successive probe failures.
- When the circuit opens mid-retry-schedule, the event's attempt count and schedule are
  preserved. When the circuit closes, if the scheduled retry time has already elapsed,
  the attempt occurs immediately; otherwise the event waits for the remaining interval.
- A transient failure is any of: connection error, per-attempt request timeout, or a
  response with HTTP status 5xx or 429 — as defined in feature 001. All other outcomes
  are permanent failures.
- The signing and retry rules from features 001 and 002 apply to all delivery attempts,
  including circuit-breaker probe attempts.
- Concurrent event submissions (where E1's request is still being processed when E2's
  request arrives) have undefined relative ordering. Producers must submit events
  sequentially — waiting for a 202 acknowledgement before submitting the next — to rely
  on the ordering guarantee in FR-008.
- Migrating existing events and endpoints (created before this feature) to have a
  `tenant_id` is a `plan.md` concern. This spec assumes all new endpoints and events
  require a valid `tenant_id`.
- Dead Letter Queue handling is deferred to feature 004; an event marked
  `permanently_failed` (including via a probe permanent failure) is a terminal state in
  this feature.
