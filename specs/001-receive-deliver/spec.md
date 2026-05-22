# Feature Specification: Receive & Deliver

**Created**: 2026-05-09
**Status**: Draft
**Input**: Foundational MVP — accept events from a producer and deliver them to a registered HTTP endpoint, with exponential-backoff retry on transient failures.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Register a destination endpoint (Priority: P1)

A producer registers an HTTP/HTTPS URL with the system as a destination for future webhook deliveries. The system assigns a unique identifier so subsequent event submissions can reference it. Registered endpoints can be retrieved by their identifier.

**Why this priority**: Without registered endpoints there is nowhere to deliver events. This is the prerequisite for every other story.

**Independent Test**: Can be fully tested by registering an endpoint, retrieving it by its returned identifier, and verifying the persisted URL matches the input. Delivers value as the addressable list of destinations.

**Acceptance Scenarios**:

1. **Given** no endpoints exist, **When** the producer registers a valid HTTPS URL, **Then** the system returns 201 with a unique endpoint identifier and the registered URL.
2. **Given** an endpoint is registered, **When** the producer fetches it by its identifier, **Then** the system returns 200 with the endpoint URL and creation timestamp.
3. **Given** the producer submits a malformed URL or a URL with an unsupported scheme, **When** the registration is processed, **Then** the system returns 400 with a validation error and persists nothing.
4. **Given** an endpoint identifier does not exist, **When** the producer fetches it, **Then** the system returns 404.

---

### User Story 2 - Submit and deliver event (happy path) (Priority: P1)

A producer submits a JSON event payload referencing a registered endpoint. The system acknowledges acceptance synchronously with a delivery identifier, then asynchronously delivers the payload via HTTP POST to the registered URL. When the endpoint responds with a 2xx status code, the delivery is marked as delivered.

**Why this priority**: This is the core value of the service. Together with US1 it forms the smallest demonstrable MVP.

**Independent Test**: Can be fully tested by registering an endpoint that returns 200, submitting an event, and verifying that (a) the API returns 202 with a `delivery_id`, (b) the endpoint receives an HTTP POST with the original payload, (c) the delivery status reflects "delivered".

**Acceptance Scenarios**:

1. **Given** a registered endpoint, **When** the producer submits a valid event, **Then** the API returns 202 with a unique `delivery_id` within 1 second (synchronous acknowledgment).
2. **Given** an event has been accepted with status 202, **When** the asynchronous delivery pipeline processes it and the endpoint responds 2xx within the timeout, **Then** the endpoint receives exactly one HTTP POST whose body equals the submitted payload, and the delivery transitions to `delivered`.
3. **Given** an asynchronous delivery attempt is being executed, **When** the outbound HTTP POST is built, **Then** it carries `Content-Type: application/json` and its body is the producer's payload byte-for-byte.
4. **Given** a producer submits an event referencing an endpoint identifier that does not exist, **When** the request is processed, **Then** the API returns 404 synchronously and no delivery is persisted.
5. **Given** a producer submits an event whose payload exceeds 1 MB, **When** the request is processed, **Then** the API returns 413 (Payload Too Large) synchronously and no delivery is persisted.
6. **Given** a delivery attempt is in flight, **When** the attempt is executed, **Then** the request timeout is 30 seconds.

---

### User Story 3 - Retry on transient failure with exponential backoff (Priority: P2)

When a delivery attempt fails transiently (timeout, connection error, response 5xx, response 429), the system schedules a retry on a fixed exponential-backoff sequence. After all attempts are exhausted without success, the delivery is marked permanently failed.

**Why this priority**: Real endpoints have transient failures. Without retry, the service is just a thin proxy and adds no value over a direct HTTP call. Builds on US2 — same delivery flow, different terminal state on failure.

**Independent Test**: Can be fully tested by registering an endpoint that returns 503 for the first N attempts and then 200, submitting an event, and verifying that the system retries on the published schedule and eventually marks the delivery as `delivered`.

**Acceptance Scenarios**:

1. **Given** an event has been accepted (status 202) for a registered endpoint, **When** the first delivery attempt fails with any retry-eligible outcome (request timeout, connection error, 5xx response, or 429 response), **Then** the attempt is recorded with the matching outcome (`timeout` for timeouts, `transient_failure` otherwise) and the next attempt is scheduled for 1 second after the failed attempt timestamp.
2. **Given** an accepted event whose endpoint fails the first 3 attempts (5xx) and succeeds on the 4th (2xx), **When** the asynchronous pipeline completes all four attempts, **Then** the delivery transitions to `delivered` and the inter-attempt intervals (1→2, 2→3, 3→4) are approximately 1s, 5s, 30s.
3. **Given** an accepted event whose endpoint returns a transient error on every attempt, **When** the asynchronous pipeline executes all 9 attempts without a 2xx response, **Then** the delivery transitions to `permanently_failed` and the system performs no further attempts.
4. **Given** an accepted event whose endpoint returns a 4xx response other than 429 on any attempt, **When** the response is observed, **Then** the delivery transitions to `permanently_failed` immediately and no further attempts are scheduled.
5. **Given** an accepted event whose endpoint returns 429 on an attempt, **When** the response is observed, **Then** the system schedules the next retry per the standard backoff sequence (no honoring of `Retry-After`).
6. **Given** an accepted event whose endpoint does not respond, **When** the 30-second per-attempt timeout fires, **Then** the attempt is recorded with outcome `timeout` and a retry is scheduled per the backoff sequence.

The full retry sequence (9 attempts in total = 1 initial + 8 retries): attempt 1 immediate; subsequent attempts at +1s, +5s, +30s, +5min, +30min, +2h, +8h, +24h relative to the previous failed attempt.

---

### User Story 4 - Inspect delivery attempts (Priority: P3)

A producer queries a delivery by its identifier and sees the overall status, attempt count, next scheduled attempt (if applicable), and an ordered history of attempts with timestamps and response details.

**Why this priority**: Useful for debugging and auditing, but the MVP can be demonstrated without it (a developer can inspect persisted state directly during a demo). Becomes essential once multiple producers consume the system.

**Independent Test**: Submit an event to a flaky endpoint, query the delivery by its identifier, and verify the response includes overall status and a list of attempts with timestamps, status codes, and error reasons.

**Acceptance Scenarios**:

1. **Given** a delivery has at least one recorded attempt, **When** the producer fetches the delivery by identifier, **Then** the response includes overall status, total attempt count, and an ordered list of attempts containing timestamp, response status code (if any), and error reason (if any).
2. **Given** a delivery is in retry state, **When** the delivery is fetched, **Then** the response includes the next scheduled attempt timestamp.
3. **Given** a delivery identifier does not exist, **When** the delivery is fetched, **Then** the system returns 404.

---

### Edge Cases

- **Endpoint domain does not resolve**: counted as a connection failure, retried per the US3 schedule.
- **Endpoint returns 3xx**: the system follows up to one HTTP redirect; the final response status determines the outcome (2xx → delivered, 5xx/429 → retry, 4xx-except-429 → permanently_failed). A response that points to further redirects beyond the first is treated as a permanent failure.
- **Endpoint responds 200 in 25 seconds**: counted as success (response received within the 30-second timeout).
- **Same payload submitted twice**: produces two independent deliveries with distinct identifiers; deduplication is out of scope (deferred to feature 002).
- **Endpoint unreachable for the entire ~35-hour retry window**: after the 9th attempt fails, the delivery is marked `permanently_failed` and the producer must inspect (US4) or resubmit; no automatic recovery beyond the schedule.
- **System crash mid-attempt**: in-flight attempts may be replayed on restart from persisted scheduled state. Delivery semantics are at-least-once for v1; the same request body may reach the endpoint more than once.
- **Endpoint returns 200 to a body the producer did not submit**: not detectable by this system; payload integrity end-to-end is the producer's responsibility (HMAC signing comes in feature 002).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST allow a producer to register a destination endpoint by providing a URL using the http or https scheme.
- **FR-002**: System MUST validate that the registered URL is well-formed and rejects any other scheme or malformed input with a 400 response.
- **FR-003**: System MUST assign a globally unique identifier to each registered endpoint and return it in the registration response.
- **FR-004**: System MUST persist registered endpoints with their URL and a creation timestamp.
- **FR-005**: System MUST allow a producer to retrieve a registered endpoint by its identifier and respond 404 when the identifier does not exist.
- **FR-006**: System MUST allow a producer to submit an event referencing a registered endpoint identifier and a JSON payload.
- **FR-007**: System MUST reject event submissions whose payload exceeds 1 MB with a 413 response and persist nothing.
- **FR-008**: System MUST reject event submissions referencing a non-existent endpoint with a 404 response and persist nothing.
- **FR-009**: System MUST acknowledge accepted event submissions synchronously with a 202 response containing a unique delivery identifier.
- **FR-010**: System MUST attempt delivery asynchronously by sending an HTTP POST to the registered endpoint URL with the original payload as the request body and `Content-Type: application/json`.
- **FR-011**: System MUST apply a 30-second request timeout to each delivery attempt.
- **FR-012**: System MUST mark a delivery as `delivered` when an attempt receives a 2xx response.
- **FR-013**: System MUST treat as transient (retry-eligible) any of: connection error, request timeout, response with status 5xx, response with status 429.
- **FR-014**: System MUST treat as permanent (NOT retry-eligible) any 4xx response other than 429, marking the delivery as `permanently_failed` immediately.
- **FR-015**: System MUST schedule retries on the following sequence relative to the most recent failed attempt: +1s, +5s, +30s, +5min, +30min, +2h, +8h, +24h, for a total of 9 attempts (1 initial + 8 retries).
- **FR-016**: System MUST mark a delivery as `permanently_failed` when all 9 attempts complete without a 2xx response.
- **FR-017**: System MUST persist each delivery attempt with its sequence number, timestamp, response status code (if any), error reason (if any), and outcome.
- **FR-018**: System MUST allow a producer to retrieve a delivery by its identifier and observe overall status, attempt count, next scheduled attempt time (if applicable), and the ordered list of attempts.
- **FR-019**: System MUST follow at most one HTTP redirect during a delivery attempt; the final response determines the outcome.

### Key Entities

- **Endpoint**: a registered destination for webhook deliveries. Attributes: identifier, URL, creation timestamp.
- **Event**: a unit of payload submitted by the producer for delivery. Attributes: identifier, target endpoint identifier, payload, submission timestamp.
- **Delivery**: an instance of an event being delivered to an endpoint. Attributes: identifier, references the event, overall status (`pending`, `in_flight`, `delivered`, `permanently_failed`), attempt count, next scheduled attempt time. For this feature, one event corresponds to exactly one delivery.
- **Attempt**: a single HTTP POST execution within a delivery. Attributes: sequence number, timestamp, response status code (if any), error reason (if any), outcome (`success`, `transient_failure`, `permanent_failure`, `timeout`).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A producer can register an endpoint and submit an event end-to-end (from API call to receiving a `delivery_id`) in under 1 second on a healthy system, p95.
- **SC-002**: For a healthy endpoint that responds 2xx within 1 second, 99% of events are delivered within 5 seconds of submission.
- **SC-003**: For an endpoint that fails transiently and recovers within ~36 hours, 100% of events submitted during the outage are eventually delivered before exhausting the retry schedule.
- **SC-004**: For any submitted event, a producer can determine its current delivery status by fetching `delivery_id`; the response reflects the most recent recorded attempt within 1 second of that attempt completing.
- **SC-005**: After a delivery is marked `permanently_failed`, the system makes zero further outbound POST requests for that delivery (verifiable by traffic capture).
- **SC-006**: Each delivery attempt that receives no response within 30 seconds is recorded with outcome `timeout` and triggers a retry per the backoff schedule.
- **SC-007**: Across 1,000 events submitted to a healthy endpoint at a steady rate of 50/s, the system delivers 100% with zero data loss across a controlled restart of the system mid-run (at-least-once guarantee).

## Assumptions

- Single trusted producer; no multi-tenant isolation, no API authentication or authorization. (Multi-tenancy and access control deferred to feature 003.)
- HMAC signing of outbound deliveries and `Idempotency-Key` handling on inbound submissions are out of scope. Duplicate inbound submissions of the same payload yield independent deliveries. (Deferred to feature 002.)
- Per-tenant or per-resource ordering of deliveries is not guaranteed in this feature. (Deferred to feature 003.)
- Circuit breaking based on endpoint health is not implemented. The system retries every transient failure on the published schedule until exhausted, regardless of how many other deliveries to the same endpoint are currently failing. (Deferred to feature 003.)
- Dead Letter Queue handling and manual replay of `permanently_failed` deliveries are out of scope. After the 9th failed attempt, the delivery's terminal state is final. (Deferred to feature 004.)
- Endpoint owners are not authenticated, notified, or registered as a separate concept; the producer is the only API actor.
- Event payloads are treated as opaque JSON bytes. The system does not enforce a payload schema; that responsibility lies with the producer.
- Delivery semantics are at-least-once. After a system crash mid-attempt, the same payload may be POSTed to the endpoint more than once. End-to-end deduplication on the receiver side is the producer's responsibility (alleviated by `Idempotency-Key` in feature 002).
- Retry timestamps are wall-clock times computed at scheduling. Drift of less than 1 minute between scheduled and executed attempt time is acceptable.
- HTTP redirects: the system follows exactly one redirect per attempt. A second redirect or a redirect to a non-http(s) scheme is treated as a permanent failure of that attempt.
- All outbound deliveries use `Content-Type: application/json`. Non-JSON payloads are not supported in this feature.
- Special headers from the destination such as `Retry-After` (on 429 or 503) are not honored in this feature; the system uses its standard backoff schedule for all retries.
- The system runs in a single logical environment for v1. Geographic distribution and active-active replication are out of scope.
