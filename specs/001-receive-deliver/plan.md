# Implementation Plan: Receive & Deliver

**Date**: 2026-05-09
**Spec**: ./spec.md

## Summary

Build the foundational webhook delivery pipeline: an HTTP API that accepts events for a registered endpoint and asynchronously delivers them via outbound `POST` with exponential-backoff retry on transient failures. PostgreSQL is the source of truth and acts as a transactional outbox; a polling scheduler publishes ready deliveries to Kafka; consumer workers execute the HTTP attempts and update state. The architecture follows a **pure-domain layered** style (domain types are framework-free; ports are introduced only where they earn their boilerplate).

## Technical Context

**Language/Version**: Go 1.23
**Primary Dependencies**:

- `net/http` (stdlib) — HTTP server and outbound client
- `jackc/pgx/v5` — PostgreSQL driver
- `pressly/goose/v3` — schema migrations
- `segmentio/kafka-go` — Kafka producer/consumer (pure Go, no CGo)
- `cenkalti/backoff/v4` — backoff sequence and jitter
- `prometheus/client_golang` — metrics
- `caarlos0/env/v11` — typed config from environment
- `log/slog` (stdlib) — structured logging
- `stretchr/testify` — test assertions
- `testcontainers/testcontainers-go` — integration tests with real Postgres + Kafka

**Storage**: PostgreSQL 16 (transactional outbox + auditable history). Redis 7 is provisioned in `docker-compose` for upcoming features (002+) but is **not used by 001**.

**Messaging**: Apache Kafka 3.7. Single topic `webhook.deliveries` partitioned by `endpoint_id` (forward-compatible with feature 003's ordering requirement). Recommended config: 12 partitions, replication factor 1 in dev, 3 in prod.

**Testing**: stdlib `testing` + testify. Unit tests for pure logic (validation, backoff math, state transitions). Integration tests via testcontainers spin up Postgres + Kafka. E2E test uses `httptest.Server` as the destination endpoint.

**Target Platform**: Linux container; runs as a single binary with subcommands (`api`, `worker`, `scheduler`) so each role can be scaled independently.

**Project Type**: web service (with background workers).

**Performance Goals**:

- API ingest: 5,000 submitted events/second sustained, p95 enqueue latency < 50 ms
- Delivery throughput: limited by destination endpoints, not by the system
- For a healthy endpoint (2xx within 1 s), 99% of events delivered within 5 s of submission (SC-002)

**Constraints**:

- At-least-once delivery semantics (a destination may receive the same payload more than once after a worker crash)
- Single 30-second timeout per delivery attempt (FR-011)
- Maximum 1 MB request payload (FR-007)
- No data loss across controlled restarts of any component (SC-007)

**Scale/Scope**: target v1 footprint of ~1k endpoints, ~100k events/day. Architecture chosen to grow to ~10k endpoints and ~10M events/day without redesign (vertical scale of Postgres + horizontal scale of workers).

## Project Structure

### Documentation (this feature)

```text
specs/001-receive-deliver/
├── spec.md              # WHAT (approved)
├── plan.md              # this file — HOW
└── tasks.md             # ORDER (created after this plan is approved)
```

### Source Code (repository root)

```text
cmd/
└── webhookd/
    └── main.go              # entrypoint with subcommands: api | worker | scheduler

internal/
├── domain/                  # pure types, ZERO external deps
│   ├── endpoint.go          # Endpoint + invariants (URL validation predicate)
│   ├── event.go             # Event
│   ├── delivery.go          # Delivery + state machine (Schedule, MarkInFlight, …)
│   ├── attempt.go           # Attempt + Outcome enum
│   └── retry.go             # backoff schedule (pure function: attempt# → duration)
├── api/                     # HTTP layer
│   ├── server.go            # net/http stdlib server, route registration
│   ├── middleware.go        # request id, recover, logging, metrics
│   ├── handlers_endpoint.go # POST/GET endpoints
│   ├── handlers_event.go    # POST events
│   ├── handlers_delivery.go # GET deliveries
│   └── dto.go               # request/response DTOs (separate from domain)
├── service/                 # orchestration
│   ├── endpoint_service.go
│   ├── event_service.go
│   └── delivery_service.go
├── store/                   # PostgreSQL adapter (pgx)
│   ├── postgres.go          # connection pool, helpers
│   ├── endpoint_store.go
│   ├── delivery_store.go    # includes the polling query
│   ├── attempt_store.go
│   ├── migrations/          # goose migration files (.sql)
│   │   ├── 001_init.sql
│   │   └── …
│   └── rows.go              # row structs ↔ domain conversions
├── queue/                   # Kafka adapter (segmentio/kafka-go)
│   ├── publisher.go
│   └── consumer.go
├── scheduler/               # background poller
│   └── scheduler.go         # tick → claim ready deliveries → publish to Kafka
├── delivery/                # background worker
│   ├── worker.go            # consumes Kafka, runs HTTP POST
│   ├── http_client.go       # configured http.Client + redirect policy
│   └── outcome.go           # response → outcome classification
├── recovery/                # stuck-lease reaper
│   └── reaper.go            # periodically resurrects in_flight deliveries past lease
├── observability/
│   ├── logger.go            # slog setup
│   └── metrics.go           # prometheus collectors
└── config/
    └── config.go            # env → typed struct via caarlos0/env

tests/
├── integration/             # testcontainers-based
│   ├── api_test.go
│   ├── pipeline_test.go     # end-to-end: submit → deliver → verify
│   └── retry_test.go        # flaky destination, verify backoff timings
└── unit/                    # leaf packages also have *_test.go inline
    └── retry_schedule_test.go
```

**Structure Decision**: Pure-Domain Layered. `internal/domain` is the kernel; it has no imports from `pgx`, `kafka-go`, `chi`, etc. Other packages depend on `domain`; `domain` depends on nothing. Where mocking is genuinely useful (e.g., `service` testing without spinning up Postgres), interfaces are introduced **at the consumer side** (`service` defines a small `DeliveryStore` interface that `store/delivery_store.go` satisfies). No upfront port/adapter scaffolding for components we don't actually swap.

## Technical Design

### Components & responsibilities

```
                         ┌──────────────────┐
   Producer ──HTTP POST──►   API Server     │
                         │ (cmd: webhookd api)
                         │  - validates     │
                         │  - persists      │
                         │  - returns 202   │
                         └────────┬─────────┘
                                  │ INSERT delivery (status=scheduled, next_attempt_at=NOW())
                                  ▼
                         ┌──────────────────┐
                         │   PostgreSQL     │ ◄── source of truth (also: outbox)
                         └────┬─────────────┘
                              │
                ┌─────────────┘ poll every 500ms (FOR UPDATE SKIP LOCKED)
                ▼
       ┌──────────────────┐
       │   Scheduler      │ ── publish delivery_id ──►   Kafka topic
       │ (cmd: scheduler) │                              webhook.deliveries
       │  - claims batch  │                              (key: endpoint_id)
       │  - sets in_flight│
       │  - publishes     │                                    │
       └──────────────────┘                                    │
                                                               │
                                              ┌────────────────┘
                                              ▼
                                       ┌──────────────────┐
                                       │   Worker         │
                                       │ (cmd: worker)    │
                                       │  - consumes msg  │
                                       │  - HTTP POST     │
                                       │  - records attempt│
                                       │  - updates state │
                                       └────────┬─────────┘
                                                │
                                  HTTP POST ────┘
                                       ▼
                                  Destination endpoint

       ┌──────────────────┐
       │   Reaper         │  every 1 min:  UPDATE deliveries SET status='scheduled'
       │ (cmd: scheduler) │                WHERE status='in_flight' AND lease_until < NOW()
       └──────────────────┘                (resurrects work after worker crash)
```

The same binary runs in three modes (`webhookd api`, `webhookd worker`, `webhookd scheduler`) so each can be scaled and deployed independently. `Reaper` runs alongside `scheduler` (lightweight, no need for separate process).

### Data model

PostgreSQL schema (initial migration `001_init.sql`):

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE endpoints (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url         TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_endpoints_url_scheme CHECK (url ~* '^https?://')
);

CREATE TABLE events (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_id   UUID NOT NULL REFERENCES endpoints(id),
    payload       JSONB NOT NULL,
    submitted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_events_endpoint ON events(endpoint_id);

CREATE TYPE delivery_status AS ENUM (
    'scheduled', 'in_flight', 'delivered', 'permanently_failed'
);

CREATE TABLE deliveries (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id              UUID NOT NULL REFERENCES events(id),
    endpoint_id           UUID NOT NULL REFERENCES endpoints(id),
    status                delivery_status NOT NULL DEFAULT 'scheduled',
    attempt_count         INT NOT NULL DEFAULT 0,
    next_attempt_at       TIMESTAMPTZ NOT NULL,
    in_flight_lease_until TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Hot path: scheduler polls only "scheduled" rows by next_attempt_at
CREATE INDEX idx_deliveries_scheduled
    ON deliveries (next_attempt_at)
    WHERE status = 'scheduled';

-- Used by the reaper for stuck-lease recovery
CREATE INDEX idx_deliveries_in_flight_lease
    ON deliveries (in_flight_lease_until)
    WHERE status = 'in_flight';

CREATE INDEX idx_deliveries_event ON deliveries (event_id);

CREATE TYPE attempt_outcome AS ENUM (
    'success', 'transient_failure', 'permanent_failure', 'timeout'
);

CREATE TABLE attempts (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id          UUID NOT NULL REFERENCES deliveries(id),
    sequence             INT NOT NULL,
    started_at           TIMESTAMPTZ NOT NULL,
    completed_at         TIMESTAMPTZ,
    response_status_code INT,
    outcome              attempt_outcome NOT NULL,
    error_reason         TEXT,
    UNIQUE (delivery_id, sequence)
);
CREATE INDEX idx_attempts_delivery ON attempts (delivery_id);
```

**Domain types** in `internal/domain/` mirror these tables but carry no `db:` or `json:` tags. The `store/rows.go` package converts `deliveriesRow ↔ domain.Delivery`. The `api/dto.go` package converts `domain.Delivery ↔ DeliveryResponseDTO`.

### API contracts

All routes live under `/v1`. JSON-only. No authentication (per spec assumption).

#### POST /v1/endpoints

```http
POST /v1/endpoints
Content-Type: application/json

{ "url": "https://example.com/webhook" }
```

Responses:

- `201 Created` — `{ "id": "uuid", "url": "...", "created_at": "ISO8601" }`
- `400 Bad Request` — `{ "error": "invalid_url", "detail": "scheme must be http or https" }`

#### GET /v1/endpoints/{id}

- `200 OK` — `{ "id": "...", "url": "...", "created_at": "..." }`
- `404 Not Found` — `{ "error": "endpoint_not_found" }`

#### POST /v1/events

```http
POST /v1/events
Content-Type: application/json

{
  "endpoint_id": "uuid",
  "payload": { "any": "json" }
}
```

Responses:

- `202 Accepted` — `{ "delivery_id": "uuid", "event_id": "uuid" }`
- `404 Not Found` — endpoint does not exist (`{ "error": "endpoint_not_found" }`)
- `413 Payload Too Large` — body exceeds 1 MB
- `400 Bad Request` — malformed JSON or missing required fields

#### GET /v1/deliveries/{id}

```json
200 OK
{
  "id": "uuid",
  "event_id": "uuid",
  "endpoint_id": "uuid",
  "status": "scheduled" | "in_flight" | "delivered" | "permanently_failed",
  "attempt_count": 3,
  "next_attempt_at": "2026-05-09T14:00:00Z",
  "attempts": [
    {
      "sequence": 1,
      "started_at": "...",
      "completed_at": "...",
      "response_status_code": 503,
      "outcome": "transient_failure",
      "error_reason": null
    }
  ]
}
```

- `404 Not Found` — `{ "error": "delivery_not_found" }`

### Critical flows

#### Flow 1 — Submit event (synchronous ACK + persist)

```
1. API receives POST /v1/events
2. Validate Content-Length ≤ 1 MB; if not, 413
3. Validate JSON body; if not, 400
4. BEGIN TX
     SELECT id FROM endpoints WHERE id = :endpoint_id   -- 404 if absent
     INSERT INTO events       RETURNING id
     INSERT INTO deliveries (event_id, endpoint_id, status='scheduled', next_attempt_at=NOW())
                              RETURNING id
   COMMIT
5. Return 202 Accepted { delivery_id, event_id }
```

The 202 is returned immediately after COMMIT. The producer is acknowledged before any Kafka publish — that's the **transactional outbox** guarantee: the delivery row in Postgres IS the durable work item. Even if the API process crashes the next millisecond, the delivery is safe and the scheduler will pick it up.

#### Flow 2 — Scheduler tick (every 500 ms)

```sql
BEGIN;

WITH claimed AS (
    SELECT id
    FROM deliveries
    WHERE status = 'scheduled'
      AND next_attempt_at <= NOW()
    ORDER BY next_attempt_at
    LIMIT 100
    FOR UPDATE SKIP LOCKED
)
UPDATE deliveries d
SET status = 'in_flight',
    in_flight_lease_until = NOW() + interval '5 minutes',
    updated_at = NOW()
FROM claimed
WHERE d.id = claimed.id
RETURNING d.id, d.endpoint_id;

COMMIT;
```

The returned `(id, endpoint_id)` rows are then published to Kafka:

```go
for _, row := range claimed {
    publisher.Publish(ctx, "webhook.deliveries",
        Key:   row.EndpointID.Bytes(),  // partition by endpoint_id (003-ready)
        Value: row.ID.Bytes(),
    )
}
```

If publishing to Kafka fails after COMMIT, the deliveries are stuck in `in_flight` until the lease expires (5 min) — at which point the **reaper** resurrects them. This is the at-least-once trade-off documented in the spec.

#### Flow 3 — Worker consumes and delivers

```
1. Consume message {delivery_id} from Kafka topic webhook.deliveries
2. Begin attempt:
     SELECT d.id, d.attempt_count, d.status, e.url, ev.payload
       FROM deliveries d
       JOIN endpoints e   ON e.id = d.endpoint_id
       JOIN events    ev  ON ev.id = d.event_id
      WHERE d.id = :delivery_id
3. If d.status != 'in_flight', skip (already processed by another worker / lease expired)
4. INSERT INTO attempts (delivery_id, sequence=attempt_count+1, started_at=NOW(), outcome='transient_failure' [placeholder], …)
   (placeholder outcome until response observed)
5. Execute HTTP POST endpoint.url with payload, timeout=30s, max 1 redirect
6. Classify response → outcome (see "Outcome classification" below)
7. BEGIN TX
     UPDATE attempts SET completed_at=NOW(), response_status_code=…, outcome=…, error_reason=…
     CASE outcome:
       'success' →
         UPDATE deliveries SET status='delivered', updated_at=NOW(),
                                in_flight_lease_until=NULL, attempt_count=attempt_count+1
       'permanent_failure' →
         UPDATE deliveries SET status='permanently_failed', updated_at=NOW(),
                                in_flight_lease_until=NULL, attempt_count=attempt_count+1
       'transient_failure' or 'timeout' →
         IF attempt_count+1 >= MAX_ATTEMPTS:
            UPDATE deliveries SET status='permanently_failed', …
         ELSE:
            UPDATE deliveries SET status='scheduled',
                                   next_attempt_at = NOW() + retry.Delay(attempt_count+1),
                                   in_flight_lease_until=NULL,
                                   attempt_count=attempt_count+1
   COMMIT
8. Commit Kafka offset (only after Postgres commit succeeds)
```

#### Flow 4 — Stuck-lease recovery (reaper, every 1 min)

```sql
UPDATE deliveries
SET status = 'scheduled',
    in_flight_lease_until = NULL,
    updated_at = NOW()
WHERE status = 'in_flight'
  AND in_flight_lease_until <= NOW();
```

This is the at-least-once guarantee in action. If a worker crashed mid-delivery, the lease expires after 5 minutes and the row is reset to `scheduled`. On the next scheduler tick, it gets republished to Kafka and retried. The destination may receive the payload twice — that's the trade-off documented in the spec, alleviated by `Idempotency-Key` (feature 002).

### Outcome classification

```go
func classify(resp *http.Response, err error) Outcome {
    switch {
    case err != nil && errors.Is(err, context.DeadlineExceeded):
        return OutcomeTimeout
    case err != nil:
        return OutcomeTransientFailure // connection error, DNS, etc.
    case resp.StatusCode >= 200 && resp.StatusCode < 300:
        return OutcomeSuccess
    case resp.StatusCode == 429:
        return OutcomeTransientFailure
    case resp.StatusCode >= 500:
        return OutcomeTransientFailure
    case resp.StatusCode >= 400:
        return OutcomePermanentFailure
    default: // 1xx, 3xx that survived redirect handling
        return OutcomePermanentFailure
    }
}
```

### Retry schedule

Pure function in `internal/domain/retry.go`:

```go
// schedule[i] = delay BEFORE attempt (i+2). attempt 1 happens immediately;
// after attempt 1 fails, wait schedule[0] before attempt 2; etc.
var schedule = []time.Duration{
    1 * time.Second,
    5 * time.Second,
    30 * time.Second,
    5 * time.Minute,
    30 * time.Minute,
    2 * time.Hour,
    8 * time.Hour,
    24 * time.Hour,
}

// MaxAttempts is initial + len(schedule) retries.
const MaxAttempts = 1 + 8 // = 9 attempts in total

// Delay returns the wait duration before attempt n (1-indexed).
// Delay(1) is undefined (initial attempt, no wait).
func Delay(attemptNumber int) time.Duration {
    if attemptNumber < 2 || attemptNumber-2 >= len(schedule) {
        return 0
    }
    base := schedule[attemptNumber-2]
    // Add ±15% jitter to avoid thundering herd
    jitter := time.Duration(rand.Int63n(int64(base) * 30 / 100)) - base*15/100
    return base + jitter
}
```

### External dependencies

- PostgreSQL 16 — primary store, transactional outbox, audit log. PgBouncer (transaction pooling) recommended in production; see Configuration & Observability.
- Kafka 3.7 — work queue, partitioned by `endpoint_id`
- Redis 7 — provisioned in dev infra but unused in 001 (introduced by 002)

## Configuration & Observability

### Environment variables

All settings load via `caarlos0/env/v11` from process environment. Defaults shown.

| Variable | Default | Purpose |
|---|---|---|
| `API_PORT` | `8080` | HTTP API listen port |
| `METRICS_PORT` | `9090` | Prometheus metrics endpoint listen port |
| `DATABASE_URL` | (required) | Postgres connection string |
| `DATABASE_POOL_MAX` | `50` (api) / `20` (worker) / `5` (scheduler) | pgx pool max connections, role-specific |
| `KAFKA_BROKERS` | `localhost:9092` | comma-separated broker list |
| `KAFKA_DELIVERY_TOPIC` | `webhook.deliveries` | topic name |
| `KAFKA_CONSUMER_GROUP` | `webhook-workers` | worker consumer group id |
| `SCHEDULER_TICK_MS` | `500` | poll interval for the scheduler claim query |
| `IN_FLIGHT_LEASE_SECONDS` | `300` | how long a claimed delivery stays `in_flight` before the reaper resurrects it |
| `REAPER_TICK_SECONDS` | `60` | poll interval for stuck-lease recovery |
| `WORKER_CONCURRENCY` | `64` | max concurrent HTTP attempts per worker process |
| `WORKER_HTTP_TIMEOUT_SECONDS` | `30` | per-attempt HTTP request timeout (FR-011) |
| `LOG_LEVEL` | `info` | slog level (`debug`, `info`, `warn`, `error`) |
| `LOG_FORMAT` | `json` | `json` or `text` |

### Prometheus metrics

Exposed at `/metrics` on `METRICS_PORT`.

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `webhook_events_submitted_total` | counter | `endpoint_id` | API ingest rate |
| `webhook_events_rejected_total` | counter | `reason` | API rejections (`payload_too_large`, `endpoint_not_found`, `bad_request`) |
| `webhook_delivery_attempts_total` | counter | `endpoint_id`, `outcome` | Attempts grouped by `success`/`transient_failure`/`permanent_failure`/`timeout` |
| `webhook_delivery_attempt_duration_seconds` | histogram | `endpoint_id` | HTTP attempt latency distribution |
| `webhook_scheduler_claimed_total` | counter | — | Deliveries claimed per tick (cumulative) |
| `webhook_scheduler_queue_depth` | gauge | — | `count(*) WHERE status='scheduled' AND next_attempt_at <= NOW()` (sampled per tick) |
| `webhook_delivery_lease_resurrected_total` | counter | — | Reaper resurrections (signals worker crashes or lease tuned too low) |
| `webhook_endpoint_failure_streak` | gauge | `endpoint_id` | Consecutive failed attempts for the endpoint (foundation for circuit breaker in 003) |

### Operational notes

- **PgBouncer** (transaction pooling) is **recommended in production** but **not required for the MVP**. With multiple instances per role × `DATABASE_POOL_MAX`, the connection count to Postgres grows fast (e.g. 5 API × 50 = 250). Without PgBouncer, raise Postgres `max_connections` accordingly. With PgBouncer, effective fan-out is amplified 10–50×.
- **Worker concurrency** of 64 means up to 64 in-flight HTTP attempts per worker process, sharing a pgx pool of 20. Brief queueing for a DB connection at attempt-completion time is acceptable; alternative is enlarging the worker pool.
- **Scheduler** is multi-instance safe by design (`FOR UPDATE SKIP LOCKED`). Run 1–2 instances; more adds polling load with no throughput gain.

## Testing Strategy

| Layer | Tooling | Coverage |
|---|---|---|
| Unit | stdlib + testify | Pure functions: `domain/retry.Delay`, `delivery/outcome.classify`, URL validation, payload size guard, state machine transitions in `domain.Delivery` |
| Integration | testcontainers (Postgres + Kafka) | `store/*` against real Postgres; `queue/*` against real Kafka; scheduler claim query under concurrency (multiple goroutines polling the same table) |
| End-to-end | testcontainers + `httptest.Server` as destination | Full pipeline: register endpoint → submit event → assert destination received POST → assert delivery status = `delivered`. Plus: flaky destination returns 503 N times → eventually `delivered` with N+1 attempts recorded |
| Crash/recovery | testcontainers, kill worker mid-attempt | Verify reaper resurrects in_flight row after lease, second worker picks it up, delivery completes (at-least-once behavior, SC-007) |

CI runs unit tests on every push; integration tests on PR (~3 min runtime).

## Trade-offs

| Decision | Chosen | Rejected | Reason |
|---|---|---|---|
| Retry scheduler | Postgres polling (`FOR UPDATE SKIP LOCKED`) | Redis sorted sets | Webhook events are valuable; Postgres durability is non-negotiable. Polling latency (~500 ms) is acceptable since the smallest interval is 1 s. |
| Outbox pattern | Deliveries table doubles as outbox | Separate `outbox` table | The deliveries row is already the work item with status; a separate outbox would duplicate state for no gain. |
| HTTP framework | `net/http` stdlib | `chi`, `gin`, `fiber` | Go 1.22+ ServeMux is sufficient for our 4 routes. Zero deps, no abstraction tax. |
| Architecture | Pure-domain layered | Hexagonal/Clean | At our scope (~6 routes, ~5 entities) hexagonal would be ~30% more boilerplate for marginal benefit. Pure domain preserves migration optionality. |
| At-least-once | Accepted via `in_flight_lease_until` + reaper | Two-phase commit (Postgres ↔ Kafka) | XA transactions across heterogeneous systems are operationally painful. At-least-once + idempotency keys (002) is the standard tradeoff. |
| Kafka partition key | `endpoint_id` | `delivery_id` (round-robin) | Anticipates 003's per-endpoint ordering requirement; for 001 the only effect is grouping by endpoint, which doesn't hurt throughput. |
| Process model | Single binary, three subcommands | Three separate binaries | Simpler ops (one image, one chart). Subcommand isolation is enough; no cross-subcommand state leaks. |

## Resolved Decisions

These were initially open and have been settled before tasks generation.

| Topic | Decision | Rationale |
|---|---|---|
| Retry attempts (FR-015) | 9 attempts total (1 initial + 8 retries: 1s, 5s, 30s, 5min, 30min, 2h, 8h, 24h). Spec updated. | ~34.6 h window covers "next business day" recovery, in line with Stripe / GitHub / Mailchimp |
| Lease duration | 5 min default (`IN_FLIGHT_LEASE_SECONDS=300`) | ~8.5× margin over expected attempt processing; tunable via metrics |
| Scheduler tick | 500 ms default (`SCHEDULER_TICK_MS=500`) | Adds ~250 ms average latency to retry timing — negligible vs the 1 s minimum interval |
| API / metrics ports | 8080 / 9090 | Industry convention |
| Worker concurrency | 64 per worker process (`WORKER_CONCURRENCY=64`) | Saturates a CPU under typical 30 s outbound timeouts; scales horizontally |
| PgBouncer | Recommended in production, optional for MVP | Operational note documented; raising Postgres `max_connections` is sufficient for v1 |

## Review Checklist

- [x] Every FR from spec has a clear implementation path in this plan
- [x] Every SC from spec has a way to be measured post-implementation
- [x] Error scenarios from spec are covered, not only the happy path
- [x] Library choices are justified (not just "I know this one")
- [x] Testing strategy covers the spec's acceptance scenarios
- [x] No `[NEEDS CLARIFICATION]` markers remain
