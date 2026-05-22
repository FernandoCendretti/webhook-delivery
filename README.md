# webhook-delivery

A reliable webhook (HTTP callback) delivery service with exponential retry, per-tenant ordering, circuit breaking, and manual replay from a Dead Letter Queue.

> **Status**: MVP complete. All Phase 1–7 tasks delivered; ready for integration testing.

## Problem

Every SaaS platform needs to notify customers when something happens (payment approved, order created, invoice issued). The standard mechanism is an HTTP `POST` to a customer-provided endpoint — a **webhook**. The hard part is not sending the POST; it's doing it reliably when:

- The customer endpoint is down for hours → events must not be lost; they must be persisted and retried
- Multiple events for the same resource must arrive in order (e.g. `order.created` before `order.cancelled`)
- A slow customer must not block delivery to other customers
- Customers want auditability: see attempts, see responses, replay old events on demand

Stripe, Asaas and Mercado Pago handle this well. Smaller companies reimplement it badly. This project is a didactic MVP of that kind of service.

## Stack (fixed at project level)

| Component | Purpose |
| --- | --- |
| **Go** | HTTP API + delivery workers |
| **Apache Kafka** | Durable buffer + per-tenant partitioning to preserve ordering |
| **Redis** | Circuit breaker state, idempotency keys, per-endpoint rate limiting |
| **PostgreSQL** | Auditable storage of endpoints, events, and delivery attempts |

Internal architecture decisions (layering, package layout, ORM vs raw SQL, etc.) are **not** fixed here — they belong in each feature's `plan.md`.

## Methodology: Spec-Driven Development (SDD)

Every feature passes through three artifacts before any code is written:

1. `spec.md` — **WHAT** to build (user stories, functional requirements, acceptance criteria). No stack, no architecture.
2. `plan.md` — **HOW** to build it (technical design, API contracts, data models, testing strategy).
3. `tasks.md` — Granular **breakdown** of the plan into ordered, testable tasks.

Implementation only starts after all three are reviewed. See [`docs/sdd-guide.md`](docs/sdd-guide.md) for the full guide.

## Repository layout

```text
.
├── README.md
├── LICENSE
├── CLAUDE.md                    # Operating rules for AI agents in this repo
├── docs/
│   └── sdd-guide.md             # SDD methodology guide
└── specs/
    ├── README.md                # How to start a new feature spec
    ├── templates/               # Canonical templates
    │   ├── spec-template.md
    │   ├── plan-template.md
    │   └── tasks-template.md
    └── 001-<feature>/           # One folder per feature (created as needed)
        ├── spec.md
        ├── plan.md
        └── tasks.md
```

## Initial roadmap (to be refined in specs)

- **001 — Receive & Deliver**: register endpoint, ingest event, deliver with simple exponential retry
- **002 — Signature & Idempotency**: HMAC signature and `Idempotency-Key` header
- **003 — Order & Circuit Breaker**: per-tenant ordering via Kafka partitioning + Redis-backed circuit breaker
- **004 — DLQ & Replay**: dead letter queue + inspection and manual replay endpoints

Each item becomes a `spec.md` + `plan.md` + `tasks.md` triple before being implemented.

## Credits

Templates in `specs/templates/` are adapted from the official [GitHub Spec Kit](https://github.com/github/spec-kit) (MIT-licensed). The adaptations in this repository are also released under [MIT](LICENSE).

## Running locally

```bash
make infra-up        # start Postgres + Kafka via Docker Compose
make migrate         # run database migrations
make run-api         # HTTP API on :8080, Prometheus metrics on :9090
make run-scheduler   # scheduler + reaper
make run-worker      # delivery workers (default concurrency: 64)
```

### Quick smoke-test

```bash
# 1. Register an endpoint
curl -s -X POST http://localhost:8080/v1/endpoints \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://webhook.site/YOUR_TOKEN"}' | jq .

# 2. Submit an event
curl -s -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"endpoint_id":"<id from step 1>","payload":{"hello":"world"}}' | jq .

# 3. Poll delivery status
curl -s http://localhost:8080/v1/deliveries/<delivery_id> | jq .
```

---

## Operations

### Environment variables

#### Shared (all subcommands)

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | *(required)* | PostgreSQL DSN (`postgres://user:pass@host/db`) |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `KAFKA_DELIVERY_TOPIC` | `webhook.deliveries` | Topic shared by scheduler and workers |
| `METRICS_PORT` | `9090` | Prometheus `/metrics` port |
| `LOG_LEVEL` | `info` | `debug|info|warn|error` |
| `LOG_FORMAT` | `json` | `json|text` |

#### API subcommand

| Variable | Default | Description |
|---|---|---|
| `API_PORT` | `8080` | REST API listen port |
| `DATABASE_POOL_MAX` | `50` | Max Postgres connections. Calibrated for ~1 000 req/s at <10 ms p99. Raise when pool-wait latency appears in metrics. |

#### Worker subcommand

| Variable | Default | Description |
|---|---|---|
| `DATABASE_POOL_MAX` | `20` | Max Postgres connections. Should be ≥ `WORKER_CONCURRENCY / 2`; each transaction is very short-lived. |
| `WORKER_CONCURRENCY` | `64` | Parallel delivery goroutines per replica. Effective throughput ≈ concurrency / HTTP_TIMEOUT deliveries/sec. Scale horizontally before raising past 128. |
| `WORKER_HTTP_TIMEOUT_SECONDS` | `30` | Per-attempt HTTP timeout. Must be < `IN_FLIGHT_LEASE_SECONDS`. |
| `KAFKA_CONSUMER_GROUP` | `webhook-workers` | Consumer group shared by all worker replicas. |

#### Scheduler subcommand

| Variable | Default | Description |
|---|---|---|
| `DATABASE_POOL_MAX` | `5` | Intentionally small; the scheduler uses one connection at a time. |
| `SCHEDULER_TICK_MS` | `500` | Poll interval (ms). Lower values reduce latency; each tick is one SELECT FOR UPDATE query. |
| `IN_FLIGHT_LEASE_SECONDS` | `300` | How long a delivery may remain `in_flight` before the reaper reclaims it. Must exceed `WORKER_HTTP_TIMEOUT_SECONDS`. |
| `REAPER_TICK_SECONDS` | `60` | How often the reaper scans for expired leases. |

### Prometheus metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `webhook_events_submitted_total` | Counter | `endpoint_id` | Events accepted by the API |
| `webhook_events_rejected_total` | Counter | `reason` | Events rejected (bad payload, unknown endpoint, …) |
| `webhook_delivery_attempts_total` | Counter | `endpoint_id`, `outcome` | HTTP delivery outcomes (`success`, `transient_failure`, `permanent_failure`, `timeout`) |
| `webhook_delivery_attempt_duration_seconds` | Histogram | `endpoint_id` | End-to-end HTTP round-trip time per attempt |
| `webhook_scheduler_claimed_total` | Counter | — | Deliveries claimed (moved to `in_flight`) per scheduler tick |
| `webhook_delivery_lease_resurrected_total` | Counter | — | Expired `in_flight` leases reset to `scheduled` by the reaper |
| `webhook_endpoint_failure_streak` | Gauge | `endpoint_id` | Consecutive failures without a success; useful for circuit-breaker alerting |

### Common scenarios

#### Deliveries stuck in `scheduled`

1. Verify the scheduler is running: `kubectl logs -l app=webhookd-scheduler`.
2. Check `webhook_scheduler_claimed_total` is increasing.
3. If the metric is flat, confirm `KAFKA_BROKERS` is reachable from the scheduler pod.
4. If claims succeed but deliveries don't advance, check worker logs for consumer-group lag: `kafka-consumer-groups.sh --describe --group webhook-workers`.

#### Lease tuning (high-latency endpoints)

If customer endpoints are known to be slow (e.g. p99 > 60 s), set:

```
IN_FLIGHT_LEASE_SECONDS=600        # 2× the actual HTTP timeout
WORKER_HTTP_TIMEOUT_SECONDS=60     # still < lease
REAPER_TICK_SECONDS=120
```

#### Running behind PgBouncer

pgx/v5 uses extended query protocol (`pgx` execution mode). Configure PgBouncer in **transaction pooling** mode and add to your DSN:

```
DATABASE_URL=postgres://user:pass@pgbouncer:5432/db?default_query_exec_mode=simple_protocol
```

#### Profiling a live process

Each subcommand accepts `--pprof-addr`. Bind it to localhost to avoid exposure:

```bash
webhookd worker --pprof-addr=127.0.0.1:6061
# then from the same host:
go tool pprof http://127.0.0.1:6061/debug/pprof/profile?seconds=30
```

#### Scaling horizontally

- **API**: stateless — run N replicas behind a load balancer.
- **Worker**: each replica joins the same `KAFKA_CONSUMER_GROUP`; Kafka distributes partitions automatically. Scale until `webhook_delivery_attempt_duration_seconds` saturates or pool waits appear.
- **Scheduler**: run a single active replica. For HA, deploy two with a Postgres advisory lock or a leader-election sidecar; both will try to claim, but `FOR UPDATE SKIP LOCKED` makes double-claiming safe (it just wastes one round-trip).

---

## Running integration tests

Tests spin up Postgres and Kafka via [Testcontainers](https://testcontainers.com/); Docker must be available.

```bash
# All integration tests (may take several minutes)
go test -tags=integration ./tests/integration/...

# Skip the 1 000-event load test
go test -tags=integration -short ./tests/integration/...
```
