# webhook-delivery

A reliable webhook (HTTP callback) delivery service with exponential retry, per-tenant ordering, circuit breaking, and manual replay from a Dead Letter Queue.

> **Status**: 🚧 Planning phase. No production code yet. The first `spec.md` is being drafted.

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

Not runnable yet — no code. Track progress through the `specs/` folder.
