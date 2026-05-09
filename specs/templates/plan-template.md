<!--
  Adapted from GitHub Spec Kit (https://github.com/github/spec-kit)
  License: MIT — Copyright (c) GitHub, Inc.

  Usage: copy this file into specs/<NNN>-<slug>/plan.md and fill in.
  Do NOT edit this template directly.
-->

# Implementation Plan: [FEATURE]

**Date**: [DATE]
**Spec**: [link to specs/<NNN>-<slug>/spec.md]

## Summary

[3–5 lines: extract the primary requirement from the spec and the chosen technical approach]

## Technical Context

<!--
  ACTION REQUIRED: Replace this section with concrete technical details.
-->

**Language/Version**: [e.g. Go 1.23]
**Primary Dependencies**: [e.g. chi, sqlx, segmentio/kafka-go, redis/go-redis]
**Storage**: [e.g. PostgreSQL 16, Redis 7]
**Messaging**: [e.g. Apache Kafka 3.7]
**Testing**: [e.g. Go test + testify; integration via testcontainers-go]
**Target Platform**: [e.g. Linux container]
**Project Type**: [e.g. web service, CLI, library]
**Performance Goals**: [e.g. 5k events/sec sustained, p95 enqueue < 50ms]
**Constraints**: [e.g. at-least-once delivery, no event loss on broker failover]
**Scale/Scope**: [e.g. 10k tenants, 100k endpoints, 1M events/day]

## Project Structure

### Documentation (this feature)

```text
specs/<NNN>-<slug>/
├── spec.md              # WHAT (already written)
├── plan.md              # this file — HOW
└── tasks.md             # ORDER (created after plan is approved)
```

### Source Code (repository root)

<!--
  ACTION REQUIRED: Replace the placeholder tree below with the concrete layout for this feature.
-->

```text
cmd/
└── <binary-name>/
    └── main.go

internal/
├── api/                 # HTTP handlers
├── delivery/            # webhook delivery workers and retry logic
├── queue/               # Kafka producer/consumer wrappers
├── store/               # PostgreSQL access
├── cache/               # Redis access (circuit breaker, idempotency)
└── domain/              # core types, no external deps

tests/
├── integration/
└── unit/
```

**Structure Decision**: [Document the selected structure and reference the real directories captured above]

## Technical Design

### Components & responsibilities

[Describe each component, its responsibility, and how it talks to others. Include a small diagram if helpful.]

### Data model

[Tables, columns, indexes, constraints. Include migration strategy.]

### API contracts

[Endpoints, request/response shapes, status codes, headers. Reference OpenAPI if applicable.]

### Critical flows

[Step-by-step description of the main flows, including failure paths.]

### External dependencies

[Brokers, databases, external services this feature depends on.]

## Testing Strategy

- **Unit**: [what is covered]
- **Integration**: [what is covered, how infra is provisioned in CI]
- **Contract** (if applicable): [consumer/provider contracts]
- **End-to-end** (if applicable): [scenarios and runbook]

## Trade-offs

<!--
  ACTION REQUIRED: Document the alternatives considered and why they were rejected.
-->

| Decision | Chosen | Rejected | Reason |
| --- | --- | --- | --- |
| [Area] | [Option A] | [Option B] | [Why A wins for this feature] |

## Open Questions

<!--
  Questions that arose during planning and must be answered before implementation.
-->

- [ ] [Question]

## Review Checklist

- [ ] Every FR from spec has a clear implementation path in this plan
- [ ] Every SC from spec has a way to be measured post-implementation
- [ ] Error scenarios from spec are covered, not only the happy path
- [ ] Library choices are justified (not just "I know this one")
- [ ] Testing strategy covers the spec's acceptance scenarios
- [ ] No `[NEEDS CLARIFICATION]` markers remain
