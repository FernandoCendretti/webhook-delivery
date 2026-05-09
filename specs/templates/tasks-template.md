<!--
  Adapted from GitHub Spec Kit (https://github.com/github/spec-kit)
  License: MIT — Copyright (c) GitHub, Inc.

  Usage: copy this file into specs/<NNN>-<slug>/tasks.md and fill in.
  Do NOT edit this template directly.
-->

# Tasks: [FEATURE NAME]

**Input**: Design documents from `specs/<NNN>-<slug>/`
**Prerequisites**: `plan.md` (required), `spec.md` (required for user stories)

**Tests**: Tasks below assume tests are part of the deliverable. If the spec explicitly defers tests, drop the test tasks.

**Organization**: Tasks are grouped by user story so each story can be implemented and validated independently.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g. US1, US2, US3)
- Each task description must include the concrete file path

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic scaffolding

- [ ] T001 Initialize Go module and base directory layout per `plan.md`
- [ ] T002 [P] Configure linter (golangci-lint) and formatter
- [ ] T003 [P] Add docker-compose.yml with Postgres, Redis, Kafka for local development
- [ ] T004 Add CI workflow (lint + unit tests) in `.github/workflows/ci.yml`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can begin

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T005 Setup database migrations framework and initial schema in `internal/store/migrations/`
- [ ] T006 [P] Implement Kafka producer/consumer wrappers in `internal/queue/`
- [ ] T007 [P] Implement Redis client wrapper in `internal/cache/`
- [ ] T008 Implement HTTP server bootstrap and middleware in `internal/api/server.go`
- [ ] T009 Configure structured logging and metrics in `internal/observability/`
- [ ] T010 Configure environment loading in `internal/config/`

**Checkpoint**: Foundation ready — user story implementation can begin in parallel

---

## Phase 3: User Story 1 - [Title] (Priority: P1) 🎯 MVP

**Goal**: [Brief description of what this story delivers]

**Independent Test**: [How to verify this story works on its own]

### Tests for User Story 1

> Write these tests FIRST and ensure they FAIL before implementation.

- [ ] T011 [P] [US1] Contract test for [endpoint] in `tests/integration/<name>_test.go`
- [ ] T012 [P] [US1] Integration test for [user journey] in `tests/integration/<name>_test.go`

### Implementation for User Story 1

- [ ] T013 [P] [US1] Create [Entity1] type in `internal/domain/<entity1>.go`
- [ ] T014 [P] [US1] Create [Entity2] type in `internal/domain/<entity2>.go`
- [ ] T015 [US1] Implement [Service] in `internal/<area>/<file>.go` (depends on T013, T014)
- [ ] T016 [US1] Implement [endpoint/feature] in `internal/api/<handler>.go`
- [ ] T017 [US1] Add validation and error handling
- [ ] T018 [US1] Add structured logging for user story 1 operations

**Checkpoint**: User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - [Title] (Priority: P2)

**Goal**: [Brief description]

**Independent Test**: [How to verify this story works on its own]

### Tests for User Story 2

- [ ] T019 [P] [US2] Contract test for [endpoint] in `tests/integration/<name>_test.go`
- [ ] T020 [P] [US2] Integration test for [user journey] in `tests/integration/<name>_test.go`

### Implementation for User Story 2

- [ ] T021 [P] [US2] Create [Entity] type in `internal/domain/<entity>.go`
- [ ] T022 [US2] Implement [Service] in `internal/<area>/<file>.go`
- [ ] T023 [US2] Implement [endpoint/feature] in `internal/api/<handler>.go`
- [ ] T024 [US2] Integrate with User Story 1 components (if needed)

**Checkpoint**: User Stories 1 and 2 should both work independently

---

## Phase 5: User Story 3 - [Title] (Priority: P3)

**Goal**: [Brief description]

**Independent Test**: [How to verify this story works on its own]

### Tests for User Story 3

- [ ] T025 [P] [US3] Contract test for [endpoint] in `tests/integration/<name>_test.go`
- [ ] T026 [P] [US3] Integration test for [user journey] in `tests/integration/<name>_test.go`

### Implementation for User Story 3

- [ ] T027 [P] [US3] Create [Entity] type in `internal/domain/<entity>.go`
- [ ] T028 [US3] Implement [Service] in `internal/<area>/<file>.go`
- [ ] T029 [US3] Implement [endpoint/feature] in `internal/api/<handler>.go`

**Checkpoint**: All user stories should now be independently functional

---

[Add more user story phases as needed, following the same pattern]

---

## Phase N: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] TXXX [P] Documentation updates in `docs/`
- [ ] TXXX Code cleanup and refactoring
- [ ] TXXX Performance tuning across all stories
- [ ] TXXX [P] Additional unit tests in `tests/unit/`
- [ ] TXXX Security hardening (authn/authz, secrets, rate limiting)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Stories (Phase 3+)**: All depend on Foundational completion
  - Stories can proceed in parallel if staffed
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Final Phase)**: Depends on all desired user stories being complete

### Within Each User Story

- Tests MUST be written and FAIL before implementation
- Domain types before services
- Services before handlers
- Core implementation before integration
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked `[P]` can run in parallel
- All Foundational tasks marked `[P]` can run in parallel within Phase 2
- Once Foundational is done, user stories can start in parallel
- Tests for a story marked `[P]` can run in parallel
- Domain types within a story marked `[P]` can run in parallel

---

## Implementation Strategy

### MVP first (User Story 1 only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: test User Story 1 independently
5. Demo / deploy if ready

### Incremental delivery

1. Setup + Foundational → foundation ready
2. Add User Story 1 → test independently → demo (MVP!)
3. Add User Story 2 → test independently → demo
4. Add User Story 3 → test independently → demo

Each story adds value without breaking previous stories.

---

## Notes

- `[P]` tasks = different files, no dependencies
- `[Story]` label maps task to a specific user story for traceability
- Each user story should be independently completable and testable
- Verify tests fail before implementing
- Commit after each task or logical group
- Stop at any checkpoint to validate the story independently
- Avoid: vague tasks, same-file conflicts, cross-story dependencies that break independence
