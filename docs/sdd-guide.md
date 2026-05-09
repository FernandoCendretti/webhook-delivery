# Spec-Driven Development (SDD) — Practical guide

This guide describes the SDD methodology applied in this project. Concepts are based on [GitHub Spec Kit](https://github.com/github/spec-kit); the writing here is original.

## Core idea

> Before writing code, write what the code must do. Before planning, validate the *what*. Before implementing, validate the *how*.

SDD splits the work into three artifacts with strictly separate responsibilities:

| Artifact | Answers | Does NOT contain |
| --- | --- | --- |
| `spec.md` | **WHAT** to build | Stack, code, architecture decisions |
| `plan.md` | **HOW** to build it | Granular task breakdown |
| `tasks.md` | In **WHAT ORDER** to build | Justifications (those live in plan) |

The order is strict. Skipping a step (e.g. coding straight from `spec.md`) is where most rework is born.

---

## Workflow

```
idea
  │
  ▼
[1] SPEC      ──► review ──► approved?
                              │ no  → refine SPEC
                              │ yes
                              ▼
[2] PLAN      ──► review ──► approved?
                              │ no  → refine PLAN
                              │ yes
                              ▼
[3] TASKS     ──► review ──► approved?
                              │ no  → refine TASKS
                              │ yes
                              ▼
[4] IMPLEMENTATION (one task at a time, in order)
```

Each review gate is a chance to **STOP** if anything is vague. One hour spent clarifying a requirement saves a day of rework during implementation.

---

## 1. SPEC — What to build

Defines the product without committing to technology.

### Required content

- **Prioritized user stories** (P1, P2, P3...). Each story must be **independently testable** — implementing only it should still yield a viable MVP increment.
- **Acceptance scenarios** in Given/When/Then form, per story.
- **Edge cases** (what happens at boundaries / under failure?).
- **Functional requirements** (`FR-001`, `FR-002`...) numbered, in "System MUST ..." form.
- **Success criteria** (`SC-001`...) measurable and technology-agnostic.
- **Assumptions** (defaults adopted when the requester didn't specify something).

### What does NOT belong in SPEC

- Library or framework names
- Code snippets
- File paths
- Internal architecture decisions ("use repository pattern", "split into microservices")
- Database schema

> Rule of thumb: if you change the stack (e.g. swap Postgres for DynamoDB), the SPEC should **not** change. If it does, you have implementation details leaking in.

### How to mark unknowns

Instead of guessing, write:

```markdown
- **FR-007**: System MUST authenticate using [NEEDS CLARIFICATION: SSO or API key?]
```

This is more valuable than a silent decision: a human reviewer validates.

---

## 2. PLAN — How to build it

This is where you commit to technology and architecture. The project-level stack (Go, Kafka, Redis, Postgres) is fixed in `README.md`; the PLAN decides **everything else**.

### Required content

- **Summary**: 3–5 lines connecting the primary requirement to the chosen technical approach.
- **Technical context**: language version, primary dependencies, storage, testing, target platform, performance goals, scale constraints.
- **Project structure**: concrete folder layout (`/cmd`, `/internal`, `/pkg`, etc.) — replaces the placeholders from the template.
- **Technical design**: component architecture, critical flows, data models, API contracts.
- **Testing strategy**: unit, integration, contract, e2e — which apply, where they live.
- **Explicit trade-offs**: what was discarded and why.

### Critical review before approving

Ask explicitly:

- [ ] Does every FR in SPEC have a clear implementation path in PLAN?
- [ ] Does every SC in SPEC have a way to be measured after implementation?
- [ ] Are error scenarios addressed (not only the happy path)?
- [ ] Do the chosen libraries solve the problem without unnecessary complexity?
- [ ] Does the testing strategy cover the SPEC's acceptance scenarios?

> Errors in PLAN propagate into implementation. This review is the cheapest in the cycle.

---

## 3. TASKS — In what order to build

Decomposition of PLAN into executable, testable items.

### Structure

- **Phase 1: Setup** — initialization (module, lint, basic CI).
- **Phase 2: Foundational** — infra that **all** user stories depend on (base schema, configuration, middleware).
- **Phase 3..N: User Story X** — one phase per user story. Each phase must be deliverable and independently testable.
- **Final phase: Polish** — cross-cutting refactor, performance, hardening.

### Notation

- `[P]` — task can run **in parallel** with other `[P]` tasks of the same phase (different files, no dependency).
- `[US1]`, `[US2]` — which user story this task serves.

### Granularity

Each task must:

- Reference a concrete file path
- Be conclusive (`Implement X in path/y.go`), not vague (`Work on the service`)
- Map to a single commit or a small commit cluster

---

## 4. Implementation

Starts only after `spec.md` + `plan.md` + `tasks.md` are approved.

Rules during implementation:

- Execute **one task at a time** (or one `[P]` group).
- Mark `[ ]` → `[x]` upon completion.
- If you discover the PLAN is wrong, **STOP** and update the PLAN before continuing to code.
- Small commits aligned with tasks.

---

## Operational practices

### Use separate chats per phase

Drafting SPEC, PLAN and implementing are all context-heavy. Recommended:

- **Chat 1**: draft and refine SPEC
- **Chat 2**: attach SPEC, draft PLAN
- **Chat 3**: attach SPEC + PLAN, draft TASKS
- **Chat 4..N**: implement tasks

Each chat starts with a lean, focused context.

### Iterative buildup

The first feature establishes conventions. From the second onward, reference earlier features in the SPEC to keep consistency. The `specs/` folder becomes living documentation of the project.

### When SPEC is not ready

Symptoms:

- You start sentences with "it depends" when explaining a requirement
- There are unanswered `[NEEDS CLARIFICATION]` markers
- Acceptance scenarios are vague ("the system works correctly")
- You cannot write an acceptance test from what is written

In these cases: **do not advance to PLAN**. Go back to the user and ask.

---

## One-page summary

1. **SPEC** = WHAT. Prioritized user stories, numbered FRs, measurable SCs. No stack.
2. **PLAN** = HOW. Technology, architecture, contracts, testing strategy. No tasks.
3. **TASKS** = ORDER. Executable list grouped by user story, with `[P]` and `[USx]` markers.
4. Each gate is a stop. Don't skip.
5. Methodology invariant: changing stack should not change SPEC. Changing requirement should change all three — and that's fine.
