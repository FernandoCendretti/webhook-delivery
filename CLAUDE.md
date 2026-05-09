# CLAUDE.md — webhook-delivery

Operating rules for any AI agent (Claude Code, Cursor, etc.) working in this repository.

## Methodology: Spec-Driven Development

This project follows **SDD**. Read [`docs/sdd-guide.md`](docs/sdd-guide.md) before doing any non-trivial work.

## Execution gates

You **MUST NOT** write production code unless, for the feature in question:

1. A reviewed `specs/<feature>/spec.md` exists, describing **WHAT** to build (no implementation details).
2. A reviewed `specs/<feature>/plan.md` exists, describing **HOW** to build it (architecture, libraries, contracts).
3. A reviewed `specs/<feature>/tasks.md` exists, breaking the plan into ordered, testable tasks.

If any of these is missing or ambiguous, **STOP** and produce/refine the missing artifact before implementing. Do not silently infer details.

## When something is unclear, ask

Mark ambiguous requirements in `spec.md` as `[NEEDS CLARIFICATION: <question>]` and surface those questions to the user before generating `plan.md`. Pausing is cheaper than assuming.

## Stack constraints (decided)

- Language: **Go**
- Messaging: **Apache Kafka**
- Cache & coordination: **Redis**
- Storage: **PostgreSQL**

These are fixed at the project level. Do not propose alternatives (e.g., RabbitMQ instead of Kafka) unless the user explicitly asks.

## Using the templates

Files in `specs/templates/` are **canonical**. Copy them into the feature folder (`specs/<NNN>-<slug>/`) and fill them in. Do not edit the templates themselves.

Feature numbering: prefix the folder with `NNN-` (e.g. `001-receive-deliver`, `002-signature-idempotency`) in creation order.

## Reply style

- Conversation language with the user: **Portuguese (PT-BR)**.
- Repository artifacts (specs, plans, tasks, docs, code, comments, commit messages): **English**.
- Be concise. Summarize decisions instead of dumping the full document.
- Use code blocks only when needed; avoid emojis in artifacts.
- After updating a spec/plan/tasks file, show only the conceptual diff or the relevant sections — the user can read the file.

## What NOT to do in this repository

- Do not write production code outside the SDD flow.
- Do not add documentation outside `docs/` or `specs/` without being asked.
- Do not modify the templates in `specs/templates/`.
- Do not add library dependencies that are not declared in the feature's `plan.md`.
