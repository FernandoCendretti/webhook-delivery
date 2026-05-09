# specs/

This folder holds the Spec-Driven Development artifacts for every feature in the project.

## Layout

```text
specs/
├── README.md                 # this file
├── templates/                # canonical templates (DO NOT EDIT)
│   ├── spec-template.md
│   ├── plan-template.md
│   └── tasks-template.md
└── NNN-<slug>/               # one folder per feature, in creation order
    ├── spec.md
    ├── plan.md
    └── tasks.md
```

## Starting a new feature

1. Pick the next number in sequence (`001`, `002`, ...) and a short kebab-case slug.
   Example: `002-signature-idempotency`.
2. Create the folder: `mkdir specs/002-signature-idempotency`.
3. Copy the templates into the new folder:
   ```bash
   cp specs/templates/spec-template.md specs/002-signature-idempotency/spec.md
   ```
4. Draft `spec.md`. Do **not** create `plan.md` or `tasks.md` yet.
5. Review `spec.md` with the user. Mark unresolved questions as `[NEEDS CLARIFICATION: ...]`.
6. Once the SPEC is approved, copy the plan template:
   ```bash
   cp specs/templates/plan-template.md specs/002-signature-idempotency/plan.md
   ```
7. Draft `plan.md`. Review.
8. Once the PLAN is approved, copy the tasks template:
   ```bash
   cp specs/templates/tasks-template.md specs/002-signature-idempotency/tasks.md
   ```
9. Implement tasks in order, marking `[ ]` → `[x]` as you go.

## Rules

- Templates in `specs/templates/` are immutable. Copy them — never edit them in place.
- Each feature folder must contain at least `spec.md`. `plan.md` and `tasks.md` are added as the feature progresses through the workflow.
- See [`../docs/sdd-guide.md`](../docs/sdd-guide.md) for the full methodology.

## Conventions

- File names lowercase, kebab-case.
- Feature folders prefixed with a 3-digit number to preserve ordering.
- Reference IDs (`FR-001`, `SC-001`, `T001`) are local to the feature; numbering restarts in each folder.
- When a feature builds on previous ones, link to them at the top of `spec.md` (e.g. `Depends on: 001-receive-deliver`).
