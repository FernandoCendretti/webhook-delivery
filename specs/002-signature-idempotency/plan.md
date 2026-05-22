<!--
  Adapted from specs/templates/plan-template.md
  Feature: 002-signature-idempotency
-->

# Implementation Plan: Signature & Idempotency

**Date**: 2026-05-22
**Spec**: ./spec.md

## Summary

Feature 002 adds two cross-cutting concerns to the existing delivery pipeline. Signing
injects two authentication headers (`X-Webhook-Timestamp`, `X-Webhook-Signature`) into
every outgoing delivery POST; the signature is HMAC-SHA256 over `timestamp + "." + body`
using a per-endpoint secret stored as raw bytes in PostgreSQL and fetched at attempt time.
Idempotency deduplicates producer re-submissions on `POST /v1/events` using a new
`idempotency_records` table and PostgreSQL advisory locks for safe concurrent serialization.
No new external libraries are introduced; all cryptographic primitives come from the Go
standard library. Redis remains provisioned but unused in this feature.

## Technical Context

**Language/Version**: Go 1.25 (same as feature 001)
**Primary Dependencies**: same as feature 001; no additions
**Storage**: PostgreSQL 16 — two new migrations; Redis 7 provisioned but unused
**Messaging**: Apache Kafka 3.7 (unchanged)
**Testing**: stdlib `testing` + testify; integration via testcontainers (Postgres + Kafka)
**Target Platform**: Linux container, same binary (`webhookd api | worker | scheduler`)
**Performance Goals**: signing adds < 1 µs per delivery attempt (HMAC-SHA256 in stdlib);
idempotency check adds one advisory-lock acquisition + one SELECT per duplicated request
**Constraints**: at-least-once Kafka delivery still applies; signing secret fetched at
attempt time (not enqueue time) to satisfy FR-016

## Project Structure

### Documentation (this feature)

```text
specs/002-signature-idempotency/
├── spec.md              # WHAT (approved)
├── plan.md              # this file — HOW
└── tasks.md             # ORDER (created after plan is approved)
```

### Source Code — new and modified files

```text
internal/
├── domain/
│   ├── endpoint.go          MODIFIED — add SigningSecret []byte field
│   └── errors.go            MODIFIED — add ErrConflict sentinel
├── signing/
│   └── signer.go            NEW — pure Sign(secret, timestamp, body) function
├── api/
│   ├── dto.go               MODIFIED — add EndpointCreatedResponse (includes signing_secret);
│   │                                    add RotateSecretResponse; add IdempotencyKey DTOs
│   ├── handlers_endpoint.go MODIFIED — update Create handler (201 uses EndpointCreatedResponse);
│   │                                    add rotateSecretHandler
│   └── handlers_event.go    MODIFIED — add Idempotency-Key header parsing + validation;
│                                        change body reading to io.ReadAll + json.Unmarshal
├── service/
│   ├── endpoint_service.go  MODIFIED — Create generates + stores signing_secret;
│   │                                    add RotateSecret method
│   └── event_service.go     MODIFIED — Submit gains idempotencyKey + rawBody parameters;
│                                        adds idempotency check-and-set flow
├── store/
│   ├── endpoint_store.go    MODIFIED — Insert stores signing_secret; GetByID excludes it;
│   │                                    add UpdateSecret(id, newSecret) for rotation;
│   │                                    LoadForWorker (in delivery_store) gains signing_secret
│   ├── delivery_store.go    MODIFIED — WorkerDelivery gains SigningSecret []byte;
│   │                                    LoadForWorker query JOINs signing_secret
│   ├── idempotency_store.go NEW — Lookup, Claim, Complete operations
│   └── migrations/
│       ├── 002_signing_secret.sql  NEW — adds signing_secret column to endpoints
│       └── 003_idempotency.sql     NEW — creates idempotency_records table
├── delivery/
│   └── worker.go            MODIFIED — doHTTP gains signing header injection;
│                                        process passes signing_secret from WorkerDelivery
└── recovery/
    └── reaper.go            MODIFIED — add periodic purge of expired idempotency_records
```

**Structure Decision**: The signing logic lives in a dedicated `internal/signing/` package
to keep it pure (no store or HTTP deps) and independently testable. The idempotency store
is a separate type in `store/idempotency_store.go` rather than mixed into `event_service.go`
because it has its own query surface and will simplify testing. No new top-level binary or
subcommand is required — all changes are incremental additions to the existing layers.

## Technical Design

### Components & responsibilities

```
POST /v1/endpoints (Create)
  api.endpointHandler.Create
    → endpoint_service.Create(url)
        → crypto/rand.Read(32 bytes)  [secret generation]
        → endpoint_store.Insert(endpoint + secret)
        → returns Endpoint + secret for 201 response

GET /v1/endpoints/{id}
  api.endpointHandler.GetByID
    → endpoint_service.Get(id)
        → endpoint_store.GetByID(id)  [no signing_secret in result]
        → returns Endpoint without secret

POST /v1/endpoints/{id}/rotate-secret
  api.endpointHandler.RotateSecret  [NEW handler]
    → endpoint_service.RotateSecret(id)
        → crypto/rand.Read(32 bytes)
        → endpoint_store.UpdateSecret(id, newSecret)
        → returns new secret for 200 response

POST /v1/events (with optional Idempotency-Key)
  api.eventHandler.Submit
    → parse + validate Idempotency-Key header
    → io.ReadAll(body) — raw bytes for hash
    → event_service.Submit(endpointID, payload, idempotencyKey, rawBody)
        → [if idempotencyKey != ""] idempotency check-and-set (see Flow C)
        → INSERT event + delivery
        → [if idempotencyKey != ""] idempotency_store.Complete(...)
        → returns Delivery

Kafka worker (each delivery attempt)
  delivery.Worker.process(deliveryID)
    → delivery_store.LoadForWorker(deliveryID)  [fetches endpoint.signing_secret]
    → doHTTP(url, payload, signingSecret)
        → ts = time.Now().Unix()
        → sig = signing.Sign(signingSecret, ts, payload)
        → adds headers X-Webhook-Timestamp, X-Webhook-Signature
        → executes HTTP POST

Reaper (extended)
  recovery.Reaper.tick()
    → [existing] resurrect stuck in_flight deliveries
    → [new] DELETE FROM idempotency_records WHERE expires_at <= NOW()
```

### Data model

#### Migration 002 — `002_signing_secret.sql`

Adds the `signing_secret` column to the existing `endpoints` table. The column is `BYTEA`
to store raw random bytes directly — the hex encoding is only used in API responses.

```sql
ALTER TABLE endpoints
    ADD COLUMN signing_secret BYTEA;

UPDATE endpoints
    SET signing_secret = gen_random_bytes(32)
    WHERE signing_secret IS NULL;

ALTER TABLE endpoints
    ALTER COLUMN signing_secret SET NOT NULL;
```

The backfill (`UPDATE`) handles any rows that existed before the migration — in dev this is
typically zero rows, but the migration is safe regardless.

#### Migration 003 — `003_idempotency.sql`

```sql
CREATE TABLE idempotency_records (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_id     UUID        NOT NULL REFERENCES endpoints(id),
    idempotency_key TEXT        NOT NULL,
    payload_hash    TEXT        NOT NULL,   -- hex(SHA-256(raw request body))
    event_id        UUID        REFERENCES events(id),
    delivery_id     UUID        REFERENCES deliveries(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ NOT NULL,   -- created_at + 24h at insert time
    UNIQUE (endpoint_id, idempotency_key)
);

-- The reaper uses this index to delete expired rows efficiently.
CREATE INDEX idx_idempotency_expires
    ON idempotency_records (expires_at);
```

`event_id` and `delivery_id` are NULL until the event is successfully created and the
idempotency record is completed (marked with the resulting IDs). A NULL `event_id` means
the owning request is still in-flight or failed; the advisory lock prevents two requests
from observing NULL simultaneously for the same key.

#### `domain.Endpoint` extension

```go
type Endpoint struct {
    ID            uuid.UUID
    URL           string
    CreatedAt     time.Time
    SigningSecret []byte  // non-nil only when returned by Insert or explicit secret fetch
}
```

`GetByID` leaves `SigningSecret` nil. `LoadForWorker` populates it. This prevents the
secret from leaking into read responses without requiring a separate type.

### API contracts

All routes under `/v1`. JSON-only. No changes to existing 001 contracts except where noted.

#### POST /v1/endpoints — updated 201 response

Request body unchanged: `{ "url": "https://..." }`

```json
201 Created
{
  "id": "uuid",
  "url": "https://example.com/webhook",
  "created_at": "2026-05-22T10:00:00Z",
  "signing_secret": "a3f1...64-char-lowercase-hex-string"
}
```

The `signing_secret` appears **only** in this 201 response. All other endpoint responses
(GET, rotate-secret handler excluded) omit it. The DTOs ensure this at the type level:
`EndpointCreatedResponse` includes `signing_secret`; `EndpointResponse` does not.

Error responses unchanged (400 for invalid URL).

#### GET /v1/endpoints/{id} — unchanged

Response body unchanged: `{ "id", "url", "created_at" }` — no `signing_secret`.

#### POST /v1/endpoints/{id}/rotate-secret — new endpoint

```http
POST /v1/endpoints/{id}/rotate-secret
(no request body)
```

```json
200 OK
{
  "signing_secret": "b7c2...64-char-lowercase-hex-string"
}
```

Error responses:

| Status | Body | Condition |
|--------|------|-----------|
| `404 Not Found` | `{ "error": "endpoint_not_found" }` | `id` does not exist |

#### POST /v1/events — updated to accept `Idempotency-Key`

New optional request header:
```
Idempotency-Key: <string, 1–255 printable ASCII chars, 0x21–0x7E>
```

Request body and success response unchanged: `202 { "delivery_id": "uuid", "event_id": "uuid" }`

New error responses:

| Status | Body | Condition |
|--------|------|-----------|
| `400 Bad Request` | `{ "error": "invalid_idempotency_key", "detail": "..." }` | Header present but empty, exceeds 255 chars, or contains non-printable/non-ASCII characters |
| `409 Conflict` | `{ "error": "idempotency_conflict", "detail": "payload differs from original submission" }` | Same `(endpoint_id, idempotency_key)` exists with a different payload hash within the retention window |

Replayed 202 responses (same key, same payload, within 24 h) return the original
`delivery_id` and `event_id` with status 202 — indistinguishable from a fresh request
from the producer's perspective.

### Critical flows

#### Flow A — Create endpoint with signing secret

```
1. api.endpointHandler.Create receives POST /v1/endpoints
2. Validate URL (existing logic)
3. endpoint_service.Create(url):
     a. crypto/rand.Read(32 bytes) → rawSecret
     b. domain.Endpoint{URL: url, SigningSecret: rawSecret}
     c. endpoint_store.Insert(ctx, &ep)
          INSERT INTO endpoints (url, signing_secret) VALUES ($1, $2)
          RETURNING id, created_at
     d. return ep
4. Respond 201 EndpointCreatedResponse:
     { id, url, created_at, signing_secret: hex.EncodeToString(rawSecret) }
```

#### Flow B — Rotate signing secret

```
1. api.endpointHandler.RotateSecret receives POST /v1/endpoints/{id}/rotate-secret
2. Parse {id} from URL path → uuid.Parse
3. endpoint_service.RotateSecret(ctx, id):
     a. crypto/rand.Read(32 bytes) → newSecret
     b. endpoint_store.UpdateSecret(ctx, id, newSecret)
          UPDATE endpoints SET signing_secret=$1, updated_at=NOW()
          WHERE id=$2
          RETURNING id   -- returns ErrNotFound if 0 rows affected
     c. return newSecret
4. Respond 200: { signing_secret: hex.EncodeToString(newSecret) }
```

For concurrent rotation (US3-SC5): PostgreSQL UPDATE serializes writes to the same row.
Both callers succeed with 200; the active secret is whichever UPDATE committed last.
Both callers receive a valid (but possibly different) `signing_secret` value.

#### Flow C — POST /v1/events with Idempotency-Key

```
1. api.eventHandler.Submit receives POST /v1/events
2. rawBody, err := io.ReadAll(http.MaxBytesReader(r.Body, 1<<20))
   → 413 if body exceeds 1 MiB
3. json.Unmarshal(rawBody, &req)
   → 400 if malformed
4. Parse Idempotency-Key header:
   a. if header absent → idempotencyKey = "", proceed without idempotency
   b. if header present and empty → 400 invalid_idempotency_key
   c. if header present and non-empty → validate: len ≤ 255 AND all bytes in [0x21, 0x7E]
      → 400 if invalid
5. event_service.Submit(ctx, endpointID, payload, idempotencyKey, rawBody)

   Inside Submit (single database transaction):
   BEGIN;

   [if idempotencyKey != ""]
   5a. Acquire advisory lock (blocks concurrent requests with same key):
         SELECT pg_advisory_xact_lock($lockKey)
         lockKey = int64(fnv.New64a().Write(endpointID[:] + ":" + key).Sum64())

   5b. Look up existing idempotency record:
         SELECT payload_hash, event_id, delivery_id, expires_at
         FROM idempotency_records
         WHERE endpoint_id=$1 AND idempotency_key=$2 AND expires_at > NOW()

   5c. If record found AND event_id IS NOT NULL (complete record):
         - Compute payloadHash = hex(sha256(rawBody))
         - If payloadHash == record.payload_hash:
             ROLLBACK
             return (Delivery{EventID: record.event_id, ID: record.delivery_id}, nil)
         - Else:
             ROLLBACK
             return (nil, domain.ErrConflict)

   5d. If no record found (fresh request):
         INSERT INTO idempotency_records
           (endpoint_id, idempotency_key, payload_hash, expires_at)
         VALUES ($1, $2, hex(sha256(rawBody)), NOW() + interval '24 hours')
         -- advisory lock guarantees no concurrent insert reaches here simultaneously

   [always, after idempotency gate]
   5e. SELECT id FROM endpoints WHERE id=$endpointID
       → if not found: ROLLBACK, return ErrNotFound

   5f. INSERT INTO events (endpoint_id, payload) RETURNING id → eventID
   5g. INSERT INTO deliveries (...) RETURNING id, ... → delivery

   [if idempotencyKey != ""]
   5h. UPDATE idempotency_records
       SET event_id=$eventID, delivery_id=$delivery.ID
       WHERE endpoint_id=$1 AND idempotency_key=$2

   COMMIT;
6. Return delivery → handler responds 202 { delivery_id, event_id }
```

Non-2xx paths (404, 413, 400, 409): transaction is rolled back. The idempotency record
inserted in step 5d is rolled back with it, satisfying FR-008 (no record for failed
requests).

The advisory lock (step 5a) ensures that two concurrent requests with the same key
serialize through steps 5b–5h. After the first request commits, the second acquires the
lock, finds a complete record in step 5b, and returns the cached response.

Producer timeout and retry: if the API process commits the transaction (step 5h) but the
TCP connection is dropped before the 202 response reaches the producer, the producer
retries with the same `Idempotency-Key`. The retry hits step 5c and finds a complete
record (event_id IS NOT NULL, same payload hash) — it returns 202 with the original
`event_id` and `delivery_id`. No duplicate event is created. This is the primary
motivation for the feature and confirms correct behavior under the most common failure
mode (network timeout between a successful server-side commit and client receipt).

#### Flow D — Worker signs and delivers

```
1. Worker.process(ctx, deliveryID)
2. wd := delivery_store.LoadForWorker(ctx, deliveryID)
     Query (updated from feature 001):
       SELECT d.*, e.url, e.signing_secret, ev.payload
       FROM deliveries d
       JOIN endpoints e  ON e.id = d.endpoint_id
       JOIN events    ev ON ev.id = d.event_id
       WHERE d.id = $1
   WorkerDelivery gains SigningSecret []byte
3. [existing] Skip if d.status != 'in_flight'
4. [existing] InsertStarted attempt
5. doHTTP(ctx, wd.EndpointURL, wd.Payload, wd.SigningSecret):
     a. ts := time.Now().Unix()
     b. sig := signing.Sign(wd.SigningSecret, ts, wd.Payload)
     c. Build http.Request with headers:
          Content-Type: application/json
          X-Webhook-Timestamp: strconv.FormatInt(ts, 10)
          X-Webhook-Signature: sig
     d. HTTPClient.Do(req)
6. [existing] Classify outcome, persist attempt, update delivery state
```

Because `LoadForWorker` reads `signing_secret` from the database at the start of each
`process()` call, the secret used is the one active at attempt time — satisfying FR-016
even if the secret was rotated after the delivery was enqueued.

The timestamp `ts` is generated inside `doHTTP` immediately before the HTTP call — not
reused from any prior attempt — satisfying FR-015.

#### Flow E — Reaper purge of expired idempotency records

The existing `Reaper.tick()` gains a second SQL statement:

```sql
DELETE FROM idempotency_records
WHERE expires_at <= NOW();
```

This runs every `REAPER_TICK_SECONDS` (default 60 s). Purging is best-effort: the spec
allows it (`MAY purge`) and FR-006 requires that expired records are *treated as absent*
regardless of physical purge. The `expires_at > NOW()` predicate in all lookups enforces
this independently.

### Signing function

Located in `internal/signing/signer.go`. Zero external dependencies.

```go
package signing

import (
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "strconv"
)

// Sign computes the HMAC-SHA256 signature for a single webhook delivery attempt.
// secret is the raw signing secret bytes stored for the endpoint.
// ts is the Unix epoch timestamp (whole seconds) of this specific attempt.
// body is the raw request body bytes.
// Returns the lowercase hexadecimal encoding of the HMAC-SHA256 digest.
func Sign(secret []byte, ts int64, body []byte) string {
    mac := hmac.New(sha256.New, secret)
    mac.Write([]byte(strconv.FormatInt(ts, 10)))
    mac.Write([]byte("."))
    mac.Write(body)
    return hex.EncodeToString(mac.Sum(nil))
}
```

This matches the consumer verification procedure in the spec exactly:
`HMAC-SHA256(key=secret, data=timestamp + "." + body)` → hex-encode → lowercase.

### Advisory lock key computation

Located in `internal/store/idempotency_store.go` (private helper):

```go
import "hash/fnv"

func lockKey(endpointID uuid.UUID, idempotencyKey string) int64 {
    h := fnv.New64a()
    h.Write(endpointID[:])
    h.Write([]byte{':'})
    h.Write([]byte(idempotencyKey))
    return int64(h.Sum64())
}
```

FNV-64a is fast, deterministic, and available in the Go stdlib. Sign-casting to `int64`
is safe (reinterpretation). Collisions produce false serialization (two unrelated requests
block on each other briefly) but never produce incorrect results — correctness is enforced
by the actual `(endpoint_id, idempotency_key)` lookup in step 5b.

## Library & Dependency Decisions

No new external libraries are added to `go.mod`.

| Function | Package | Source |
|----------|---------|--------|
| HMAC-SHA256 signing | `crypto/hmac`, `crypto/sha256` | Go stdlib |
| Hex encoding | `encoding/hex` | Go stdlib |
| Random secret generation | `crypto/rand` | Go stdlib |
| Payload hashing (idempotency) | `crypto/sha256` | Go stdlib |
| Advisory lock key hashing | `hash/fnv` | Go stdlib |
| Timestamp formatting | `strconv` | Go stdlib |
| Raw body reading | `io` | Go stdlib |

All approved infrastructure (PostgreSQL via pgx/v5, Kafka via segmentio/kafka-go) is
retained unchanged.

## Testing Strategy

| Layer | Scenario | How |
|-------|----------|-----|
| Unit | `signing.Sign` — known vectors including empty body, known secret + timestamp | `signing/signer_test.go`; compare against reference HMAC-SHA256 |
| Unit | `signing.Sign` — output is always 64-char lowercase hex | assert `len == 64` and `strings.ToLower(sig) == sig` |
| Unit | Idempotency-Key validation: accept 1 char, accept 255 chars, reject empty, reject 256 chars, reject byte 0x20 (space), reject byte 0x7F (DEL), reject non-ASCII | `api/handlers_event_test.go` |
| Unit | `lockKey` produces stable output for same inputs | `store/idempotency_store_test.go` |
| Integration | `POST /v1/endpoints` → 201 with `signing_secret` present and 64-char lowercase hex | real Postgres (testcontainers) |
| Integration | `GET /v1/endpoints/{id}` → 200 with no `signing_secret` field | same container |
| Integration | `POST .../rotate-secret` → 200 with new secret; `POST .../rotate-secret` on non-existent endpoint → 404 | real Postgres |
| Integration | Worker delivery produces `X-Webhook-Timestamp` and `X-Webhook-Signature` headers; `signing.Sign` with known secret verifies against the received headers | real Postgres + Kafka (testcontainers) + `httptest.Server` capturing headers |
| Integration | After rotation, worker uses new secret: submit event after rotate → outgoing POST signature validates against new secret and fails against old secret | same test suite |
| Integration | Idempotency: submit event (key K, payload P) → 202; resubmit (key K, same P) → 202 with same `event_id` and `delivery_id`; verify 1 event row, 1 delivery row, 1 idempotency record | real Postgres |
| Integration | Idempotency: submit (key K, payload P) → 202; resubmit (key K, different P) → 409 | real Postgres |
| Integration | Idempotency: submit without `Idempotency-Key` twice → two distinct events created | real Postgres |
| Integration | Idempotency: expired record → resubmit with same key creates new event | inject `expires_at = NOW() - 1s` via SQL; then resubmit |
| Integration | Idempotency: concurrent requests — two goroutines submit simultaneously with same key and payload; verify exactly 1 event row; both callers receive 202 with same `event_id` | parallel goroutines in test |
| Integration | Idempotency: non-2xx path (submit to non-existent endpoint_id with idempotency key) → 404; verify no idempotency record created | real Postgres |
| Integration | Reaper purges expired idempotency records | insert record with past `expires_at`; run reaper tick; verify row deleted |
| E2E | Full pipeline: register endpoint, submit event with idempotency key, assert outgoing POST has correct auth headers, verify signature with consumer procedure, resubmit → same response | testcontainers + `httptest.Server` |

Coverage notes: SC-001 and SC-002 (all deliveries carry correct headers and valid
signatures) are covered by the worker integration test that inspects outgoing POST headers.
SC-003 (no duplicate records) is covered by the idempotency integration tests. SC-004
(rotation invalidates old secret) requires capturing headers before and after rotation.
SC-005 (secret never in endpoint read responses) is enforced at the DTO type level and
verified by the GET endpoint integration test. SC-006 (concurrent idempotency) is covered
by the parallel-goroutines integration test. SC-007 (keys scoped per endpoint) is verified
by submitting the same key to two distinct endpoints and asserting two separate event rows.

## Trade-offs

| Decision | Chosen | Rejected | Reason |
|----------|--------|----------|--------|
| Idempotency store | PostgreSQL | Redis | Postgres ACID + advisory lock gives the atomic check-and-set required by FR-010 without distributed coordination; Redis would need Lua scripts + Redlock for equivalent atomicity. Postgres is already the source of truth and handles concurrent serialization cleanly. |
| Concurrent idempotency | `pg_advisory_xact_lock` | `INSERT ON CONFLICT ... SELECT FOR UPDATE` | Advisory lock serializes before the INSERT, so the second request always sees a complete record after the lock is released — no state machine needed. `SELECT FOR UPDATE` on its own doesn't handle the rollback case where the inserting request fails before completing. |
| Secret storage format | `BYTEA` (raw bytes) | `TEXT` (hex string) | Avoids double-encoding; `signing.Sign` receives `[]byte` directly without a decode step. The hex encoding is a presentation concern handled only in DTOs and API responses. |
| Signing placement | `doHTTP` in worker (at attempt time) | At enqueue time in `event_service.Submit` | FR-016: the secret used must be the one active when the signature is computed. Reading from the database in `LoadForWorker` at attempt time naturally satisfies this. Signing at enqueue time would require persisting the signature and would use a stale secret after rotation. |
| Payload hash for idempotency | `SHA-256(rawBody)` → hex string stored in `TEXT` | Full body stored in JSONB | Storage efficiency: 64 bytes per record vs potentially 1 MiB. SHA-256 is collision-resistant for this use case. |
| Idempotency record completeness | Two phases: insert (partial) then update (complete) | Single insert with all fields | The event and delivery IDs are not known until after the Postgres INSERT. The two-phase approach within a single transaction is necessary; the advisory lock ensures no reader observes the partial state. |
| Redis usage | Not used in 002 | Redis for idempotency TTL | Redis TTL is best-effort and subject to clock skew; Postgres `expires_at` with a predicate check (`AND expires_at > NOW()`) gives exact boundary semantics required by FR-006 (exactly-24-hour boundary). |

## Open Questions

None. All open items from the spec were resolved before this plan was written.

## Review Checklist

- [ ] Every FR from spec has a clear implementation path in this plan
- [ ] Every SC from spec has a way to be measured post-implementation
- [ ] Error scenarios from spec are covered, not only the happy path
- [ ] Library choices are justified (not just "I know this one")
- [ ] Testing strategy covers the spec's acceptance scenarios
- [ ] No `[NEEDS CLARIFICATION]` markers remain
