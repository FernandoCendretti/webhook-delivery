<!--
  Adapted from specs/templates/spec-template.md
  Feature: 002-signature-idempotency
-->

# Feature Specification: Signature & Idempotency

**Created**: 2026-05-22
**Status**: Draft
**Input**: User description: "HMAC webhook signing and idempotent event submission"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Verify Webhook Authenticity (Priority: P1)

A consumer receiving webhooks needs to confirm each request genuinely came from this
system and that the payload has not been tampered with in transit. When registering an
endpoint, the producer receives a `signing_secret`. Every HTTP POST from our system
carries authentication headers that allow the consumer to verify the request's origin
and payload integrity, as defined in the Signing Scheme Contract section. The consumer
validates those headers — and optionally checks the timestamp age — before processing.

**Why this priority**: Without authenticity verification, any actor who knows the
consumer's URL can forge webhooks. Without a timestamp in the signed content, a
legitimately captured request can be replayed later with the same valid signature. Both
are baseline security requirements before production use.

**Independent Test**: Register an endpoint, submit an event, inspect the outgoing POST
— it must carry both `X-Webhook-Timestamp` and `X-Webhook-Signature`. Apply the
consumer verification procedure from the Signing Scheme Contract section of this spec;
the computed value must equal `X-Webhook-Signature` exactly.

**Acceptance Scenarios**:

1. **Given** a producer registers a new endpoint, **When** the API responds with 201,
   **Then** the response body includes a non-empty `signing_secret` field that is
   **not** present in subsequent `GET /v1/endpoints/{id}` responses.

2. **Given** a consumer has the `signing_secret` and receives a webhook POST, **When**
   they follow the consumer verification procedure in the Signing Scheme Contract section
   of this spec, **Then** the computed value equals the `X-Webhook-Signature` header value.

3. **Given** a delivery attempt is retried due to transient failure, **When** the system
   resends the POST, **Then** the timestamp and signature headers are recomputed for that
   attempt (a new timestamp is used, yielding a new but valid signature).

---

### User Story 2 - Safe Event Re-submission (Priority: P1)

A producer whose `POST /v1/events` call times out at the network layer cannot tell
whether the event was registered. Re-submitting without a guard creates a duplicate
delivery. The producer includes a client-chosen `Idempotency-Key` header; if the same
key is submitted again with the same payload within the 24-hour retention window, the
system returns the original response without creating a second event or delivery.

**Why this priority**: At-least-once delivery from the infrastructure side is acceptable,
but duplicate event *creation* from the producer side leads to double-charging,
double-notifications, or corrupted state — high business impact.

**Independent Test**: Submit an event with `Idempotency-Key: test-key-1`, record the
returned `delivery_id`. Submit again with the same key and identical payload. The
response must be identical and exactly one delivery must exist in the system.

**Acceptance Scenarios**:

1. **Given** a producer submits an event with `Idempotency-Key: K` and receives 202,
   **When** they submit again with the same key and identical payload within 24 hours,
   **Then** the system returns 202 with the same `delivery_id` and `event_id` — no new
   event or delivery record is created.

2. **Given** a producer submits an event with `Idempotency-Key: K`, **When** they submit
   again with the same key but a **different payload**, **Then** the system returns
   409 Conflict.

3. **Given** a producer submits an event **without** an `Idempotency-Key` header,
   **When** the system processes the request, **Then** it is handled normally (key is
   optional) and a new event is always created.

4. **Given** an idempotency record exists, **When** more than 24 hours have elapsed since
   the original submission, **Then** a new submission with the same key is treated as a
   fresh request.

5. **Given** an idempotency record exists and exactly 24 hours have elapsed since the
   original successful response was returned, **When** a new submission arrives with the
   same key and payload, **Then** the system treats it as still within the retention
   window and returns the original response.

6. **Given** two concurrent requests are submitted in parallel with the same
   `Idempotency-Key` and identical payload to the same endpoint, **When** both complete,
   **Then** exactly one event record exists in the system and both callers receive 202
   with the same `event_id` and `delivery_id`.

7. **Given** a producer submits an event with `Idempotency-Key: K` and the request fails
   with 400 Bad Request (e.g., malformed payload), **When** the producer corrects the
   payload and resubmits with the same key, **Then** the corrected request is processed
   as a fresh request — no idempotency record is created for failed (non-2xx) responses.

8. **Given** a producer submits an event with an `Idempotency-Key` header whose value
   contains at least one character outside the printable ASCII range (0x21–0x7E),
   **When** the system processes the request, **Then** it returns 400 Bad Request and
   no event or idempotency record is created.

---

### User Story 3 - Rotate Signing Secret (Priority: P2)

A producer suspects their `signing_secret` has been compromised, or wants to rotate it
as a periodic security practice. They call the rotation endpoint once; the system
replaces the existing secret with a single new one and returns it in the response. The
old secret is invalidated from that point on: all delivery attempts signed after the
rotation response is returned use the new secret. Calling the endpoint multiple times
generates a new secret each time, each replacing the previous — only the last secret
issued is ever valid.

**Why this priority**: Secret rotation is operationally necessary for long-lived
integrations but is not required to deliver initial value.

**Independent Test**: Register an endpoint, rotate its secret, submit an event — the
outgoing POST must carry a signature that is valid against the new secret and invalid
against the old one.

**Acceptance Scenarios**:

1. **Given** a producer calls `POST /v1/endpoints/{id}/rotate-secret`, **When** the
   rotation succeeds, **Then** the response includes exactly one new `signing_secret`,
   the previous secret is invalidated, and all delivery attempts for which signing
   occurs after the rotation response is returned use the new secret.

2. **Given** a producer calls `POST /v1/endpoints/{id}/rotate-secret` three times in
   sequence, **When** all three succeed, **Then** only the secret returned by the third
   call is valid; the secrets from the first and second calls are already invalidated.

3. **Given** a rotation is requested for a non-existent endpoint, **When** the API
   processes the request, **Then** it returns 404.

4. **Given** a delivery attempt failed with a transient error before the rotation was
   called, **When** the rotation succeeds and the system subsequently retries that
   delivery, **Then** the POST received by the consumer carries a signature that is
   valid against the new secret, not the pre-rotation secret.

5. **Given** two concurrent `POST /v1/endpoints/{id}/rotate-secret` calls are in flight
   simultaneously for the same endpoint, **When** both complete, **Then** exactly one
   secret is active for that endpoint, both callers receive a 200 response containing a
   `signing_secret` value, and the two callers MAY receive different values — only the
   last secret persisted is the currently active one.

---

### Edge Cases

- What happens when the payload is an empty body? `X-Webhook-Timestamp` and
  `X-Webhook-Signature` must still be computed and sent.
- What happens when `Idempotency-Key` is exactly 255 characters? Accepted normally.
  What happens when it exceeds 255 characters? Returns 400 Bad Request.
- What happens when the same `Idempotency-Key` is used across different `endpoint_id`
  values? Keys are scoped per `(endpoint_id, idempotency_key)` — no collision.
- What is the format of the `signing_secret` value in the response body? The secret is
  returned as a lowercase hexadecimal string (see Signing Scheme Contract section). A
  32-byte secret produces a 64-character string with no whitespace or control characters.
- What happens if the producer never saves the `signing_secret` after registration?
  The only recovery path is to rotate it; there is no retrieval endpoint.
- What happens if a valid webhook is captured by a third party and replayed later?
  The consumer can reject the request by checking that `X-Webhook-Timestamp` is within
  a freshness window of their choosing. The timestamp is part of the signed content,
  so it cannot be altered without invalidating the signature.
- What happens if `rotate-secret` is called concurrently by two clients? At most one
  active secret exists per endpoint at any time. Callers of concurrent rotation requests
  may observe different return values; the active secret is whichever rotation was last
  accepted by the system. Producers must treat concurrent rotation as an operational
  hazard to avoid.
- What happens when two concurrent `POST /v1/events` requests with the same
  `Idempotency-Key` but **different payloads** arrive simultaneously? Exactly one must
  succeed with 202 and the other must receive 409 Conflict — the system must not create
  two events or accept both payloads under the same key.

## Signing Scheme Contract

The following is a **public API contract**: it defines exactly what consumers must
implement to verify webhooks from this system. Changing any part of it after consumers
have integrated is a breaking change and requires a spec revision and consumer migration.

### Authentication headers

Every outgoing delivery POST carries:

- `X-Webhook-Timestamp` — the Unix timestamp of that delivery attempt, expressed as a
  decimal integer (whole seconds since 1970-01-01T00:00:00Z).
- `X-Webhook-Signature` — the authentication signature of the request.

### Signing algorithm

| Element          | Value                                                          |
|------------------|----------------------------------------------------------------|
| Algorithm        | HMAC-SHA256                                                    |
| Signed content   | `X-Webhook-Timestamp` value + literal `"."` + raw body bytes  |
| Output encoding  | lowercase hexadecimal                                          |

### Consumer verification procedure

1. Collect: `signing_secret`, the `X-Webhook-Timestamp` header value, and the raw
   request body bytes.
2. Compute `HMAC-SHA256(key=signing_secret, data=timestamp + "." + raw_body)`.
3. Hex-encode the result in lowercase.
4. Compare to `X-Webhook-Signature` — they must be equal.
5. Optionally reject requests where `X-Webhook-Timestamp` falls outside an
   acceptable freshness window.

### `signing_secret` wire format

The `signing_secret` is returned as a lowercase hexadecimal string. A 32-byte secret
produces a 64-character string.

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: When `POST /v1/endpoints` succeeds, the system MUST generate a
  `signing_secret` of at least 32 bytes of cryptographically random data and return it
  in the 201 response body.
- **FR-002**: Any endpoint that returns an Endpoint resource representation MUST NOT
  include the `signing_secret` in its response.
- **FR-003**: For every delivery attempt, the system MUST include an
  `X-Webhook-Timestamp` header as specified in the Signing Scheme Contract section of
  this spec. The value identifies the moment of that delivery attempt and is part of the
  signed content used to produce `X-Webhook-Signature`.
- **FR-004**: For every delivery attempt, the system MUST include an
  `X-Webhook-Signature` header computed as specified in the Signing Scheme Contract
  section of this spec, such that a consumer holding the `signing_secret` can
  independently verify the authenticity of the request and the integrity of the
  payload by following the consumer verification procedure in that section.
- **FR-005**: `POST /v1/events` MUST accept an optional `Idempotency-Key` header
  (string, 1–255 characters in the ASCII range 0x21–0x7E). If the header is present
  but empty or exceeds 255 characters, the system MUST return 400 Bad Request.
- **FR-006**: If an `Idempotency-Key` is provided and a prior **successful** (2xx)
  response exists for the same `(endpoint_id, idempotency_key)` pair within the 24-hour
  retention window, the system MUST return the original response (same status code and
  body) without creating a new event or delivery. A record whose 24-hour window —
  measured from the time the original successful response was produced — has elapsed
  MUST be treated as absent, regardless of whether it has been physically purged.
- **FR-007**: If an `Idempotency-Key` is provided and a prior request with the same
  `(endpoint_id, idempotency_key)` pair exists with a **different payload**, the system
  MUST return 409 Conflict.
- **FR-008**: Idempotency records MUST only be persisted for requests that produced a
  successful (2xx) response. Non-2xx responses MUST NOT create or update an idempotency
  record.
- **FR-009**: Idempotency records MUST be retained for at least 24 hours from the time
  the original successful response was produced. A record that has existed for exactly
  24 hours is still within the retention window. The system MAY purge records after the
  24-hour window has elapsed.
- **FR-010**: When two or more concurrent requests arrive with the same
  `(endpoint_id, idempotency_key)` pair and identical payload, the system MUST create
  exactly one event and return the same 202 response to all callers.
- **FR-011**: `POST /v1/endpoints/{id}/rotate-secret` MUST replace the existing
  `signing_secret` with a new secret of at least 32 bytes of cryptographically random
  data and return it in the response. The previous secret is invalidated from the moment
  the new secret is persisted by the system; if the rotation response is subsequently
  lost in transit, the caller must re-call the endpoint — the prior secret is already
  invalid.
- **FR-012**: All delivery attempts signed after the rotation response is returned to
  the caller MUST use the new `signing_secret`.
- **FR-013**: Idempotency keys MUST be scoped to the `(endpoint_id, idempotency_key)`
  pair. The same key value submitted to different endpoints is treated as independent
  and does not collide.
- **FR-014**: If no `Idempotency-Key` header is present in `POST /v1/events`, the system
  MUST process the request as a new event unconditionally — no idempotency check is
  performed and a new event is always created.
- **FR-015**: The `X-Webhook-Timestamp` value MUST reflect the moment of the specific
  delivery attempt being made. For retry attempts, a new timestamp value MUST be used;
  the timestamp from an earlier attempt MUST NOT be reused.
- **FR-016**: The `signing_secret` used for a delivery attempt MUST be the one active
  at the moment that attempt's signature is computed — not the secret that was active
  when the event was originally enqueued.

### Key Entities *(include if feature involves data)*

- **Endpoint** (extended): gains a `signing_secret` attribute — a cryptographically
  random value known only to the system and the producer, used to authenticate outgoing
  deliveries. Only one active secret per endpoint at any time. Returned only at creation
  (`POST /v1/endpoints`) and rotation (`POST /v1/endpoints/{id}/rotate-secret`); never
  exposed through read endpoints.
- **IdempotencyRecord**: associates an `(endpoint_id, idempotency_key)` pair with the
  original response status, response body, and sufficient information to detect payload
  changes on re-submission. Created only on successful (2xx) responses. Subject to
  the 24-hour retention window.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of outgoing delivery POSTs carry both an `X-Webhook-Timestamp` header
  and an `X-Webhook-Signature` header.
- **SC-002**: For 100% of outgoing delivery POSTs, applying the consumer verification
  procedure defined in the Signing Scheme Contract section of this spec produces a value
  equal to the `X-Webhook-Signature` header.
- **SC-003**: Re-submitting the same event with an `Idempotency-Key` within the 24-hour
  retention window produces zero additional Event records, zero additional Delivery
  records, and exactly one IdempotencyRecord for that `(endpoint_id, idempotency_key)`
  pair.
- **SC-004**: After a secret rotation, 0% of delivery POSTs signed after the rotation
  response was returned carry signatures that verify against the prior secret; 100%
  verify against the new secret.
- **SC-005**: The `signing_secret` field is never present in any response from any
  endpoint that returns an Endpoint resource representation.
- **SC-006**: When two or more concurrent requests with the same
  `(endpoint_id, idempotency_key)` and identical payload are processed simultaneously,
  exactly one event record is created and all callers receive a 202 response with the
  same `event_id` and `delivery_id`.
- **SC-007**: Submitting the same `Idempotency-Key` value to two different endpoints
  creates two independent events — no collision occurs between keys scoped to different
  endpoints.

## Assumptions

- "Producer" refers to the entity submitting events via `POST /v1/events`. "Consumer"
  refers to the entity receiving webhook HTTP POSTs at a registered endpoint.
- The signing scheme documented in the Signing Scheme Contract section constitutes a
  public API contract. Changing any part of it after consumers have integrated is a
  breaking change and requires a spec revision.
- The `signing_secret` is stored in a form that allows the system to produce outgoing
  signatures at delivery time. The specific storage strategy is a `plan.md` decision.
- `endpoint_id` in `POST /v1/events` is the UUID issued by `POST /v1/endpoints` (see
  feature 001). It is conveyed in the request body; the exact field name is defined in
  `plan.md`.
- Duplicate calls to `POST /v1/endpoints` always create a new endpoint — there is no
  deduplication mechanism for endpoint registration.
- Secret rotation is a hard cut (Option A): one active secret per endpoint at all times.
  The old secret is invalidated from the moment the new secret is persisted; there is no
  dual-secret overlap window.
- The 24-hour retention window for idempotency records is a fixed system default;
  per-endpoint configuration is out of scope.
- Consumer-side freshness validation of `X-Webhook-Timestamp` is the consumer's
  responsibility. This system provides the header as part of the signed content so the
  timestamp cannot be altered without invalidating the signature; it does not enforce a
  freshness window on behalf of the consumer.
- Consumer-side signature verification is the consumer's responsibility; this system
  provides the headers and documents the signing scheme as a public contract in this
  spec (see Signing Scheme Contract section).
- Idempotency applies only to `POST /v1/events`; other write operations (`POST
  /v1/endpoints`, rotation) are not idempotent by design.
- Multiple signing secrets per endpoint (e.g., for zero-downtime rotation with overlap)
  are out of scope.
- Secret rotation does not affect idempotency records. A re-submission with the same
  `Idempotency-Key` within the retention window always returns the original response
  regardless of whether the signing secret was rotated between the original request
  and the re-submission.
- Authorization enforcement is out of scope for this feature. Any caller who knows
  the `endpoint_id` can rotate its secret. A future authentication feature will gate
  these endpoints.
