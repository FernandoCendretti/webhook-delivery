# API Reference

Base URL: `http://<host>/v1`

---

## Tenants

Every endpoint belongs to a tenant. Tenant identity is used to enforce per-tenant
FIFO delivery ordering (FR-008): a later delivery is only dispatched after all
earlier deliveries for the same tenant have reached a terminal state.

### POST /v1/tenants

Create a new tenant.

**Request body**

```json
{
  "name": "acme-corp"
}
```

- `name`: optional string (1–255 characters). Omit or set to `null` for an unnamed tenant.

**Response — 201 Created**

```json
{
  "id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "name": "acme-corp",
  "created_at": "2024-01-15T10:00:00Z"
}
```

---

### GET /v1/tenants/{id}

Retrieve tenant metadata.

**Response — 200 OK**

```json
{
  "id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "name": "acme-corp",
  "created_at": "2024-01-15T10:00:00Z"
}
```

- `name` is omitted from the response when the tenant has no name.

**Errors**

| Status | Error body | Meaning |
|--------|-----------|---------|
| 404 | `{ "error": "tenant_not_found" }` | No tenant with the given ID |

---

## Endpoints

### POST /v1/endpoints

Register a new webhook endpoint under an existing tenant.

**Request body**

```json
{
  "url": "https://example.com/webhook",
  "tenant_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d"
}
```

- `tenant_id`: required. The tenant that owns this endpoint.

**Response — 201 Created**

```json
{
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "url": "https://example.com/webhook",
  "tenant_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "created_at": "2024-01-15T10:00:00Z",
  "signing_secret": "a3b4c5d6e7f8..."
}
```

- `signing_secret`: 64-character lowercase hex string (32 raw bytes). **Returned once only** — store it securely; it cannot be retrieved again.

**Errors**

| Status | Error body | Meaning |
|--------|-----------|---------|
| 400 | `{ "error": "invalid_url" }` | Missing or non-HTTP(S) `url` |
| 400 | `{ "error": "missing_tenant_id" }` | `tenant_id` field absent |
| 422 | `{ "error": "tenant_not_found" }` | No tenant with the given `tenant_id` |

---

### GET /v1/endpoints/{id}

Retrieve endpoint metadata. The `signing_secret` is **never** included in read responses (SC-005).

**Response — 200 OK**

```json
{
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "url": "https://example.com/webhook",
  "tenant_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "created_at": "2024-01-15T10:00:00Z"
}
```

**Errors**

| Status | Error body | Meaning |
|--------|-----------|---------|
| 404 | `{ "error": "endpoint_not_found" }` | No endpoint with the given ID |

---

### POST /v1/endpoints/{id}/rotate-secret

Replace the signing secret for an endpoint with a newly generated one. All deliveries
signed after this call returns will use the new secret; the old secret is immediately
invalidated (SC-004).

**Request body**: empty (no body required).

**Response — 200 OK**

```json
{
  "signing_secret": "f1e2d3c4b5a6..."
}
```

- `signing_secret`: the new 64-character lowercase hex signing secret.

**Errors**

| Status | Error body | Meaning |
|--------|-----------|---------|
| 404 | `{ "error": "endpoint_not_found" }` | No endpoint with the given ID |

---

## Events

### POST /v1/events

Submit an event for delivery to a registered endpoint. Optionally idempotent via the
`Idempotency-Key` header.

**Headers**

| Header | Required | Description |
|--------|----------|-------------|
| `Content-Type` | Yes | Must be `application/json` |
| `Idempotency-Key` | No | Deduplication key; 1–255 printable ASCII chars (0x21–0x7E) |

**Request body**

```json
{
  "endpoint_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "tenant_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "payload": { "event": "order.created", "order_id": 42 }
}
```

- `tenant_id`: required. Must match the tenant that owns the endpoint (FR-007).

**Response — 202 Accepted**

```json
{
  "event_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "delivery_id": "1b9d6bcd-bbfd-4b2d-9b5d-ab8dfbbd4bed"
}
```

**Idempotency semantics**

When `Idempotency-Key` is present:

- If a prior request with the same `(endpoint_id, key)` completed within the 24-hour
  window **and** the payload hash matches: returns the original `event_id` and
  `delivery_id` with status 202 (no new records created).
- If the window has elapsed (strict `<` comparison — a record at exactly `expires_at = NOW()`
  is still considered within the window per FR-009): treated as a fresh submission.
- If the same key was used with a **different** payload: returns 409 Conflict.

**Errors**

| Status | Error body | Meaning |
|--------|-----------|---------|
| 400 | `{ "error": "invalid_idempotency_key" }` | Key present but empty, >255 chars, or contains bytes outside 0x21–0x7E |
| 400 | `{ "error": "missing_tenant_id" }` | `tenant_id` field absent |
| 404 | `{ "error": "endpoint_not_found" }` | No endpoint with the given `endpoint_id` |
| 409 | `{ "error": "idempotency_conflict" }` | Same key used with a different payload within the 24-hour window |
| 422 | `{ "error": "tenant_not_found" }` | No tenant with the given `tenant_id` |
| 422 | `{ "error": "tenant_endpoint_mismatch" }` | `tenant_id` does not match the endpoint's owner |

---

## Deliveries

### GET /v1/deliveries/{id}

Retrieve delivery status and attempt history.

**Response — 200 OK**

```json
{
  "id": "1b9d6bcd-bbfd-4b2d-9b5d-ab8dfbbd4bed",
  "endpoint_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "event_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "status": "delivered",
  "attempt_count": 1,
  "created_at": "2024-01-15T10:00:01Z",
  "updated_at": "2024-01-15T10:00:03Z",
  "attempts": [
    {
      "sequence": 1,
      "started_at": "2024-01-15T10:00:02Z",
      "completed_at": "2024-01-15T10:00:03Z",
      "outcome": "success",
      "response_status_code": 200
    }
  ]
}
```

Possible `status` values: `scheduled`, `in_flight`, `delivered`, `permanently_failed`.

Possible `outcome` values: `success`, `failure`.

**Errors**

| Status | Error body | Meaning |
|--------|-----------|---------|
| 404 | `{ "error": "delivery_not_found" }` | No delivery with the given ID |

---

## Circuit Breaker

Each endpoint has an independent circuit breaker that stops deliveries when the
endpoint is consistently failing. The circuit transitions between three states:

| State | Meaning |
|-------|---------|
| `closed` | Normal operation — deliveries are dispatched. |
| `open` | Endpoint suspended — no deliveries dispatched until `suspended_until`. |
| `half-open` | Probe mode — a single probe delivery is dispatched to test recovery. |

**Thresholds (default configuration)**

- Opens after **5 consecutive transient failures** on the same endpoint.
- Suspension window: **60 seconds**.
- After suspension, one probe is sent; success → `closed`; failure → `open` again.
- A first transient failure after a successful probe immediately reopens the circuit
  (sensitive recovery, FR-019).

### GET /v1/endpoints/{id}/circuit-breaker

Retrieve the current circuit breaker state for an endpoint.

**Response — 200 OK (closed)**

```json
{
  "endpoint_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "state": "closed",
  "consecutive_failures": 0
}
```

**Response — 200 OK (open)**

```json
{
  "endpoint_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "state": "open",
  "consecutive_failures": 5,
  "suspended_until": "2024-01-15T10:01:00Z"
}
```

- `suspended_until` is only present when `state` is `"open"`.
- The internal `half_open` state is rendered as `"half-open"` (hyphen) in the API.

**Errors**

| Status | Error body | Meaning |
|--------|-----------|---------|
| 400 | `{ "error": "invalid_endpoint_id" }` | Path segment is not a valid UUID |
| 404 | `{ "error": "endpoint_not_found" }` | No endpoint with the given ID |

---

## Signing Scheme

Every outgoing delivery POST includes two headers that allow the receiving server to
verify authenticity (FR-003, FR-004):

| Header | Format | Description |
|--------|--------|-------------|
| `X-Webhook-Timestamp` | int64 (Unix seconds) | Time of signing |
| `X-Webhook-Signature` | 64-char lowercase hex | HMAC-SHA256 signature |

**Consumer verification procedure**

Given `secret` (raw bytes from `signing_secret`, hex-decoded), `timestamp` (from header),
and `body` (raw request body bytes):

```
message = timestamp_string + "." + hex(sha256(body))
signature = hex(hmac-sha256(secret, message))
```

Assert that `signature == X-Webhook-Signature` and that `|now - timestamp| <= tolerance`
(recommended tolerance: 300 seconds).
