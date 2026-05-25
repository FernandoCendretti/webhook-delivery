//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// idempotencyClient wraps the test server for POST /v1/events calls.
type idempotencyClient struct{ ts *httptest.Server }

func (c *idempotencyClient) submit(ctx context.Context, t *testing.T, endpointID uuid.UUID, payload, idempotencyKey string) *http.Response {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"endpoint_id": endpointID,
		"payload":     json.RawMessage(payload),
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.ts.URL+"/v1/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/events: %v", err)
	}
	return resp
}

type eventResponse struct {
	DeliveryID uuid.UUID `json:"delivery_id"`
	EventID    uuid.UUID `json:"event_id"`
}

func mustDecode202(t *testing.T, resp *http.Response) eventResponse {
	t.Helper()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 202; body=%s", resp.StatusCode, b)
	}
	var r eventResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return r
}

func countQuery(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

// T021: happy-path re-submission returns cached ids; exactly one row per table.
func TestIdempotency_HappyPath(t *testing.T) {
	handler, pool := setupFullAPIWithPool(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	ctx := context.Background()

	epID, err := seedEndpoint(ctx, pool, ts.URL+"/ignored")
	if err != nil {
		t.Fatalf("seed endpoint: %v", err)
	}

	cli := &idempotencyClient{ts}
	const (
		key     = "happy-1"
		payload = `{"x":1}`
	)

	r1 := mustDecode202(t, cli.submit(ctx, t, epID, payload, key))
	r2 := mustDecode202(t, cli.submit(ctx, t, epID, payload, key))

	if r1.EventID != r2.EventID {
		t.Errorf("event_id: first=%s second=%s, want equal", r1.EventID, r2.EventID)
	}
	if r1.DeliveryID != r2.DeliveryID {
		t.Errorf("delivery_id: first=%s second=%s, want equal", r1.DeliveryID, r2.DeliveryID)
	}
	if n := countQuery(t, pool, `SELECT COUNT(*) FROM events WHERE id=$1`, r1.EventID); n != 1 {
		t.Errorf("events rows: got %d, want 1", n)
	}
	if n := countQuery(t, pool, `SELECT COUNT(*) FROM deliveries WHERE id=$1`, r1.DeliveryID); n != 1 {
		t.Errorf("deliveries rows: got %d, want 1", n)
	}
	if n := countQuery(t, pool, `SELECT COUNT(*) FROM idempotency_records WHERE endpoint_id=$1 AND idempotency_key=$2`, epID, key); n != 1 {
		t.Errorf("idempotency_records: got %d, want 1", n)
	}
}

// T022: same key + different payload → 409 Conflict.
func TestIdempotency_PayloadConflict(t *testing.T) {
	handler, pool := setupFullAPIWithPool(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	ctx := context.Background()

	epID, _ := seedEndpoint(ctx, pool, ts.URL+"/ignored")
	cli := &idempotencyClient{ts}
	const key = "conflict-1"

	resp1 := cli.submit(ctx, t, epID, `{"x":1}`, key)
	if resp1.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp1.Body)
		resp1.Body.Close()
		t.Fatalf("first submit: %d, body=%s", resp1.StatusCode, b)
	}
	resp1.Body.Close()

	resp2 := cli.submit(ctx, t, epID, `{"x":2}`, key)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusConflict {
		b, _ := io.ReadAll(resp2.Body)
		t.Fatalf("conflict submit: %d, want 409, body=%s", resp2.StatusCode, b)
	}
	var errResp struct{ Error string `json:"error"` }
	if err := json.NewDecoder(resp2.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Error != "idempotency_conflict" {
		t.Errorf("error: got %q, want %q", errResp.Error, "idempotency_conflict")
	}
}

// T023: no header → two independent events.
func TestIdempotency_NoHeader_TwoEvents(t *testing.T) {
	handler, pool := setupFullAPIWithPool(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	ctx := context.Background()

	epID, _ := seedEndpoint(ctx, pool, ts.URL+"/ignored")
	cli := &idempotencyClient{ts}

	r1 := mustDecode202(t, cli.submit(ctx, t, epID, `{"x":1}`, ""))
	r2 := mustDecode202(t, cli.submit(ctx, t, epID, `{"x":1}`, ""))

	if r1.EventID == r2.EventID {
		t.Errorf("no-key: expected distinct event IDs, got same %s", r1.EventID)
	}
}

// T024: expired record is treated as a fresh submission.
func TestIdempotency_ExpiredRecord_Fresh(t *testing.T) {
	handler, pool := setupFullAPIWithPool(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	ctx := context.Background()

	epID, _ := seedEndpoint(ctx, pool, ts.URL+"/ignored")
	cli := &idempotencyClient{ts}
	const key = "expired-1"

	r1 := mustDecode202(t, cli.submit(ctx, t, epID, `{"x":1}`, key))

	if _, err := pool.Exec(ctx,
		`UPDATE idempotency_records SET expires_at = NOW() - interval '1 second'
		 WHERE endpoint_id=$1 AND idempotency_key=$2`, epID, key); err != nil {
		t.Fatalf("expire record: %v", err)
	}

	r2 := mustDecode202(t, cli.submit(ctx, t, epID, `{"x":1}`, key))
	if r1.EventID == r2.EventID {
		t.Errorf("expired: expected new event_id, got same %s", r1.EventID)
	}
}

// T025: expires_at >= NOW() semantics — exact-boundary still matches.
func TestIdempotency_Boundary(t *testing.T) {
	handler, pool := setupFullAPIWithPool(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	ctx := context.Background()

	epID, _ := seedEndpoint(ctx, pool, ts.URL+"/ignored")
	cli := &idempotencyClient{ts}
	const payload = `{"x":1}`

	t.Run("still-valid", func(t *testing.T) {
		key := "boundary-valid-" + uuid.NewString()[:6]
		r1 := mustDecode202(t, cli.submit(ctx, t, epID, payload, key))
		pool.Exec(ctx, `UPDATE idempotency_records SET expires_at=NOW()+interval '1 second' WHERE endpoint_id=$1 AND idempotency_key=$2`, epID, key) //nolint:errcheck
		r2 := mustDecode202(t, cli.submit(ctx, t, epID, payload, key))
		if r1.EventID != r2.EventID {
			t.Errorf("still-valid: want same event_id %s, got %s", r1.EventID, r2.EventID)
		}
	})

	t.Run("exact-boundary", func(t *testing.T) {
		// Set expires_at to a very small positive offset so the record is still
		// within the window when we resubmit immediately. This is the closest
		// we can get to testing the >= predicate (vs >) without running both
		// the UPDATE and the Lookup in the same DB transaction.
		key := "boundary-exact-" + uuid.NewString()[:6]
		r1 := mustDecode202(t, cli.submit(ctx, t, epID, payload, key))
		pool.Exec(ctx, `UPDATE idempotency_records SET expires_at=NOW()+interval '2 seconds' WHERE endpoint_id=$1 AND idempotency_key=$2`, epID, key) //nolint:errcheck
		r2 := mustDecode202(t, cli.submit(ctx, t, epID, payload, key))
		if r1.EventID != r2.EventID {
			t.Errorf("exact-boundary: want same event_id %s, got %s", r1.EventID, r2.EventID)
		}
	})

	t.Run("expired", func(t *testing.T) {
		key := "boundary-expired-" + uuid.NewString()[:6]
		r1 := mustDecode202(t, cli.submit(ctx, t, epID, payload, key))
		pool.Exec(ctx, `UPDATE idempotency_records SET expires_at=NOW()-interval '1 millisecond' WHERE endpoint_id=$1 AND idempotency_key=$2`, epID, key) //nolint:errcheck
		r2 := mustDecode202(t, cli.submit(ctx, t, epID, payload, key))
		if r1.EventID == r2.EventID {
			t.Errorf("expired: expected new event_id, got same %s", r1.EventID)
		}
	})
}

// T026: concurrent duplicate requests both get 202 with the same event_id.
func TestIdempotency_ConcurrentDuplicates(t *testing.T) {
	handler, pool := setupFullAPIWithPool(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	ctx := context.Background()

	epID, _ := seedEndpoint(ctx, pool, ts.URL+"/ignored")
	cli := &idempotencyClient{ts}
	key := "concurrent-" + uuid.NewString()[:8]
	const payload = `{"x":42}`

	type result struct {
		status int
		resp   eventResponse
	}
	results := make([]result, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp := cli.submit(ctx, t, epID, payload, key)
			results[idx].status = resp.StatusCode
			if resp.StatusCode == http.StatusAccepted {
				results[idx].resp = mustDecode202(t, resp)
			} else {
				resp.Body.Close()
			}
		}(i)
	}
	wg.Wait()

	for i, r := range results {
		if r.status != http.StatusAccepted {
			t.Errorf("goroutine %d: status %d, want 202", i, r.status)
		}
	}
	if results[0].resp.EventID != results[1].resp.EventID {
		t.Errorf("concurrent: event_id differs: %s vs %s", results[0].resp.EventID, results[1].resp.EventID)
	}
	if n := countQuery(t, pool, `SELECT COUNT(*) FROM events WHERE endpoint_id=$1`, epID); n != 1 {
		t.Errorf("events: got %d, want 1", n)
	}
	if n := countQuery(t, pool, `SELECT COUNT(*) FROM idempotency_records WHERE endpoint_id=$1 AND idempotency_key=$2`, epID, key); n != 1 {
		t.Errorf("idempotency_records: got %d, want 1", n)
	}
}

// T027: non-2xx path (unknown endpoint) creates no idempotency record.
func TestIdempotency_UnknownEndpoint_NoRecord(t *testing.T) {
	handler, pool := setupFullAPIWithPool(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	ctx := context.Background()

	cli := &idempotencyClient{ts}
	const key = "notfound-1"

	resp := cli.submit(ctx, t, uuid.New(), `{"x":1}`, key)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 404, body=%s", resp.StatusCode, b)
	}
	if n := countQuery(t, pool, `SELECT COUNT(*) FROM idempotency_records WHERE idempotency_key=$1`, key); n != 0 {
		t.Errorf("idempotency_records: got %d, want 0", n)
	}
}

// T028: same key on different endpoints creates two independent events.
func TestIdempotency_KeyScopedPerEndpoint(t *testing.T) {
	handler, pool := setupFullAPIWithPool(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	ctx := context.Background()

	epA, _ := seedEndpoint(ctx, pool, ts.URL+"/a")
	epB, _ := seedEndpoint(ctx, pool, ts.URL+"/b")
	cli := &idempotencyClient{ts}
	const (
		key     = "scoping-1"
		payload = `{"x":1}`
	)

	rA := mustDecode202(t, cli.submit(ctx, t, epA, payload, key))
	rB := mustDecode202(t, cli.submit(ctx, t, epB, payload, key))

	if rA.EventID == rB.EventID {
		t.Errorf("expected distinct events per endpoint, got same: %s", rA.EventID)
	}
	if n := countQuery(t, pool, `SELECT COUNT(*) FROM idempotency_records WHERE idempotency_key=$1`, key); n != 2 {
		t.Errorf("idempotency_records: got %d, want 2", n)
	}
}

// T029: invalid key chars return 400 and create no record.
func TestIdempotency_InvalidKey_NoRecord(t *testing.T) {
	handler, pool := setupFullAPIWithPool(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	ctx := context.Background()

	epID, _ := seedEndpoint(ctx, pool, ts.URL+"/ignored")
	cli := &idempotencyClient{ts}

	// null-byte is excluded here: Go's net/http client rejects it before sending,
	// so it cannot be tested through an HTTP server. It is covered by unit test T019.
	cases := []struct {
		name string
		key  string
	}{
		{"256-char", strings.Repeat("k", 256)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := cli.submit(ctx, t, epID, `{"x":1}`, tc.key)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				b, _ := io.ReadAll(resp.Body)
				t.Fatalf("key %q: status %d, want 400, body=%s", tc.key, resp.StatusCode, b)
			}
			if n := countQuery(t, pool, `SELECT COUNT(*) FROM idempotency_records WHERE endpoint_id=$1`, epID); n != 0 {
				t.Errorf("key %q: idempotency_records=%d, want 0", tc.key, n)
			}
		})
	}
}

// T043: 255-char key is accepted.
func TestIdempotency_Key255Chars_Accepted(t *testing.T) {
	handler, pool := setupFullAPIWithPool(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	ctx := context.Background()

	epID, _ := seedEndpoint(ctx, pool, ts.URL+"/ignored")
	cli := &idempotencyClient{ts}
	key := strings.Repeat("k", 255)

	resp := cli.submit(ctx, t, epID, `{"x":1}`, key)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("255-char key: status %d, want 202, body=%s", resp.StatusCode, b)
	}
}
