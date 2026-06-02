package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
)

// mockDLQDeliveryStore is a test double for dlqDeliveryStore.
type mockDLQDeliveryStore struct {
	entries  []domain.DLQEntry
	delivery *domain.Delivery
	err      error
	// GetByID behaviour (used by Replay)
	getByIDDelivery *domain.Delivery
	getByIDErr      error
	// CreateReplay behaviour (used by Replay)
	createReplayDelivery *domain.Delivery
	createReplayErr      error
	// recorded args
	lastFilter        domain.DLQFilter
	lastPage          int
	lastLimit         int
	createReplayCalls int
	// BulkReplay support
	listPermanentlyFailedIDsResult []uuid.UUID
	listPermanentlyFailedIDsErr    error
	hasNonTerminalReplayResult     bool
	hasNonTerminalReplayErr        error
	listPFIDsCalls                 int
	hasNTRCalls                    int
	createReplayCallsForIDs        []uuid.UUID
}

func (m *mockDLQDeliveryStore) ListPermanentlyFailed(_ context.Context, filter domain.DLQFilter, page, limit int) ([]domain.DLQEntry, error) {
	m.lastFilter = filter
	m.lastPage = page
	m.lastLimit = limit
	return m.entries, m.err
}

func (m *mockDLQDeliveryStore) GetPermanentlyFailed(_ context.Context, _ uuid.UUID) (*domain.Delivery, error) {
	return m.delivery, m.err
}

func (m *mockDLQDeliveryStore) GetByID(_ context.Context, _ uuid.UUID) (*domain.Delivery, error) {
	return m.getByIDDelivery, m.getByIDErr
}

func (m *mockDLQDeliveryStore) CreateReplay(_ context.Context, _, _, sourceID uuid.UUID) (*domain.Delivery, error) {
	m.createReplayCalls++
	m.createReplayCallsForIDs = append(m.createReplayCallsForIDs, sourceID)
	return m.createReplayDelivery, m.createReplayErr
}

func (m *mockDLQDeliveryStore) ListPermanentlyFailedIDs(_ context.Context, _ domain.DLQFilter) ([]uuid.UUID, error) {
	m.listPFIDsCalls++
	return m.listPermanentlyFailedIDsResult, m.listPermanentlyFailedIDsErr
}

func (m *mockDLQDeliveryStore) HasNonTerminalReplay(_ context.Context, _ uuid.UUID) (bool, error) {
	m.hasNTRCalls++
	return m.hasNonTerminalReplayResult, m.hasNonTerminalReplayErr
}

type mockDLQEndpointStore struct {
	endpoint *domain.Endpoint
}

func (m *mockDLQEndpointStore) GetByID(_ context.Context, _ uuid.UUID) (*domain.Endpoint, error) {
	if m.endpoint == nil {
		return nil, domain.ErrNotFound
	}
	return m.endpoint, nil
}

type mockDLQAttemptStore struct {
	attempts []domain.Attempt
	err      error
}

func (m *mockDLQAttemptStore) ListByDelivery(_ context.Context, _ uuid.UUID) ([]domain.Attempt, error) {
	return m.attempts, m.err
}

type mockDLQTenantStore struct {
	tenant *domain.Tenant
}

func (m *mockDLQTenantStore) GetByID(_ context.Context, _ uuid.UUID) (*domain.Tenant, error) {
	if m.tenant == nil {
		return nil, domain.ErrNotFound
	}
	return m.tenant, nil
}

func newTestDLQService(ds *mockDLQDeliveryStore) service.DLQService {
	return service.NewDLQService(ds, &mockDLQEndpointStore{}, &mockDLQAttemptStore{}, &mockDLQTenantStore{tenant: &domain.Tenant{ID: uuid.New()}})
}

func newTestDLQServiceWithAttempts(ds *mockDLQDeliveryStore, as *mockDLQAttemptStore) service.DLQService {
	ep := &domain.Endpoint{ID: uuid.New(), TenantID: uuid.New()}
	return service.NewDLQService(ds, &mockDLQEndpointStore{endpoint: ep}, as, &mockDLQTenantStore{tenant: &domain.Tenant{ID: uuid.New()}})
}

func TestDLQServiceList_EmptyResult(t *testing.T) {
	mock := &mockDLQDeliveryStore{entries: []domain.DLQEntry{}}
	svc := newTestDLQService(mock)

	entries, pg, err := svc.List(context.Background(), domain.DLQFilter{}, 1, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("entries: got %d, want 0", len(entries))
	}
	if pg.HasNext {
		t.Error("HasNext should be false for empty result")
	}
}

func TestDLQServiceList_PageLessThan1Rejected(t *testing.T) {
	mock := &mockDLQDeliveryStore{}
	svc := newTestDLQService(mock)

	_, _, err := svc.List(context.Background(), domain.DLQFilter{}, 0, 20)
	if err == nil {
		t.Fatal("expected error for page=0, got nil")
	}
}

func TestDLQServiceList_LimitBelowMinRejected(t *testing.T) {
	mock := &mockDLQDeliveryStore{}
	svc := newTestDLQService(mock)

	_, _, err := svc.List(context.Background(), domain.DLQFilter{}, 1, 0)
	if err == nil {
		t.Fatal("expected error for limit=0, got nil")
	}
}

func TestDLQServiceList_LimitAboveMaxRejected(t *testing.T) {
	mock := &mockDLQDeliveryStore{}
	svc := newTestDLQService(mock)

	_, _, err := svc.List(context.Background(), domain.DLQFilter{}, 1, 101)
	if err == nil {
		t.Fatal("expected error for limit=101, got nil")
	}
}

func TestDLQServiceList_FiltersPropagedToStore(t *testing.T) {
	tenantID := uuid.New()
	endpointID := uuid.New()
	mock := &mockDLQDeliveryStore{
		entries: []domain.DLQEntry{
			{
				DeliveryID:   uuid.New(),
				EventID:      uuid.New(),
				EndpointID:   endpointID,
				TenantID:     tenantID,
				AttemptCount: 3,
				FailedAt:     time.Now(),
			},
		},
	}
	svc := newTestDLQService(mock)

	filter := domain.DLQFilter{TenantID: &tenantID, EndpointID: &endpointID}
	_, _, err := svc.List(context.Background(), filter, 2, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.lastFilter.TenantID == nil || *mock.lastFilter.TenantID != tenantID {
		t.Errorf("TenantID not propagated; got %v", mock.lastFilter.TenantID)
	}
	if mock.lastFilter.EndpointID == nil || *mock.lastFilter.EndpointID != endpointID {
		t.Errorf("EndpointID not propagated; got %v", mock.lastFilter.EndpointID)
	}
	if mock.lastPage != 2 {
		t.Errorf("page: got %d, want 2", mock.lastPage)
	}
	if mock.lastLimit != 11 {
		t.Errorf("limit: got %d, want 11 (limit+1 for HasNext)", mock.lastLimit)
	}
}

func TestDLQServiceList_HasNextTrue(t *testing.T) {
	// Return limit+1 items from store — service should set HasNext=true and trim to limit.
	entries := make([]domain.DLQEntry, 6) // limit=5 → store asked for 6
	for i := range entries {
		entries[i] = domain.DLQEntry{DeliveryID: uuid.New(), FailedAt: time.Now()}
	}
	mock := &mockDLQDeliveryStore{entries: entries}
	svc := newTestDLQService(mock)

	result, pg, err := svc.List(context.Background(), domain.DLQFilter{}, 1, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 5 {
		t.Errorf("result len: got %d, want 5", len(result))
	}
	if !pg.HasNext {
		t.Error("HasNext should be true")
	}
}

func TestDLQServiceList_StoreErrorPropagated(t *testing.T) {
	mock := &mockDLQDeliveryStore{err: errors.New("db error")}
	svc := newTestDLQService(mock)

	_, _, err := svc.List(context.Background(), domain.DLQFilter{}, 1, 20)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// --- US2: DLQService.Detail ---

func TestDLQServiceDetail_NotFound(t *testing.T) {
	ds := &mockDLQDeliveryStore{err: domain.ErrNotFound}
	svc := newTestDLQService(ds)

	_, err := svc.Detail(context.Background(), uuid.New())
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestDLQServiceDetail_FailedAtIsMaxCompletedAt(t *testing.T) {
	deliveryID := uuid.New()
	now := time.Now().UTC().Truncate(time.Millisecond)
	earlier := now.Add(-5 * time.Minute)
	later := now.Add(-1 * time.Minute)

	ds := &mockDLQDeliveryStore{
		delivery: &domain.Delivery{
			ID:         deliveryID,
			EventID:    uuid.New(),
			EndpointID: uuid.New(),
			Status:     domain.StatusPermanentlyFailed,
		},
	}
	as := &mockDLQAttemptStore{
		attempts: []domain.Attempt{
			{Sequence: 1, CompletedAt: &earlier, Outcome: domain.OutcomeTransientFailure},
			{Sequence: 2, CompletedAt: &later, Outcome: domain.OutcomePermanentFailure},
		},
	}
	svc := newTestDLQServiceWithAttempts(ds, as)

	detail, err := svc.Detail(context.Background(), deliveryID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !detail.FailedAt.Equal(later) {
		t.Errorf("FailedAt: got %v, want %v", detail.FailedAt, later)
	}
	if len(detail.Attempts) != 2 {
		t.Errorf("Attempts len: got %d, want 2", len(detail.Attempts))
	}
}

// --- US3: DLQService.Replay ---

// newReplayService builds a DLQService with the given delivery store and an
// endpoint store that resolves (endpoint != nil) or is gone (endpoint == nil).
func newReplayService(ds *mockDLQDeliveryStore, endpoint *domain.Endpoint) service.DLQService {
	return service.NewDLQService(ds, &mockDLQEndpointStore{endpoint: endpoint}, &mockDLQAttemptStore{}, &mockDLQTenantStore{tenant: &domain.Tenant{ID: uuid.New()}})
}

func TestDLQServiceReplay_NotFound(t *testing.T) {
	ds := &mockDLQDeliveryStore{getByIDErr: domain.ErrNotFound}
	svc := newReplayService(ds, &domain.Endpoint{ID: uuid.New(), TenantID: uuid.New()})

	_, err := svc.Replay(context.Background(), uuid.New())
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if ds.createReplayCalls != 0 {
		t.Errorf("CreateReplay should not be called when delivery is missing")
	}
}

func TestDLQServiceReplay_WrongStatusConflict(t *testing.T) {
	ds := &mockDLQDeliveryStore{
		getByIDDelivery: &domain.Delivery{
			ID:         uuid.New(),
			EventID:    uuid.New(),
			EndpointID: uuid.New(),
			Status:     domain.StatusScheduled,
		},
	}
	svc := newReplayService(ds, &domain.Endpoint{ID: uuid.New(), TenantID: uuid.New()})

	_, err := svc.Replay(context.Background(), uuid.New())
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("expected ErrConflict for non-permanently_failed delivery, got %v", err)
	}
	if ds.createReplayCalls != 0 {
		t.Errorf("CreateReplay should not be called for wrong-status delivery")
	}
}

func TestDLQServiceReplay_EndpointGoneUnprocessable(t *testing.T) {
	ds := &mockDLQDeliveryStore{
		getByIDDelivery: &domain.Delivery{
			ID:         uuid.New(),
			EventID:    uuid.New(),
			EndpointID: uuid.New(),
			Status:     domain.StatusPermanentlyFailed,
		},
	}
	// endpoint == nil → endpoint store returns ErrNotFound.
	svc := newReplayService(ds, nil)

	_, err := svc.Replay(context.Background(), uuid.New())
	if !errors.Is(err, domain.ErrUnprocessable) {
		t.Fatalf("expected ErrUnprocessable for deleted endpoint, got %v", err)
	}
	if ds.createReplayCalls != 0 {
		t.Errorf("CreateReplay should not be called when endpoint is gone")
	}
}

func TestDLQServiceReplay_UniqueViolationConflict(t *testing.T) {
	ds := &mockDLQDeliveryStore{
		getByIDDelivery: &domain.Delivery{
			ID:         uuid.New(),
			EventID:    uuid.New(),
			EndpointID: uuid.New(),
			Status:     domain.StatusPermanentlyFailed,
		},
		createReplayErr: &pgconn.PgError{Code: "23505"},
	}
	svc := newReplayService(ds, &domain.Endpoint{ID: uuid.New(), TenantID: uuid.New()})

	_, err := svc.Replay(context.Background(), uuid.New())
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("expected ErrConflict for 23505 unique violation, got %v", err)
	}
}

func TestDLQServiceReplay_HappyPath(t *testing.T) {
	newDelivery := &domain.Delivery{ID: uuid.New(), Status: domain.StatusScheduled}
	ds := &mockDLQDeliveryStore{
		getByIDDelivery: &domain.Delivery{
			ID:         uuid.New(),
			EventID:    uuid.New(),
			EndpointID: uuid.New(),
			Status:     domain.StatusPermanentlyFailed,
		},
		createReplayDelivery: newDelivery,
	}
	svc := newReplayService(ds, &domain.Endpoint{ID: uuid.New(), TenantID: uuid.New()})

	got, err := svc.Replay(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || got.ID != newDelivery.ID {
		t.Errorf("returned delivery: got %v, want %v", got, newDelivery)
	}
	if got.Status != domain.StatusScheduled {
		t.Errorf("status: got %q, want scheduled", got.Status)
	}
	if ds.createReplayCalls != 1 {
		t.Errorf("CreateReplay calls: got %d, want 1", ds.createReplayCalls)
	}
}

// --- US4: DLQService.BulkReplay ---

func newBulkReplayService(ds *mockDLQDeliveryStore, ep *domain.Endpoint, ts *mockDLQTenantStore) service.DLQService {
	return service.NewDLQService(ds, &mockDLQEndpointStore{endpoint: ep}, &mockDLQAttemptStore{}, ts)
}

func TestDLQServiceBulkReplay_EmptyFilter(t *testing.T) {
	ds := &mockDLQDeliveryStore{}
	svc := newBulkReplayService(ds, &domain.Endpoint{ID: uuid.New()}, &mockDLQTenantStore{tenant: &domain.Tenant{ID: uuid.New()}})

	_, err := svc.BulkReplay(context.Background(), domain.DLQFilter{})
	if !errors.Is(err, domain.ErrUnprocessable) {
		t.Fatalf("expected ErrUnprocessable for empty filter, got %v", err)
	}
	if ds.listPFIDsCalls != 0 {
		t.Error("ListPermanentlyFailedIDs should not be called for empty filter")
	}
}

func TestDLQServiceBulkReplay_EndpointNotFound(t *testing.T) {
	ds := &mockDLQDeliveryStore{}
	epID := uuid.New()
	svc := newBulkReplayService(ds, nil, &mockDLQTenantStore{tenant: &domain.Tenant{ID: uuid.New()}})

	filter := domain.DLQFilter{EndpointID: &epID}
	_, err := svc.BulkReplay(context.Background(), filter)
	if !errors.Is(err, domain.ErrUnprocessable) {
		t.Fatalf("expected ErrUnprocessable for missing endpoint, got %v", err)
	}
	if ds.listPFIDsCalls != 0 {
		t.Error("ListPermanentlyFailedIDs should not be called when endpoint is not found")
	}
}

func TestDLQServiceBulkReplay_TenantNotFound(t *testing.T) {
	ds := &mockDLQDeliveryStore{}
	tenantID := uuid.New()
	svc := newBulkReplayService(ds, &domain.Endpoint{ID: uuid.New()}, &mockDLQTenantStore{})

	filter := domain.DLQFilter{TenantID: &tenantID}
	_, err := svc.BulkReplay(context.Background(), filter)
	if !errors.Is(err, domain.ErrUnprocessable) {
		t.Fatalf("expected ErrUnprocessable for missing tenant, got %v", err)
	}
	if ds.listPFIDsCalls != 0 {
		t.Error("ListPermanentlyFailedIDs should not be called when tenant is not found")
	}
}

func TestDLQServiceBulkReplay_SkipsNonTerminalReplays(t *testing.T) {
	id1 := uuid.New()
	ds := &mockDLQDeliveryStore{
		listPermanentlyFailedIDsResult: []uuid.UUID{id1},
		hasNonTerminalReplayResult:     true,
	}
	epID := uuid.New()
	svc := newBulkReplayService(ds, &domain.Endpoint{ID: epID}, &mockDLQTenantStore{tenant: &domain.Tenant{ID: uuid.New()}})

	filter := domain.DLQFilter{EndpointID: &epID}
	count, err := svc.BulkReplay(context.Background(), filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("count: got %d, want 0 (all skipped due to non-terminal replay)", count)
	}
	if ds.createReplayCalls != 0 {
		t.Errorf("CreateReplay should not be called when all have non-terminal replays; got %d calls", ds.createReplayCalls)
	}
}

func TestDLQServiceBulkReplay_UniqueViolationIsSkip(t *testing.T) {
	id1 := uuid.New()
	ds := &mockDLQDeliveryStore{
		listPermanentlyFailedIDsResult: []uuid.UUID{id1},
		hasNonTerminalReplayResult:     false,
		getByIDDelivery: &domain.Delivery{
			ID:         id1,
			EventID:    uuid.New(),
			EndpointID: uuid.New(),
			Status:     domain.StatusPermanentlyFailed,
		},
		createReplayErr: &pgconn.PgError{Code: "23505"},
	}
	epID := uuid.New()
	svc := newBulkReplayService(ds, &domain.Endpoint{ID: epID}, &mockDLQTenantStore{tenant: &domain.Tenant{ID: uuid.New()}})

	filter := domain.DLQFilter{EndpointID: &epID}
	count, err := svc.BulkReplay(context.Background(), filter)
	if err != nil {
		t.Fatalf("expected no error for 23505 (treated as skip), got %v", err)
	}
	if count != 0 {
		t.Errorf("count: got %d, want 0 (23505 is a skip, not a success)", count)
	}
}

func TestDLQServiceBulkReplay_HappyPath(t *testing.T) {
	id1, id2, id3 := uuid.New(), uuid.New(), uuid.New()
	delivery := &domain.Delivery{
		ID:         uuid.New(),
		EventID:    uuid.New(),
		EndpointID: uuid.New(),
		Status:     domain.StatusPermanentlyFailed,
	}
	newDelivery := &domain.Delivery{ID: uuid.New(), Status: domain.StatusScheduled}
	ds := &mockDLQDeliveryStore{
		listPermanentlyFailedIDsResult: []uuid.UUID{id1, id2, id3},
		hasNonTerminalReplayResult:     false,
		getByIDDelivery:                delivery,
		createReplayDelivery:           newDelivery,
	}
	epID := uuid.New()
	svc := newBulkReplayService(ds, &domain.Endpoint{ID: epID}, &mockDLQTenantStore{tenant: &domain.Tenant{ID: uuid.New()}})

	filter := domain.DLQFilter{EndpointID: &epID}
	count, err := svc.BulkReplay(context.Background(), filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("count: got %d, want 3", count)
	}
	if ds.createReplayCalls != 3 {
		t.Errorf("CreateReplay calls: got %d, want 3", ds.createReplayCalls)
	}
}
