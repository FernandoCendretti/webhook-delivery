package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
)

// mockDLQDeliveryStore is a test double for dlqDeliveryStore.
type mockDLQDeliveryStore struct {
	entries []domain.DLQEntry
	err     error
	// recorded args
	lastFilter domain.DLQFilter
	lastPage   int
	lastLimit  int
}

func (m *mockDLQDeliveryStore) ListPermanentlyFailed(_ context.Context, filter domain.DLQFilter, page, limit int) ([]domain.DLQEntry, error) {
	m.lastFilter = filter
	m.lastPage = page
	m.lastLimit = limit
	return m.entries, m.err
}

type mockDLQEndpointStore struct{}

func (m *mockDLQEndpointStore) GetByID(_ context.Context, _ uuid.UUID) (*domain.Endpoint, error) {
	return nil, domain.ErrNotFound
}

type mockDLQAttemptStore struct{}

func newTestDLQService(ds *mockDLQDeliveryStore) service.DLQService {
	return service.NewDLQService(ds, &mockDLQEndpointStore{}, &mockDLQAttemptStore{})
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
