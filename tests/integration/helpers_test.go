//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/FernandoCendretti/webhook-delivery/internal/api"
	"github.com/FernandoCendretti/webhook-delivery/internal/delivery"
	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/queue"
	"github.com/FernandoCendretti/webhook-delivery/internal/recovery"
	"github.com/FernandoCendretti/webhook-delivery/internal/scheduler"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

// setupFullAPI builds an http.Handler with endpoints, events, and deliveries routes wired.
func setupFullAPI(t *testing.T, pool *pgxpool.Pool) http.Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := api.NewServer(api.ServerConfig{APIAddr: ":0", Logger: logger})
	endpointSvc := service.NewEndpointService(store.NewEndpointStore(pool))
	s.RegisterEndpoints(endpointSvc)
	s.RegisterEvents(service.NewEventService(pool, endpointSvc))
	s.RegisterDeliveries(service.NewDeliveryService(store.NewDeliveryStore(pool)))
	return s.Mux()
}

// setupFullAPIWithPool creates the postgres container and returns a wired handler + pool.
func setupFullAPIWithPool(t *testing.T) (http.Handler, *pgxpool.Pool) {
	t.Helper()
	_, pool := setupAPI(t)
	return setupFullAPI(t, pool), pool
}

// testKafkaBrokers starts a Kafka testcontainer and returns broker addresses.
func testKafkaBrokers(t *testing.T) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:7.6.0",
		ExposedPorts: []string{"9092/tcp"},
		Env: map[string]string{
			"KAFKA_NODE_ID":                        "1",
			"KAFKA_PROCESS_ROLES":                  "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":       "1@localhost:9093",
			"KAFKA_LISTENERS":                      "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
			"KAFKA_ADVERTISED_LISTENERS":           "PLAINTEXT://localhost:9092",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_CONTROLLER_LISTENER_NAMES":      "CONTROLLER",
			"KAFKA_INTER_BROKER_LISTENER_NAME":     "PLAINTEXT",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":      "true",
			"CLUSTER_ID":                           "MkU3OEVBNTcwNTJENDM2Qk",
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(90 * time.Second),
	}

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("kafka host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "9092")
	if err != nil {
		t.Fatalf("kafka port: %v", err)
	}
	return []string{host + ":" + port.Port()}
}

// testPipeline holds the running scheduler+worker and helper stores.
type testPipeline struct {
	DS       *store.DeliveryStore
	AS       *store.AttemptStore
	EventSvc *service.EventService
}

// startPipeline launches an in-process scheduler (with reaper) and worker backed
// by real Postgres + Kafka containers. The topic is unique per call to avoid
// cross-test interference. leaseSeconds controls the in-flight lease duration.
func startPipeline(
	ctx context.Context,
	t *testing.T,
	pool *pgxpool.Pool,
	brokers []string,
	leaseSeconds int,
) *testPipeline {
	t.Helper()

	topic := "wh.test." + uuid.NewString()[:8]
	ds := store.NewDeliveryStore(pool)
	as := store.NewAttemptStore(pool)
	endpointSvc := service.NewEndpointService(store.NewEndpointStore(pool))
	eventSvc := service.NewEventService(pool, endpointSvc)

	pub := queue.NewPublisher(queue.PublisherConfig{Brokers: brokers, Topic: topic})
	t.Cleanup(func() { _ = pub.Close() })

	sched := scheduler.New(scheduler.Config{
		DeliveryStore: ds,
		Publisher:     pub,
		BatchSize:     10,
		LeaseDuration: time.Duration(leaseSeconds) * time.Second,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	go func() { _ = sched.Run(ctx, 10*time.Millisecond) }()

	reap := recovery.New(recovery.Config{
		Store:    ds,
		Interval: time.Duration(leaseSeconds/2+1) * time.Second,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	go func() { _ = reap.Run(ctx) }()

	cons := queue.NewConsumer(queue.ConsumerConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: "test-wkr-" + uuid.NewString()[:8],
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	t.Cleanup(func() { _ = cons.Close() })

	w := delivery.NewWorker(delivery.WorkerConfig{
		DeliveryStore: ds,
		AttemptStore:  as,
		Consumer:      cons,
		Pool:          pool,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	go func() { _ = w.Run(ctx) }()

	return &testPipeline{DS: ds, AS: as, EventSvc: eventSvc}
}

// waitForDeliveryStatus polls until the delivery reaches want or ctx expires.
func waitForDeliveryStatus(ctx context.Context, ds *store.DeliveryStore, id uuid.UUID, want domain.DeliveryStatus) error {
	for {
		select {
		case <-ctx.Done():
			d, _ := ds.GetByID(context.Background(), id)
			return fmt.Errorf("timeout: want status %q, got %q", want, d.Status)
		default:
		}
		d, err := ds.GetByID(ctx, id)
		if err != nil {
			return err
		}
		if d.Status == want {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// countAttempts returns the number of attempt rows for a delivery.
func countAttempts(ctx context.Context, pool *pgxpool.Pool, deliveryID uuid.UUID) (int, error) {
	var n int
	err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM attempts WHERE delivery_id = $1`, deliveryID).Scan(&n)
	return n, err
}

// listen opens a random TCP port on localhost and returns the listener.
func listen() (net.Listener, error) {
	return net.Listen("tcp", "127.0.0.1:0")
}

// seedEndpoint inserts an endpoint row directly and returns its ID.
func seedEndpoint(ctx context.Context, pool *pgxpool.Pool, url string) (uuid.UUID, error) {
	var id uuid.UUID
	err := pool.QueryRow(ctx,
		`INSERT INTO endpoints (url) VALUES ($1) RETURNING id`, url).Scan(&id)
	return id, err
}

// submitEvent calls the EventService to create an event + delivery.
func submitEvent(ctx context.Context, svc *service.EventService, endpointID uuid.UUID) (uuid.UUID, error) {
	payload, _ := json.Marshal(map[string]string{"test": "payload"})
	d, err := svc.Submit(ctx, endpointID, payload)
	if err != nil {
		return uuid.Nil, err
	}
	return d.ID, nil
}
