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
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/segmentio/kafka-go"
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

// sharedKafkaBroker holds the broker address of the single Kafka container
// started by TestMain and shared across all tests in the package.
var sharedKafkaBroker string

// setupFullAPI builds an http.Handler with endpoints, events, deliveries, and
// tenant routes wired. Requires all six migrations to be applied on pool.
func setupFullAPI(t *testing.T, pool *pgxpool.Pool) http.Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := api.NewServer(api.ServerConfig{APIAddr: ":0", Logger: logger})
	tenantSvc := service.NewTenantService(store.NewTenantStore(pool))
	endpointSvc := service.NewEndpointService(store.NewEndpointStore(pool), tenantSvc)
	s.RegisterTenants(tenantSvc)
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

// newKafkaContainer starts a Kafka testcontainer and returns its address and a
// termination function. Used by TestMain to create the single shared container.
func newKafkaContainer() (addr string, terminate func(), err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:7.6.0",
		ExposedPorts: []string{"9092/tcp"},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{
				network.MustParsePort("9092/tcp"): []network.PortBinding{
					{HostIP: netip.MustParseAddr("0.0.0.0"), HostPort: "9092"},
				},
			}
		},
		Env: map[string]string{
			"KAFKA_NODE_ID":                          "1",
			"KAFKA_PROCESS_ROLES":                    "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":         "1@localhost:9093",
			"KAFKA_LISTENERS":                        "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
			"KAFKA_ADVERTISED_LISTENERS":             "PLAINTEXT://localhost:9092",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":   "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_CONTROLLER_LISTENER_NAMES":        "CONTROLLER",
			"KAFKA_INTER_BROKER_LISTENER_NAME":       "PLAINTEXT",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":        "true",
			"CLUSTER_ID":                             "MkU3OEVBNTcwNTJENDM2Qk",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("Kafka Server started").WithStartupTimeout(90*time.Second),
			wait.ForListeningPort("9092/tcp").WithStartupTimeout(30*time.Second),
		),
	}

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return "", nil, fmt.Errorf("start kafka container: %w", err)
	}
	return "localhost:9092", func() { _ = ctr.Terminate(context.Background()) }, nil
}

// testKafkaBrokers returns the address of the shared Kafka container started by
// TestMain. Tests are sequential (no t.Parallel()), so a single broker is safe.
func testKafkaBrokers(t *testing.T) []string {
	t.Helper()
	if sharedKafkaBroker == "" {
		t.Fatal("sharedKafkaBroker not initialised — TestMain must start Kafka before tests run")
	}
	return []string{sharedKafkaBroker}
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
//
// Cleanup order (LIFO): cons.Close unblocks the reader → pipeCancel stops
// sched/reap goroutines → wg.Wait ensures all goroutines exit before the test
// framework tears down the Kafka container.
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

	pipeCtx, pipeCancel := context.WithCancel(ctx)

	sched := scheduler.New(scheduler.Config{
		DeliveryStore: ds,
		Publisher:     pub,
		BatchSize:     10,
		LeaseDuration: time.Duration(leaseSeconds) * time.Second,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	reap := recovery.New(recovery.Config{
		Store:    ds,
		Interval: time.Duration(leaseSeconds/2+1) * time.Second,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	cons := queue.NewConsumer(queue.ConsumerConfig{
		Brokers:           brokers,
		Topic:             topic,
		GroupID:           "test-wkr-" + uuid.NewString()[:8],
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		StartOffset:       kafka.FirstOffset,
		SessionTimeout:    6 * time.Second,
		HeartbeatInterval: 2 * time.Second,
	})

	w := delivery.NewWorker(delivery.WorkerConfig{
		DeliveryStore: ds,
		AttemptStore:  as,
		Consumer:      cons,
		Pool:          pool,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); _ = sched.Run(pipeCtx, 10*time.Millisecond) }()
	go func() { defer wg.Done(); _ = reap.Run(pipeCtx) }()
	go func() { defer wg.Done(); _ = w.Run(pipeCtx) }()

	t.Cleanup(func() {
		_ = cons.Close() // unblocks FetchMessage so the worker goroutine can exit
		pipeCancel()     // signals sched and reap goroutines to stop
		wg.Wait()        // waits for all three goroutines to finish
		_ = pub.Close()  // safe to close publisher after goroutines are done
	})

	return &testPipeline{DS: ds, AS: as, EventSvc: eventSvc}
}

// waitForTopic polls until the given Kafka topic has a leader or ctx expires.
// It retries every 500 ms for up to 10 attempts to handle the window between
// publisher creation and the broker making the topic available.
func waitForTopic(ctx context.Context, brokers []string, topic string) error {
	for i := 0; i < 10; i++ {
		conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, 0)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("topic %q not ready after retries", topic)
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

// systemDefaultTenantID is the tenant inserted by migration 005 as a backfill
// for pre-existing rows. Tests that seed endpoints directly via SQL use this.
const systemDefaultTenantID = "00000000-0000-0000-0000-000000000001"

// seedEndpoint inserts an endpoint row with a random signing_secret and returns its ID.
// Uses the system-default tenant so callers do not need to create a tenant first.
func seedEndpoint(ctx context.Context, pool *pgxpool.Pool, url string) (uuid.UUID, error) {
	var id uuid.UUID
	err := pool.QueryRow(ctx,
		`INSERT INTO endpoints (url, signing_secret, tenant_id) VALUES ($1, gen_random_bytes(32), $2) RETURNING id`,
		url, systemDefaultTenantID).Scan(&id)
	return id, err
}

// submitEvent calls the EventService to create an event + delivery using the
// system-default tenant (all pre-003 test fixtures use this tenant).
func submitEvent(ctx context.Context, svc *service.EventService, endpointID uuid.UUID) (uuid.UUID, error) {
	payload, _ := json.Marshal(map[string]string{"test": "payload"})
	d, err := svc.Submit(ctx, endpointID, payload, "", payload, uuid.MustParse(systemDefaultTenantID))
	if err != nil {
		return uuid.Nil, err
	}
	return d.ID, nil
}
