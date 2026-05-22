package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// Common holds configuration shared by all three subcommands.
type Common struct {
	// LogLevel controls the minimum log severity (debug|info|warn|error).
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`
	// LogFormat selects the log encoder (json|text).
	LogFormat string `env:"LOG_FORMAT" envDefault:"json"`

	// DatabaseURL is a PostgreSQL DSN (postgres://user:pass@host/db).
	DatabaseURL string `env:"DATABASE_URL,required"`

	// KafkaBrokers is a comma-separated list of broker addresses.
	KafkaBrokers []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"localhost:9092"`
	// KafkaTopic is the Kafka topic used by both the scheduler (producer)
	// and workers (consumers) to exchange delivery tasks.
	KafkaTopic string `env:"KAFKA_DELIVERY_TOPIC" envDefault:"webhook.deliveries"`

	// MetricsPort is the port on which the Prometheus /metrics endpoint listens.
	MetricsPort int `env:"METRICS_PORT" envDefault:"9090"`
}

// API holds configuration for the HTTP API subcommand.
type API struct {
	Common
	// APIPort is the port on which the REST API listens.
	APIPort int `env:"API_PORT" envDefault:"8080"`
	// DatabasePoolMax is the maximum number of database connections held by the
	// API pool. The default (50) is calibrated for up to ~1 000 req/s at
	// <10 ms average query time (Little's Law: L = λ × W → 10 conns ≈ 1 000 req/s
	// at 10 ms; 50 gives comfortable headroom). Increase when you observe
	// pool-wait latency in the postgres_pool_acquire_duration_seconds metric.
	DatabasePoolMax int `env:"DATABASE_POOL_MAX" envDefault:"50"`
}

// Worker holds configuration for the delivery-worker subcommand.
type Worker struct {
	Common
	// DatabasePoolMax controls how many Postgres connections the worker pool
	// may open. Each goroutine acquires at most one connection per delivery,
	// so the practical cap is Concurrency + a small buffer for retries.
	// Default (20) works for Concurrency ≤ 64 because transactions are very
	// short-lived; raise to ~Concurrency/2 only if you see pool contention.
	DatabasePoolMax int `env:"DATABASE_POOL_MAX" envDefault:"20"`
	// Concurrency is the number of parallel delivery goroutines.  Each goroutine
	// blocks on HTTP for up to HTTPTimeoutSeconds, so the effective throughput is
	// Concurrency / HTTPTimeoutSeconds deliveries/sec.  Default 64 yields ~2/s
	// per worker replica at the 30 s timeout; scale horizontally before raising
	// this beyond ~128 to avoid saturating the DB connection pool.
	Concurrency int `env:"WORKER_CONCURRENCY" envDefault:"64"`
	// HTTPTimeoutSeconds is the per-attempt timeout for outbound HTTP calls.
	HTTPTimeoutSeconds int `env:"WORKER_HTTP_TIMEOUT_SECONDS" envDefault:"30"`
	// KafkaConsumerGroup is the Kafka consumer-group ID shared by all worker
	// replicas.  All replicas in the same group cooperatively partition the topic.
	KafkaConsumerGroup string `env:"KAFKA_CONSUMER_GROUP" envDefault:"webhook-workers"`
}

func (w *Worker) HTTPTimeout() time.Duration {
	return time.Duration(w.HTTPTimeoutSeconds) * time.Second
}

// Scheduler holds configuration for the scheduler+reaper subcommand.
type Scheduler struct {
	Common
	// DatabasePoolMax for the scheduler only needs to cover the batch claim
	// query (one connection at a time) plus the reaper.  Default (5) is
	// intentionally small; raise only if you run multiple scheduler replicas
	// pointing at the same DB (not recommended — use leader election instead).
	DatabasePoolMax int `env:"DATABASE_POOL_MAX" envDefault:"5"`
	// SchedulerTickMS is how often the scheduler polls for ready deliveries (ms).
	// Lowering this reduces end-to-end latency but increases DB load.
	// At the default 500 ms, the scheduler generates ~2 queries/sec.
	SchedulerTickMS int `env:"SCHEDULER_TICK_MS" envDefault:"500"`
	// InFlightLeaseSeconds is how long the scheduler holds the in_flight lease
	// on a delivery before the reaper is allowed to reclaim it.  Must be greater
	// than the longest expected worker HTTP timeout (WORKER_HTTP_TIMEOUT_SECONDS).
	// Default (300 s) provides a comfortable 10× margin over the 30 s HTTP timeout.
	InFlightLeaseSeconds int `env:"IN_FLIGHT_LEASE_SECONDS" envDefault:"300"`
	// ReaperTickSeconds is how often the reaper scans for expired leases.
	// Should be materially smaller than InFlightLeaseSeconds; default (60 s)
	// means a crashed worker loses its lease within at most 60+InFlightLease seconds.
	ReaperTickSeconds int `env:"REAPER_TICK_SECONDS" envDefault:"60"`
}

func (s *Scheduler) SchedulerTick() time.Duration {
	return time.Duration(s.SchedulerTickMS) * time.Millisecond
}

func (s *Scheduler) InFlightLease() time.Duration {
	return time.Duration(s.InFlightLeaseSeconds) * time.Second
}

func (s *Scheduler) ReaperTick() time.Duration {
	return time.Duration(s.ReaperTickSeconds) * time.Second
}

func LoadAPI() (*API, error) {
	var cfg API
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("load api config: %w", err)
	}
	return &cfg, nil
}

func LoadWorker() (*Worker, error) {
	var cfg Worker
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("load worker config: %w", err)
	}
	return &cfg, nil
}

func LoadScheduler() (*Scheduler, error) {
	var cfg Scheduler
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("load scheduler config: %w", err)
	}
	return &cfg, nil
}
