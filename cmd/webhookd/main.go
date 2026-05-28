package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof" // register pprof handlers on DefaultServeMux
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/FernandoCendretti/webhook-delivery/internal/api"
	"github.com/FernandoCendretti/webhook-delivery/internal/config"
	"github.com/FernandoCendretti/webhook-delivery/internal/delivery"
	"github.com/FernandoCendretti/webhook-delivery/internal/observability"
	"github.com/FernandoCendretti/webhook-delivery/internal/queue"
	"github.com/FernandoCendretti/webhook-delivery/internal/recovery"
	"github.com/FernandoCendretti/webhook-delivery/internal/scheduler"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	subcommand := os.Args[1]
	args := os.Args[2:]

	switch subcommand {
	case "api":
		exitOnErr(runAPI(ctx, args))
	case "worker":
		exitOnErr(runWorker(ctx, args))
	case "scheduler":
		exitOnErr(runScheduler(ctx, args))
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %q\n\n", subcommand)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `webhookd — reliable webhook delivery service

USAGE:
    webhookd <subcommand>

SUBCOMMANDS:
    api          Run the HTTP API server
    worker       Run the delivery worker (Kafka consumer + HTTP client)
    scheduler    Run the scheduler + reaper`)
}

func exitOnErr(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// startPprofServer starts a pprof debug server on addr and returns a cleanup
// function.  It is a no-op when addr is empty.  The server is intentionally
// kept on a separate listener from the API so it is never accidentally exposed
// to the public internet — bind it to 127.0.0.1 or a private interface only.
func startPprofServer(ctx context.Context, addr string, logger interface {
	InfoContext(ctx context.Context, msg string, args ...any)
}) {
	if addr == "" {
		return
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           http.DefaultServeMux, // pprof registers on DefaultServeMux
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.InfoContext(ctx, "pprof listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.InfoContext(ctx, "pprof server stopped", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
}

func runAPI(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("api", flag.ContinueOnError)
	pprofAddr := fs.String("pprof-addr", "", "if non-empty, start a pprof debug server on this address (e.g. 127.0.0.1:6060)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.LoadAPI()
	if err != nil {
		return err
	}

	logger := observability.NewLogger(observability.LoggerConfig{
		Level:  cfg.LogLevel,
		Format: cfg.LogFormat,
	})
	metrics := observability.NewMetrics()

	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: cfg.DatabaseURL,
		MaxConns:    cfg.DatabasePoolMax,
	})
	if err != nil {
		return fmt.Errorf("postgres pool: %w", err)
	}
	defer pool.Close()

	tenantSvc := service.NewTenantService(store.NewTenantStore(pool))
	endpointSvc := service.NewEndpointService(store.NewEndpointStore(pool), tenantSvc)
	eventSvc := service.NewEventService(pool, endpointSvc)
	deliveryStore := store.NewDeliveryStore(pool)
	deliverySvc := service.NewDeliveryService(deliveryStore)

	s := api.NewServer(api.ServerConfig{
		APIAddr:     fmt.Sprintf(":%d", cfg.APIPort),
		MetricsAddr: fmt.Sprintf(":%d", cfg.MetricsPort),
		Logger:      logger,
		Metrics:     metrics,
	})
	s.RegisterTenants(tenantSvc)
	s.RegisterEndpoints(endpointSvc)
	s.RegisterEvents(eventSvc)
	s.RegisterDeliveries(deliverySvc)

	startPprofServer(ctx, *pprofAddr, logger)
	logger.InfoContext(ctx, "starting api",
		"api_port", cfg.APIPort, "metrics_port", cfg.MetricsPort)
	return s.Start(ctx)
}

func runWorker(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	pprofAddr := fs.String("pprof-addr", "", "if non-empty, start a pprof debug server on this address (e.g. 127.0.0.1:6061)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.LoadWorker()
	if err != nil {
		return err
	}

	logger := observability.NewLogger(observability.LoggerConfig{
		Level:  cfg.LogLevel,
		Format: cfg.LogFormat,
	})
	metrics := observability.NewMetrics()

	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: cfg.DatabaseURL,
		MaxConns:    cfg.DatabasePoolMax,
	})
	if err != nil {
		return fmt.Errorf("postgres pool: %w", err)
	}
	defer pool.Close()

	ds := store.NewDeliveryStore(pool)
	as := store.NewAttemptStore(pool)

	_ = metrics // T044: metrics wired per metric emission point

	startPprofServer(ctx, *pprofAddr, logger)

	var wg sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		cons := queue.NewConsumer(queue.ConsumerConfig{
			Brokers: cfg.KafkaBrokers,
			Topic:   cfg.KafkaTopic,
			GroupID: cfg.KafkaConsumerGroup,
			Logger:  logger,
		})
		w := delivery.NewWorker(delivery.WorkerConfig{
			DeliveryStore: ds,
			AttemptStore:  as,
			Consumer:      cons,
			Pool:          pool,
			Logger:        logger,
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { _ = cons.Close() }()
			_ = w.Run(ctx)
		}()
	}
	logger.InfoContext(ctx, "workers started", "concurrency", cfg.Concurrency)
	wg.Wait()
	return nil
}

func runScheduler(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("scheduler", flag.ContinueOnError)
	pprofAddr := fs.String("pprof-addr", "", "if non-empty, start a pprof debug server on this address (e.g. 127.0.0.1:6062)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.LoadScheduler()
	if err != nil {
		return err
	}

	logger := observability.NewLogger(observability.LoggerConfig{
		Level:  cfg.LogLevel,
		Format: cfg.LogFormat,
	})

	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: cfg.DatabaseURL,
		MaxConns:    cfg.DatabasePoolMax,
	})
	if err != nil {
		return fmt.Errorf("postgres pool: %w", err)
	}
	defer pool.Close()

	pub := queue.NewPublisher(queue.PublisherConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		Logger:  logger,
	})
	defer func() { _ = pub.Close() }()

	ds := store.NewDeliveryStore(pool)
	metrics := observability.NewMetrics()

	sched := scheduler.New(scheduler.Config{
		DeliveryStore: ds,
		Publisher:     pub,
		BatchSize:     100,
		LeaseDuration: time.Duration(cfg.InFlightLeaseSeconds) * time.Second,
		Metrics:       metrics,
		Logger:        logger,
	})

	reaper := recovery.New(recovery.Config{
		Store:    ds,
		Interval: time.Duration(cfg.ReaperTickSeconds) * time.Second,
		Metrics:  metrics,
		Logger:   logger,
	})

	startPprofServer(ctx, *pprofAddr, logger)
	tickInterval := time.Duration(cfg.SchedulerTickMS) * time.Millisecond
	logger.InfoContext(ctx, "scheduler+reaper started",
		"tick_ms", cfg.SchedulerTickMS, "reaper_tick_s", cfg.ReaperTickSeconds)

	errCh := make(chan error, 2)
	go func() { errCh <- sched.Run(ctx, tickInterval) }()
	go func() { errCh <- reaper.Run(ctx) }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}
