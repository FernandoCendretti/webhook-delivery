package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/FernandoCendretti/webhook-delivery/internal/observability"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
)

// ServerConfig holds the configuration for the HTTP API and metrics servers.
type ServerConfig struct {
	APIAddr     string
	MetricsAddr string
	Logger      *slog.Logger
	Metrics     *observability.Metrics
}

// Server manages the HTTP API and optional Prometheus metrics servers.
type Server struct {
	cfg        ServerConfig
	mux        *http.ServeMux
	apiSrv     *http.Server
	metricsSrv *http.Server
}

// NewServer constructs a Server with middlewares applied and a ready ServeMux.
func NewServer(cfg ServerConfig) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	mux := http.NewServeMux()

	apiHandler := Compose(
		RequestID,
		Recover(cfg.Logger),
		Logging(cfg.Logger),
	)(mux)

	apiSrv := &http.Server{
		Addr:              cfg.APIAddr,
		Handler:           apiHandler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	return &Server{cfg: cfg, mux: mux, apiSrv: apiSrv}
}

// Mux returns the underlying ServeMux so route registrars can attach handlers.
func (s *Server) Mux() *http.ServeMux {
	return s.mux
}

// RegisterEndpoints wires the /v1/endpoints routes. Must be called once per
// server before Start.
func (s *Server) RegisterEndpoints(svc *service.EndpointService) {
	h := newEndpointHandler(svc, s.cfg.Logger)
	s.mux.HandleFunc("POST /v1/endpoints", h.Create)
	s.mux.HandleFunc("GET /v1/endpoints/{id}", h.Get)
	s.mux.HandleFunc("POST /v1/endpoints/{id}/rotate-secret", h.RotateSecret)
}

// RegisterEvents wires the POST /v1/events route.
func (s *Server) RegisterEvents(svc eventSubmitter) {
	h := newEventHandlerWithMetrics(svc, s.cfg.Logger, s.cfg.Metrics)
	s.mux.HandleFunc("POST /v1/events", h.Submit)
}

// RegisterTenants wires the /v1/tenants routes. Must be called once per server
// before Start.
func (s *Server) RegisterTenants(svc tenantSvc) {
	h := newTenantHandler(svc, s.cfg.Logger)
	s.mux.HandleFunc("POST /v1/tenants", h.Create)
	s.mux.HandleFunc("GET /v1/tenants/{id}", h.GetByID)
}

// RegisterDeliveries wires the GET /v1/deliveries/{id} route.
func (s *Server) RegisterDeliveries(svc deliveryGetter) {
	h := newDeliveryHandler(svc, s.cfg.Logger)
	s.mux.HandleFunc("GET /v1/deliveries/{id}", h.Get)
}

// RegisterDLQ wires the /v1/dlq routes for DLQ inspection and replay.
func (s *Server) RegisterDLQ(svc service.DLQService) {
	h := newDLQHandler(svc, s.cfg.Logger)
	s.mux.HandleFunc("GET /v1/dlq", h.List)
	s.mux.HandleFunc("GET /v1/dlq/{delivery_id}", h.Detail)
	// Literal route must be registered before the wildcard pattern.
	s.mux.HandleFunc("POST /v1/dlq/replay", h.BulkReplay)
	s.mux.HandleFunc("POST /v1/dlq/{delivery_id}/replay", h.Replay)
}

// RegisterCircuitBreaker wires the GET /v1/endpoints/{id}/circuit-breaker route.
func (s *Server) RegisterCircuitBreaker(store circuitStateGetter) {
	h := newCircuitHandler(store, s.cfg.Logger)
	s.mux.HandleFunc("GET /v1/endpoints/{id}/circuit-breaker", h.GetState)
}

// Start begins listening on the configured addresses and blocks until ctx is
// cancelled or a server error occurs.
func (s *Server) Start(ctx context.Context) error {
	errCh := make(chan error, 2)

	go func() {
		s.cfg.Logger.InfoContext(ctx, "api listening", "addr", s.cfg.APIAddr)
		if err := s.apiSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("api server: %w", err)
		}
	}()

	if s.cfg.MetricsAddr != "" && s.cfg.Metrics != nil {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", s.cfg.Metrics.Handler())
		s.metricsSrv = &http.Server{
			Addr:              s.cfg.MetricsAddr,
			Handler:           metricsMux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			s.cfg.Logger.InfoContext(ctx, "metrics listening", "addr", s.cfg.MetricsAddr)
			if err := s.metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("metrics server: %w", err)
			}
		}()
	}

	select {
	case <-ctx.Done():
		return s.shutdown()
	case err := <-errCh:
		_ = s.shutdown()
		return err
	}
}

func (s *Server) shutdown() error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var apiErr, metricsErr error
	if s.apiSrv != nil {
		apiErr = s.apiSrv.Shutdown(shutdownCtx)
	}
	if s.metricsSrv != nil {
		metricsErr = s.metricsSrv.Shutdown(shutdownCtx)
	}
	if apiErr != nil {
		return apiErr
	}
	return metricsErr
}
