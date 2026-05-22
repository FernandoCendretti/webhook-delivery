package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus instrumentation for the webhook-delivery service.
type Metrics struct {
	EventsSubmitted          *prometheus.CounterVec
	EventsRejected           *prometheus.CounterVec
	DeliveryAttempts         *prometheus.CounterVec
	DeliveryAttemptDuration  *prometheus.HistogramVec
	SchedulerClaimed         prometheus.Counter
	SchedulerQueueDepth      prometheus.Gauge
	DeliveryLeaseResurrected prometheus.Counter
	EndpointFailureStreak    *prometheus.GaugeVec

	registry *prometheus.Registry
}

// NewMetrics registers all counters, gauges, and histograms on a private
// Prometheus registry and returns the populated Metrics.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{registry: reg}

	m.EventsSubmitted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "webhook_events_submitted_total",
			Help: "Total events submitted via the API.",
		},
		[]string{"endpoint_id"},
	)
	m.EventsRejected = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "webhook_events_rejected_total",
			Help: "Total events rejected at the API.",
		},
		[]string{"reason"},
	)
	m.DeliveryAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "webhook_delivery_attempts_total",
			Help: "Total delivery attempts, grouped by outcome.",
		},
		[]string{"endpoint_id", "outcome"},
	)
	m.DeliveryAttemptDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "webhook_delivery_attempt_duration_seconds",
			Help:    "Latency of outbound delivery attempts.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
		},
		[]string{"endpoint_id"},
	)
	m.SchedulerClaimed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "webhook_scheduler_claimed_total",
			Help: "Cumulative deliveries claimed by the scheduler.",
		},
	)
	m.SchedulerQueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "webhook_scheduler_queue_depth",
			Help: "Number of deliveries ready to be claimed.",
		},
	)
	m.DeliveryLeaseResurrected = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "webhook_delivery_lease_resurrected_total",
			Help: "Deliveries reset from in_flight to scheduled after the lease expired.",
		},
	)
	m.EndpointFailureStreak = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "webhook_endpoint_failure_streak",
			Help: "Consecutive failed delivery attempts per endpoint.",
		},
		[]string{"endpoint_id"},
	)

	reg.MustRegister(
		m.EventsSubmitted,
		m.EventsRejected,
		m.DeliveryAttempts,
		m.DeliveryAttemptDuration,
		m.SchedulerClaimed,
		m.SchedulerQueueDepth,
		m.DeliveryLeaseResurrected,
		m.EndpointFailureStreak,
	)
	return m
}

// Handler returns an HTTP handler that serves the Prometheus metrics endpoint.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
