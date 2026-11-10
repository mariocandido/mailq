// Package metrics defines mailq's Prometheus metrics.
//
// Note: the domain label is fine for a demo, but in production its
// cardinality must be bounded (e.g. top-N providers + "other").
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics groups every collector.
type Metrics struct {
	Enqueued   *prometheus.CounterVec
	Attempts   *prometheus.CounterVec
	Throttled  *prometheus.CounterVec
	Requeued   *prometheus.CounterVec
	Held       prometheus.Gauge
	Latency    *prometheus.HistogramVec
	Queue      *prometheus.GaugeVec
	DomainRate *prometheus.GaugeVec
}

// New registers all metrics on reg.
func New(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		Enqueued: f.NewCounterVec(prometheus.CounterOpts{
			Name: "mailq_messages_enqueued_total", Help: "Messages accepted by the API, by initial status.",
		}, []string{"status"}),
		Attempts: f.NewCounterVec(prometheus.CounterOpts{
			Name: "mailq_delivery_attempts_total", Help: "Delivery attempts by domain, class and reason.",
		}, []string{"domain", "class", "reason"}),
		Throttled: f.NewCounterVec(prometheus.CounterOpts{
			Name: "mailq_throttled_total", Help: "Messages that had to wait for the domain throttle (counted once per message).",
		}, []string{"domain"}),
		Requeued: f.NewCounterVec(prometheus.CounterOpts{
			Name: "mailq_requeued_total", Help: "Claimed messages handed back to Postgres without an attempt, by reason (lease, shutdown).",
		}, []string{"domain", "reason"}),
		Held: f.NewGauge(prometheus.GaugeOpts{
			Name: "mailq_held_messages", Help: "Claimed messages waiting in memory for their domain throttle.",
		}),
		Latency: f.NewHistogramVec(prometheus.HistogramOpts{
			Name: "mailq_delivery_duration_seconds", Help: "SMTP session duration per attempt.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
		}, []string{"domain"}),
		Queue: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "mailq_messages", Help: "Messages in the database by status.",
		}, []string{"status"}),
		DomainRate: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "mailq_domain_rate", Help: "Current adaptive send rate (msg/s) per domain.",
		}, []string{"domain"}),
	}
}
