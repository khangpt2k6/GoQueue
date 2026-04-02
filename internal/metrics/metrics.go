package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	PublishedTotal prometheus.Counter
	ConsumedTotal  prometheus.Counter
	ConsumerLag    *prometheus.GaugeVec
	PublishLatency prometheus.Histogram
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		PublishedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "goqueue_messages_published_total",
			Help: "Total number of published messages.",
		}),
		ConsumedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "goqueue_messages_consumed_total",
			Help: "Total number of consumed messages.",
		}),
		ConsumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "goqueue_consumer_lag",
			Help: "Current consumer lag by topic and group.",
		}, []string{"topic", "group"}),
		PublishLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "goqueue_publish_latency_seconds",
			Help:    "Publish handler latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	reg.MustRegister(m.PublishedTotal, m.ConsumedTotal, m.ConsumerLag, m.PublishLatency)
	return m
}

func (m *Metrics) ObservePublishLatency(start time.Time) {
	m.PublishLatency.Observe(time.Since(start).Seconds())
}

func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
