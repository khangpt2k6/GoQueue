package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	m.PublishedTotal.Inc()
	m.PublishedTotal.Inc()
	m.ConsumedTotal.Inc()
	m.ConsumerLag.WithLabelValues("orders", "payment-svc").Set(42)
	m.ObservePublishLatency(time.Now().Add(-5 * time.Millisecond))

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	names := make(map[string]bool)
	for _, f := range families {
		names[f.GetName()] = true
	}

	want := []string{
		"goqueue_messages_published_total",
		"goqueue_messages_consumed_total",
		"goqueue_consumer_lag",
		"goqueue_publish_latency_seconds",
	}
	for _, n := range want {
		if !names[n] {
			t.Errorf("metric %q not found in registry", n)
		}
	}
}
