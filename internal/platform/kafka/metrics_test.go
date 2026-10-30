package kafka

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)


func TestConsumerMetrics_StartAtZero(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	m, err := newConsumerMetrics("inventory")
	if err != nil {
		t.Fatal(err)
	}
	m.zero(context.Background(), []string{"inventory.commands"})

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	found := map[string]int{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range sum.DataPoints {
					if dp.Value != 0 {
						t.Errorf("%s starts at %d, want 0", m.Name, dp.Value)
					}
					found[m.Name]++
				}
			}
		}
	}
	want := map[string]int{
		"consumer_records_total":         2, 
		"consumer_retries_total":         2, 
		"consumer_slow_path_total":       1,
		"consumer_rebalance_hurry_total": 1,
		"consumer_commit_failures_total": 1,
		"consumer_rewinds_total":         1,
	}
	for name, n := range want {
		if found[name] != n {
			t.Errorf("%s: %d zero series, want %d", name, found[name], n)
		}
	}
}
