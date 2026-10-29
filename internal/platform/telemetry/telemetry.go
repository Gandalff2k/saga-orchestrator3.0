// Package telemetry wires OpenTelemetry traces and metrics, exported over
// OTLP/gRPC to the collector (deploy/compose/otel-collector.yaml), which fans
// out to Jaeger (traces) and Prometheus (metrics).
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// ScopeName is the instrumentation scope for hand-written spans and metrics.
const ScopeName = "github.com/Gandalff2k/saga3"

// Config becomes the OTel resource attached to every span and metric.
type Config struct {
	ServiceName string
	Version     string
	Environment string
	InstanceID  string
	// Endpoint empty = keep the global no-op providers (unit tests, quick local runs).
	Endpoint string
}

// Init installs global tracer/meter providers and the W3C propagator.
// The returned function flushes and shuts them down.
func Init(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error) {
	// Propagator is always installed: trace context must be forwarded through
	// outbox/Kafka headers even when this process doesn't export spans itself.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if cfg.Endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.name", cfg.ServiceName),
		attribute.String("service.version", cfg.Version),
		attribute.String("service.instance.id", cfg.InstanceID),
		attribute.String("deployment.environment.name", cfg.Environment),
	))
	if err != nil {
		return nil, fmt.Errorf("telemetry resource: %w", err)
	}

	// Exporters read OTEL_EXPORTER_OTLP_ENDPOINT (and friends) from env.
	traceExp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("otlp trace exporter: %w", err)
	}
	metricExp, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("otlp metric exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())), // tune via OTEL_TRACES_SAMPLER in prod
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(10*time.Second))),
	)
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)

	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx))
	}, nil
}
