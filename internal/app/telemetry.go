package app

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const telemetryScope = "github.com/supabitapp/codex-balancer"
const telemetryExportTimeout = 3 * time.Second
const telemetryShutdownTimeout = 5 * time.Second

var disabledTracer = noop.NewTracerProvider().Tracer(telemetryScope)

func (s *server) responseTracer() trace.Tracer {
	if s.tracer != nil {
		return s.tracer
	}
	return disabledTracer
}

// No global provider/propagator is installed. Export is explicitly opt-in; the
// exporter uses the standard OTLP HTTP environment settings, not inference auth.
func newTelemetry(ctx context.Context, enabled bool) (*sdktrace.TracerProvider, error) {
	if !enabled {
		return nil, nil
	}
	for _, key := range []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"} {
		if value := os.Getenv(key); value != "" {
			u, err := url.Parse(value)
			if err != nil || u.Host == "" || u.Scheme != "http" && u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
				return nil, errors.New("invalid OTLP endpoint; use an HTTP(S) URL without user info, query or fragment")
			}
		}
	}
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithTimeout(telemetryExportTimeout),
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}),
		otlptracehttp.WithMaxRequestSize(1<<20),
	)
	if err != nil {
		return nil, errors.New("could not initialize OTLP trace exporter")
	}
	name := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME"))
	if name == "" {
		name = "codex-balancer"
	}
	return sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", name))),
		sdktrace.WithRawSpanLimits(sdktrace.SpanLimits{
			AttributeValueLengthLimit: 256, AttributeCountLimit: 64, EventCountLimit: 64,
			LinkCountLimit: 128, AttributePerEventCountLimit: 32, AttributePerLinkCountLimit: 4,
		}),
		sdktrace.WithBatcher(privateSpanExporter{exporter},
			sdktrace.WithMaxQueueSize(1024), sdktrace.WithMaxExportBatchSize(128),
			sdktrace.WithBatchTimeout(time.Second), sdktrace.WithExportTimeout(telemetryExportTimeout),
		),
	), nil
}

// Collector errors can contain response bodies or credential-bearing URLs.
// Do not pass them to the SDK's global diagnostic error logger.
type privateSpanExporter struct{ sdktrace.SpanExporter }

func (e privateSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if err := e.SpanExporter.ExportSpans(ctx, spans); err != nil {
		return errors.New("OTLP trace export failed: " + telemetryErrorClass(err))
	}
	return nil
}
func (e privateSpanExporter) Shutdown(ctx context.Context) error {
	if err := e.SpanExporter.Shutdown(ctx); err != nil {
		return errors.New("OTLP trace exporter shutdown failed: " + telemetryErrorClass(err))
	}
	return nil
}

func telemetryErrorClass(err error) string {
	if err == nil {
		return "none"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var timeout net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timeout) && timeout.Timeout() {
		return "timeout"
	}
	return "failure"
}

func spanFailure(span trace.Span, err error) {
	if err != nil {
		// RecordError would capture err.Error(), potentially including payloads.
		kind := telemetryErrorClass(err)
		span.SetAttributes(attribute.String("error.type", kind))
		span.SetStatus(codes.Error, kind)
	}
}
