package app

import (
	"context"
	"crypto/rand"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// This observer never retains a request context or responseObservation. Its
// root span has links to waits, while cancellation remains server-owned.
type refreshObservation struct {
	id      string
	account string
	span    trace.Span
	tracer  trace.Tracer
	log     *slog.Logger
}

func observeRefresh(ctx, wait context.Context, tracer trace.Tracer, log *slog.Logger, account string) (context.Context, *refreshObservation) {
	if tracer == nil {
		tracer = disabledTracer
	}
	opts := []trace.SpanStartOption{trace.WithNewRoot()}
	if link := trace.LinkFromContext(wait); link.SpanContext.IsValid() {
		opts = append(opts, trace.WithLinks(link))
	}
	ctx, span := tracer.Start(ctx, "codex.credentials.refresh", opts...)
	observer := &refreshObservation{id: rand.Text(), account: account, span: span, tracer: tracer, log: log}
	span.SetAttributes(attribute.String("operation_id", observer.id), attribute.String("account", account))
	return ctx, observer
}

func (o *refreshObservation) waiter(ctx context.Context, first bool, count int) {
	if link := trace.LinkFromContext(ctx); !first && link.SpanContext.IsValid() {
		o.span.AddLink(link)
	}
	shared := o.span.SpanContext()
	if shared.IsValid() {
		trace.SpanFromContext(ctx).AddLink(trace.Link{SpanContext: shared})
	}
	attrs := []attribute.KeyValue{attribute.String("refresh_operation_id", o.id), attribute.Bool("shared", !first), attribute.Int("waiters_at_join", count)}
	if shared.IsValid() {
		attrs = append(attrs, attribute.String("refresh_trace_id", shared.TraceID().String()), attribute.String("refresh_span_id", shared.SpanID().String()))
	}
	observation(ctx).event(ctx, "refresh_joined", attrs...)
}

func (o *refreshObservation) outcome(ctx context.Context, stage string, err error) {
	kind := telemetryErrorClass(err)
	trace.SpanFromContext(ctx).AddEvent(stage, trace.WithAttributes(attribute.String("error.type", kind)))
	if o.log != nil {
		attrs := []slog.Attr{slog.String("operation_id", o.id), slog.String("account", o.account), slog.String("stage", stage), slog.String("error_type", kind)}
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			attrs = append(attrs, slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
		}
		o.log.LogAttrs(ctx, slog.LevelDebug, "credential refresh operation", attrs...)
	}
}
