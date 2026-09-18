package app

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func isolateTelemetryEnvironment(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "OTEL_") {
			t.Setenv(key, "")
			os.Unsetenv(key)
		}
	}
}

func TestTelemetryRequiresOptIn(t *testing.T) {
	isolateTelemetryEnvironment(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "invalid endpoint containing PRIVATE_CONFIG")
	provider, err := newTelemetry(context.Background(), false)
	if err != nil || provider != nil {
		t.Fatalf("disabled telemetry initialized: %v %v", provider, err)
	}
	_, err = newTelemetry(context.Background(), true)
	if err == nil || strings.Contains(err.Error(), "PRIVATE_CONFIG") {
		t.Fatalf("invalid endpoint diagnostic=%v", err)
	}
}

func TestTelemetryOTLPExportAndShutdown(t *testing.T) {
	isolateTelemetryEnvironment(t)
	requests := make(chan *collectortrace.ExportTraceServiceRequest, 16)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" || r.Header.Get("Authorization") != "collector-test-key" {
			t.Errorf("collector request headers/path=%s %v", r.URL.Path, r.Header)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var request collectortrace.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &request); err != nil {
			t.Error(err)
			return
		}
		requests <- &request
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer collector.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", collector.URL+"/v1/traces")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "Authorization=collector-test-key")
	t.Setenv("OTEL_SERVICE_NAME", "balancer-otel-test")
	global := otel.GetTracerProvider()
	provider, err := newTelemetry(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Shutdown(context.Background())
	if otel.GetTracerProvider() != global {
		t.Fatal("changed global tracer provider")
	}
	upstream := newHTTPUpstream(t, func(_ *http.Request, conn *testResponseStream, _ []byte) {
		sendHTTPEvents(t, conn, httpCreatedEvent, httpCompletedEvent)
	})
	srv, proxy := newWebSocketProxy(t, upstream.URL, []*Account{testAccount("a", 0)})
	srv.tracer = provider.Tracer(telemetryScope)
	srv.admission = newAdmissionGate(1)
	response := postResponse(t, proxy.URL, `{"model":"m","instructions":"PRIVATE_INSTRUCTIONS","input":"PRIVATE_INPUT"}`, nil)
	readHTTPBody(t, response)
	assertHTTPClean(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := provider.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	foundRoot, foundService := false, false
	for len(requests) > 0 {
		request := <-requests
		for _, resource := range request.ResourceSpans {
			for _, attr := range resource.Resource.Attributes {
				if attr.Key == "service.name" && attr.Value.GetStringValue() == "balancer-otel-test" {
					foundService = true
				}
			}
			for _, scope := range resource.ScopeSpans {
				for _, span := range scope.Spans {
					if span.Name == "POST /v1/responses" {
						foundRoot = true
						if len(span.TraceId) != 16 || len(span.SpanId) != 8 {
							t.Fatal("invalid trace identity")
						}
					}
					for _, attr := range span.Attributes {
						for _, secret := range []string{"PRIVATE_INSTRUCTIONS", "PRIVATE_INPUT", "collector-test-key", "token-a"} {
							if strings.Contains(attr.Value.GetStringValue(), secret) {
								t.Errorf("exported %s", secret)
							}
						}
					}
				}
			}
		}
	}
	if !foundRoot || !foundService {
		t.Fatalf("exported root=%t service=%t", foundRoot, foundService)
	}
}

type unsafeTestExporter struct{}

func (unsafeTestExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return errors.New("collector error SECRET_HEADER SECRET_BODY")
}
func (unsafeTestExporter) Shutdown(context.Context) error {
	return errors.New("shutdown SECRET_HEADER")
}

func TestTelemetryExporterErrorsArePrivate(t *testing.T) {
	exporter := privateSpanExporter{unsafeTestExporter{}}
	for _, err := range []error{exporter.ExportSpans(context.Background(), nil), exporter.Shutdown(context.Background())} {
		if err == nil || strings.Contains(err.Error(), "SECRET_") {
			t.Fatalf("unsafe exporter error: %v", err)
		}
	}
}

func TestTelemetrySlowCollectorDoesNotBlockInference(t *testing.T) {
	isolateTelemetryEnvironment(t)
	started, release := make(chan struct{}), make(chan struct{})
	var first, unblock sync.Once
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		first.Do(func() { close(started) })
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer collector.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", collector.URL+"/v1/traces")
	provider, err := newTelemetry(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Shutdown(context.Background())
	defer unblock.Do(func() { close(release) })
	tracer := provider.Tracer(telemetryScope)
	_, warmup := tracer.Start(context.Background(), "warmup")
	warmup.End()
	flushed := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		flushed <- provider.ForceFlush(ctx)
	}()
	waitHTTPSignal(t, started)
	// Fill the bounded queue while export is blocked. OnEnd must drop rather
	// than blocking the request on the collector or an ever-growing queue.
	for range 2200 {
		_, span := tracer.Start(context.Background(), "queue-pressure")
		span.End()
	}
	upstream := newHTTPUpstream(t, func(_ *http.Request, conn *testResponseStream, _ []byte) {
		sendHTTPEvents(t, conn, httpCreatedEvent, httpCompletedEvent)
	})
	srv, proxy := newWebSocketProxy(t, upstream.URL, []*Account{testAccount("a", 0)})
	srv.tracer = tracer
	srv.admission = newAdmissionGate(1)
	response := postResponseTimeout(t, proxy.URL, `{"model":"m"}`, nil, time.Second)
	readHTTPBody(t, response)
	assertHTTPClean(t, srv)
	if response.StatusCode != 200 {
		t.Fatalf("collector affected inference: %d", response.StatusCode)
	}
	unblock.Do(func() { close(release) })
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
}

func TestTelemetryRefreshCompletionEndsBeforeShutdownFlush(t *testing.T) {
	srv, account, consumed := consumedRefreshServer(t, 200, `{"access_token":"rotated-access","refresh_token":"rotated-refresh"}`)
	exporter := tracetest.NewInMemoryExporter()
	var ended atomic.Int64
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(&completionSpanRecorder{exporter: exporter, ended: &ended}), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer provider.Shutdown(context.Background())
	srv.tracer = provider.Tracer(telemetryScope)
	lifetime, cancelRuntime := context.WithCancel(srv.ctx)
	defer cancelRuntime()
	srv.ctx = lifetime
	srv.pool.storageMu.Lock()
	var release sync.Once
	unlock := func() { release.Do(srv.pool.storageMu.Unlock) }
	defer unlock()
	ctx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	result := make(chan bool, 1)
	go func() { result <- srv.refreshedContext(ctx, account, account.id()) }()
	waitHTTPSignal(t, consumed)
	cancelRequest()
	<-result
	grace, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	joining := &refreshWaitContext{Context: grace, joined: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- srv.stopRefreshesContext(joining, cancelRuntime) }()
	waitHTTPSignal(t, joining.joined)
	if ended.Load() != 0 {
		t.Fatal("shared span ended before publication")
	}
	unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if ended.Load() != 1 {
		t.Fatal("shutdown join overtook shared span completion")
	}
	if account.persisted().AccessToken != "rotated-access" {
		t.Fatal("telemetry changed publication lifetime")
	}
	if err := provider.ForceFlush(grace); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, span := range exporter.GetSpans() {
		if span.Name == "codex.credentials.refresh" {
			found = true
		}
	}
	if !found {
		t.Fatal("late shared operation missing before exporter shutdown")
	}
}

type completionSpanRecorder struct {
	exporter *tracetest.InMemoryExporter
	ended    *atomic.Int64
}

func (p *completionSpanRecorder) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (p *completionSpanRecorder) OnEnd(span sdktrace.ReadOnlySpan) {
	p.exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{span})
	if span.Name() == "codex.credentials.refresh" {
		p.ended.Add(1)
	}
}
func (p *completionSpanRecorder) Shutdown(ctx context.Context) error { return p.exporter.Shutdown(ctx) }
func (p *completionSpanRecorder) ForceFlush(context.Context) error   { return nil }

func TestTelemetryDisabledAndUnsampledLogs(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "unsampled"}[enabled], func(t *testing.T) {
			srv := newTestServer(t, nil)
			logs, _, _ := observeTestServer(t, srv)
			srv.tracer = nil
			if enabled {
				provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
				defer provider.Shutdown(context.Background())
				srv.tracer = provider.Tracer(telemetryScope)
			}
			request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{`))
			response := httptest.NewRecorder()
			srv.routes().ServeHTTP(response, request)
			if response.Header().Get(responseRequestIDHeader) == "" {
				t.Fatal("request lacks correlation")
			}
			records := observationRecords(t, logs)
			if len(records) == 0 || records[len(records)-1]["stage"] != "finished" {
				t.Fatal("lost lifecycle")
			}
			if enabled && (records[len(records)-1]["trace_id"] == nil || records[len(records)-1]["trace_sampled"] != false) {
				t.Fatal("unsampled trace lost log correlation")
			}
		})
	}
}
