package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/klauspost/compress/zstd"
)

func zstdRequest(t *testing.T, data []byte) []byte {
	t.Helper()
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	return encoder.EncodeAll(data, nil)
}

func postEncodedResponse(t *testing.T, url, path string, body []byte, headers http.Header) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header = headers.Clone()
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestResponsesPOSTAliasesShareHTTPAdapter(t *testing.T) {
	for _, path := range testResponsePaths {
		for _, encoding := range []string{"identity", "zstd"} {
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/stream_%t", path, encoding, stream), func(t *testing.T) {
					captured := make(chan []byte, 1)
					upstream := newHTTPUpstream(t, func(r *http.Request, c *websocket.Conn, data []byte) {
						if r.Header.Get("Content-Encoding") != "" {
							t.Error("request compression header reached upstream WebSocket")
						}
						captured <- data
						sendHTTPEvents(t, c, httpCreatedEvent, httpCompletedEvent)
					})
					srv, proxy := newWebSocketProxy(t, upstream.URL, []*Account{testAccount("a", 0)})
					srv.admission = newAdmissionGate(1)
					logs, _, _ := observeTestServer(t, srv)
					body := []byte(fmt.Sprintf(`{"model":"m","input":"full history","stream":%t,"unknown":9007199254740993}`, stream))
					if encoding == "zstd" {
						body = zstdRequest(t, body)
					}
					resp := postEncodedResponse(t, proxy.URL, path, body, http.Header{"Content-Encoding": {encoding}})
					output := readHTTPBody(t, resp)
					if resp.StatusCode != 200 {
						t.Fatalf("status=%d %s", resp.StatusCode, output)
					}
					wantType := "application/json"
					if stream {
						wantType = "text/event-stream"
					}
					if resp.Header.Get("Content-Type") != wantType {
						t.Fatal(resp.Header)
					}
					data := <-captured
					if !bytes.Contains(data, []byte(`"type":"response.create"`)) || !bytes.Contains(data, []byte("9007199254740993")) {
						t.Fatalf("lost translation/precision: %s", data)
					}
					assertHTTPClean(t, srv)
					foundPath, foundEncoding := false, false
					for _, record := range observationRecords(t, logs) {
						if record["stage"] == "started" && record["http.route"] == path && record["http.request.method"] == "POST" {
							foundPath = true
						}
						if record["stage"] == "body_read" && record["content_encoding"] == encoding {
							foundEncoding = true
						}
					}
					if !foundPath || !foundEncoding {
						t.Fatal("actual alias/encoding missing from logs")
					}
					if srv.stats.snapshot().Turns != 1 || srv.stats.snapshot().MonthlyUsage.TotalTokens != 14 {
						t.Fatal("duplicate/lost accounting")
					}
				})
			}
		}
	}
}

func TestZstdRequestsRejectInvalidAndOversizedBodiesBeforeDial(t *testing.T) {
	var attempts atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { attempts.Add(1); w.WriteHeader(502) }))
	defer upstream.Close()
	srv, proxy := newWebSocketProxy(t, upstream.URL, []*Account{testAccount("a", 0)})
	srv.admission = newAdmissionGate(1)
	valid := zstdRequest(t, []byte(`{"model":"m"}`))
	badCRC := bytes.Clone(valid)
	badCRC[len(badCRC)-1] ^= 1
	tooMuch := zstdRequest(t, []byte(strings.Repeat(" ", maxHTTPResponseBody+1)))
	for _, test := range []struct {
		name     string
		body     []byte
		encoding []string
		status   int
	}{
		{"not zstd", []byte(`{"model":"m"}`), []string{"zstd"}, 400},
		{"truncated", valid[:len(valid)-1], []string{"zstd"}, 400},
		{"checksum", badCRC, []string{"zstd"}, 400},
		{"trailing wire junk", append(bytes.Clone(valid), []byte("junk")...), []string{"zstd"}, 400},
		{"trailing decoded JSON", append(bytes.Clone(valid), valid...), []string{"zstd"}, 400},
		{"decoded limit", tooMuch, []string{"zstd"}, 413},
		{"encoded limit", make([]byte, maxHTTPResponseBody+1), []string{"zstd"}, 413},
		// Zstd frame header: non-single-segment, 16 MiB window, empty last block.
		{"window limit", []byte{0x28, 0xb5, 0x2f, 0xfd, 0, 0x70, 1, 0, 0}, []string{"zstd"}, 413},
		{"stacked encodings", valid, []string{"zstd, identity"}, 415},
		{"duplicate header", valid, []string{"zstd", "zstd"}, 415},
		{"gzip", valid, []string{"gzip"}, 415},
		{"brotli", valid, []string{"br"}, 415},
	} {
		t.Run(test.name, func(t *testing.T) {
			resp := postEncodedResponse(t, proxy.URL, "/v1/codex/responses", test.body, http.Header{"Content-Encoding": test.encoding})
			body := readHTTPBody(t, resp)
			if resp.StatusCode != test.status {
				t.Fatalf("status=%d want=%d body=%s", resp.StatusCode, test.status, body)
			}
			assertHTTPClean(t, srv)
		})
	}
	if attempts.Load() != 0 {
		t.Fatalf("invalid requests attempted %d handshakes", attempts.Load())
	}
}

func TestZstdRequestBoundaryAndConcatenatedFrames(t *testing.T) {
	upstream := newHTTPUpstream(t, func(_ *http.Request, c *websocket.Conn, _ []byte) {
		sendHTTPEvents(t, c, httpCreatedEvent, httpCompletedEvent)
	})
	srv, proxy := newWebSocketProxy(t, upstream.URL, []*Account{testAccount("a", 0)})
	srv.admission = newAdmissionGate(1)
	const minimal = `{"model":"m"}`
	exact := append([]byte(minimal), []byte(strings.Repeat(" ", maxHTTPResponseBody-len(minimal)))...)
	concatenated := append(zstdRequest(t, []byte(`{"model":`)), zstdRequest(t, []byte(`"m"}`))...)
	for _, body := range [][]byte{zstdRequest(t, exact), concatenated} {
		resp := postEncodedResponse(t, proxy.URL, "/codex/responses", body, http.Header{"Content-Encoding": {"ZSTD"}})
		if output := readHTTPBody(t, resp); resp.StatusCode != 200 {
			t.Fatalf("status=%d %s", resp.StatusCode, output)
		}
		assertHTTPClean(t, srv)
	}
}

func TestZstdAliasAuthenticationAndAdmissionBeforeBodyRead(t *testing.T) {
	srv := newTestServer(t, nil)
	srv.admission = newAdmissionGate(1)
	srv.lookupAPIKey = func(string) (string, bool, error) { return "", false, nil }
	for _, path := range testResponsePaths {
		request := httptest.NewRequest(http.MethodPost, path, &unreadResponseBody{t: t})
		request.Header.Set("Content-Encoding", "zstd")
		response := httptest.NewRecorder()
		srv.routes().ServeHTTP(response, request)
		if response.Code != 401 {
			t.Fatal(response.Code)
		}
	}
	srv.admission = newAdmissionGate(0)
	for _, path := range testResponsePaths {
		request := httptest.NewRequest(http.MethodPost, path, &unreadResponseBody{t: t})
		request.Header.Set("Content-Encoding", "zstd")
		response := httptest.NewRecorder()
		srv.routes().ServeHTTP(response, request)
		if response.Code != 503 {
			t.Fatal(response.Code)
		}
	}
}

type unreadResponseBody struct{ t *testing.T }

func (b *unreadResponseBody) Read([]byte) (int, error) {
	b.t.Error("rejected request read body")
	return 0, io.EOF
}

func TestZstdBodyCancellationReleasesAdmission(t *testing.T) {
	srv := newTestServer(t, []*Account{testAccount("a", 0)})
	srv.admission = newAdmissionGate(1)
	started, handled := make(chan struct{}), make(chan struct{})
	handler := srv.routes()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		defer close(handled)
		handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, proxy.URL+"/v1/codex/responses", reader)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Encoding", "zstd")
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, _ := http.DefaultClient.Do(request)
		if resp != nil {
			resp.Body.Close()
		}
	}()
	waitHTTPSignal(t, started)
	cancel()
	writer.Close()
	waitHTTPSignal(t, done)
	waitHTTPSignal(t, handled)
	assertHTTPClean(t, srv)
}

func TestResponsesMethodRejectionsAreLogged(t *testing.T) {
	srv := newTestServer(t, nil)
	logs, _, _ := observeTestServer(t, srv)
	for _, path := range testResponsePaths {
		for _, method := range []string{"PUT", "DELETE", "OPTIONS", "GET"} {
			request := httptest.NewRequest(method, path+"?secret=PRIVATE_QUERY", &unreadResponseBody{t: t})
			request.Header.Set("Authorization", "Bearer PRIVATE_KEY")
			response := httptest.NewRecorder()
			srv.routes().ServeHTTP(response, request)
			if response.Code != 405 || response.Header().Get(responseRequestIDHeader) == "" {
				t.Fatalf("%s %s status=%d", method, path, response.Code)
			}
			if method != "GET" && response.Header().Get("Allow") != "GET, HEAD, POST" {
				t.Fatal("missing Allow")
			}
		}
	}
	if strings.Contains(logs.String(), "PRIVATE_QUERY") || strings.Contains(logs.String(), "PRIVATE_KEY") {
		t.Fatal("rejection logs exposed credentials/query")
	}
	if strings.Count(logs.String(), `"http_status":405`) < 12 {
		t.Fatal("unlogged method/upgrade rejections")
	}
}

func TestUpstreamWebSocketFailureLogsMetadataWithoutReason(t *testing.T) {
	upstream := newHTTPUpstream(t, func(_ *http.Request, c *websocket.Conn, _ []byte) {
		sendHTTPEvents(t, c, httpCreatedEvent)
		c.Close(websocket.StatusPolicyViolation, "SECRET_UPSTREAM_REASON")
	})
	srv, proxy := newWebSocketProxy(t, upstream.URL, []*Account{testAccount("a", 0)})
	srv.admission = newAdmissionGate(1)
	logs := &testLogBuffer{}
	srv.log = slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	conn, _ := dialWebSocket(t, proxy.URL, http.Header{"Session-Id": {"session"}})
	defer conn.CloseNow()
	writeWebSocketEvent(t, conn, map[string]string{"type": "response.create", "model": "m"})
	if event := readWebSocketEvent(t, conn); event.Type != "response.created" {
		t.Fatal(event.Type)
	}
	failure := readWebSocketFailure(t, conn, "upstream_websocket_closed")
	if !strings.Contains(failure.Error.Message, "SECRET_UPSTREAM_REASON") {
		t.Fatal("upstream close reason lost")
	}
	readCloseStatus(t, conn, websocket.StatusPolicyViolation)
	assertHTTPClean(t, srv)
	found := false
	for _, line := range strings.Split(logs.String(), "\n") {
		var event map[string]any
		if json.Unmarshal([]byte(line), &event) != nil || event["msg"] != "upstream websocket failure" {
			continue
		}
		found = true
		if event["phase"] != "read" || event["close_status"] != float64(1008) || event["accepted_pending_turns"] != float64(1) || event["last_event_type"] != "response.created" || event["inference_replayed"] != false {
			t.Fatalf("diagnostics=%v", event)
		}
	}
	if !found || strings.Contains(logs.String(), "SECRET_UPSTREAM_REASON") {
		t.Fatal("missing/unsafe upstream diagnostics")
	}
	if got := websocketFailureClass(fmt.Errorf("wrapped: %w", io.EOF)); got != "eof" {
		t.Fatal(got)
	}
	if got := websocketFailureClass(errors.New("SECRET_ERROR")); got != "transport_failure" {
		t.Fatal(got)
	}
}
