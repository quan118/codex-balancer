package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type websocketFailureEvent struct {
	Type                string                     `json:"type"`
	Status              int                        `json:"status"`
	UpstreamStatus      int                        `json:"upstream_status"`
	UpstreamCloseStatus int                        `json:"upstream_close_status"`
	Retryable           bool                       `json:"retryable"`
	Error               responseErrorPayload       `json:"error"`
	Headers             map[string]json.RawMessage `json:"headers"`
}

func readWebSocketFailure(t *testing.T, conn *websocket.Conn, code string) websocketFailureEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var event websocketFailureEvent
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatal(err)
	}
	if event.Type != "error" || event.Error.Code != code || event.Error.Message == "" {
		t.Fatalf("failure = %s, want %s with a message", data, code)
	}
	return event
}

func TestUpstreamCloseDuringWritePreservesDetails(t *testing.T) {
	for _, mode := range []string{"websocket"} {
		t.Run(mode, func(t *testing.T) {
			type connectionKey struct{}
			finished := make(chan struct{})
			upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.CloseNow()
				conn.SetReadLimit(1024)
				_, reader, err := conn.Reader(r.Context())
				if err != nil {
					t.Error(err)
					return
				}
				if _, err := io.CopyN(io.Discard, reader, 1024); err != nil {
					t.Error(err)
					return
				}
				payload := append([]byte{3, 241}, []byte("request exceeds upstream limit token-a")...)
				frame := append([]byte{0x88, byte(len(payload))}, payload...)
				if _, err := r.Context().Value(connectionKey{}).(net.Conn).Write(frame); err != nil {
					t.Error(err)
				}
				<-finished
			}))
			upstream.Config.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
				if err := conn.(*net.TCPConn).SetReadBuffer(1024); err != nil {
					t.Error(err)
				}
				return context.WithValue(ctx, connectionKey{}, conn)
			}
			upstream.Start()
			defer upstream.Close()
			defer close(finished)
			srv, proxy := newWebSocketProxy(t, upstream.URL, []*Account{testAccount("a", 0)})
			srv.admission = newAdmissionGate(1)
			logs := captureTestLogs(srv)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			payload := fmt.Sprintf(`{"model":"m","stream":%t,"input":%q}`, mode == "sse", strings.Repeat("x", 1<<20))
			if mode == "websocket" {
				payload = `{"type":"response.create",` + payload[1:]
				conn, _ := dialWebSocket(t, proxy.URL, nil)
				defer conn.CloseNow()
				if err := conn.Write(ctx, websocket.MessageText, []byte(payload)); err != nil {
					t.Fatal(err)
				}
				_, data, err := conn.Read(ctx)
				if err != nil {
					t.Fatal(err)
				}
				var failure websocketFailureEvent
				if err := json.Unmarshal(data, &failure); err != nil {
					t.Fatal(err)
				}
				if failure.Status != 502 || failure.Error.Code != "request_too_large" || failure.UpstreamCloseStatus != 1009 || !failure.Retryable || !strings.Contains(failure.Error.Message, "request exceeds upstream limit [redacted]") {
					t.Fatalf("failure = %s", data)
				}
				readCloseStatus(t, conn, websocket.StatusMessageTooBig)
			} else {
				response := postResponseTimeout(t, proxy.URL, payload, nil, 20*time.Second)
				body := readHTTPBody(t, response)
				if response.StatusCode != 400 || !strings.Contains(body, "request_too_large") || !strings.Contains(body, "request exceeds upstream limit [redacted]") || strings.Contains(body, "token-a") {
					t.Fatalf("status=%d body=%s", response.StatusCode, body)
				}
			}
			assertHTTPClean(t, srv)
			if text := logs.String(); !strings.Contains(text, `"phase":"write"`) || !strings.Contains(text, `"close_status":1009`) {
				t.Fatalf("write failure lost close status: %s", text)
			}
		})
	}
}

func TestUpstreamClosePreservesDetailsAcrossTransports(t *testing.T) {
	for _, test := range []struct {
		close websocket.StatusCode
		code  string
		http  int
		ws    int
	}{
		{websocket.StatusMessageTooBig, "request_too_large", 502, 502},
		{websocket.StatusPolicyViolation, "upstream_websocket_closed", 400, 400},
		{websocket.StatusProtocolError, "upstream_websocket_closed", 400, 400},
		{websocket.StatusInvalidFramePayloadData, "upstream_websocket_closed", 400, 400},
		{websocket.StatusInternalError, "upstream_websocket_closed", 502, 502},
	} {
		for _, mode := range []string{"websocket"} {
			t.Run(fmt.Sprintf("%d/%s", test.close, mode), func(t *testing.T) {
				upstream := newHTTPUpstream(t, func(_ *http.Request, conn *testResponseStream, _ []byte) {
					if mode == "sse" {
						sendHTTPEvents(t, conn, httpCreatedEvent)
					}
					conn.Close(test.close, "original reason token-a")
				})
				srv, proxy := newWebSocketProxy(t, upstream.URL, []*Account{testAccount("a", 0)})
				srv.admission = newAdmissionGate(1)
				if mode == "websocket" {
					conn, _ := dialWebSocket(t, proxy.URL, nil)
					defer conn.CloseNow()
					writeWebSocketEvent(t, conn, map[string]string{"type": "response.create", "model": "m"})
					event := readWebSocketFailure(t, conn, test.code)
					if event.Status != test.ws || event.UpstreamCloseStatus != int(test.close) || !strings.Contains(event.Error.Message, "original reason [redacted]") {
						t.Fatalf("failure = %+v", event)
					}
					readCloseStatus(t, conn, test.close)
				} else {
					response := postResponse(t, proxy.URL, fmt.Sprintf(`{"model":"m","stream":%t}`, mode == "sse"), nil)
					body := readHTTPBody(t, response)
					status := test.http
					if mode == "sse" {
						status = 200
						if strings.Contains(body, "response.completed") || strings.Count(body, "data: [DONE]") != 1 {
							t.Fatalf("invalid terminal stream: %s", body)
						}
					}
					if response.StatusCode != status || !strings.Contains(body, test.code) || !strings.Contains(body, "original reason [redacted]") || strings.Contains(body, "token-a") {
						t.Fatalf("status=%d body=%s", response.StatusCode, body)
					}
				}
				assertHTTPClean(t, srv)
			})
		}
	}
}

func TestWebSocketRetryPreservesOriginalErrorFields(t *testing.T) {
	for _, kind := range []string{"error", "response.failed"} {
		t.Run(kind, func(t *testing.T) {
			upstream := newHTTPUpstream(t, func(_ *http.Request, conn *testResponseStream, _ []byte) {
				details := `{"code":"server_is_overloaded","type":"server_error","message":"capacity token-\u0061","param":"model","extra":9007199254740993}`
				field := `"error":` + details
				if kind == "response.failed" {
					field = `"response":{"error":` + details + `}`
				}
				sendHTTPEvents(t, conn, fmt.Sprintf(`{"type":%q,"status":503,"headers":{"Retry-After":"7","Authorization":"private-credential","x-codex-turn-state":"private-state"},%s}`, kind, field))
			})
			_, proxy := newWebSocketProxy(t, upstream.URL, []*Account{testAccount("a", 0)})
			conn, _ := dialWebSocket(t, proxy.URL, nil)
			defer conn.CloseNow()
			writeWebSocketEvent(t, conn, map[string]string{"type": "response.create", "model": "m"})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, data, err := conn.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var event websocketFailureEvent
			if err := json.Unmarshal(data, &event); err != nil {
				t.Fatal(err)
			}
			if event.Status != 502 || event.UpstreamStatus != 503 || !event.Retryable || event.Error.Code != "server_is_overloaded" || event.Error.Message != "capacity [redacted]" || string(event.Error.Param) != `"model"` || websocketEventHeaders(event.Headers).Get("Retry-After") != "7" || !strings.Contains(string(data), "9007199254740993") || strings.Contains(string(data), "token-a") || strings.Contains(string(data), "private-") {
				t.Fatalf("failure = %s", data)
			}
			readCloseStatus(t, conn, websocket.StatusServiceRestart)
		})
	}
}

func TestWebSocketHandshakePreservesRejection(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "17")
		w.Header().Set("X-Codex-Rate-Limit-Reached-Type", "workspace_limit")
		w.WriteHeader(403)
		io.WriteString(w, `{"error":{"code":"usage_limit_reached","type":"usage_limit_reached","message":"quota token-a","param":"model"}}`)
	}))
	defer upstream.Close()
	_, proxy := newWebSocketProxy(t, upstream.URL, []*Account{testAccount("a", 0)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(proxy.URL, "http")+"/v1/responses", nil)
	if err == nil {
		conn.CloseNow()
		t.Fatal("rejected handshake succeeded")
	}
	if response == nil {
		t.Fatal(err)
	}
	body := readHTTPBody(t, response)
	if response.StatusCode != 429 || response.Header.Get("Retry-After") != "17" || !strings.Contains(body, "usage_limit_reached") || !strings.Contains(body, "quota [redacted]") || strings.Contains(body, "token-a") {
		t.Fatalf("status=%d body=%s", response.StatusCode, body)
	}
}

func TestSetupAuthenticationFailurePreservesMessage(t *testing.T) {
	for _, mode := range []string{"websocket", "http"} {
		t.Run(mode, func(t *testing.T) {
			refreshes := useOAuthRefreshServer(t)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(401)
				io.WriteString(w, `{"error":{"code":"invalid_api_key","type":"authentication_error","message":"original auth rejection token-a"}}`)
			}))
			defer upstream.Close()
			_, proxy := newWebSocketProxy(t, upstream.URL, []*Account{testAccount("a", 0)})
			var response *http.Response
			if mode == "http" {
				response = postResponse(t, proxy.URL, `{"model":"m"}`, nil)
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				conn, failed, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(proxy.URL, "http")+"/v1/responses", nil)
				if err == nil {
					conn.CloseNow()
					t.Fatal("rejected handshake succeeded")
				}
				response = failed
			}
			if response == nil {
				t.Fatal("missing failure response")
			}
			body := readHTTPBody(t, response)
			if response.StatusCode != 503 || !strings.Contains(body, "invalid_api_key") || !strings.Contains(body, "original auth rejection [redacted]") || strings.Contains(body, "token-a") || refreshes() != 1 {
				t.Fatalf("status=%d refreshes=%d body=%s", response.StatusCode, refreshes(), body)
			}
		})
	}
}
