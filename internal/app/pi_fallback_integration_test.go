package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Opt-in: read-only pi checkout with installed dependencies and Node with zstd.
// No pi application/config/auth/plugins are loaded; inference is local-only.
func TestPiWebSocketToHTTPFallback(t *testing.T) {
	checkout := os.Getenv("CODEX_BALANCER_TEST_PI")
	if checkout == "" {
		t.Skip("set CODEX_BALANCER_TEST_PI to a pi checkout with installed dependencies")
	}
	if _, err := os.Stat(filepath.Join(checkout, "packages/ai/src/api/openai-codex-responses.ts")); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"", "/v1"} {
		t.Run("base"+prefix, func(t *testing.T) {
			var requests, connections, posts atomic.Int64
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				connections.Add(1)
				if r.Header.Get("Chatgpt-Account-Id") != "a" {
					t.Error("portable fallback moved a healthy retained owner")
				}
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.CloseNow()
				for {
					_, data, err := conn.Read(r.Context())
					if err != nil {
						return
					}
					fields, err := responseObject(data)
					if err != nil {
						t.Error(err)
						return
					}
					n := requests.Add(1)
					if n == 2 {
						if string(fields["previous_response_id"]) != `"resp-first"` {
							t.Error("pi did not send an incremental WebSocket turn")
						}
						sendHTTPEvents(t, conn, `{"type":"response.created","response":{"id":"resp-interrupted","model":"gpt-6-astra","status":"in_progress"}}`,
							`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg-partial","role":"assistant","status":"in_progress","content":[]}}`,
							`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg-partial","delta":"PARTIAL"}`)
						<-release // client proves it received partial output before closure
						conn.Close(websocket.StatusGoingAway, "synthetic interruption")
						return
					}
					if n >= 3 {
						if fields["previous_response_id"] != nil {
							t.Error("HTTP fallback forwarded socket-scoped response ID")
						}
						for _, value := range []string{"FIRST_TURN", "FIRST_ANSWER", "opaque-history", "INTERRUPTED_TURN", "CONTINUE_TURN"} {
							if !strings.Contains(string(fields["input"]), value) {
								t.Errorf("fallback lost %s", value)
							}
						}
					}
					text, id := "FIRST_ANSWER", "resp-first"
					if n >= 3 {
						text, id = "RECOVERED", fmt.Sprintf("resp-%d", n)
					}
					sendPiFallbackTurn(t, conn, id, text, n == 1)
				}
			}))
			defer upstream.Close()
			a, b := testAccount("a", 0), testAccount("b", 20)
			srv := newTestServer(t, []*Account{a, b})
			srv.upstream = upstream.URL
			srv.admission = newAdmissionGate(4)
			key, err := generateAPIKey()
			if err != nil {
				t.Fatal(err)
			}
			if err := srv.pool.store.addAPIKey(storedAPIKey{Name: "pi-test", Secret: key, CreatedAt: time.Now()}); err != nil {
				t.Fatal(err)
			}
			srv.lookupAPIKey = srv.pool.store.apiKeyName
			handler := srv.routes()
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/_test/release" {
					b.mu.Lock()
					b.RoutingMode = routingModePriority
					b.mu.Unlock()
					unblock()
					w.WriteHeader(204)
					return
				}
				if r.Method == http.MethodPost {
					posts.Add(1)
					if r.URL.Path != prefix+"/codex/responses" || r.Header.Get("Content-Encoding") != "zstd" {
						t.Errorf("fallback request=%s %s encoding=%s", r.Method, r.URL.Path, r.Header.Get("Content-Encoding"))
					}
				}
				handler.ServeHTTP(w, r)
			}))
			defer proxy.Close()
			dir, home := t.TempDir(), t.TempDir()
			if err := os.Symlink(checkout, filepath.Join(dir, "pi")); err != nil {
				t.Fatal(err)
			}
			script, err := os.ReadFile("../../scripts/pi-fallback-smoke.mjs")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "smoke.mjs"), script, 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "node", filepath.Join(dir, "smoke.mjs"))
			command.Dir = dir
			command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "XDG_CONFIG_HOME=" + home, "XDG_DATA_HOME=" + home, "XDG_CACHE_HOME=" + home, "PI_CODING_AGENT_DIR=" + home, "CODEX_BALANCER_TEST_URL=" + proxy.URL + prefix, "CODEX_BALANCER_TEST_KEY=" + key}
			output, err := command.CombinedOutput()
			unblock()
			if err != nil {
				t.Fatalf("pi adapter: %v\n%s", err, output)
			}
			t.Log(strings.TrimSpace(string(output)))
			assertHTTPClean(t, srv)
			if requests.Load() != 4 || connections.Load() != 3 || posts.Load() != 2 {
				t.Fatalf("requests=%d upstream sockets=%d HTTP posts=%d", requests.Load(), connections.Load(), posts.Load())
			}
			usage, err := srv.pool.store.apiKeyUsage()
			if err != nil || usage["pi-test"].TotalTokens != 42 || srv.stats.snapshot().Turns != 4 {
				t.Fatalf("usage=%v turns=%d err=%v", usage, srv.stats.snapshot().Turns, err)
			}
		})
	}
}

func sendPiFallbackTurn(t *testing.T, conn *websocket.Conn, id, text string, reasoning bool) {
	t.Helper()
	sendHTTPEvents(t, conn, fmt.Sprintf(`{"type":"response.created","response":{"id":%q,"model":"gpt-6-astra","status":"in_progress"}}`, id))
	index := 0
	if reasoning {
		sendHTTPEvents(t, conn, `{"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs-first","summary":[]}}`,
			`{"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"rs-first","summary":[],"encrypted_content":"opaque-history"}}`)
		index = 1
	}
	item, _ := json.Marshal(map[string]any{"type": "message", "id": "msg-" + id, "role": "assistant", "status": "completed", "content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}}})
	sendHTTPEvents(t, conn,
		fmt.Sprintf(`{"type":"response.output_item.added","output_index":%d,"item":{"type":"message","id":%q,"role":"assistant","status":"in_progress","content":[]}}`, index, "msg-"+id),
		fmt.Sprintf(`{"type":"response.output_text.delta","output_index":%d,"content_index":0,"item_id":%q,"delta":%q}`, index, "msg-"+id, text),
		fmt.Sprintf(`{"type":"response.output_item.done","output_index":%d,"item":%s}`, index, item),
		fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"model":"gpt-6-astra","status":"completed","output":[],"usage":{"input_tokens":10,"input_tokens_details":{"cached_tokens":8},"output_tokens":4,"total_tokens":14}}}`, id))
}
