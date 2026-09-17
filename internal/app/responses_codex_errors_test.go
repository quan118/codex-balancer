package app

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestCodexAppServerSurfacesUpstreamFailures(t *testing.T) {
	for _, transport := range []string{"websocket", "http"} {
		for _, scenario := range []string{"message too big", "policy", "capacity", "authentication"} {
			t.Run(transport+"/"+scenario, func(t *testing.T) {
				testCodexAppServerUpstreamFailure(t, transport == "websocket", scenario)
			})
		}
	}
}

func testCodexAppServerUpstreamFailure(t *testing.T, websockets bool, scenario string) {
	if os.Getenv("CODEX_BALANCER_TEST_CODEX") == "" {
		t.Skip("set CODEX_BALANCER_TEST_CODEX to run the app-server integration test")
	}
	var attempts atomic.Int64
	if scenario == "authentication" {
		useOAuthRefreshServer(t)
	}
	upstream := newWebSocketUpstream(t, func(_ string, conn *websocket.Conn, request websocketEnvelope) {
		if request.Generate != nil && !*request.Generate {
			sendHTTPEvents(t, conn, httpCreatedEvent, httpCompletedEvent)
			return
		}
		attempt := attempts.Add(1)
		switch scenario {
		case "message too big":
			conn.Close(websocket.StatusMessageTooBig, "request exceeds upstream limit")
			return
		case "policy":
			conn.Close(websocket.StatusPolicyViolation, "upstream policy detail")
			return
		case "capacity":
			if attempt <= 2 {
				sendHTTPEvents(t, conn, `{"type":"response.failed","response":{"error":{"code":"server_is_overloaded","message":"original capacity detail"}}}`)
				return
			}
		case "authentication":
			if attempt == 1 {
				sendHTTPEvents(t, conn, `{"type":"error","status":401,"error":{"code":"unauthorized","message":"original authentication detail"}}`)
				return
			}
		}
		sendHTTPEvents(t, conn, httpCreatedEvent, httpCompletedEvent)
	})
	defer upstream.Close()
	_, proxy := newWebSocketProxy(t, upstream.URL, []*Account{testAccount("a", 0)})
	turn, events := runCodexErrorTurn(t, proxy.URL, websockets)
	if scenario == "message too big" || scenario == "policy" || scenario == "capacity" && !websockets {
		want := "request_too_large"
		switch scenario {
		case "policy":
			want = "upstream policy detail"
		case "capacity":
			want = "capacity"
		}
		if turn["status"] != "failed" || attempts.Load() != 1 || !strings.Contains(events, want) || strings.Contains(events, "websocket closed by server") {
			t.Fatalf("attempts=%d turn=%v events=%s", attempts.Load(), turn, events)
		}
	} else {
		want := int64(3)
		if scenario == "authentication" {
			want = 2
		}
		if turn["status"] != "completed" || attempts.Load() != want {
			t.Fatalf("attempts=%d turn=%v events=%s", attempts.Load(), turn, events)
		}
		if scenario == "capacity" && !strings.Contains(events, "original capacity detail") {
			t.Fatalf("retry diagnostics lost original error: %s", events)
		}
	}
}

func runCodexErrorTurn(t *testing.T, url string, websockets bool) (map[string]any, string) {
	t.Helper()
	binary := os.Getenv("CODEX_BALANCER_TEST_CODEX")
	home, cwd := t.TempDir(), t.TempDir()
	config := fmt.Sprintf(`model = "gpt-5.4"
model_provider = "balancer"
[features]
plugins = false
unbounded_connection_retries = false
[model_providers.balancer]
name = "OpenAI"
base_url = %q
experimental_bearer_token = "test-key"
supports_websockets = %t
requires_openai_auth = false
request_max_retries = 2
stream_max_retries = 4
`, url+"/v1", websockets)
	if err := os.WriteFile(filepath.Join(home, "config.toml"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "app-server")
	cmd.Env = append(os.Environ(), "CODEX_HOME="+home)
	cmd.Dir = cwd
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr testLogBuffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { stdin.Close(); cmd.Process.Kill(); cmd.Wait() }()
	encoder := json.NewEncoder(stdin)
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), 8<<20)
	var events strings.Builder
	send := func(id int, method string, params any) {
		t.Helper()
		if err := encoder.Encode(map[string]any{"id": id, "method": method, "params": params}); err != nil {
			t.Fatal(err)
		}
	}
	read := func(method string, id float64) map[string]any {
		t.Helper()
		for scanner.Scan() {
			data := scanner.Bytes()
			events.Write(data)
			events.WriteByte('\n')
			var event map[string]any
			if err := json.Unmarshal(data, &event); err != nil {
				t.Fatal(err)
			}
			if event["error"] != nil {
				t.Fatalf("RPC error: %v", event)
			}
			if method != "" && event["method"] == method || id != 0 && event["id"] == id {
				return event
			}
		}
		t.Fatalf("app-server ended: %v; stderr: %s; events: %s", scanner.Err(), stderr.String(), events.String())
		return nil
	}
	send(1, "initialize", map[string]any{"clientInfo": map[string]string{"name": "balancer_test", "version": "1"}})
	read("", 1)
	if err := encoder.Encode(map[string]any{"method": "initialized", "params": map[string]any{}}); err != nil {
		t.Fatal(err)
	}
	send(2, "thread/start", map[string]any{"cwd": cwd, "approvalPolicy": "never", "sandbox": "read-only"})
	started := read("", 2)
	thread := started["result"].(map[string]any)["thread"].(map[string]any)["id"]
	send(3, "turn/start", map[string]any{"threadId": thread, "input": []any{map[string]string{"type": "text", "text": "test upstream errors"}}})
	completed := read("turn/completed", 0)
	return completed["params"].(map[string]any)["turn"].(map[string]any), events.String()
}
