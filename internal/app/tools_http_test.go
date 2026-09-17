package app

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestToolEndpointsProxyWithPoolCredentials(t *testing.T) {
	var mu sync.Mutex
	calls := []string{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		account := r.Header.Get("chatgpt-account-id")
		mu.Lock()
		calls = append(calls, account+" "+r.URL.Path)
		mu.Unlock()
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer token-"+account || r.Header.Get("Cookie") != "" || r.Header.Get("X-Codex-Window-Id") != "w1" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("upstream request = %s %v", r.Method, r.Header)
		}
		w.Header().Set("Content-Type", "application/json")
		if account == "spent" {
			w.WriteHeader(http.StatusTooManyRequests)
			io.WriteString(w, `{"error":{"type":"usage_limit_reached","message":"quota"}}`)
			return
		}
		w.Header().Set("X-Request-Id", "req-1")
		fmt.Fprintf(w, `{"echo":%s,"path":%q}`, body, r.URL.Path)
	}))
	defer upstream.Close()
	spent, fresh := testAccount("spent", 0), testAccount("fresh", 50)
	srv, proxy := newWebSocketProxy(t, upstream.URL, []*Account{spent, fresh})
	post := func(path, key string) (*http.Response, string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, proxy.URL+path, strings.NewReader(`{"model":"m","input":"find"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Cookie", "private")
		req.Header.Set("X-Codex-Window-Id", "w1")
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, string(body)
	}
	for _, path := range toolEndpoints {
		resp, body := post(path, "")
		want := fmt.Sprintf(`"path":%q`, strings.TrimPrefix(path, "/v1"))
		if resp.StatusCode != http.StatusOK || !strings.Contains(body, want) || !strings.Contains(body, `"input":"find"`) || resp.Header.Get("X-Request-Id") != "req-1" {
			t.Fatalf("%s: status=%d headers=%v body=%s", path, resp.StatusCode, resp.Header, body)
		}
	}
	if !spent.routingCandidate().spent {
		t.Fatal("usage-limited account was not marked spent")
	}
	mu.Lock()
	got := fmt.Sprint(calls)
	mu.Unlock()
	if got != "[spent /alpha/search fresh /alpha/search fresh /images/generations fresh /images/edits]" {
		t.Fatalf("upstream calls = %s", got)
	}
	srv.lookupAPIKey = func(string) (string, bool, error) { return "", false, nil }
	if resp, _ := post("/v1/alpha/search", "wrong"); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d", resp.StatusCode)
	}
}

func TestToolEndpointsReturnLastRejectionWhenPoolExhausted(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "9")
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":{"code":"rate_limit_exceeded","message":"slow down token-`+r.Header.Get("chatgpt-account-id")+`"}}`)
	}))
	defer upstream.Close()
	a, b := testAccount("a", 0), testAccount("b", 10)
	_, proxy := newWebSocketProxy(t, upstream.URL, []*Account{a, b})
	resp, err := http.Post(proxy.URL+"/v1/alpha/search", "application/json", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") != "9" || !strings.Contains(string(body), "rate_limit_exceeded") || strings.Contains(string(body), "token-") {
		t.Fatalf("status=%d headers=%v body=%s", resp.StatusCode, resp.Header, body)
	}
	for _, account := range []*Account{a, b} {
		if account.routingCandidate().cooldown.IsZero() {
			t.Fatalf("%s not cooled after a transient 429", account.id())
		}
	}
}
