package app

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestResponsesHTTPRequiresJSONBody(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	response := httptest.NewRecorder()
	new(server).routes().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestServerAcceptsMultipleDatabaseAPIKeysAndSeesChanges(t *testing.T) {
	store, err := openStateStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for index, secret := range []string{"secret-a", "secret-b"} {
		if err := store.addAPIKey(storedAPIKey{Name: fmt.Sprintf("client-%d", index), Secret: secret, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	server := &server{lookupAPIKey: store.apiKeyName}
	identityRequest := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	identityRequest.Header.Set("Authorization", "Bearer secret-a")
	identity, valid := server.authorizeAPIKey(identityRequest)
	if !valid || identity.name != "client-0" || identity.suffix != "t-a" {
		t.Fatalf("API key identity = %+v, %t", identity, valid)
	}
	assertStatus := func(secret string, want int) {
		t.Helper()
		request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
		if secret != "" {
			request.Header.Set("Authorization", "Bearer "+secret)
		}
		response := httptest.NewRecorder()
		server.routes().ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("key %q status = %d, want %d", secret, response.Code, want)
		}
	}
	assertStatus("secret-a", http.StatusOK)
	assertStatus("secret-b", http.StatusOK)
	assertStatus("wrong", http.StatusUnauthorized)
	assertStatus("", http.StatusUnauthorized)

	if err := store.addAPIKey(storedAPIKey{Name: "client-2", Secret: "secret-c", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	assertStatus("secret-c", http.StatusOK)
	if revoked, err := store.revokeAPIKey("client-0", time.Now()); err != nil || !revoked {
		t.Fatalf("revoke = %t, error = %v", revoked, err)
	}
	assertStatus("secret-a", http.StatusUnauthorized)
}

func TestRetryAfterHeaderAcceptsHTTPDate(t *testing.T) {
	want := time.Now().UTC().Add(5 * time.Minute).Truncate(time.Second)
	headers := http.Header{"Retry-After": {want.Format(http.TimeFormat)}}
	if got := retryAfterHeader(headers); !got.Equal(want) {
		t.Fatalf("retry after = %s, want %s", got, want)
	}
}

func TestRateLimitedCooldownIgnoresUsageWindowReset(t *testing.T) {
	account := testAccount("a", 0)
	reset := time.Now().Add(48 * time.Hour)
	account.rateLimited(http.Header{
		"X-Codex-Primary-Used-Percent": {"95"}, "X-Codex-Primary-Reset-At": {fmt.Sprint(reset.Unix())},
	}, 0)
	if cooldown := account.routingCandidate().cooldown; cooldown.After(time.Now().Add(2 * minCooldown)) {
		t.Fatalf("transient cooldown %s follows the usage window instead of a short backoff", time.Until(cooldown))
	}
	account.rateLimited(http.Header{"Retry-After": {"20"}}, 0)
	if cooldown := account.routingCandidate().cooldown; cooldown.Before(time.Now().Add(15*time.Second)) || cooldown.After(time.Now().Add(25*time.Second)) {
		t.Fatalf("cooldown %s ignores Retry-After", time.Until(cooldown))
	}
	account.rateLimited(http.Header{"Retry-After": {"86400"}}, 0)
	if cooldown := account.routingCandidate().cooldown; cooldown.After(time.Now().Add(maxCooldown + time.Minute)) {
		t.Fatalf("cooldown %s exceeds the cap", time.Until(cooldown))
	}
}

func TestWebSocketUpgradeBudgetFailsFast(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer slow.Close()
	srv, proxy := newWebSocketProxy(t, slow.URL, []*Account{testAccount("a", 0)})
	srv.upgradeWait = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	conn, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(proxy.URL, "http")+"/v1/responses", nil)
	if err == nil {
		conn.CloseNow()
		t.Fatal("upgrade succeeded against a stalled upstream")
	}
	if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("response = %v", resp)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "upgrade_timeout") {
		t.Fatalf("body = %s", body)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("upgrade refusal took %s", elapsed)
	}
	if !srv.pool.all()[0].routingCandidate().cooldown.IsZero() {
		t.Fatal("budget overrun penalized the account")
	}
}
