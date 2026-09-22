package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type resetTestAPI struct {
	mu       sync.Mutex
	credits  map[string][]resetCredit
	restored map[string]bool
	consumed []string
}

func newResetTestAPI(t *testing.T, accounts ...*Account) *resetTestAPI {
	t.Helper()
	api := &resetTestAPI{
		credits:  map[string][]resetCredit{},
		restored: map[string]bool{},
	}
	for _, account := range accounts {
		_, credits, _ := account.bankedResets()
		api.credits[account.id()] = credits
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.mu.Lock()
		defer api.mu.Unlock()
		id := r.Header.Get("chatgpt-account-id")
		switch r.Method + " " + r.URL.Path {
		case "GET /rate-limit-reset-credits":
			json.NewEncoder(w).Encode(resetCreditsPayload{AvailableCount: int64(len(api.credits[id])), Credits: api.credits[id]})
		case "POST /rate-limit-reset-credits/consume":
			var request consumeResetCreditRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if request.CreditID == "" || request.RedeemRequestID != request.CreditID {
				t.Errorf("consume request = %+v", request)
			}
			api.consumed = append(api.consumed, request.CreditID)
			api.restored[id] = true
			api.credits[id] = nil
			json.NewEncoder(w).Encode(consumeResetCreditResponse{Code: "reset", WindowsReset: 2})
		case "GET /usage":
			used := 100.0
			if api.restored[id] {
				used = 0
			}
			json.NewEncoder(w).Encode(map[string]any{
				"rate_limit": map[string]any{
					"limit_reached":    used == 100,
					"primary_window":   map[string]any{"used_percent": used, "limit_window_seconds": 18000},
					"secondary_window": map[string]any{"used_percent": used, "limit_window_seconds": 604800},
				},
				"rate_limit_reset_credits": map[string]any{"available_count": len(api.credits[id])},
			})
		default:
			t.Errorf("unexpected reset API request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	previous := accountAPIBaseURL
	accountAPIBaseURL = upstream.URL
	t.Cleanup(func() {
		upstream.Close()
		accountAPIBaseURL = previous
	})
	return api
}

func (api *resetTestAPI) assertConsumed(t *testing.T, want ...string) {
	t.Helper()
	api.mu.Lock()
	defer api.mu.Unlock()
	if !reflect.DeepEqual(api.consumed, want) {
		t.Fatalf("consumed = %v, want %v", api.consumed, want)
	}
}

func testSpentAccountWithReset(id string, expiresAt time.Time) *Account {
	account := testAccount(id, 100)
	account.markSpent()
	adoptTestResetCredit(account, expiresAt)
	return account
}

func resetRecoveryServer(accounts ...*Account) *server {
	return &server{
		pool:   &Pool{accounts: accounts},
		client: http.DefaultClient,
		stats:  newStatsWithPrices(priceSnapshot{}),
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestNextResetCreditOrdersUndatedCreditsLast(t *testing.T) {
	now := time.Now()
	later := now.Add(72 * time.Hour)
	credits := []resetCredit{
		{ID: "undated", ResetType: "codex_rate_limits", Status: "available"},
		{ID: "dated", ResetType: "codex_rate_limits", Status: "available", ExpiresAt: &later},
	}
	if credit, ok := nextResetCredit(credits, now, 0); !ok || credit.ID != "dated" {
		t.Fatalf("credit = %q, %v, want dated", credit.ID, ok)
	}
	if credit, ok := nextResetCredit(credits[:1], now, 0); !ok || credit.ID != "undated" {
		t.Fatalf("credit = %q, %v, want undated", credit.ID, ok)
	}
	if credit, ok := expiringResetCredit(credits, now); ok {
		t.Fatalf("priority credit = %q, want none", credit.ID)
	}
}

func TestUsagePollingPreservesResetsWhenPoolIsExhausted(t *testing.T) {
	for _, force := range []bool{true, false} {
		name := "scheduled"
		if force {
			name = "full"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Now()
			later := testSpentAccountWithReset("later", now.Add(7*24*time.Hour))
			soon := testSpentAccountWithReset("soon", now.Add(time.Hour))
			api := newResetTestAPI(t, later, soon)
			s := resetRecoveryServer(later, soon)
			if force {
				s.pollAllUsage(context.Background())
			} else {
				s.pollDueUsage(context.Background(), time.Minute)
			}
			api.assertConsumed(t)
			if got := s.pool.route(nil, nil).account; got != nil {
				t.Fatalf("route after polling = %v, want none", got)
			}
		})
	}
}

func TestHTTPPreservesResetsWhenPoolIsExhausted(t *testing.T) {
	account := testSpentAccountWithReset("account", time.Now().Add(time.Hour))
	api := newResetTestAPI(t, account)
	s := resetRecoveryServer(account)
	router, err := newResponseAccountRouter(s, httptest.NewRequest(http.MethodPost, "/v1/responses", nil), websocketRoute{}, "", "")
	if err != nil {
		t.Fatal(err)
	}
	selected, err := router.selectHTTPAccount(websocketEnvelope{})
	if selected != nil || !errors.Is(err, errNoAccountAvailable) {
		t.Fatalf("selected = %v, error = %v, want no account available", selected, err)
	}
	api.assertConsumed(t)
}

func TestWebSocketPreservesResetsWhenPoolIsExhausted(t *testing.T) {
	account := testSpentAccountWithReset("account", time.Now().Add(time.Hour))
	api := newResetTestAPI(t, account)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("exhausted pool must not dial upstream")
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer upstream.Close()
	s := resetRecoveryServer(account)
	s.upstream = upstream.URL
	dialer, err := newResponsesWebSocketDialer(s, httptest.NewRequest(http.MethodGet, "/v1/responses", nil), websocketRoute{}, "", "")
	if err != nil {
		t.Fatal(err)
	}
	dial, response, err := dialer.dial()
	if dial != nil || response != nil || !errors.Is(err, errNoAccountAvailable) {
		t.Fatalf("dial = %v, response = %v, error = %v, want no account available", dial, response, err)
	}
	api.assertConsumed(t)
}

func TestWebSocketUsageLimitPreservesResets(t *testing.T) {
	for _, healthyFallback := range []bool{false, true} {
		name := "exhausted pool"
		if healthyFallback {
			name = "healthy fallback"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Now()
			spent := testSpentAccountWithReset("spent", now.Add(30*time.Minute))
			limited := testSpentAccountWithReset("limited", now.Add(2*time.Hour))
			limited.spent = false
			limited.RoutingMode = routingModePriority
			accounts := []*Account{spent, limited}
			var healthy *Account
			if healthyFallback {
				healthy = testAccount("healthy", 20)
				accounts = append(accounts, healthy)
			}
			api := newResetTestAPI(t, accounts...)
			var requestedMu sync.Mutex
			var requested []string
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				id := r.Header.Get("chatgpt-account-id")
				requestedMu.Lock()
				requested = append(requested, id)
				requestedMu.Unlock()
				if id == limited.id() {
					w.WriteHeader(http.StatusTooManyRequests)
					io.WriteString(w, `{"error":{"code":"usage_limit_reached"}}`)
					return
				}
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.CloseNow()
				conn.Read(r.Context())
			}))
			defer upstream.Close()
			s := resetRecoveryServer(accounts...)
			s.upstream = upstream.URL
			dialer, err := newResponsesWebSocketDialer(s, httptest.NewRequest(http.MethodGet, "/v1/responses", nil), websocketRoute{}, "", "")
			if err != nil {
				t.Fatal(err)
			}
			dial, response, err := dialer.dial()
			if dial != nil {
				defer dial.conn.CloseNow()
			}
			wantRequests := []string{limited.id()}
			if healthyFallback {
				if err != nil || response != nil || dial == nil || dial.account != healthy {
					t.Fatalf("dial = %v, response = %v, error = %v, want healthy account", dial, response, err)
				}
				wantRequests = append(wantRequests, healthy.id())
			} else {
				if dial != nil || response != nil || !errors.Is(err, errNoAccountAvailable) {
					t.Fatalf("dial = %v, response = %v, error = %v, want no account available", dial, response, err)
				}
				var rejection *websocketSetupError
				if !errors.As(err, &rejection) || rejection.status != http.StatusTooManyRequests || rejection.details.Code != "usage_limit_reached" {
					t.Fatalf("error = %v, want upstream usage limit", err)
				}
			}
			api.assertConsumed(t)
			if !limited.routingCandidate().spent || !spent.routingCandidate().spent {
				t.Fatal("exhausted accounts must remain spent")
			}
			requestedMu.Lock()
			defer requestedMu.Unlock()
			if !reflect.DeepEqual(requested, wantRequests) {
				t.Fatalf("upstream accounts = %v, want %v", requested, wantRequests)
			}
		})
	}
}
