package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func testUsageAccount(id string, primary, secondary float64) *Account {
	a := testAccount(id, primary)
	a.primary.minutes = 300
	a.secondary.usedPercent = secondary
	a.secondary.minutes = 10080
	return a
}

func TestClientUsageIncludesExhaustedAndCoolingAccounts(t *testing.T) {
	a := testUsageAccount("a", 20, 40)
	b := testUsageAccount("b", 100, 80)
	b.spent = true
	c := testUsageAccount("c", 0, 0)
	c.cooldown = time.Now().Add(time.Minute)
	paused := testUsageAccount("paused", 100, 100)
	paused.Paused = true
	reauth := testUsageAccount("reauth", 100, 100)
	reauth.Reauth = "expired"
	disabled := testUsageAccount("disabled", 100, 100)
	disabled.planType = "business"
	unknown := testAccount("unknown", 0)
	unknown.primary, unknown.secondary = window{}, window{}
	p := &Pool{accounts: []*Account{a, b, c, paused, reauth, disabled, unknown}}
	usage := p.clientUsage()
	if usage.Primary == nil || usage.Primary.UsedPercent != 40 || usage.Primary.Minutes != 300 {
		t.Fatalf("primary = %+v", usage.Primary)
	}
	if usage.Secondary == nil || usage.Secondary.UsedPercent != 40 || usage.Secondary.Minutes != 10080 {
		t.Fatalf("secondary = %+v", usage.Secondary)
	}
	if a.primary.usedPercent != 20 || b.primary.usedPercent != 100 {
		t.Fatal("pool reporting changed account usage")
	}
}

func TestClientUsageUnknownAndIncompatibleWindows(t *testing.T) {
	for _, scenario := range []string{"empty", "unknown", "invalid", "incompatible", "partial", "clamped"} {
		t.Run(scenario, func(t *testing.T) {
			a := testUsageAccount("a", 20, 40)
			b := testUsageAccount("b", 80, 60)
			p := &Pool{accounts: []*Account{a, b}}
			wantPrimary, wantSecondary := -1.0, 50.0
			switch scenario {
			case "empty":
				p.accounts = nil
				wantSecondary = -1
			case "unknown":
				a.primary, b.primary = window{}, window{}
			case "invalid":
				a.primary.usedPercent, b.primary.usedPercent = math.NaN(), math.Inf(1)
			case "incompatible":
				b.primary.minutes = 60
			case "partial":
				a.primary, b.secondary = window{}, window{}
				wantPrimary, wantSecondary = 80, 40
			case "clamped":
				a.primary.usedPercent, b.primary.usedPercent = -10, 130
				wantPrimary = 50
			}
			usage := p.clientUsage()
			for _, check := range []struct {
				window *clientUsageWindow
				want   float64
			}{{usage.Primary, wantPrimary}, {usage.Secondary, wantSecondary}} {
				if check.want < 0 {
					if check.window != nil {
						t.Fatalf("unknown window = %+v", check.window)
					}
				} else if check.window == nil || check.window.UsedPercent != check.want {
					t.Fatalf("window = %+v, want %g", check.window, check.want)
				}
			}
		})
	}
}

func TestClientUsageHeadersReplaceOnlyDefaultQuota(t *testing.T) {
	headers := http.Header{}
	for name, value := range map[string]string{
		"x-codex-primary-used-percent": "99", "x-codex-primary-reset-at": "1700000000",
		"x-codex-secondary-reset-after-seconds": "30", "x-codex-credits-balance": "123",
		"x-codex-secondary-primary-used-percent": "75", "Retry-After": "7", "X-Request-Id": "request",
	} {
		headers.Set(name, value)
	}
	usage := clientUsage{Primary: &clientUsageWindow{UsedPercent: 50, Minutes: 300}}
	usage.writeHeaders(headers)
	if headers.Get("x-codex-primary-used-percent") != "50" || headers.Get("x-codex-primary-window-minutes") != "300" {
		t.Fatalf("pooled headers = %v", headers)
	}
	for _, name := range []string{"x-codex-primary-reset-at", "x-codex-secondary-reset-after-seconds", "x-codex-credits-balance"} {
		if headers.Get(name) != "" {
			t.Fatalf("account header retained: %s", name)
		}
	}
	if headers.Get("x-codex-secondary-primary-used-percent") != "75" || headers.Get("Retry-After") != "7" || headers.Get("X-Request-Id") != "request" {
		t.Fatalf("unrelated headers changed: %v", headers)
	}
	clientUsage{}.writeHeaders(headers)
	if headers.Get("x-codex-primary-used-percent") != "" || headers.Get("x-codex-limit-name") != "" {
		t.Fatalf("unknown pool retained usage: %v", headers)
	}
}

func TestClientUsageEventPreservesAccountAccounting(t *testing.T) {
	a := testUsageAccount("a", 20, 40)
	b := testUsageAccount("b", 80, 60)
	p := &Pool{accounts: []*Account{a, b}}
	data := []byte(`{"type":"response.created","headers":{"x-codex-primary-used-percent":"40","x-codex-primary-window-minutes":"300","x-codex-secondary-used-percent":"80","x-codex-secondary-window-minutes":"10080","Retry-After":"7"},"response":{"id":"r","unknown":9007199254740993}}`)
	var event websocketEnvelope
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatal(err)
	}
	result := p.clientUsageEvent(a, data, event)
	var reported websocketEnvelope
	if err := json.Unmarshal(result, &reported); err != nil {
		t.Fatal(err)
	}
	headers := websocketEventHeaders(reported.Headers)
	if headers.Get("x-codex-primary-used-percent") != "60" || headers.Get("x-codex-secondary-used-percent") != "70" || headers.Get("Retry-After") != "7" {
		t.Fatalf("reported headers = %v", headers)
	}
	if websocketEventHeaders(event.Headers).Get("x-codex-primary-used-percent") != "40" || a.primary.usedPercent != 40 || a.secondary.usedPercent != 80 || b.primary.usedPercent != 80 {
		t.Fatal("pooled usage replaced upstream accounting")
	}
	if !strings.Contains(string(result), "9007199254740993") {
		t.Fatalf("response changed: %s", result)
	}
}

func TestClientUsageRateLimitEvent(t *testing.T) {
	a := testUsageAccount("a", 20, 40)
	b := testUsageAccount("b", 80, 60)
	p := &Pool{accounts: []*Account{a, b}}
	data := []byte(`{"type":"codex.rate_limits","plan_type":"pro","rate_limits":{"allowed":false,"limit_reached":true,"primary":{"used_percent":100,"window_minutes":300,"reset_at":1900000000},"secondary":{"used_percent":80,"window_minutes":10080}},"credits":{"balance":"123"},"unknown":9007199254740993}`)
	result := p.clientUsageEvent(a, data, websocketEnvelope{Type: "codex.rate_limits"})
	assertClientUsageEvent(t, result, 90, 70)
	if a.primary.usedPercent != 100 || a.secondary.usedPercent != 80 || a.primary.resetsAt.Unix() != 1900000000 || b.primary.usedPercent != 80 {
		t.Fatal("event usage was not adopted by its account")
	}
	if !strings.Contains(string(result), "9007199254740993") || strings.Contains(string(result), "limit_reached") {
		t.Fatalf("unexpected event fields: %s", result)
	}
	for _, name := range []string{"codex_secondary", "codex_other"} {
		separate := []byte(fmt.Sprintf(`{"type":"codex.rate_limits","metered_limit_name":%q,"rate_limits":{"primary":{"used_percent":5,"window_minutes":60}}}`, name))
		if got := p.clientUsageEvent(a, separate, websocketEnvelope{Type: "codex.rate_limits"}); string(got) != string(separate) || a.primary.usedPercent != 100 {
			t.Fatalf("separate quota changed default usage: %s", got)
		}
	}
}

func TestAccountObservesDefaultSecondaryQuota(t *testing.T) {
	a := testUsageAccount("a", 20, 40)
	headers := http.Header{}
	headers.Set("x-codex-secondary-used-percent", "60")
	headers.Set("x-codex-secondary-window-minutes", "10080")
	headers.Set("x-codex-secondary-primary-used-percent", "99")
	a.observe(headers)
	if a.secondary.usedPercent != 60 || a.secondary.minutes != 10080 {
		t.Fatalf("secondary = %+v", a.secondary)
	}
}

func TestClientUsagePartialEventPreservesWindowMetadata(t *testing.T) {
	a := testUsageAccount("a", 20, 40)
	a.primary.resetsAt = time.Unix(1900000000, 0)
	b := testUsageAccount("b", 80, 60)
	p := &Pool{accounts: []*Account{a, b}}
	data := []byte(`{"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":40}}}`)
	result := p.clientUsageEvent(a, data, websocketEnvelope{Type: "codex.rate_limits"})
	assertClientUsageEvent(t, result, 60, 50)
	if a.primary.minutes != 300 || a.primary.resetsAt.Unix() != 1900000000 || a.secondary.usedPercent != 40 {
		t.Fatal("partial quota event replaced known window metadata")
	}
}

func assertClientUsageEvent(t *testing.T, data []byte, primary, secondary float64) {
	t.Helper()
	var event struct {
		Usage clientUsage `json:"rate_limits"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatal(err)
	}
	if event.Usage.Primary == nil || event.Usage.Primary.UsedPercent != primary || event.Usage.Secondary == nil || event.Usage.Secondary.UsedPercent != secondary {
		t.Fatalf("usage event = %s, want %g/%g", data, primary, secondary)
	}
	for _, field := range []string{`"reset_at"`, `"credits"`, `"plan_type"`} {
		if strings.Contains(string(data), field) {
			t.Fatalf("account-specific field %s in %s", field, data)
		}
	}
}

func TestWebSocketReportsClientUsageAcrossRoutes(t *testing.T) {
	for _, routed := range []string{"a", "b"} {
		t.Run(routed, func(t *testing.T) {
			a := testUsageAccount("a", 20, 40)
			b := testUsageAccount("b", 80, 60)
			selected := a
			if routed == "b" {
				selected = b
			}
			selected.RoutingMode = routingModePriority
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("Chatgpt-Account-Id"); got != routed {
					t.Errorf("routed to %s, want %s", got, routed)
				}
				w.Header().Set("x-codex-primary-used-percent", fmt.Sprint(selected.primary.usedPercent))
				w.Header().Set("x-codex-primary-window-minutes", "300")
				w.Header().Set("x-codex-primary-reset-at", "1900000000")
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.CloseNow()
				if _, _, err := conn.Read(r.Context()); err != nil {
					return
				}
				sendHTTPEvents(t, conn, `{"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":40,"window_minutes":300},"secondary":{"used_percent":80,"window_minutes":10080}}}`, httpCreatedEvent, httpCompletedEvent)
				conn.Read(r.Context())
			}))
			defer upstream.Close()
			_, proxy := newWebSocketProxy(t, upstream.URL, []*Account{a, b})
			conn, response := dialWebSocket(t, proxy.URL, nil)
			defer conn.CloseNow()
			if response.Header.Get("x-codex-primary-used-percent") != "50" || response.Header.Get("x-codex-secondary-used-percent") != "50" || response.Header.Get("x-codex-primary-reset-at") != "" {
				t.Fatalf("handshake usage = %v", response.Header)
			}
			writeWebSocketEvent(t, conn, map[string]any{"type": "response.create", "model": "m"})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, data, err := conn.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			primary, secondary := 60.0, 70.0
			if routed == "b" {
				primary, secondary = 30, 60
			}
			assertClientUsageEvent(t, data, primary, secondary)
			if readWebSocketEvent(t, conn).Type != "response.created" || readWebSocketEvent(t, conn).Type != "response.completed" {
				t.Fatal("response did not complete")
			}
		})
	}
}

func TestHTTPReportsClientUsage(t *testing.T) {
	for _, mode := range []string{"stream", "buffered", "json"} {
		t.Run(mode, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("x-codex-primary-used-percent", "20")
				w.Header().Set("x-codex-primary-window-minutes", "300")
				if mode == "json" {
					w.Header().Set("Content-Type", "application/json")
					io.WriteString(w, `{"id":"r","status":"completed","output":[]}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				for _, event := range []string{`{"type":"codex.rate_limits","plan_type":"pro","rate_limits":{"primary":{"used_percent":40,"window_minutes":300,"reset_at":1900000000},"secondary":{"used_percent":80,"window_minutes":10080}}}`, httpCreatedEvent, httpCompletedEvent} {
					fmt.Fprintf(w, "data: %s\n\n", event)
				}
			}))
			defer upstream.Close()
			a, b := testUsageAccount("a", 20, 40), testUsageAccount("b", 80, 60)
			srv, proxy := newWebSocketProxy(t, upstream.URL, []*Account{a, b})
			srv.admission = newAdmissionGate(1)
			response := postResponse(t, proxy.URL, fmt.Sprintf(`{"model":"m","stream":%t}`, mode == "stream"), nil)
			body := readHTTPBody(t, response)
			primary, secondary := "60", "70"
			if mode == "json" {
				primary, secondary = "50", "50"
			}
			if response.StatusCode != 200 || response.Header.Get("x-codex-primary-used-percent") != primary || response.Header.Get("x-codex-secondary-used-percent") != secondary {
				t.Fatalf("response = %d %v %s", response.StatusCode, response.Header, body)
			}
			if mode == "stream" {
				found := false
				for _, line := range strings.Split(body, "\n") {
					if strings.HasPrefix(line, "data: ") && strings.Contains(line, `"codex.rate_limits"`) {
						assertClientUsageEvent(t, []byte(strings.TrimPrefix(line, "data: ")), 60, 70)
						found = true
					}
				}
				if !found {
					t.Fatalf("missing pooled event: %s", body)
				}
			} else if !json.Valid([]byte(body)) {
				t.Fatalf("invalid response: %s", body)
			}
			assertHTTPClean(t, srv)
		})
	}
}
