package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/language/display"
)

const (
	countryLookupEndpoint = "https://api.country.is/"
	countryLookupTimeout  = 5 * time.Second
	countryLookupRetry    = 30 * time.Second
	countryLookupBodyMax  = 64 << 10
	countryLookupBatchMax = 100
)

type countryState struct {
	code    string
	ready   bool
	retryAt time.Time
}

type countryResolver struct {
	mu     sync.Mutex
	states map[string]countryState
}

type countryResult struct {
	IP      string `json:"ip"`
	Country string `json:"country"`
}

func (r *countryResolver) refresh(threads []ThreadSnapshot) {
	ips := r.queue(threads)
	if len(ips) == 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), countryLookupTimeout)
		defer cancel()
		codes, _ := fetchCountryCodes(ctx, http.DefaultClient, countryLookupEndpoint, ips)
		r.apply(ips, codes)
	}()
}

func (r *countryResolver) queue(threads []ThreadSnapshot) []string {
	live := make(map[string]struct{}, len(threads))
	for _, thread := range threads {
		if ip, ok := countryIP(thread.ClientIP); ok {
			live[ip] = struct{}{}
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.states == nil {
		r.states = map[string]countryState{}
	}
	for ip := range r.states {
		if _, ok := live[ip]; !ok {
			delete(r.states, ip)
		}
	}
	for _, state := range r.states {
		if !state.ready {
			return nil
		}
	}
	ips := make([]string, 0, len(live))
	now := time.Now()
	for ip := range live {
		if state, ok := r.states[ip]; ok && (state.retryAt.IsZero() || now.Before(state.retryAt)) {
			continue
		}
		r.states[ip] = countryState{}
		ips = append(ips, ip)
		if len(ips) == countryLookupBatchMax {
			break
		}
	}
	return ips
}

func (r *countryResolver) code(rawIP string) string {
	ip, ok := countryIP(rawIP)
	if !ok {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.states[ip]
	if !ok || !state.ready {
		return ""
	}
	return state.code
}

func (r *countryResolver) apply(ips []string, codes map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	for _, ip := range ips {
		if _, ok := r.states[ip]; ok {
			state := countryState{code: codes[ip], ready: true}
			if state.code == "" {
				state.retryAt = now.Add(countryLookupRetry)
			}
			r.states[ip] = state
		}
	}
}

func fetchCountryCodes(ctx context.Context, client *http.Client, endpoint string, ips []string) (map[string]string, error) {
	body, err := json.Marshal(ips)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("country lookup returned %s", response.Status)
	}
	var results []countryResult
	if err := json.NewDecoder(io.LimitReader(response.Body, countryLookupBodyMax)).Decode(&results); err != nil {
		return nil, err
	}
	codes := make(map[string]string, len(results))
	for _, result := range results {
		ip, ok := countryIP(result.IP)
		code := strings.ToUpper(result.Country)
		if ok && validCountryCode(code) {
			codes[ip] = code
		}
	}
	return codes, nil
}

func countryIP(raw string) (string, bool) {
	ip, err := netip.ParseAddr(raw)
	if err != nil {
		return "", false
	}
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return "", false
	}
	return ip.String(), true
}

func countryLabel(code string) string {
	if !validCountryCode(code) {
		return ""
	}
	flag := []rune{
		'🇦' + rune(code[0]-'A'),
		'🇦' + rune(code[1]-'A'),
	}
	return string(flag)
}

func countryName(code string) string {
	region, err := language.ParseRegion(code)
	if err != nil || !region.IsCountry() {
		return ""
	}
	return display.English.Regions().Name(region)
}

func validCountryCode(code string) bool {
	return len(code) == 2 && code[0] >= 'A' && code[0] <= 'Z' && code[1] >= 'A' && code[1] <= 'Z'
}
