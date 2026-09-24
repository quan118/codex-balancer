package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/mail"
	"slices"
	"strings"
	"sync"
	"time"
)

type fastMode string

const (
	fastModeDefault fastMode = "default"
	fastModeOn      fastMode = "on"
	fastModeOff     fastMode = "off"
)

func (m fastMode) valid() bool {
	return m == fastModeDefault || m == fastModeOn || m == fastModeOff
}

func (m fastMode) label() string {
	switch m {
	case fastModeOn:
		return "force fast"
	case fastModeOff:
		return "force standard"
	default:
		return "default"
	}
}

func (m fastMode) description() string {
	switch m {
	case fastModeOn:
		return "Forces fast mode for all requests, overriding the client's preference."
	case fastModeOff:
		return "Forces standard mode for all requests, overriding the client's preference."
	default:
		return "Uses the client's fast mode preference for each request."
	}
}

// Preserve all request fields, including fields unknown to websocketEnvelope.
func (m fastMode) override(data []byte, tier string) ([]byte, string, error) {
	if m != fastModeOn && m != fastModeOff {
		return data, tier, nil
	}
	tier = "default"
	if m == fastModeOn {
		tier = serviceTierPriority
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, "", err
	}
	fields["service_tier"], _ = json.Marshal(tier)
	data, err := json.Marshal(fields)
	return data, tier, err
}

// Closing a policy's notification channel reaches every relay that captured it,
// even a relay that has not yet entered its event loop.
type fastModePolicy struct {
	mu      sync.Mutex
	mode    fastMode
	changed chan struct{}
}

func (p *fastModePolicy) snapshot() (fastMode, <-chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init()
	return p.mode, p.changed
}

func (p *fastModePolicy) init() {
	if p.changed == nil {
		p.mode = fastModeDefault
		p.changed = make(chan struct{})
	}
}

func (p *fastModePolicy) set(mode fastMode) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init()
	if p.mode == mode {
		return false
	}
	p.mode = mode
	close(p.changed)
	p.changed = make(chan struct{})
	return true
}

func (s *server) reloadSettings() error {
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()
	value, err := s.pool.store.raw.FastMode()
	if err != nil {
		return err
	}
	mode := fastMode(value)
	if !mode.valid() {
		return fmt.Errorf("invalid stored fast mode %q", value)
	}
	if s.fastMode.set(mode) {
		s.log.Info("fast mode applied; restarting existing websockets", "fast_mode", mode)
	}
	value, err = s.pool.store.raw.BlockedEmails()
	if err != nil {
		return err
	}
	var emails []string
	if err := json.Unmarshal([]byte(value), &emails); err != nil {
		return fmt.Errorf("decode blocked emails: %w", err)
	}
	newlyBlocked, changed := s.pool.setBlockedEmails(emails)
	for _, id := range newlyBlocked {
		s.invalidateAccount(id, routingReasonOwnerBlocked)
	}
	if changed && s.catalog != nil {
		s.catalog.invalidate()
	}
	return nil
}

func parseBlockedEmails(value string) ([]string, error) {
	emails := []string{}
	seen := map[string]bool{}
	for _, raw := range strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == '\n' }) {
		email := strings.ToLower(strings.TrimSpace(raw))
		if email == "" {
			continue
		}
		address, err := mail.ParseAddress(email)
		if err != nil || address.Address != email || strings.ContainsAny(email, " \t\r") {
			return nil, fmt.Errorf("invalid account email %q", raw)
		}
		if !seen[email] {
			seen[email] = true
			emails = append(emails, email)
		}
	}
	slices.Sort(emails)
	return emails, nil
}

func (s *server) saveBlockedEmails(value string) error {
	emails, err := parseBlockedEmails(value)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(emails)
	if err != nil {
		return err
	}
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()
	if err := s.pool.store.raw.SetBlockedEmails(string(encoded)); err != nil {
		return err
	}
	newlyBlocked, changed := s.pool.setBlockedEmails(emails)
	for _, id := range newlyBlocked {
		s.invalidateAccount(id, routingReasonOwnerBlocked)
	}
	if changed && s.catalog != nil {
		s.catalog.invalidate()
	}
	return nil
}

func (s *server) watchSettings(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.reloadSettings(); err != nil {
				s.log.Warn("settings watch failed", "error", err)
			}
		}
	}
}

func (s *server) saveFastMode(mode fastMode) error {
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()
	if !mode.valid() {
		return fmt.Errorf("invalid fast mode %q", mode)
	}
	if err := s.pool.store.raw.SetFastMode(string(mode)); err != nil {
		return err
	}
	s.fastMode.set(mode)
	return nil
}
