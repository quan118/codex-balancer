package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type responsesWebSocketRelay struct {
	server                *server
	downstream            responsesDownstream
	request               *http.Request
	apiKey                apiKeyIdentity
	route                 websocketRoute
	thread                string
	ctx                   context.Context
	cancel                context.CancelFunc
	messages              chan websocketMessage
	readers               []<-chan struct{}
	invalidations         chan websocketInvalidation
	liveThreads           map[string]struct{}
	current               *websocketDial
	turns                 []websocketTurn
	pending               []websocketMessage
	pendingBytes          int
	pinned                bool
	socketID              uint64
	fastMode              fastMode
	policyChanged         <-chan struct{}
	idleTimeout           time.Duration
	messageLimit          int64
	generationSpan        trace.Span
	upstreamOpened        time.Time
	lastUpstreamEvent     time.Time
	lastUpstreamKind      string
	receivedUpstreamBytes int64
}

type websocketInvalidation struct {
	account string
	reason  string
}

func newResponsesWebSocketRelay(s *server, downstream responsesDownstream, request *http.Request, initial *websocketDial, route websocketRoute, apiKey apiKeyIdentity, mode fastMode, changed <-chan struct{}) *responsesWebSocketRelay {
	ctx, cancel := context.WithCancel(request.Context())
	if s.ctx != nil {
		stop := context.AfterFunc(s.ctx, cancel)
		cancelContext := cancel
		cancel = func() { stop(); cancelContext() }
	}
	return &responsesWebSocketRelay{
		server:         s,
		messageLimit:   maxWebSocketMessage,
		fastMode:       mode,
		policyChanged:  changed,
		downstream:     downstream,
		request:        request,
		apiKey:         apiKey,
		route:          route,
		thread:         route.key(),
		ctx:            ctx,
		cancel:         cancel,
		messages:       make(chan websocketMessage, 8),
		invalidations:  make(chan websocketInvalidation, 4),
		liveThreads:    map[string]struct{}{},
		current:        initial,
		upstreamOpened: time.Now(),
	}
}

func (r *responsesWebSocketRelay) run() {
	r.readers = append(r.readers, readWebSocketMessages(r.ctx, r.downstream, true, r.messages))
	r.socketID = r.registerActiveSocket(r.current.account.id())
	r.server.websocketOpened(r.thread, r.current.account)
	defer r.close()
	if !r.server.accountRoutable(r.current.account.id()) || !r.current.claim.active() {
		r.closeDownstream(websocket.StatusServiceRestart, "account became unavailable during connection setup")
		return
	}
	var idle <-chan time.Time
	var timer *time.Timer
	if r.idleTimeout > 0 {
		timer = time.NewTimer(r.idleTimeout)
		defer timer.Stop()
		idle = timer.C
	}
	for {
		select {
		case <-idle:
			r.closeDownstream(websocket.StatusServiceRestart, "upstream response timed out")
			return
		case <-r.policyChanged:
			r.restartForFastMode()
			return
		case message := <-r.messages:
			if timer != nil {
				timer.Reset(r.idleTimeout)
			}
			if r.fastModeChanged() {
				return
			}
			if message.downstream {
				if !r.handleDownstream(message) {
					return
				}
			} else if !r.handleUpstream(message) {
				return
			}
		case invalidation := <-r.invalidations:
			if invalidation.account != r.current.account.id() {
				continue
			}
			r.closeDownstream(websocket.StatusServiceRestart, "account unavailable: "+invalidation.reason)
			return
		case <-r.ctx.Done():
			r.closeDownstream(websocket.StatusServiceRestart, "request canceled or server shutting down")
			return
		}
	}
}

func (r *responsesWebSocketRelay) registerActiveSocket(account string) uint64 {
	return r.server.activeWebSockets.add(account, func(account, reason string) {
		select {
		case r.invalidations <- websocketInvalidation{account: account, reason: reason}:
		case <-r.ctx.Done():
		}
	})
}

func (r *responsesWebSocketRelay) close() {
	r.cancel()
	r.current.conn.CloseNow()
	for _, done := range r.readers {
		<-done
	}
	r.current.releaseClaim()
	r.server.activeWebSockets.remove(r.socketID, r.current.account.id())
	r.server.websocketClosed(r.thread, r.current.account)
	for thread := range r.liveThreads {
		r.server.stats.deactivateThread(thread)
	}
	if observed := observation(r.ctx); observed != nil {
		observed.cleaned, observed.account = true, r.current.account.id()
		observed.event(r.ctx, "cleanup", attribute.String("account", observed.account), attribute.Int("pending_turns", len(r.turns)), attribute.Int("readers_joined", len(r.readers)), attribute.Bool("claim_released", true), attribute.Bool("active_registry_removed", true))
		if r.generationSpan != nil {
			if observed.outcome != "completed" && observed.outcome != "incomplete" {
				r.generationSpan.SetStatus(codes.Error, observed.outcome)
			}
			r.generationSpan.End()
		}
	}
}

func (r *responsesWebSocketRelay) closeDownstream(status websocket.StatusCode, reason string) {
	observation(r.ctx).event(r.ctx, "relay_close", attribute.Int("websocket_close_status", int(status)), attribute.String("reason", reason), attribute.String("retry_owner", "client"), attribute.Bool("inference_replayed", false))
	if err := r.downstream.Close(status, reason); err != nil {
		r.server.log.Debug("downstream websocket close failed", "thread", r.thread, "status", status, "error", err)
	}
}

func (r *responsesWebSocketRelay) refuseTurn(status websocket.StatusCode, failure httpResponseFailure) {
	observation(r.ctx).event(r.ctx, "relay_close", attribute.Int("websocket_close_status", int(status)), attribute.String("reason", failure.Code), attribute.String("retry_owner", "client"), attribute.Bool("inference_replayed", false))
	if err := r.downstream.requestFailed(r.ctx, failure, status); err != nil && !errors.Is(err, errResponseFinished) {
		r.server.log.Debug("downstream request refusal failed", "thread", r.thread, "status", status, "error", err)
	}
}

func (r *responsesWebSocketRelay) switchAccount(next *websocketDial, model, serviceTier string) bool {
	previous := r.current
	if previous.claim != nil {
		transferred := r.server.transferWebSocketClaim(previous.claim, next.account.id(), next.priorOwner, next.routingReason)
		if transferred == nil {
			next.conn.CloseNow()
			next.releaseClaim()
			r.closeDownstream(websocket.StatusServiceRestart, "account became unavailable during account switch")
			return false
		}
		previous.claim = nil
		next.claim = transferred
	}
	previous.conn.CloseNow()
	r.server.websocketClosed(r.thread, previous.account)
	r.current = next
	r.upstreamOpened = time.Now()
	r.lastUpstreamEvent = time.Time{}
	r.lastUpstreamKind = ""
	r.receivedUpstreamBytes = 0
	if !r.server.activeWebSockets.move(r.socketID, previous.account.id(), r.current.account.id()) {
		r.socketID = r.registerActiveSocket(r.current.account.id())
	}
	r.current.conn.SetReadLimit(r.messageLimit)
	r.server.websocketOpened(r.thread, r.current.account)
	if !r.server.accountRoutable(r.current.account.id()) || !r.current.claim.active() {
		r.closeDownstream(websocket.StatusServiceRestart, "account became unavailable during connection setup")
		return false
	}
	if observed := observation(r.ctx); observed != nil {
		observed.account = r.current.account.id()
		observed.event(r.ctx, "account_switch_ready", attribute.String("from_account", previous.account.id()), attribute.String("account", observed.account), attribute.String("phase", "before_transmission"), attribute.Bool("write_attempted", observed.writeAttempted), attribute.Bool("response_created", false))
	}
	r.server.log.Info("websocket selected model-compatible account",
		"thread", r.thread,
		"from_account", previous.account.id(),
		"to_account", r.current.account.id(),
		"model", model,
		"service_tier", serviceTier,
	)
	return true
}

func (r *responsesWebSocketRelay) writeUpstream(message websocketMessage) bool {
	ctx, cancel := context.WithTimeout(r.ctx, upstreamWait)
	defer cancel()
	observed := observation(r.ctx)
	if observed != nil {
		observed.writeAttempted = true
		observed.writeAt = time.Now()
	}
	observed.event(r.ctx, "upstream_write_started", attribute.String("account", r.current.account.id()), attribute.Int("bytes", len(message.data)), attribute.String("replay_owner_after_write_attempt", "client"))
	started := time.Now()
	if err := r.current.conn.Write(ctx, message.kind, message.data); err != nil {
		r.logUpstreamFailure(err, "write", len(message.data))
		observed.event(r.ctx, "upstream_write_failed", attribute.String("error_type", telemetryErrorClass(err)), attribute.Bool("possibly_transmitted", true))
		r.downstream.upstreamFailed(r.ctx, err)
		return false
	}
	if observed != nil {
		observed.writeSucceeded = true
	}
	observed.event(r.ctx, "upstream_write_finished", attribute.Int64("elapsed_ms", time.Since(started).Milliseconds()), attribute.Bool("success", true))
	return true
}

func (r *responsesWebSocketRelay) handleDownstream(message websocketMessage) bool {
	if message.err != nil {
		r.logDownstreamClose(message.err)
		return false
	}
	var event websocketEnvelope
	responseCreate := message.kind == websocket.MessageText && json.Unmarshal(message.data, &event) == nil && event.Type == "response.create"
	if !r.pinned && !responseCreate {
		return r.queuePending(message)
	}
	if !responseCreate {
		return r.writeUpstream(message)
	}
	return r.handleResponseCreate(message, event)
}

func (r *responsesWebSocketRelay) logDownstreamClose(err error) {
	level := slog.LevelWarn
	status := websocket.CloseStatus(err)
	if errors.Is(err, context.Canceled) || status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway {
		level = slog.LevelDebug
	}
	r.server.log.Log(r.ctx, level, "downstream websocket closed", "thread", r.thread, "account", r.current.account.id(), "active_turns", len(r.turns), "status", status, "error", err)
}

func (r *responsesWebSocketRelay) queuePending(message websocketMessage) bool {
	r.pendingBytes += len(message.data)
	if r.pendingBytes > maxWebSocketMessage {
		r.closeDownstream(websocket.StatusMessageTooBig, "messages before first turn are too large")
		return false
	}
	r.pending = append(r.pending, message)
	return true
}

func (r *responsesWebSocketRelay) handleResponseCreate(message websocketMessage, event websocketEnvelope) bool {
	if r.fastModeChanged() {
		return false
	}
	var err error
	message.data, event.ServiceTier, err = r.fastMode.override(message.data, event.ServiceTier)
	if err != nil {
		r.closeDownstream(websocket.StatusInternalError, "could not apply fast mode")
		return false
	}

	if !r.current.claim.active() {
		r.closeDownstream(websocket.StatusServiceRestart, "route owner changed before turn")
		return false
	}
	if r.pinned && r.current.account.routingCandidate().spent && websocketRequestPortable(event) {
		r.closeDownstream(websocket.StatusServiceRestart, "account exhausted before a new turn")
		return false
	}
	allowed := r.server.allowedAccounts(event.Model, event.ServiceTier)
	observation(r.ctx).event(r.ctx, "turn_preflight", attribute.String("account", r.current.account.id()), attribute.Bool("pinned", r.pinned), attribute.Bool("account_move", r.current.moved), attribute.Bool("model_tier_allowed", accountAllowed(allowed, r.current.account.id())), attribute.Bool("catalog_filter_active", allowed != nil), attribute.Bool("portable_frame", websocketRequestPortable(event)), attribute.Bool("turn_state_header_present", strings.TrimSpace(r.request.Header.Get(codexTurnStateKey)) != ""))
	if (r.current.moved || !accountAllowed(allowed, r.current.account.id())) && !websocketRequestPortable(event) {
		r.refuseTurn(websocket.StatusTryAgainLater, httpResponseFailure{Status: 400, Code: "account_bound_request", Type: "invalid_request_error", Message: errAccountBoundTurn.Error()})
		return false
	}
	if !r.ensureCompatibleAccount(event, allowed) {
		return false
	}
	if !r.pinned && !r.pin() {
		return false
	}
	// First-turn model selection may have waited for another handshake.
	if r.fastModeChanged() {
		return false
	}
	if observed := observation(r.ctx); observed != nil && r.generationSpan == nil {
		r.ctx, r.generationSpan = observed.start(r.ctx, "codex.response", attribute.String("account", r.current.account.id()), attribute.String("model", event.Model), attribute.String("effective_tier", event.ServiceTier))
	}
	if !r.writeUpstream(message) {
		return false
	}
	r.startTurn(event)
	return true
}

func (r *responsesWebSocketRelay) ensureCompatibleAccount(event websocketEnvelope, allowed map[string]bool) bool {
	if accountAllowed(allowed, r.current.account.id()) {
		return true
	}
	if r.pinned {
		r.closeDownstream(websocket.StatusServiceRestart, "requested model requires another account")
		return false
	}
	preflight, span := observation(r.ctx).start(r.request.Context(), "codex.model_preflight", attribute.String("from_account", r.current.account.id()), attribute.Bool("portable_frame", websocketRequestPortable(event)))
	defer span.End()
	request := r.request.WithContext(preflight)
	var next *websocketDial
	var failed *http.Response
	var err error
	if r.current.claim != nil {
		next, failed, err = r.server.dialResponsesWebSocketReplacing(request, r.route, event.Model, event.ServiceTier, r.current.claim)
	} else {
		next, failed, err = r.server.dialResponsesWebSocket(request, r.route, event.Model, event.ServiceTier)
	}
	if err != nil || failed != nil {
		span.SetStatus(codes.Error, "model_preflight_failed")
		defer closeWebSocketResponse(failed)
		r.server.log.Warn("model-compatible websocket unavailable", "thread", r.thread, "model", event.Model, "service_tier", event.ServiceTier, "error", err)
		r.downstream.setupFailed(r.ctx, failed, err)
		return false
	}
	return r.switchAccount(next, event.Model, event.ServiceTier)
}

func (r *responsesWebSocketRelay) pin() bool {
	r.pinned = true
	r.readers = append(r.readers, readWebSocketMessages(r.ctx, r.current.conn, false, r.messages))
	for _, queued := range r.pending {
		if !r.writeUpstream(queued) {
			return false
		}
	}
	r.pending = nil
	return true
}

func (r *responsesWebSocketRelay) startTurn(event websocketEnvelope) {
	metadata := requestTurnMetadata("", event.ClientMetadata)
	statsThread := statsThreadKey(r.thread, metadata)
	counted := event.Generate == nil || *event.Generate
	r.turns = append(r.turns, websocketTurn{
		sent:        time.Now(),
		model:       event.Model,
		effort:      event.Reasoning.Effort,
		serviceTier: event.ServiceTier,
		metadata:    metadata,
		counted:     counted,
		turnState:   event.ClientMetadata[codexTurnStateKey],
		statsThread: statsThread,
	})
	attrs := []any{"thread", statsThread, "service_tier", event.ServiceTier}
	attrs = append(attrs, routingLogAttrs(r.current.account.routingCandidate(), time.Now())...)
	r.server.log.Debug("websocket turn received", attrs...)
}

func (r *responsesWebSocketRelay) handleUpstream(message websocketMessage) bool {
	if message.err != nil {
		r.logUpstreamFailure(message.err, "read", len(message.data))
		observation(r.ctx).event(r.ctx, "upstream_closed", attribute.String("error_type", telemetryErrorClass(message.err)), attribute.Int("close_status", int(websocket.CloseStatus(message.err))))
		r.downstream.upstreamFailed(r.ctx, message.err)
		return false
	}
	r.lastUpstreamEvent = time.Now()
	r.receivedUpstreamBytes += int64(len(message.data))
	r.lastUpstreamKind = "invalid_frame"
	var err error
	message, err = r.downstream.prepare(message)
	if err != nil {
		r.closeDownstream(websocket.StatusInternalError, "invalid upstream response")
		return false
	}
	var event websocketEnvelope
	rejection := websocketRejectionNone
	parsed := message.kind == websocket.MessageText && json.Unmarshal(message.data, &event) == nil
	if parsed {
		r.lastUpstreamKind = websocketDiagnosticEvent(event.Type)
	}
	if parsed && websocketRejection(event) == websocketRejectionUnauthorized {
		r.handleInBandUnauthorized()
		r.downstream.reject(r.ctx, message, websocket.StatusServiceRestart, "account rejected websocket request")
		return false
	}
	if parsed {
		retryUsage := websocketRejection(event) == websocketRejectionUsageLimit && r.canRetryUsageLimit()
		if rejected := websocketRejection(event); rejected != websocketRejectionNone {
			observation(r.ctx).event(r.ctx, "reconnect_decision", attribute.String("rejection", string(rejected)), attribute.Bool("usage_reconnect_signal", retryUsage), attribute.Int("pending_turns", len(r.turns)), attribute.Bool("accepted_before_rejection", len(r.turns) > 0 && r.turns[0].created), attribute.Bool("turn_state_metadata_present", len(r.turns) > 0 && strings.TrimSpace(r.turns[0].turnState) != ""), attribute.Bool("turn_state_header_present", strings.TrimSpace(r.request.Header.Get(codexTurnStateKey)) != ""), attribute.Bool("upstream_turn_state_present", r.current.resp != nil && strings.TrimSpace(r.current.resp.Header.Get(codexTurnStateKey)) != ""), attribute.String("retry_owner", "client"), attribute.Bool("inference_replayed", false))
		}
		var allowed bool
		rejection, allowed = r.handleUpstreamEvent(event)
		if !allowed {
			return false
		}
		if retryUsage {
			r.server.preserveWebSocketRetryOwner(r.current)
			r.downstream.reject(r.ctx, message, websocket.StatusServiceRestart, "account exhausted; reconnect with full history")
			return false
		}
		if rejection == websocketRejectionModelCapacity {
			r.server.preserveWebSocketRetryOwner(r.current)
			r.downstream.reject(r.ctx, message, websocket.StatusServiceRestart, "model at capacity")
			return false
		}
	}
	if err := r.downstream.Write(r.ctx, message.kind, message.data); err != nil {
		if errors.Is(err, errResponseFinished) {
			return false
		}
		r.server.log.Warn("downstream websocket response write failed", "thread", r.thread, "account", r.current.account.id(), "active_turns", len(r.turns), "error", err)
		return false
	}
	return r.afterUpstreamEvent(rejection)
}

// A reconnect drops Codex's socket-scoped response ID, but not its turn-state
// token. Only retry a single unaccepted request with no account-bound token.
// The replacement relay still requires a full replay before forwarding it.
func (r *responsesWebSocketRelay) canRetryUsageLimit() bool {
	if r.route.key() == "" || len(r.turns) != 1 || r.turns[0].created {
		return false
	}
	turn := r.turns[0]
	if strings.TrimSpace(turn.turnState) != "" || strings.TrimSpace(r.request.Header.Get(codexTurnStateKey)) != "" {
		return false
	}
	if r.current.resp != nil && strings.TrimSpace(r.current.resp.Header.Get(codexTurnStateKey)) != "" {
		return false
	}
	allowed := r.server.allowedAccounts(turn.model, turn.serviceTier)
	now := time.Now()
	for _, account := range r.server.pool.all() {
		candidate := account.routingCandidate()
		if candidate.id != r.current.account.id() && candidate.available(now) && accountAllowed(allowed, candidate.id) {
			return true
		}
	}
	return false
}

func (r *responsesWebSocketRelay) handleInBandUnauthorized() {
	account := r.current.account
	if !account.markRejectedAccessToken(r.current.accessToken) {
		return
	}
	if !r.server.refreshedContext(r.ctx, account, account.id()) && !account.needsReauth() {
		account.clearRejectedAccessToken(r.current.accessToken)
	}
}

func (r *responsesWebSocketRelay) handleUpstreamEvent(event websocketEnvelope) (websocketRejectionKind, bool) {
	headers := websocketEventHeaders(event.Headers)
	if len(headers) > 0 {
		r.current.account.observe(headers)
	}
	rejection := websocketRejection(event)
	if rejection != websocketRejectionNone {
		r.server.handleWebSocketRejection(r.current.account, rejection, headers, r.thread)
	}
	switch event.Type {
	case "response.created":
		if !r.responseCreated() {
			return rejection, false
		}
	case "error", "response.completed", "response.failed", "response.incomplete":
		r.responseFinished(event)
	}
	return rejection, true
}

func (r *responsesWebSocketRelay) afterUpstreamEvent(rejection websocketRejectionKind) bool {
	switch rejection {
	case websocketRejectionNone, websocketRejectionConnectionLimit:
		return true
	}
	r.closeDownstream(websocket.StatusServiceRestart, "account rejected websocket request")
	return false
}

func (r *responsesWebSocketRelay) responseCreated() bool {
	for index := range r.turns {
		if r.turns[index].created {
			continue
		}
		turn := &r.turns[index]
		routeThread := r.route.thread
		if routeThread == "" {
			routeThread = turn.statsThread
		}
		acceptedAt := time.Now()
		acceptance := r.server.acceptWebSocketRoute(r.current, storedRoute{At: acceptedAt, Session: r.route.session, Thread: routeThread, Account: r.current.account.id()})
		if !acceptance.allowed {
			r.closeDownstream(websocket.StatusServiceRestart, "route owner became unavailable")
			return false
		}
		r.turns[index].created = true
		if observed := observation(r.ctx); observed != nil {
			observed.accepted, observed.account = true, r.current.account.id()
			attrs := observed.safe([]attribute.KeyValue{attribute.String("account", observed.account), attribute.String("prior_owner", r.current.priorOwner), attribute.String("routing_reason", string(r.current.routingReason)), attribute.Bool("accepted_switch", acceptance.logSwitch), attribute.Bool("route_persisted", acceptance.persisted)})
			observed.root.SetAttributes(attrs...)
			trace.SpanFromContext(r.ctx).SetAttributes(attrs...)
			observed.emit(r.ctx, slog.LevelInfo, "response_accepted", true, attribute.String("account", observed.account), attribute.String("prior_owner", r.current.priorOwner), attribute.String("routing_reason", string(r.current.routingReason)), attribute.Bool("accepted_switch", acceptance.logSwitch), attribute.Bool("route_persisted", acceptance.persisted), attribute.Bool("write_succeeded", observed.writeSucceeded), attribute.Int64("accept_latency_ms", time.Since(turn.sent).Milliseconds()))
		}
		if turn.counted {
			if _, live := r.liveThreads[turn.statsThread]; !live {
				r.server.stats.activateThread(turn.statsThread)
				r.liveThreads[turn.statsThread] = struct{}{}
			}
		}
		r.server.stats.recordAccepted(acceptedAt, turn.statsThread, requestIP(r.request), r.apiKey.suffix, r.current.account.id(), turn.model, turn.effort, turn.serviceTier, turn.metadata, turn.counted)
		if acceptance.logSwitch {
			r.server.log.Info("websocket account switch accepted",
				"thread", turn.statsThread,
				"from_account", r.current.priorOwner,
				"to_account", r.current.account.id(),
				"routing_reason", r.current.routingReason,
				"route_persisted", acceptance.persisted,
			)
		}
		r.current.moved = false
		if turn.counted {
			r.server.stats.answered(turn.statsThread, r.current.account.id(), time.Since(turn.sent))
		}
		r.server.log.Debug("websocket response created", "thread", turn.statsThread, "turn", turn.metadata.TurnID, "account", r.current.account.id(), "latency", time.Since(turn.sent))
		return true
	}
	return true
}

func (r *responsesWebSocketRelay) responseFinished(event websocketEnvelope) {
	if len(r.turns) == 0 {
		return
	}
	turn := r.turns[0]
	if turn.counted && event.Type != "error" {
		model := event.Response.Model
		if model == "" {
			model = turn.model
		}
		serviceTier := event.Response.ServiceTier
		if serviceTier == "" {
			serviceTier = turn.serviceTier
		}
		observation(r.ctx).usage(r.ctx, model, serviceTier, event.Type, event.Response.Usage)
		if !event.Response.Usage.empty() {
			logResponseUsage(r.server.log, turn.statsThread, r.current.account.id(), model, serviceTier, turn.metadata, time.Since(turn.sent), event.Response.Usage)
		}
		r.server.stats.recordAPIKeyUsage(r.apiKey.name, turn.statsThread, r.current.account.id(), model, turn.effort, serviceTier, event.Response.Usage)
		if event.Type == "response.completed" {
			r.server.stats.completed(turn.statsThread, r.current.account.id(), turn.metadata, time.Since(turn.sent))
		}
	}
	r.turns = r.turns[1:]
}

func (r *responsesWebSocketRelay) restartForFastMode() {
	r.server.preserveWebSocketRetryOwner(r.current)
	observation(r.ctx).event(r.ctx, "policy_changed", attribute.String("account", r.current.account.id()), attribute.Bool("owner_preserved", true), attribute.String("retry_owner", "client"))
	r.closeDownstream(websocket.StatusServiceRestart, "fast mode changed; reconnect")
}

func (r *responsesWebSocketRelay) fastModeChanged() bool {
	select {
	case <-r.policyChanged:
		r.restartForFastMode()
		return true
	default:
		return false
	}
}
