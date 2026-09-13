package app

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

func websocketFailureClass(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "truncated_read"
	case errors.Is(err, net.ErrClosed):
		return "connection_closed"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection_reset"
	case errors.Is(err, syscall.EPIPE):
		return "broken_pipe"
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return "timeout"
	}
	if websocket.CloseStatus(err) >= 0 {
		return "websocket_close"
	}
	return "transport_failure"
}

func websocketDiagnosticEvent(kind string) string {
	switch kind {
	case "error", "response.created", "response.completed", "response.done", "response.failed", "response.incomplete",
		"response.output_text.delta", "response.reasoning_summary_text.delta", "response.function_call_arguments.delta",
		"response.custom_tool_call_input.delta", "response.output_item.added", "response.output_item.done":
		return kind
	default:
		return "other"
	}
}

func (r *responsesWebSocketRelay) logUpstreamFailure(err error, phase string, size int) {
	if r.server.log == nil {
		return
	}
	age, idle := int64(-1), int64(-1)
	if !r.upstreamOpened.IsZero() {
		age = time.Since(r.upstreamOpened).Milliseconds()
	}
	if !r.lastUpstreamEvent.IsZero() {
		idle = time.Since(r.lastUpstreamEvent).Milliseconds()
	}
	accepted := 0
	for _, turn := range r.turns {
		if turn.created {
			accepted++
		}
	}
	var closed websocket.CloseError
	hasReason := errors.As(err, &closed) && closed.Reason != ""
	method, path := responseLogRoute(r.request)
	// Never log err.Error() or an upstream close reason: either may contain
	// payloads/credentials. These fields also work for legacy GET/WS clients.
	r.server.log.Warn("upstream websocket failure",
		"phase", phase, "error_class", websocketFailureClass(err), "close_status", int(websocket.CloseStatus(err)), "close_reason_present", hasReason,
		"http.request.method", method, "http.route", path, "socket_id", r.socketID, "account", r.current.account.id(),
		"session_hash", r.server.logFingerprint("session", []byte(r.route.session)), "thread_hash", r.server.logFingerprint("thread", []byte(r.route.thread)),
		"connection_age_ms", age, "since_last_event_ms", idle, "last_event_type", r.lastUpstreamKind,
		"pending_turns", len(r.turns), "accepted_pending_turns", accepted, "frame_bytes", size, "upstream_bytes", r.receivedUpstreamBytes, "read_limit_bytes", r.messageLimit,
		"retry_owner", "client", "inference_replayed", false)
}
