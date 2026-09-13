package app

import (
	"crypto/rand"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
)

// Register explicit method fallbacks for these exact paths. A generic response
// writer wrapper would interfere with WebSocket hijacking/body-limit controls.
func (s *server) responsesMethodNotAllowed(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", "GET, HEAD, POST")
	w.Header().Set("Connection", "close") // reject without draining a slow body
	observed := observation(r.Context())
	observed.event(r.Context(), "method_not_allowed", attribute.String("allowed_methods", "GET, HEAD, POST"))
	observed.reject(r.Context(), http.StatusMethodNotAllowed, "method_not_allowed")
	writeHTTPResponseError(w, http.StatusMethodNotAllowed, "use POST for HTTP Responses or GET with a WebSocket upgrade")
}

func (s *server) logResponseRejection(w http.ResponseWriter, r *http.Request, status int, reason string) {
	requestID := rand.Text()
	w.Header().Set(responseRequestIDHeader, requestID)
	if s.log == nil {
		return
	}
	method, path := responseLogRoute(r)
	s.log.Warn("responses request rejected", "request_id", requestID,
		"http.request.method", method, "http.route", path, "http_status", status, "reason", reason)
}

func responseLogRoute(r *http.Request) (method, path string) {
	method = r.Method
	switch method {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "CONNECT", "TRACE":
	default:
		method = "_OTHER"
	}
	// Do not capture arbitrary paths, query strings or credentials in telemetry.
	switch r.URL.Path {
	case "/v1/responses", "/codex/responses", "/v1/codex/responses":
		path = r.URL.Path
	default:
		path = "responses"
	}
	return method, path
}
