package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
)

// Initial handshakes and first-turn model preflight use the same mapping. The
// relay retains ownership of failed.Body until this adapter has consumed it.
func (d *httpResponsesDownstream) setupFailed(ctx context.Context, failed *http.Response, err error) error {
	failure := httpResponseFailure{Status: 502, Code: "upstream_unavailable", Message: "upstream connection unavailable; retry with full history"}
	if errors.Is(err, errNoAccountAvailable) || errors.Is(err, errRouteOwnerUnavailable) {
		failure.Status, failure.Code, failure.Message = 503, "route_unavailable", "no eligible account or route owner temporarily unavailable; retry"
	}
	if errors.Is(err, errAccountBoundTurn) {
		failure.Status, failure.Code, failure.Message = 409, "account_bound_request", errAccountBoundTurn.Error()
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		failure.Status, failure.Code = 504, "upstream_timeout"
	}
	var rejection *websocketSetupError
	if errors.As(err, &rejection) {
		details, _ := json.Marshal(rejection.details)
		failure = responseFailure(responseFields{"error": details}, rejection.status)
		if rejection.retryAfter != "" && !d.committed {
			d.writer.Header().Set("Retry-After", rejection.retryAfter)
		}
	}
	if failed != nil {
		failure = httpResponseFailure{Status: failed.StatusCode, Code: "upstream_rejected", Message: "upstream rejected connection"}
		if failed.Body != nil {
			data, err := io.ReadAll(io.LimitReader(failed.Body, maxUpstreamErrorBody+1))
			if err == nil && len(data) <= maxUpstreamErrorBody {
				if fields, err := responseObject(data); err == nil {
					failure = responseFailure(fields, failed.StatusCode)
				}
			}
		}
		if !d.committed {
			copyHTTPResponseHeaders(d.writer.Header(), failed.Header)
		}
	}
	if ctx.Err() != nil {
		failure.Status, failure.Code, failure.Message = 503, "request_canceled", "request canceled or server shutting down"
	}
	upstreamStatus, mappedStatus := 0, failure.Status
	if failed != nil {
		upstreamStatus = failed.StatusCode
	} else if rejection != nil {
		upstreamStatus = rejection.status
	}
	if mappedStatus < 400 || mappedStatus > 599 {
		mappedStatus = 502
	}
	observation(ctx).event(ctx, "setup_failed", attribute.Int("upstream_status", upstreamStatus), attribute.Int("mapped_status", mappedStatus), attribute.String("error_code", failure.Code), attribute.String("retry_after", d.writer.Header().Get("Retry-After")), attribute.String("error_type", telemetryErrorClass(err)), attribute.Bool("inference_sent", false))
	return d.fail(failure)
}
