package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/coder/websocket"
)

func upstreamFailure(err error) (httpResponseFailure, websocket.StatusCode) {
	failure := httpResponseFailure{Status: 502, Code: "upstream_disconnected", Type: "upstream_error"}
	status := websocket.StatusServiceRestart
	var closed websocket.CloseError
	if errors.As(err, &closed) {
		status = closed.Code
		if status == websocket.StatusNoStatusRcvd || status == websocket.StatusAbnormalClosure || status == websocket.StatusTLSHandshake {
			status = websocket.StatusServiceRestart
		}
		failure.CloseStatus = int(closed.Code)
		failure.Code = "upstream_websocket_closed"
		failure.Message = fmt.Sprintf("upstream WebSocket closed with code %d", closed.Code)
		switch closed.Code {
		case websocket.StatusMessageTooBig:
			failure.Code = "request_too_large"
			failure.Message += "; retry over HTTP transport"
		case websocket.StatusProtocolError, websocket.StatusUnsupportedData, websocket.StatusInvalidFramePayloadData, websocket.StatusPolicyViolation, websocket.StatusMandatoryExtension:
			failure.Status, failure.Type = 400, "invalid_request_error"
		}
		if closed.Reason != "" {
			failure.Message += ": " + closed.Reason
		}
	} else {
		kind := websocketFailureClass(err)
		failure.Message = "upstream WebSocket failed: " + kind
		if kind == "timeout" {
			failure.Status, failure.Code = 504, "upstream_timeout"
		}
	}
	return failure, status
}

func (d *httpResponsesDownstream) upstreamFailed(_ context.Context, err error) error {
	failure, _ := upstreamFailure(err)
	return d.fail(failure)
}

func (d websocketDownstream) upstreamFailed(ctx context.Context, err error) error {
	failure, status := upstreamFailure(err)
	return d.writeFailure(ctx, failure, nil, status)
}

func (d websocketDownstream) requestFailed(ctx context.Context, failure httpResponseFailure, status websocket.StatusCode) error {
	return d.writeFailure(ctx, failure, nil, status)
}

func (d websocketDownstream) setupFailed(ctx context.Context, failed *http.Response, err error) error {
	failure, headers := responseSetupFailure(ctx, failed, err)
	return d.writeFailure(ctx, failure, headers, websocket.StatusTryAgainLater)
}

func (d websocketDownstream) writeFailure(ctx context.Context, failure httpResponseFailure, headers http.Header, closeStatus websocket.StatusCode) error {
	if failure.Type == "" {
		failure.Type = "balancer_error"
	}
	status := failure.Status
	if status >= 400 && status < 500 && status != 408 && status != 429 {
		status = 400
	}
	if status < 400 || status > 599 {
		status = 502
	}
	fields := map[string]any{
		"type": "error", "status": status, "headers": websocketErrorHeaders(headers),
		"error": failure.errorObject(),
	}
	if failure.UpstreamStatus != 0 {
		fields["upstream_status"] = failure.UpstreamStatus
	}
	if failure.CloseStatus != 0 {
		fields["upstream_close_status"] = failure.CloseStatus
	}
	if failure.CloseStatus == int(websocket.StatusMessageTooBig) {
		fields["retryable"] = true
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	return d.writeErrorEvent(ctx, data, closeStatus)
}

func (d websocketDownstream) reject(ctx context.Context, message websocketMessage, status websocket.StatusCode, reason string) error {
	fields, err := responseObject(message.data)
	if err != nil {
		return d.Close(status, reason)
	}
	failure := responseFailure(fields, 502)
	details, err := responseObject(fields["error"])
	if err != nil {
		response, _ := responseObject(fields["response"])
		details, err = responseObject(response["error"])
	}
	if err != nil {
		details = responseFields{}
	}
	for name, value := range map[string]string{"code": failure.Code, "type": failure.Type, "message": failure.Message} {
		if text, ok := responseString(details[name]); !ok || text == "" {
			details[name], _ = json.Marshal(value)
		}
	}
	fields["upstream_type"] = fields["type"]
	var envelope websocketEnvelope
	json.Unmarshal(message.data, &envelope)
	if originalStatus := websocketStatus(envelope); originalStatus != 0 {
		fields["upstream_status"], _ = json.Marshal(originalStatus)
	}
	fields["headers"], _ = json.Marshal(websocketErrorHeaders(websocketEventHeaders(envelope.Headers)))
	fields["type"] = json.RawMessage(`"error"`)
	fields["status"] = json.RawMessage(`502`)
	delete(fields, "status_code")
	fields["retryable"] = json.RawMessage(`true`)
	fields["error"], _ = json.Marshal(details)
	data, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	return d.writeErrorEvent(ctx, data, status)
}

func (d websocketDownstream) writeErrorEvent(ctx context.Context, data []byte, status websocket.StatusCode) error {
	ctx, cancel := context.WithTimeout(ctx, httpResponseIOWait)
	defer cancel()
	defer d.Close(status, "upstream request failed")
	return d.Write(ctx, websocket.MessageText, d.redact(data))
}

func websocketErrorHeaders(headers http.Header) map[string]string {
	filtered := http.Header{}
	copyHTTPResponseHeaders(filtered, headers)
	values := map[string]string{}
	for name := range filtered {
		values[name] = filtered.Get(name)
	}
	return values
}
