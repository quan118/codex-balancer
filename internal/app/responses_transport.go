package app

import (
	"context"
	"errors"
	"net/http"

	"github.com/coder/websocket"
)

var errResponseFinished = errors.New("response transport finished")

// The relay owns routing and turn state. A downstream owns only framing,
// validation and termination. HTTP supplies exactly one response.create.
type responsesDownstream interface {
	Read(context.Context) (websocket.MessageType, []byte, error)
	Write(context.Context, websocket.MessageType, []byte) error
	Close(websocket.StatusCode, string) error
	prepare(websocketMessage) (websocketMessage, error)
	reject(context.Context, websocketMessage, websocket.StatusCode, string) error
	requestFailed(context.Context, httpResponseFailure, websocket.StatusCode) error
	setupFailed(context.Context, *http.Response, error) error
	upstreamFailed(context.Context, error) error
}

type websocketDownstream struct {
	*websocket.Conn
	responsesRedactor
}

func (d websocketDownstream) prepare(message websocketMessage) (websocketMessage, error) {
	return message, nil
}
