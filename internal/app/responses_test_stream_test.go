package app

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/coder/websocket"
)

type testResponseStream struct {
	conn    *websocket.Conn
	writer  http.ResponseWriter
	request *http.Request
	read    bool
	closed  bool
}

func acceptResponseTestStream(w http.ResponseWriter, r *http.Request, options *websocket.AcceptOptions) (*testResponseStream, error) {
	stream := &testResponseStream{writer: w, request: r}
	if r.Method == http.MethodGet {
		var err error
		stream.conn, err = websocket.Accept(w, r, options)
		return stream, err
	}
	if r.Method != http.MethodPost || r.Header.Get("Upgrade") != "" {
		return nil, fmt.Errorf("unexpected upstream request: %s %s", r.Method, r.Header.Get("Upgrade"))
	}
	w.Header().Set("Content-Type", "text/event-stream")
	return stream, nil
}

func (s *testResponseStream) SetReadLimit(limit int64) {
	if s.conn != nil {
		s.conn.SetReadLimit(limit)
	}
}

func (s *testResponseStream) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	if s.conn != nil {
		return s.conn.Read(ctx)
	}
	if s.closed {
		return 0, nil, io.EOF
	}
	if !s.read {
		s.read = true
		data, err := io.ReadAll(s.request.Body)
		return websocket.MessageText, data, err
	}
	<-ctx.Done()
	return 0, nil, ctx.Err()
}

func (s *testResponseStream) Write(ctx context.Context, kind websocket.MessageType, data []byte) error {
	if s.conn != nil {
		return s.conn.Write(ctx, kind, data)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(s.writer, "data: %s\n\n", data); err != nil {
		return err
	}
	return http.NewResponseController(s.writer).Flush()
}

func (s *testResponseStream) CloseNow() error {
	s.closed = true
	if s.conn != nil {
		return s.conn.CloseNow()
	}
	conn, _, err := http.NewResponseController(s.writer).Hijack()
	if err != nil {
		return err
	}
	return conn.Close()
}

func (s *testResponseStream) Close(status websocket.StatusCode, reason string) error {
	if s.conn != nil {
		return s.conn.Close(status, reason)
	}
	return s.CloseNow()
}
