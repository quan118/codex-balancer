package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/klauspost/compress/zstd"
)

var errResponseBodyTooLarge = errors.New("decoded response request exceeds size limit")

func responseContentEncoding(headers http.Header) (string, bool) {
	values := headers.Values("Content-Encoding")
	if len(values) == 0 {
		return "identity", true
	}
	if len(values) != 1 {
		return "", false
	}
	switch strings.ToLower(strings.TrimSpace(values[0])) {
	case "", "identity":
		return "identity", true
	case "zstd":
		return "zstd", true
	default:
		return "", false
	}
}

// The wire body and decoded JSON each have their own limit. Buffer the bounded
// wire body, then decode synchronously: no decoder workers can outlive a canceled
// HTTP request. Context checks also apply between decoded blocks/reader calls.
func readResponseBody(ctx context.Context, w http.ResponseWriter, request *http.Request, encoding string) ([]byte, int, error) {
	body := http.MaxBytesReader(w, request.Body, maxHTTPResponseBody)
	defer body.Close()
	wire, err := io.ReadAll(responseContextReader{ctx: ctx, reader: body})
	if err != nil || encoding == "identity" {
		return wire, len(wire), err
	}
	decoder, err := zstd.NewReader(responseContextReader{ctx: ctx, reader: bytes.NewReader(wire)},
		zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxMemory(maxHTTPResponseBody), zstd.WithDecoderMaxWindow(maxHTTPResponseBody),
		zstd.WithDecodeBuffersBelow(0),
	)
	if err != nil {
		return nil, len(wire), err
	}
	defer decoder.Close()
	data, err := io.ReadAll(io.LimitReader(responseContextReader{ctx: ctx, reader: decoder}, maxHTTPResponseBody+1))
	if len(data) > maxHTTPResponseBody {
		return nil, len(wire), errResponseBodyTooLarge
	}
	if cause := ctx.Err(); cause != nil {
		return nil, len(wire), cause
	}
	return data, len(wire), err
}

type responseContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r responseContextReader) Read(data []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(data)
}

func responseBodyStatus(err error) int {
	var wireLimit *http.MaxBytesError
	if errors.As(err, &wireLimit) || errors.Is(err, errResponseBodyTooLarge) || errors.Is(err, zstd.ErrDecoderSizeExceeded) || errors.Is(err, zstd.ErrWindowSizeExceeded) {
		return http.StatusRequestEntityTooLarge
	}
	return http.StatusBadRequest
}
