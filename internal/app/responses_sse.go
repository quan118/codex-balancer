package app

import (
	"bufio"
	"bytes"
	"io"
)

func readResponseSSE(reader io.Reader, deliver func([]byte) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), maxHTTPOutput)
	var data []byte
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			if len(data) == 0 {
				continue
			}
			data = data[:len(data)-1]
			if bytes.Equal(data, []byte("[DONE]")) {
				return io.ErrUnexpectedEOF
			}
			if err := deliver(data); err != nil {
				return err
			}
			data = data[:0]
			continue
		}
		field, value, found := bytes.Cut(line, []byte(":"))
		if !bytes.Equal(field, []byte("data")) {
			continue
		}
		if found && len(value) > 0 && value[0] == ' ' {
			value = value[1:]
		}
		if len(data)+len(value)+1 > maxHTTPOutput {
			return errHTTPInvalidResponse
		}
		data = append(data, value...)
		data = append(data, '\n')
	}
	if err := scanner.Err(); err != nil {
		if err == bufio.ErrTooLong {
			return errHTTPInvalidResponse
		}
		return err
	}
	return io.ErrUnexpectedEOF
}
