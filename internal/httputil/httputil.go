package httputil

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

// CloseResponseBody closes resp.Body, logging at debug level on failure.
func CloseResponseBody(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	if err := resp.Body.Close(); err != nil {
		slog.Debug("close response body", "error", err)
	}
}

// ReadResponseBody reads and returns the full response body.
func ReadResponseBody(resp *http.Response) ([]byte, error) {
	if resp == nil || resp.Body == nil {
		return nil, fmt.Errorf("response body is nil")
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}
	return body, nil
}
