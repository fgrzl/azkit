package httputil

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

// ResponseBodyDrainLimit bounds how many bytes CloseResponseBody drains before
// Close. Reading the body to EOF is what lets net/http return the connection to
// the idle pool (HTTP/1.1 keep-alive), but an unbounded drain on a pathologically
// large unconsumed body could stall the caller, so we cap it. Azure Table
// single-entity / page responses are far below this; if a body exceeds the cap we
// stop draining and Close, which simply means that one connection is not reused.
const ResponseBodyDrainLimit = 4 << 20 // 4 MiB

// DrainResponseBody reads resp.Body to EOF (bounded by ResponseBodyDrainLimit)
// without closing it. Draining before Close is required for HTTP/1.1 keep-alive:
// net/http only returns a connection to the idle pool when the body has been read
// to EOF. The drain is best-effort and ignores read errors (e.g. a context-canceled
// body); callers must still Close the body afterwards. Safe on a nil resp or nil Body.
//
// Callers that own the Body.Close() call (so the bodyclose linter can track it) should
// call DrainResponseBody and then Body.Close(); callers without that constraint can use
// CloseResponseBody which does both.
func DrainResponseBody(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	// Discard remaining bytes so the underlying conn reaches EOF and can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, ResponseBodyDrainLimit))
}

// CloseResponseBody drains resp.Body to EOF (bounded by ResponseBodyDrainLimit)
// and then closes it, logging at debug level on failure. The drain is best-effort;
// Close always runs even if the drain errors, so the connection is never leaked.
// Safe on a nil resp or nil Body.
func CloseResponseBody(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	DrainResponseBody(resp)
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
