package tables

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// closeResponseBody closes the response body in-package so bodyclose can track Body.Close().
func closeResponseBody(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	if err := resp.Body.Close(); err != nil {
		slog.Debug("close response body", "error", err)
	}
}

var (
	bufferPool = sync.Pool{
		New: func() interface{} {
			buf := bytes.NewBuffer(make([]byte, 0, BufferPoolDefaultSize))
			return buf
		},
	}

	builderPool = sync.Pool{
		New: func() interface{} {
			b := &strings.Builder{}
			b.Grow(512)
			return b
		},
	}

	boundaryCounter atomic.Uint64
)

func nextBoundaryID(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), boundaryCounter.Add(1))
}
