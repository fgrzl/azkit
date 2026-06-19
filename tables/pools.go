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

	"github.com/fgrzl/azkit/internal/httputil"
)

// closeResponseBody drains the body to EOF (so net/http can reuse the connection
// for keep-alive) and then closes it in-package so bodyclose can track Body.Close().
func closeResponseBody(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	httputil.DrainResponseBody(resp)
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
