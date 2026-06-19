package tables

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/fgrzl/azkit/internal/testutil"
	"github.com/stretchr/testify/require"
)

// countingListener wraps a net.Listener and counts accepted connections so a test
// can assert that sequential requests reuse a single keep-alive connection.
type countingListener struct {
	net.Listener
	accepted atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return conn, err
}

func newCountingServer(t *testing.T, handler http.Handler) (*httptest.Server, *countingListener) {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	cl := &countingListener{Listener: server.Listener}
	server.Listener = cl
	server.Start()
	return server, cl
}

// TestShouldReuseSingleConnectionWhenGetEntityCalledSequentially proves the
// drain-before-close fix: GetEntity uses json.NewDecoder which leaves trailing
// bytes unread, so without draining each call would open a fresh connection.
// After the fix the connection is returned to the idle pool and reused.
func TestShouldReuseSingleConnectionWhenGetEntityCalledSequentially(t *testing.T) {
	// Arrange
	want := Entity{PartitionKey: "pk", RowKey: "rk", Value: []byte("hello")}
	body, _ := json.Marshal(want)
	// Append trailing bytes after the JSON value so json.NewDecoder leaves data
	// unread; the drain must consume these for the connection to be reusable.
	// The trailer must exceed net/http's own best-effort Close-time slurp
	// (~2 KiB) so that only an explicit drain returns the conn to the idle pool.
	body = append(body, '\n')
	body = append(body, bytes.Repeat([]byte("x"), 8<<10)...)
	server, cl := newCountingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	// Act
	const n = 20
	for i := 0; i < n; i++ {
		data, err := client.GetEntity(context.Background(), "pk", "rk")
		require.NoError(t, err)
		require.NotNil(t, data)
	}

	// Assert
	require.Equal(t, int64(1), cl.accepted.Load(),
		"sequential GetEntity calls must reuse a single keep-alive connection")
}

// TestShouldReuseSingleConnectionWhenFetchPageCalledSequentially covers the pager
// json.NewDecoder read path under the same drain-before-close fix.
func TestShouldReuseSingleConnectionWhenFetchPageCalledSequentially(t *testing.T) {
	// Arrange
	page := PageResponse{Value: []Entity{{PartitionKey: "pk", RowKey: "rk", Value: []byte("v")}}}
	body, _ := json.Marshal(page)
	body = append(body, '\n')
	body = append(body, bytes.Repeat([]byte("x"), 8<<10)...)
	server, cl := newCountingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No continuation headers -> single page, pager done after one fetch.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	// Act
	const n = 20
	for i := 0; i < n; i++ {
		pager := client.NewListEntitiesPager("", "", 0)
		entities, err := pager.FetchPage(context.Background())
		require.NoError(t, err)
		require.Len(t, entities, 1)
	}

	// Assert
	require.Equal(t, int64(1), cl.accepted.Load(),
		"sequential FetchPage calls must reuse a single keep-alive connection")
}
