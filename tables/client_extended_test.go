package tables

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgrzl/azkit/credentials"
	"github.com/fgrzl/azkit/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldReturnErrorWhenSASAccountNameEmpty(t *testing.T) {
	_, err := NewHTTPTableClientWithSAS("", "sig", "t", false, "")
	require.Error(t, err)
}

func TestShouldReturnErrorWhenManagedIdentityNil(t *testing.T) {
	_, err := NewHTTPTableClientWithManagedIdentity("acct", nil, "t", false, "")
	require.Error(t, err)
}

func TestShouldAddEntityWhenResponseIs201(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	ent, err := json.Marshal(Entity{PartitionKey: "pk", RowKey: "rk", Value: []byte("v")})
	require.NoError(t, err)

	err = client.AddEntity(context.Background(), ent)
	assert.NoError(t, err)
}

func TestShouldReturnErrorWhenAddEntityResponseIs409(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"odata.error":{"code":"EntityAlreadyExists","message":{"value":"exists"}}}`))
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	ent, _ := json.Marshal(Entity{PartitionKey: "pk", RowKey: "rk"})
	err = client.AddEntity(context.Background(), ent)
	require.Error(t, err)
	var azErr *AzureError
	require.True(t, errors.As(err, &azErr))
	assert.Equal(t, http.StatusConflict, azErr.StatusCode)
}

func TestShouldReturnErrorWhenAddEntityDataIsInvalidJSON(t *testing.T) {
	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, "http://localhost")
	require.NoError(t, err)

	err = client.AddEntity(context.Background(), []byte("{"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal entity")
}

func TestShouldDeleteEntityWhenResponseIs204(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DELETE", r.Method)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	err = client.DeleteEntity(context.Background(), "pk", "rk")
	assert.NoError(t, err)
}

func TestShouldUpsertEntityWhenResponseIs204(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "PATCH", r.Method)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	ent, _ := json.Marshal(Entity{PartitionKey: "pk", RowKey: "rk"})
	err = client.UpsertEntity(context.Background(), ent, "Merge")
	assert.NoError(t, err)
}

func TestShouldRetryOn429ThenSucceed(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	err = client.CreateTable(context.Background())
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, attempts.Load(), int32(2))
}

func TestShouldReturnErrorWhenBatchExceedsMaxSize(t *testing.T) {
	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, "http://localhost")
	require.NoError(t, err)

	entities := make([][]byte, AzureBatchMaxEntities+1)
	err = client.AddEntityBatch(context.Background(), entities)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum")
}

func TestShouldFetchPageWhenListReturns200(t *testing.T) {
	body := `{"value":[{"PartitionKey":"pk","RowKey":"rk"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	pager := client.NewListEntitiesPager("", "", 10)
	entities, err := pager.FetchPage(context.Background())
	require.NoError(t, err)
	require.Len(t, entities, 1)
	assert.Equal(t, "pk", entities[0].PartitionKey)
}

func TestShouldReturnErrorWhenFetchPageResponseIs500(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("error"))
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	pager := client.NewListEntitiesPager("", "", 10)
	_, err = pager.FetchPage(context.Background())
	require.Error(t, err)
}

func TestShouldReturnErrorWhenManagedIdentityTokenFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv("IDENTITY_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("IDENTITY_HEADER", "test-header")
	cred := credentials.NewManagedIdentityCredential("")

	client, err := NewHTTPTableClientWithManagedIdentity("acct", cred, "t", false, server.URL)
	require.NoError(t, err)

	err = client.CreateTable(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "managed identity token")
}

func TestEntityRequestBodyEncodesValueAsBase64(t *testing.T) {
	data, err := entityRequestBody(Entity{PartitionKey: "p", RowKey: "r", Value: []byte("hi")})
	require.NoError(t, err)
	assert.Contains(t, string(data), "aGk=")
}

func TestIsTransientStatusMatchesAzureError(t *testing.T) {
	assert.True(t, isTransientStatus(http.StatusTooManyRequests))
	assert.False(t, isTransientStatus(http.StatusNotFound))
}

func TestShouldReturnErrorWhenRetryExhaustedOn503(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	err = client.CreateTable(context.Background())
	require.Error(t, err)
}

func TestShouldSubmitMixedBatchWhenPutAndDelete(t *testing.T) {
	var receivedBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.Path, "$batch")
		body, _ := io.ReadAll(r.Body)
		receivedBody = string(body)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("HTTP/1.1 204 No Content\r\n\r\n"))
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	ent, _ := json.Marshal(Entity{PartitionKey: "pk", RowKey: "rk1", Value: []byte("v")})
	ops := []BatchOp{
		{Type: BatchInsertReplace, Entity: ent},
		{Type: BatchDelete, PartitionKey: "pk", RowKey: "rk2"},
	}
	err = client.SubmitBatch(context.Background(), ops)
	require.NoError(t, err)

	assert.Contains(t, receivedBody, "Content-Type: application/http")
	assert.Contains(t, receivedBody, "PUT ")
	assert.Contains(t, receivedBody, "DELETE ")
	// DELETE part must have multipart headers immediately before the DELETE line
	deleteIdx := strings.Index(receivedBody, "DELETE ")
	partHeaderIdx := strings.LastIndex(receivedBody[:deleteIdx], "Content-Transfer-Encoding: binary")
	assert.Greater(t, partHeaderIdx, -1, "DELETE must be preceded by multipart part headers")
}

func TestShouldAddEntityBatchWhenResponseIs202(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.Path, "$batch")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("HTTP/1.1 204 No Content\r\n\r\n"))
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	ent, _ := json.Marshal(Entity{PartitionKey: "pk", RowKey: "rk"})
	err = client.AddEntityBatch(context.Background(), [][]byte{ent})
	assert.NoError(t, err)
}

func TestShouldCreateClientWithSAS(t *testing.T) {
	client, err := NewHTTPTableClientWithSAS("acct", "sig=value", "table", false, "")
	require.NoError(t, err)
	assert.True(t, client.useSAS)
}

func TestDiagnoseAuthReturnsOKWhenListTablesSucceeds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/Tables" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"value":[]}`))
		}
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	diag := client.DiagnoseAuth(context.Background())
	assert.Equal(t, http.StatusOK, diag.ResponseStatus)
	assert.Contains(t, diag.Suggestion, "working")
}

func TestShouldCancelRetryWhenContextCanceled(t *testing.T) {
	block := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		<-block
	}))
	defer server.Close()
	defer close(block)

	client, err := NewHTTPTableClient("devstoreaccount1", testutil.ValidBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = client.CreateTable(ctx)
	require.Error(t, err)
}
