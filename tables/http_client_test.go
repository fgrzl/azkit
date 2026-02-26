package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validBase64AccountKey is a valid base64-encoded key for client tests.
const validBase64AccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

// --- AzureError ---

func TestShouldFormatErrorMessageWhenAzureErrorHasAllFields(t *testing.T) {
	// Arrange
	err := &AzureError{StatusCode: 404, Code: "ResourceNotFound", RequestID: "req-1", Message: "not found"}

	// Act
	s := err.Error()

	// Assert
	assert.Contains(t, s, "404")
	assert.Contains(t, s, "ResourceNotFound")
	assert.Contains(t, s, "req-1")
	assert.Contains(t, s, "not found")
}

func TestShouldReturnTrueForIsTransientWhenStatusIs429(t *testing.T) {
	// Arrange
	err := &AzureError{StatusCode: http.StatusTooManyRequests}

	// Act
	ok := err.IsTransient()

	// Assert
	assert.True(t, ok)
}

func TestShouldReturnTrueForIsTransientWhenStatusIs503(t *testing.T) {
	// Arrange
	err := &AzureError{StatusCode: http.StatusServiceUnavailable}

	// Act
	ok := err.IsTransient()

	// Assert
	assert.True(t, ok)
}

func TestShouldReturnTrueForIsTransientWhenStatusIs408(t *testing.T) {
	// Arrange
	err := &AzureError{StatusCode: http.StatusRequestTimeout}

	// Act
	ok := err.IsTransient()

	// Assert
	assert.True(t, ok)
}

func TestShouldReturnFalseForIsTransientWhenStatusIs404(t *testing.T) {
	// Arrange
	err := &AzureError{StatusCode: http.StatusNotFound}

	// Act
	ok := err.IsTransient()

	// Assert
	assert.False(t, ok)
}

func TestShouldReturnFalseForIsTransientWhenStatusIs400(t *testing.T) {
	// Arrange
	err := &AzureError{StatusCode: http.StatusBadRequest}

	// Act
	ok := err.IsTransient()

	// Assert
	assert.False(t, ok)
}

// --- ParseAzureError ---

func TestShouldExtractCodeAndMessageWhenResponseIsODataJSON(t *testing.T) {
	// Arrange
	body := []byte(`{"odata.error":{"code":"ResourceNotFound","message":{"lang":"en-US","value":"The specified resource does not exist."}}}`)
	resp := &http.Response{StatusCode: 404, Header: http.Header{"X-Ms-Request-Id": {"req-123"}}}

	// Act
	azErr := ParseAzureError(resp, body)

	// Assert
	require.NotNil(t, azErr)
	assert.Equal(t, 404, azErr.StatusCode)
	assert.Equal(t, "ResourceNotFound", azErr.Code)
	assert.Equal(t, "The specified resource does not exist.", azErr.Message)
	assert.Equal(t, "req-123", azErr.RequestID)
}

func TestShouldExtractCodeAndMessageWhenResponseIsXML(t *testing.T) {
	// Arrange
	body := []byte(`<m:error><m:code>AuthenticationFailed</m:code><m:message>Server failed to authenticate the request.</m:message></m:error>`)
	resp := &http.Response{StatusCode: 403, Header: http.Header{}}

	// Act
	azErr := ParseAzureError(resp, body)

	// Assert
	require.NotNil(t, azErr)
	assert.Equal(t, 403, azErr.StatusCode)
	assert.Equal(t, "AuthenticationFailed", azErr.Code)
	assert.Contains(t, azErr.Message, "authenticate")
}

func TestShouldUseRawBodyAsMessageWhenNoStructuredContent(t *testing.T) {
	// Arrange
	body := []byte("plain text error")
	resp := &http.Response{StatusCode: 500, Header: http.Header{}}

	// Act
	azErr := ParseAzureError(resp, body)

	// Assert
	require.NotNil(t, azErr)
	assert.Equal(t, 500, azErr.StatusCode)
	assert.Equal(t, "plain text error", azErr.Message)
}

// --- parseBatchResponse ---

func TestShouldReturnNilWhenBatchResponseHasNoFailureLines(t *testing.T) {
	// Arrange
	body := []byte("HTTP/1.1 201 Created\r\n\r\n")

	// Act
	err := parseBatchResponse(body)

	// Assert
	assert.NoError(t, err)
}

func TestShouldReturnNilWhenBatchResponseContainsOnly2xx(t *testing.T) {
	// Arrange
	body := []byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")

	// Act
	err := parseBatchResponse(body)

	// Assert
	assert.NoError(t, err)
}

func TestShouldReturnErrorWhenBatchResponseContainsEntityFailure(t *testing.T) {
	// Arrange
	body := []byte("HTTP/1.1 409 Conflict\r\n\r\n{\"odata.error\":{\"code\":\"EntityAlreadyExists\"}}")

	// Act
	err := parseBatchResponse(body)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "409")
	assert.Contains(t, err.Error(), "EntityAlreadyExists")
}

// --- getRetryDelay ---

func TestShouldUseExponentialBackoffWhenNoResponse(t *testing.T) {
	// Act
	d0 := getRetryDelay(0, nil)
	d1 := getRetryDelay(1, nil)
	d2 := getRetryDelay(2, nil)

	// Assert
	assert.Equal(t, InitialRetryDelay, d0)
	assert.Equal(t, InitialRetryDelay*2, d1)
	assert.Equal(t, InitialRetryDelay*4, d2)
}

func TestShouldUseRetryAfterHeaderWhenStatusIs429(t *testing.T) {
	// Arrange
	resp := &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": {"5"}}}

	// Act
	d := getRetryDelay(0, resp)

	// Assert
	assert.Equal(t, 5*time.Second, d)
}

func TestShouldCapDelayAtMaxRetryDelayWhenAttemptIsHigh(t *testing.T) {
	// Act
	d := getRetryDelay(20, nil)

	// Assert
	assert.Equal(t, MaxRetryDelay, d)
}

// --- NewHTTPTableClient validation ---

func TestShouldReturnErrorWhenAccountNameIsEmpty(t *testing.T) {
	// Arrange
	accountKey := validBase64AccountKey

	// Act
	client, err := NewHTTPTableClient("", accountKey, "table", false, "")

	// Assert
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestShouldReturnErrorWhenAccountKeyIsEmpty(t *testing.T) {
	// Act
	client, err := NewHTTPTableClient("myaccount", "", "table", false, "")

	// Assert
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestShouldReturnErrorWhenAccountKeyIsInvalidBase64(t *testing.T) {
	// Act
	client, err := NewHTTPTableClient("myaccount", "not-valid-base64!!", "table", false, "")

	// Assert
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestShouldCreateClientWhenGivenValidInput(t *testing.T) {
	// Act
	client, err := NewHTTPTableClient("myaccount", validBase64AccountKey, "mytable", false, "")

	// Assert
	require.NoError(t, err)
	require.NotNil(t, client)
	assert.Equal(t, "myaccount", client.AccountName())
	assert.Equal(t, "https://myaccount.table.core.windows.net", client.Endpoint())
	assert.Equal(t, "mytable", client.TableName())
}

// --- HTTPTableClient with fake server ---

func TestShouldSucceedCreatingTableWhenResponseIs201(t *testing.T) {
	// Arrange
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.Path, "Tables")
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", validBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	// Act
	err = client.CreateTable(context.Background())

	// Assert
	assert.NoError(t, err)
}

func TestShouldSucceedCreatingTableWhenResponseIs409TableExists(t *testing.T) {
	// Arrange
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", validBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	// Act
	err = client.CreateTable(context.Background())

	// Assert
	assert.NoError(t, err)
}

func TestShouldReturnErrorWhenCreateTableResponseIs500(t *testing.T) {
	// Arrange
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", validBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	// Act
	err = client.CreateTable(context.Background())

	// Assert
	require.Error(t, err)
	var azErr *AzureError
	require.True(t, errors.As(err, &azErr))
	assert.Equal(t, 500, azErr.StatusCode)
}

func TestShouldReturnErrorWhenGetEntityResponseIs404(t *testing.T) {
	// Arrange
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", validBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	// Act
	data, err := client.GetEntity(context.Background(), "pk", "rk")

	// Assert
	require.Error(t, err)
	assert.Nil(t, data)
}

func TestShouldReturnEntityWhenGetEntityResponseIs200(t *testing.T) {
	// Arrange
	want := Entity{PartitionKey: "pk", RowKey: "rk", Value: []byte("hello")}
	body, _ := json.Marshal(want)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	defer server.Close()

	client, err := NewHTTPTableClient("devstoreaccount1", validBase64AccountKey, "TestTable", false, server.URL)
	require.NoError(t, err)

	// Act
	data, err := client.GetEntity(context.Background(), "pk", "rk")

	// Assert
	require.NoError(t, err)
	require.NotNil(t, data)
	var ent Entity
	require.NoError(t, json.Unmarshal(data, &ent))
	assert.Equal(t, "pk", ent.PartitionKey)
	assert.Equal(t, "rk", ent.RowKey)
	assert.Equal(t, []byte("hello"), ent.Value)
}
