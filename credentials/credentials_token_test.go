package credentials

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldReturnTokenWhenIdentityEndpointReturns200(t *testing.T) {
	t.Setenv("IDENTITY_ENDPOINT", "")
	t.Setenv("IDENTITY_HEADER", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"access_token": "test-access-token",
			"expires_on":   "4102444800",
		})
	}))
	defer server.Close()

	cred := NewManagedIdentityCredential("")
	cred.imdsEndpoint = server.URL
	cred.useAppService = false
	cred.httpClient = server.Client()

	token, err := cred.GetToken(context.Background())
	require.NoError(t, err)
	assert.NotEmpty(t, token)
}

func TestShouldReturnErrorWhenIdentityEndpointReturns500(t *testing.T) {
	t.Setenv("IDENTITY_ENDPOINT", "")
	t.Setenv("IDENTITY_HEADER", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("server error"))
	}))
	defer server.Close()

	cred := NewManagedIdentityCredential("")
	cred.imdsEndpoint = server.URL
	cred.useAppService = false
	cred.httpClient = server.Client()

	_, err := cred.GetToken(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestShouldReturnErrorWhenTokenResponseIsInvalidJSON(t *testing.T) {
	t.Setenv("IDENTITY_ENDPOINT", "")
	t.Setenv("IDENTITY_HEADER", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	defer server.Close()

	cred := NewManagedIdentityCredential("")
	cred.imdsEndpoint = server.URL
	cred.useAppService = false
	cred.httpClient = server.Client()

	_, err := cred.GetToken(context.Background())
	require.Error(t, err)
}

func TestShouldReturnErrorWhenExpiresOnIsInvalid(t *testing.T) {
	t.Setenv("IDENTITY_ENDPOINT", "")
	t.Setenv("IDENTITY_HEADER", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"access_token": "token",
			"expires_on":   "not-a-timestamp",
		})
	}))
	defer server.Close()

	cred := NewManagedIdentityCredential("")
	cred.imdsEndpoint = server.URL
	cred.useAppService = false
	cred.httpClient = server.Client()

	_, err := cred.GetToken(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse token expiry")
}

func TestShouldReturnCachedTokenWithoutSecondRequest(t *testing.T) {
	t.Setenv("IDENTITY_ENDPOINT", "")
	t.Setenv("IDENTITY_HEADER", "")

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		_ = json.NewEncoder(w).Encode(map[string]string{
			"access_token": "cached-token",
			"expires_on":   "4102444800",
		})
	}))
	defer server.Close()

	cred := NewManagedIdentityCredential("")
	cred.imdsEndpoint = server.URL
	cred.useAppService = false
	cred.httpClient = server.Client()

	_, err := cred.GetToken(context.Background())
	require.NoError(t, err)
	_, err = cred.GetToken(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, requests)
}

func TestShouldReturnErrorWhenContextCanceled(t *testing.T) {
	t.Setenv("IDENTITY_ENDPOINT", "")
	t.Setenv("IDENTITY_HEADER", "")

	cred := NewManagedIdentityCredential("")
	cred.imdsEndpoint = "http://127.0.0.1:1"
	cred.useAppService = false

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := cred.GetToken(ctx)
	require.Error(t, err)
}

func TestShouldUseAppServiceEndpointWhenEnvVarsSet(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "secret-header", r.Header.Get("X-IDENTITY-HEADER"))
		_ = json.NewEncoder(w).Encode(map[string]string{
			"access_token": "token",
			"expires_on":   "4102444800",
		})
	}))
	defer server.Close()

	t.Setenv("IDENTITY_ENDPOINT", server.URL)
	t.Setenv("IDENTITY_HEADER", "secret-header")

	cred := NewManagedIdentityCredential("client-id")
	cred.httpClient = server.Client()

	token, err := cred.GetToken(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "token", token)
}

func TestShouldRefreshTokenWhenNearExpiry(t *testing.T) {
	t.Setenv("IDENTITY_ENDPOINT", "")
	t.Setenv("IDENTITY_HEADER", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"access_token": "token",
			"expires_on":   "1",
		})
	}))
	defer server.Close()

	cred := NewManagedIdentityCredential("")
	cred.imdsEndpoint = server.URL
	cred.useAppService = false
	cred.httpClient = server.Client()

	_, err := cred.GetToken(context.Background())
	require.NoError(t, err)

	cred.tokenExpiry = time.Unix(1, 0)
	_, err = cred.GetToken(context.Background())
	require.NoError(t, err)
}
