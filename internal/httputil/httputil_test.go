package httputil

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldReadResponseBodyWhenBodyPresent(t *testing.T) {
	resp := &http.Response{
		Body: io.NopCloser(strings.NewReader("hello")),
	}

	body, err := ReadResponseBody(resp)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(body))
}

func TestShouldReturnErrorWhenResponseBodyNil(t *testing.T) {
	_, err := ReadResponseBody(&http.Response{})
	require.Error(t, err)
}

func TestCloseResponseBodyWhenBodyPresent(t *testing.T) {
	resp := &http.Response{
		Body: io.NopCloser(strings.NewReader("hello")),
	}
	CloseResponseBody(resp)
}

func TestCloseResponseBodyWhenNil(t *testing.T) {
	CloseResponseBody(nil)
	CloseResponseBody(&http.Response{})
}
