package httputil

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// trackingBody is a ReadCloser that records how many bytes were read and whether
// Close was called, optionally injecting a read error.
type trackingBody struct {
	r         io.Reader
	bytesRead int
	closed    bool
	readErr   error
}

func (b *trackingBody) Read(p []byte) (int, error) {
	if b.readErr != nil {
		return 0, b.readErr
	}
	n, err := b.r.Read(p)
	b.bytesRead += n
	return n, err
}

func (b *trackingBody) Close() error {
	b.closed = true
	return nil
}

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

func TestShouldDrainBodyToEOFBeforeCloseWhenBodyHasTrailingBytes(t *testing.T) {
	// Arrange
	payload := "{\"a\":1}\n\ntrailing-bytes-left-unread"
	body := &trackingBody{r: strings.NewReader(payload)}
	resp := &http.Response{Body: body}

	// Act
	CloseResponseBody(resp)

	// Assert
	assert.True(t, body.closed, "body must be closed")
	assert.Equal(t, len(payload), body.bytesRead, "body must be drained to EOF")
}

func TestShouldStillCloseWhenDrainErrors(t *testing.T) {
	// Arrange
	body := &trackingBody{r: strings.NewReader("ignored"), readErr: errors.New("read failed")}
	resp := &http.Response{Body: body}

	// Act
	CloseResponseBody(resp)

	// Assert
	assert.True(t, body.closed, "body must be closed even when the drain errors")
}

func TestShouldStopDrainingAtCapWhenBodyExceedsLimit(t *testing.T) {
	// Arrange
	body := &trackingBody{r: io.LimitReader(neverEndingReader{}, ResponseBodyDrainLimit*2)}
	resp := &http.Response{Body: body}

	// Act
	CloseResponseBody(resp)

	// Assert
	assert.True(t, body.closed, "body must be closed")
	assert.LessOrEqual(t, body.bytesRead, ResponseBodyDrainLimit, "drain must stop at the cap")
}

// neverEndingReader yields an unbounded stream of zero bytes.
type neverEndingReader struct{}

func (neverEndingReader) Read(p []byte) (int, error) { return len(p), nil }
