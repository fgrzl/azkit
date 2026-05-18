package tables

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/fgrzl/azkit/internal/httputil"
)

func getRetryDelay(attempt int, resp *http.Response) time.Duration {
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			if seconds, err := strconv.Atoi(retryAfter); err == nil {
				return time.Duration(seconds) * time.Second
			}
		}
	}

	delay := InitialRetryDelay * (1 << uint(attempt))
	if delay > MaxRetryDelay {
		delay = MaxRetryDelay
	}
	return delay
}

func (c *HTTPTableClient) retryableRequest(ctx context.Context, req *http.Request, body []byte) (*http.Response, error) {
	var lastErr error
	var resp *http.Response

	for attempt := 0; attempt < MaxRetryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("context canceled: %w", err)
		}

		retryReq := req.Clone(ctx)
		if len(body) > 0 {
			retryReq.Body = io.NopCloser(bytes.NewReader(body))
			retryReq.ContentLength = int64(len(body))
		}

		var err error
		resp, err = c.httpClient.Do(retryReq)
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() && attempt < MaxRetryAttempts-1 {
				delay := getRetryDelay(attempt, nil)
				slog.Warn("request timeout, retrying",
					"attempt", attempt+1,
					"delay", delay,
					"error", err)
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				lastErr = err
				continue
			}
			return nil, fmt.Errorf("request failed: %w", err)
		}

		if isTransientStatus(resp.StatusCode) {
			if attempt < MaxRetryAttempts-1 {
				delay := getRetryDelay(attempt, resp)
				respBody, readErr := httputil.ReadResponseBody(resp)
				closeResponseBody(resp)
				if readErr != nil {
					lastErr = readErr
					continue
				}

				slog.Warn("transient error, retrying",
					"status", resp.StatusCode,
					"attempt", attempt+1,
					"delay", delay,
					"request_id", resp.Header.Get("x-ms-request-id"))

				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				lastErr = ParseAzureError(resp, respBody)
				continue
			}
			respBody, readErr := httputil.ReadResponseBody(resp)
			closeResponseBody(resp)
			if readErr != nil {
				return nil, fmt.Errorf("read response body: %w", readErr)
			}
			return nil, ParseAzureError(resp, respBody)
		}

		return resp, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
	}
	return nil, errors.New("max retries exceeded")
}
