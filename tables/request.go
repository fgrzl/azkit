package tables

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/fgrzl/azkit/internal/httputil"
)

type requestOpts struct {
	method       string
	url          string
	body         []byte
	resourcePath string
	okStatuses   map[int]bool
	setJSON      bool
	extraHeaders map[string]string
}

func (c *HTTPTableClient) executeRequest(ctx context.Context, opts requestOpts) (int, error) {
	var bodyReader io.Reader
	if len(opts.body) > 0 {
		bodyReader = bytes.NewReader(opts.body)
	}

	req, err := http.NewRequestWithContext(ctx, opts.method, opts.url, bodyReader)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}

	if err := c.signRequest(req, opts.method, opts.body, opts.resourcePath); err != nil {
		return 0, fmt.Errorf("sign request: %w", err)
	}
	if opts.setJSON {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range opts.extraHeaders {
		req.Header.Set(k, v)
	}
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.retryableRequest(ctx, req, opts.body)
	if err != nil {
		return 0, fmt.Errorf("execute request: %w", err)
	}
	defer closeResponseBody(resp)

	if opts.okStatuses[resp.StatusCode] {
		return resp.StatusCode, nil
	}

	respBody, readErr := httputil.ReadResponseBody(resp)
	if readErr != nil {
		return resp.StatusCode, fmt.Errorf("read response body: %w", readErr)
	}
	return resp.StatusCode, ParseAzureError(resp, respBody)
}

func (c *HTTPTableClient) submitMultipartBatch(ctx context.Context, batchID string, envelope []byte) error {
	req, err := http.NewRequestWithContext(ctx, "POST", c.buildBatchURL(), bytes.NewReader(envelope))
	if err != nil {
		return fmt.Errorf("create batch request: %w", err)
	}

	if err := c.signRequest(req, "POST", envelope, "/$batch"); err != nil {
		return fmt.Errorf("sign batch request: %w", err)
	}
	req.Header.Set("Content-Type", fmt.Sprintf("multipart/mixed; boundary=%s", batchID))
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.retryableRequest(ctx, req, envelope)
	if err != nil {
		return fmt.Errorf("execute batch request: %w", err)
	}
	defer closeResponseBody(resp)

	respBody, readErr := httputil.ReadResponseBody(resp)
	if readErr != nil {
		return fmt.Errorf("read response body: %w", readErr)
	}

	if resp.StatusCode != http.StatusAccepted {
		return ParseAzureError(resp, respBody)
	}

	return parseBatchResponse(respBody)
}
