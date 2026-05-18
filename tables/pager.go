package tables

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/fgrzl/azkit/internal/httputil"
)

// ListEntitiesPager provides pagination for list entities queries
type ListEntitiesPager struct {
	client            *HTTPTableClient
	filter            string
	selectCols        string
	top               int32
	continuationToken string
	ctx               context.Context
	pageIndex         int
	done              bool
}

// NewListEntitiesPager creates a new pager for listing entities
func (c *HTTPTableClient) NewListEntitiesPager(filter, selectCols string, top int32) *ListEntitiesPager {
	if top == 0 || top > AzureDefaultPageSize {
		top = AzureDefaultPageSize
	}
	return &ListEntitiesPager{
		client:     c,
		filter:     filter,
		selectCols: selectCols,
		top:        top,
		pageIndex:  -1,
	}
}

// PageResponse represents a page response
type PageResponse struct {
	Value     []Entity `json:"value"`
	NextToken string   `json:"odata.nextLink,omitempty"`
}

// FetchPage fetches the next page of results
func (p *ListEntitiesPager) FetchPage(ctx context.Context) ([]Entity, error) {
	if p.done {
		return nil, nil
	}

	p.ctx = ctx

	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	defer builderPool.Put(b)

	estimatedLen := len(p.client.endpoint) + 1 + len(p.client.tableName) + 256
	if p.client.useSAS {
		estimatedLen += len(p.client.sasToken)
	}
	b.Grow(estimatedLen)

	b.WriteString(p.client.endpoint)
	b.WriteByte('/')
	b.WriteString(p.client.tableName)
	b.WriteByte('?')
	b.WriteString("$top=")
	b.WriteString(strconv.Itoa(int(p.top)))

	if p.filter != "" {
		b.WriteString("&$filter=")
		b.WriteString(url.QueryEscape(p.filter))
	}
	if p.selectCols != "" {
		b.WriteString("&$select=")
		b.WriteString(url.QueryEscape(p.selectCols))
	}
	if p.continuationToken != "" {
		b.WriteByte('&')
		b.WriteString(p.continuationToken)
	}
	if p.client.useSAS {
		b.WriteByte('&')
		b.WriteString(p.client.sasToken)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", b.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	if err := p.client.signRequest(req, "GET", nil, fmt.Sprintf("/%s", p.client.tableName)); err != nil {
		return nil, fmt.Errorf("sign request: %w", err)
	}
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := p.client.retryableRequest(ctx, req, nil)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer closeResponseBody(resp)

	if resp.StatusCode != http.StatusOK {
		respBody, readErr := httputil.ReadResponseBody(resp)
		if readErr != nil {
			return nil, fmt.Errorf("read response body: %w", readErr)
		}
		return nil, ParseAzureError(resp, respBody)
	}

	nextPK := resp.Header.Get("x-ms-continuation-NextPartitionKey")
	nextRK := resp.Header.Get("x-ms-continuation-NextRowKey")

	if nextPK != "" || nextRK != "" {
		cb := builderPool.Get().(*strings.Builder)
		cb.Reset()
		if nextPK != "" {
			cb.WriteString("NextPartitionKey=")
			cb.WriteString(url.QueryEscape(nextPK))
		}
		if nextRK != "" {
			if cb.Len() > 0 {
				cb.WriteByte('&')
			}
			cb.WriteString("NextRowKey=")
			cb.WriteString(url.QueryEscape(nextRK))
		}
		p.continuationToken = cb.String()
		builderPool.Put(cb)
	} else {
		p.done = true
	}

	var pageResp PageResponse
	if err := json.NewDecoder(resp.Body).Decode(&pageResp); err != nil {
		return nil, fmt.Errorf("decode page response: %w", err)
	}

	p.pageIndex++
	return pageResp.Value, nil
}

// IsDone returns whether there are more pages
func (p *ListEntitiesPager) IsDone() bool {
	return p.done
}

// Close closes the pager
func (p *ListEntitiesPager) Close() error {
	return nil
}
