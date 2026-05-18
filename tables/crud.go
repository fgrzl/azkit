package tables

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/fgrzl/azkit/internal/httputil"
)

// CreateTable ensures the table exists
func (c *HTTPTableClient) CreateTable(ctx context.Context) error {
	body := map[string]string{"TableName": c.tableName}
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal create table body: %w", err)
	}

	status, err := c.executeRequest(ctx, requestOpts{
		method:       "POST",
		url:          c.endpoint + "/Tables",
		body:         data,
		resourcePath: "/Tables",
		okStatuses: map[int]bool{
			http.StatusCreated:  true,
			http.StatusOK:       true,
			http.StatusConflict: true,
		},
		setJSON: true,
	})
	if err != nil {
		return err
	}

	if status == http.StatusConflict {
		slog.Debug("table already exists", "table", c.tableName)
		return nil
	}

	slog.Info("table created", "table", c.tableName)
	return nil
}

// AddEntity inserts a new entity (insert-only semantics)
func (c *HTTPTableClient) AddEntity(ctx context.Context, data []byte) error {
	var e Entity
	if err := json.Unmarshal(data, &e); err != nil {
		return fmt.Errorf("unmarshal entity: %w", err)
	}

	body, err := entityRequestBody(e)
	if err != nil {
		return err
	}

	_, err = c.executeRequest(ctx, requestOpts{
		method:       "POST",
		url:          c.buildTableURL(),
		body:         body,
		resourcePath: fmt.Sprintf("/%s", c.tableName),
		okStatuses:   map[int]bool{http.StatusCreated: true, http.StatusNoContent: true},
		setJSON:      true,
	})
	return err
}

// UpsertEntity updates or inserts an entity (merge semantics)
func (c *HTTPTableClient) UpsertEntity(ctx context.Context, data []byte, mode string) error {
	var e Entity
	if err := json.Unmarshal(data, &e); err != nil {
		return fmt.Errorf("unmarshal entity: %w", err)
	}

	body, err := entityRequestBody(e)
	if err != nil {
		return err
	}

	method := "PATCH"
	if mode == "Replace" {
		method = "PUT"
	}

	_, err = c.executeRequest(ctx, requestOpts{
		method:       method,
		url:          c.buildEntityURL(e.PartitionKey, e.RowKey, c.entityQuerySuffix(false)),
		body:         body,
		resourcePath: fmt.Sprintf("/%s(PartitionKey='%s',RowKey='%s')", c.tableName, url.QueryEscape(e.PartitionKey), url.QueryEscape(e.RowKey)),
		okStatuses:   map[int]bool{http.StatusNoContent: true, http.StatusOK: true},
		setJSON:      true,
	})
	return err
}

// DeleteEntity removes an entity
func (c *HTTPTableClient) DeleteEntity(ctx context.Context, pk, rk string) error {
	_, err := c.executeRequest(ctx, requestOpts{
		method:       "DELETE",
		url:          c.buildEntityURL(pk, rk, c.entityQuerySuffix(false)),
		resourcePath: fmt.Sprintf("/%s(PartitionKey='%s',RowKey='%s')", c.tableName, url.QueryEscape(pk), url.QueryEscape(rk)),
		okStatuses:   map[int]bool{http.StatusNoContent: true, http.StatusNotFound: true},
		extraHeaders: map[string]string{"If-Match": "*"},
	})
	return err
}

// GetEntity retrieves a single entity
func (c *HTTPTableClient) GetEntity(ctx context.Context, pk, rk string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.buildEntityURL(pk, rk, c.entityQuerySuffix(true)), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	if err := c.signRequest(req, "GET", nil, fmt.Sprintf("/%s(PartitionKey='%s',RowKey='%s')",
		c.tableName, url.QueryEscape(pk), url.QueryEscape(rk))); err != nil {
		return nil, fmt.Errorf("sign request: %w", err)
	}
	req.Header.Set("Accept", "application/json;odata=nometadata")

	resp, err := c.retryableRequest(ctx, req, nil)
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

	var result Entity
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	out, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshal entity: %w", err)
	}
	return out, nil
}
