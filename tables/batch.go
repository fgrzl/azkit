package tables

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/url"
)

// BatchOpType distinguishes the operation kind inside a batch changeset.
type BatchOpType int

const (
	BatchInsertReplace BatchOpType = iota
	BatchDelete
)

// BatchOp represents a single operation within a mixed batch transaction.
type BatchOp struct {
	Type         BatchOpType
	Entity       []byte
	PartitionKey string
	RowKey       string
}

// AddEntityBatch inserts multiple entities in a single batch request (up to 100)
func (c *HTTPTableClient) AddEntityBatch(ctx context.Context, entities [][]byte) error {
	if len(entities) == 0 {
		return nil
	}
	if len(entities) > AzureBatchMaxEntities {
		return fmt.Errorf("batch size %d exceeds maximum of %d entities", len(entities), AzureBatchMaxEntities)
	}

	batchID := nextBoundaryID("batch")
	changesetID := nextBoundaryID("changeset")

	envelope, err := c.buildInsertBatchEnvelope(batchID, changesetID, entities)
	if err != nil {
		return err
	}

	if err := c.submitMultipartBatch(ctx, batchID, envelope); err != nil {
		return err
	}

	slog.Debug("batch entities added", "count", len(entities))
	return nil
}

// SubmitBatch executes a mixed batch of InsertReplace (PUT) and Delete operations
func (c *HTTPTableClient) SubmitBatch(ctx context.Context, ops []BatchOp) error {
	if len(ops) == 0 {
		return nil
	}
	if len(ops) > AzureBatchMaxEntities {
		return fmt.Errorf("batch size %d exceeds maximum of %d entities", len(ops), AzureBatchMaxEntities)
	}

	allDelete := true
	for _, op := range ops {
		if op.Type != BatchDelete {
			allDelete = false
			break
		}
	}
	if allDelete {
		for _, op := range ops {
			if err := c.DeleteEntity(ctx, op.PartitionKey, op.RowKey); err != nil {
				return err
			}
		}
		return nil
	}

	batchID := nextBoundaryID("batch")
	changesetID := nextBoundaryID("changeset")

	envelope, err := c.buildMixedBatchEnvelope(batchID, changesetID, ops)
	if err != nil {
		return err
	}

	if err := c.submitMultipartBatch(ctx, batchID, envelope); err != nil {
		return err
	}

	slog.Debug("batch submitted", "count", len(ops))
	return nil
}

func (c *HTTPTableClient) buildInsertBatchEnvelope(batchID, changesetID string, entities [][]byte) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	estimatedSize := len(entities)*500 + 1024
	if buf.Cap() < estimatedSize {
		buf.Grow(estimatedSize - buf.Len())
	}

	fmt.Fprintf(buf, "--%s\r\n", batchID)
	fmt.Fprintf(buf, "Content-Type: multipart/mixed; boundary=%s\r\n\r\n", changesetID)

	for i, entData := range entities {
		e, entJSON, err := entityFromBytes(entData)
		if err != nil {
			return nil, fmt.Errorf("entity %d: %w", i, err)
		}
		writeBatchPUT(buf, c, changesetID, e, entJSON)
	}

	fmt.Fprintf(buf, "--%s--\r\n", changesetID)
	fmt.Fprintf(buf, "--%s--\r\n", batchID)
	return append([]byte(nil), buf.Bytes()...), nil
}

func (c *HTTPTableClient) buildMixedBatchEnvelope(batchID, changesetID string, ops []BatchOp) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	estimatedSize := len(ops)*500 + 1024
	if buf.Cap() < estimatedSize {
		buf.Grow(estimatedSize - buf.Len())
	}

	fmt.Fprintf(buf, "--%s\r\n", batchID)
	fmt.Fprintf(buf, "Content-Type: multipart/mixed; boundary=%s\r\n\r\n", changesetID)

	for i, op := range ops {
		switch op.Type {
		case BatchInsertReplace:
			e, entJSON, err := entityFromBytes(op.Entity)
			if err != nil {
				return nil, fmt.Errorf("entity %d: %w", i, err)
			}
			writeBatchPUT(buf, c, changesetID, e, entJSON)
		case BatchDelete:
			writeBatchPartHeader(buf, changesetID)
			fmt.Fprintf(buf, "DELETE %s/%s(PartitionKey='%s',RowKey='%s') HTTP/1.1\r\n",
				c.endpoint, c.tableName,
				url.QueryEscape(op.PartitionKey), url.QueryEscape(op.RowKey))
			fmt.Fprintf(buf, "If-Match: *\r\n")
			fmt.Fprintf(buf, "Accept: application/json;odata=nometadata\r\n\r\n")
		}
	}

	fmt.Fprintf(buf, "--%s--\r\n", changesetID)
	fmt.Fprintf(buf, "--%s--\r\n", batchID)
	return append([]byte(nil), buf.Bytes()...), nil
}

func writeBatchPartHeader(buf *bytes.Buffer, changesetID string) {
	fmt.Fprintf(buf, "--%s\r\n", changesetID)
	fmt.Fprintf(buf, "Content-Type: application/http\r\n")
	fmt.Fprintf(buf, "Content-Transfer-Encoding: binary\r\n\r\n")
}

func writeBatchPUT(buf *bytes.Buffer, c *HTTPTableClient, changesetID string, e Entity, entJSON []byte) {
	writeBatchPartHeader(buf, changesetID)
	fmt.Fprintf(buf, "PUT %s/%s(PartitionKey='%s',RowKey='%s') HTTP/1.1\r\n", c.endpoint, c.tableName,
		url.QueryEscape(e.PartitionKey), url.QueryEscape(e.RowKey))
	fmt.Fprintf(buf, "Content-Type: application/json\r\n")
	fmt.Fprintf(buf, "Accept: application/json;odata=nometadata\r\n")
	fmt.Fprintf(buf, "Content-Length: %d\r\n\r\n", len(entJSON))
	buf.Write(entJSON)
	buf.WriteString("\r\n")
}
