# Tables reference

The `tables` package exposes `HTTPTableClient` for Azure Table Storage REST API **2021-06-08**.

## Client

`NewHTTPTableClient(endpoint, tableName, credential)` returns a client configured with:

- Connection pooling and timeouts tuned for Table workloads
- Automatic retries on transient failures (configurable attempts and backoff)
- Shared-key or bearer token authorization depending on credential type

## Entity operations

| Method | Description |
|--------|-------------|
| `Insert` | Create-only insert |
| `Upsert` | Insert or replace / merge modes |
| `Update` | Replace with ETag precondition |
| `Merge` | Partial update with ETag |
| `Delete` | Remove by partition and row key |
| `Get` | Single entity lookup |

Entities are `map[string]any` values including `PartitionKey`, `RowKey`, and optional `Timestamp` / `etag` system properties.

## Batch

`SubmitBatch` accepts up to **100** operations per request. Mixed insert/update/delete batches follow Azure multipart envelope rules.

## Query

`Query` returns a pager over entities matching OData `$filter`, `$select`, and continuation tokens. Use `QueryOptions` for page size and partition scoping.

## Errors

Table service errors surface as typed errors where possible (not found, conflict, throttling). Callers should retry throttling responses; the client applies bounded retries by default.

## Related code

Source layout under `tables/`: `client.go`, `crud.go`, `batch.go`, `pager.go`, `retry.go`, `auth.go`.
