# Tables reference

The `tables` package exposes `HTTPTableClient` for Azure Table Storage REST API **2021-06-08** (`AzureAPIVersion` in `client.go`).

## Constructors

| Function | Authentication |
|----------|----------------|
| `NewHTTPTableClient(accountName, accountKey, tableName, allowInsecure, customEndpoint)` | Shared key |
| `NewHTTPTableClientWithSAS(accountName, sasToken, tableName, allowInsecure, customEndpoint)` | SAS token |
| `NewHTTPTableClientWithManagedIdentity(accountName, cred, tableName, allowInsecure, customEndpoint)` | Bearer token via `*credentials.ManagedIdentityCredential` |

`customEndpoint` overrides the default `https://{account}.table.core.windows.net` when non-empty (emulators, private endpoints).

## Entity model

```go
type Entity struct {
    PartitionKey string
    RowKey       string
    Value        []byte // optional payload; encoded in request JSON when set
    Timestamp    string // populated on read
}
```

Write methods accept **JSON bytes** that unmarshal into `Entity` (`AddEntity`, `UpsertEntity`).

## Entity operations

| Method | Description |
|--------|-------------|
| `CreateTable(ctx)` | Create the table |
| `AddEntity(ctx, data []byte)` | Insert-only |
| `UpsertEntity(ctx, data []byte, mode string)` | `"Replace"` or `"Merge"` |
| `DeleteEntity(ctx, pk, rk string)` | Delete by keys |
| `GetEntity(ctx, pk, rk string) ([]byte, error)` | Raw JSON response body |

## Batch

| Method | Description |
|--------|-------------|
| `AddEntityBatch(ctx, entities [][]byte)` | Batch insert (up to 100 per Azure limit) |
| `SubmitBatch(ctx, ops []BatchOp)` | Mixed insert/update/delete batch |

## List / query

`NewListEntitiesPager(filter, selectCols string, top int32) *ListEntitiesPager`

- `FetchPage(ctx) ([]Entity, error)` — next page; empty slice when done
- OData `$filter` and `$select` passed as strings; `top` defaults to 1000 when zero

## Retries

Transient failures use built-in retry: **3 attempts** with exponential backoff (`InitialRetryDelay` 100ms, `MaxRetryDelay` 10s). Not configurable on the client today.

## Errors

Azure errors parse into `*AzureError` with HTTP status and OData error code. Use `IsTransient()` for 429/503/408 style retries.

## Related code

`tables/`: `client.go`, `crud.go`, `batch.go`, `pager.go`, `retry.go`, `auth.go`.
