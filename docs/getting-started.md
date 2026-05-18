# Getting started

## Install

```bash
go get github.com/fgrzl/azkit
```

Requires Go 1.25+ (see `go.mod`).

## Shared-key client (local dev, fazure, Azurite)

```go
client, err := tables.NewHTTPTableClient(
    "devstoreaccount1",
    accountKey, // base64 account key
    "MyTable",
    true, // allowInsecure — required for http://127.0.0.1 endpoints
    "http://127.0.0.1:10002/devstoreaccount1",
)
```

Arguments are `(accountName, accountKey, tableName, allowInsecure, customEndpoint)`. The custom endpoint is the full table service base URL (including account path for emulators).

## Managed identity

```go
cred := credentials.NewManagedIdentityCredential("") // optional client ID; uses AZURE_CLIENT_ID when empty
client, err := tables.NewHTTPTableClientWithManagedIdentity(
    "myaccount",
    cred,
    "MyTable",
    false,
    "", // empty customEndpoint → https://{account}.table.core.windows.net
)
```

`credentials.NewSharedKeyCredential` is for callers such as `kv` that need account metadata; the table client takes the raw account key string directly.

## Insert and read

```go
import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/fgrzl/azkit/tables"
)

body, err := json.Marshal(tables.Entity{
    PartitionKey: "users",
    RowKey:       "user-1",
    Value:        []byte(`{"name":"Ada"}`),
})
if err != nil {
    return err
}
if err := client.AddEntity(ctx, body); err != nil {
    return err
}

raw, err := client.GetEntity(ctx, "users", "user-1")
if err != nil {
    return err
}
fmt.Println(string(raw))
```

## List entities (paging)

```go
pager := client.NewListEntitiesPager("PartitionKey eq 'users'", "", 100)
for {
    page, err := pager.FetchPage(ctx)
    if err != nil {
        return err
    }
    if len(page) == 0 {
        break
    }
    for _, e := range page {
        _ = e.PartitionKey
    }
}
```

## Next steps

- [Tables reference](tables.md) — upsert modes, batch, retries, errors
- [Overview](overview.md) — constructors and package layout
