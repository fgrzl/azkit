# Getting started

## Install

```bash
go get github.com/fgrzl/azkit
```

Requires Go 1.20+ (see `go.mod` for the version used in CI).

## Credentials

### Shared key (local dev, Azurite, fazure)

```go
cred, err := credentials.NewSharedKeyCredential("devstoreaccount1", accountKey)
if err != nil {
    return err
}
```

### Managed identity / App Service token

Use `credentials.NewTokenCredential` with the appropriate endpoint configuration for your hosting environment. Tokens refresh automatically before expiry.

## Create a table client

```go
client, err := tables.NewHTTPTableClient(
    "http://127.0.0.1:10002/devstoreaccount1", // or https://{account}.table.core.windows.net
    "MyTable",
    cred,
)
```

## Insert and read an entity

```go
entity := tables.Entity{
    "PartitionKey": "users",
    "RowKey":       "user-1",
    "Name":         "Ada",
}

if err := client.Insert(ctx, entity); err != nil {
    return err
}

got, err := client.Get(ctx, "users", "user-1")
```

## Query with paging

```go
pager := client.Query(ctx, tables.QueryOptions{
    Filter: "PartitionKey eq 'users'",
})

for pager.Next() {
    for _, e := range pager.Page() {
        _ = e["Name"]
    }
}
```

## Next steps

- [Tables reference](tables.md) — batch operations, upsert modes, retries
- [Overview](overview.md) — design constraints and package layout
