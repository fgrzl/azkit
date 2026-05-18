[![CI](https://github.com/fgrzl/azkit/actions/workflows/ci.yml/badge.svg)](https://github.com/fgrzl/azkit/actions/workflows/ci.yml)

# azkit

Lightweight, native Go clients for **Azure Table Storage** and **Azure credential** helpers — no Azure SDK dependency.

## Packages

| Package | Import | Purpose |
|---------|--------|---------|
| `tables` | `github.com/fgrzl/azkit/tables` | HTTP REST client for Azure Table Storage (CRUD, batch, list) |
| `credentials` | `github.com/fgrzl/azkit/credentials` | Shared-key and managed-identity token helpers (used by `kv` and other modules) |

## Quick start

```bash
go get github.com/fgrzl/azkit
```

```go
import (
    "context"
    "encoding/json"

    "github.com/fgrzl/azkit/tables"
)

client, err := tables.NewHTTPTableClient(
    "devstoreaccount1",
    accountKey,
    "MyTable",
    true, // allowInsecure (Azurite / fazure)
    "http://127.0.0.1:10002/devstoreaccount1",
)
body, _ := json.Marshal(tables.Entity{
    PartitionKey: "users",
    RowKey:       "user-1",
    Value:        []byte(`{"name":"Ada"}`),
})
_ = client.AddEntity(context.Background(), body)
```

For managed identity, use `NewHTTPTableClientWithManagedIdentity`. See [docs/getting-started.md](docs/getting-started.md).

## Documentation

Full guides: **[docs/](docs/README.md)**

## Related

- [CHANGELOG](CHANGELOG.md)
- [CONTRIBUTING](CONTRIBUTING.md)
- [fgrzl/kv](https://github.com/fgrzl/kv) — Azure Table backend uses `azkit/credentials`

## License

See [LICENSE](LICENSE).
