[![CI](https://github.com/fgrzl/azkit/actions/workflows/ci.yml/badge.svg)](https://github.com/fgrzl/azkit/actions/workflows/ci.yml)

# azkit

Lightweight, native Go clients for **Azure Table Storage** and **Azure credential** helpers — no Azure SDK dependency.

## Packages

| Package | Import | Purpose |
|---------|--------|---------|
| `tables` | `github.com/fgrzl/azkit/tables` | HTTP REST client for Azure Table Storage (CRUD, batch, query, paging) |
| `credentials` | `github.com/fgrzl/azkit/credentials` | Shared-key and managed-identity token credentials |

## Quick start

```bash
go get github.com/fgrzl/azkit
```

```go
import (
    "github.com/fgrzl/azkit/credentials"
    "github.com/fgrzl/azkit/tables"
)

cred, _ := credentials.NewSharedKeyCredential(accountName, accountKey)
client, _ := tables.NewHTTPTableClient(endpoint, tableName, cred)
```

## Documentation

Full guides live in **[docs/](docs/README.md)**:

- [Overview](docs/overview.md) — design goals and package layout
- [Getting started](docs/getting-started.md) — credentials, first table operations
- [Tables reference](docs/tables.md) — client API, batching, retries

## Related

- [CHANGELOG](CHANGELOG.md) — release notes
- [fgrzl/kv](https://github.com/fgrzl/kv) — uses `azkit/credentials` for Azure Table backends

## License

See [LICENSE](LICENSE).
