# Overview

azkit is a **small, dependency-light** library for Azure Table Storage in Go. It speaks the Table REST API directly over `net/http` instead of pulling in the full Azure SDK.

## Design goals

- **Native HTTP** — predictable behavior, minimal transitive dependencies
- **Production CRUD** — entities, batch transactions, OData list with continuation
- **Shared credentials package** — `credentials` provides shared-key and managed-identity token helpers for **other** fgrzl modules (for example `kv/pkg/storage/azure`)
- **Testable** — `SetHTTPClient` for custom transports

## Packages

| Package | Role |
|---------|------|
| `github.com/fgrzl/azkit/tables` | `HTTPTableClient` — shared key, SAS, or managed identity |
| `github.com/fgrzl/azkit/credentials` | `SharedKeyCredential`, `ManagedIdentityCredential` |

The table client takes **raw keys/tokens or a managed-identity pointer** — not `*SharedKeyCredential`. KV's Azure backend uses `credentials.NewSharedKeyCredential` and passes strings into its own HTTP layer.

Internal helpers (`internal/httputil`, `internal/jwt`, `internal/validate`) are not public API.

## What azkit is not

- Not a Blob or Queue client — use [fazure](https://github.com/fgrzl/fazure) for local emulation
- Not full Azure Identity — MI/App Service token acquisition only
- Not an ORM — partition/row keys and JSON entity bodies are explicit

## Typical use cases

- **KV Azure backend** — `github.com/fgrzl/kv/pkg/storage/azure` uses `azkit/credentials`
- **Direct Table apps** — services that need Table Storage without SDK weight
- **Integration tests** — point at [fazure](https://github.com/fgrzl/fazure) with `NewHTTPTableClient(..., allowInsecure: true, customEndpoint: ...)`

## Stability

Consume via tagged releases: `go get github.com/fgrzl/azkit@vX.Y.Z`. Review [CHANGELOG](../CHANGELOG.md) on every upgrade.
