# Overview

azkit is a **small, dependency-light** library for Azure Table Storage in Go. It speaks the Table REST API directly over `net/http` instead of pulling in the full Azure SDK.

## Design goals

- **Native HTTP** — predictable behavior, minimal transitive dependencies
- **Production CRUD** — entities, batch transactions, OData queries, continuation tokens
- **Shared credentials** — account key and managed-identity token providers reusable by other fgrzl modules (for example `kv` Azure backends)
- **Testable** — table client accepts custom `http.Client`; credentials are interface-driven

## Packages

| Package | Role |
|---------|------|
| `github.com/fgrzl/azkit/tables` | `HTTPTableClient` — insert, upsert, merge, delete, query, batch |
| `github.com/fgrzl/azkit/credentials` | `SharedKeyCredential`, token credential with IMDS / App Service refresh |

Internal helpers (`internal/httputil`, `internal/jwt`, `internal/validate`) are not public API.

## What azkit is not

- Not a Blob or Queue client — use [fazure](https://github.com/fgrzl/fazure) for local emulation or Azure SDKs for cloud
- Not a full Azure identity platform — credentials cover common Table Storage and token scenarios
- Not an ORM — you work with `Entity` maps and partition/row keys explicitly

## Typical use cases

- **KV Azure backend** — `github.com/fgrzl/kv/pkg/storage/azure` uses azkit credentials
- **Custom Table apps** — services that need Table Storage without SDK weight
- **Integration tests** — point at [fazure](https://github.com/fgrzl/fazure) or Azurite with shared-key auth

## Stability

Consume via tagged releases: `go get github.com/fgrzl/azkit@vX.Y.Z`. Review [CHANGELOG](../CHANGELOG.md) on every upgrade.
