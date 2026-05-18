# Changelog

## Unreleased

### Breaking

- Renamed `tables` package from `client` to `tables`. Import path is unchanged (`github.com/fgrzl/azkit/tables`), but references must be updated: `client.HTTPTableClient` → `tables.HTTPTableClient`, etc.

### Fixed

- Mixed `SubmitBatch` operations with `BatchDelete` now include required multipart part headers before the DELETE line in the batch envelope.

### Changed

- Split monolithic `http_client.go` into focused files (`client`, `crud`, `batch`, `retry`, `auth`, etc.).
- Shared HTTP/JWT/validation helpers moved to `internal/httputil`, `internal/jwt`, and `internal/validate`.
- CRUD methods use centralized `executeRequest` and `buildEntityURL` helpers.
